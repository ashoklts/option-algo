"""
spot_historical_data.py
───────────────────────
Fetch per-minute spot price + VIX history from option_chain_index_spot.

Endpoint:
    GET /algo/spot/historical-data
        ?underlying=NIFTY
        &candle=2025-11-03T15:30:00
        &activation_mode=algo-backtest

Response (same shape as mtm/historical-data):
    {
      "NSE_01": { "timestamp": [...], "close": [...] },
      "NSE_00": { "timestamp": [...], "close": [...] }
    }
"""
from __future__ import annotations

import logging
from datetime import date as _date, datetime

log = logging.getLogger(__name__)

SPOT_COL  = "option_chain_index_spot"
VIX_TOKEN = "NSE_00"


def _parse_candle(candle: str) -> tuple[str, str]:
    raw = str(candle or "").strip()
    for sep in ("+", "Z"):
        if sep in raw:
            raw = raw.split(sep)[0].strip()
    if "T" in raw:
        date_part, time_part = raw.split("T", 1)
        candle_ts = f"{date_part}T{time_part[:5]}:00"
    else:
        date_part = raw[:10] if len(raw) >= 10 else _date.today().isoformat()
        candle_ts = f"{date_part}T15:30:00"
    return date_part, candle_ts


def _to_series(docs: list, price_field: str = "spot_price") -> dict:
    timestamps, closes = [], []
    for doc in docs:
        ts = str(doc.get("timestamp") or "").strip()
        if ts:
            timestamps.append(ts)
            closes.append(float(doc.get(price_field) or 0.0))
    return {"timestamp": timestamps, "close": closes}


def _init_kite():
    from features.kite_broker_ws import get_common_credentials, is_configured, load_credentials_from_db
    from features.kite_broker import get_kite_instance
    from features.mongo_data import MongoData

    if not is_configured():
        _db = MongoData()
        try:
            load_credentials_from_db(_db)
        finally:
            _db.close()

    if not is_configured():
        raise RuntimeError("Kite access token not configured")

    _, access_token = get_common_credentials()
    return get_kite_instance(access_token)


def _kite_candles_to_series(candles: list) -> dict:
    timestamps, closes = [], []
    for c in candles:
        dt = c.get("date")
        ts = dt.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(dt, datetime) else str(dt or "")
        if ts:
            timestamps.append(ts)
            closes.append(float(c.get("close") or 0.0))
    return {"timestamp": timestamps, "close": closes}


def _fetch_kite_spot_vix(underlying: str, trade_date: str, candle_ts: str) -> dict:
    """Fetch spot + VIX candles from Kite historical_data API (live/fast-forward)."""
    from features.spot_atm_utils import KITE_INDEX_TOKENS, INDIA_VIX_KITE_TOKEN

    ul = underlying.strip().upper()
    spot_kite_token = KITE_INDEX_TOKENS.get(ul, 0)
    if not spot_kite_token:
        log.warning("[spot_hist] no Kite token for underlying=%s", ul)
        return {}

    try:
        kite = _init_kite()
    except Exception as exc:
        log.warning("[spot_hist] Kite init error: %s", exc)
        return {}

    from_dt = datetime.strptime(f"{trade_date}T09:15:00", "%Y-%m-%dT%H:%M:%S")
    to_dt   = datetime.strptime(candle_ts,                 "%Y-%m-%dT%H:%M:%S")
    result: dict = {}

    # Spot candles
    try:
        spot_candles = kite.historical_data(spot_kite_token, from_dt, to_dt, "minute")
        series = _kite_candles_to_series(spot_candles)
        if series["timestamp"]:
            result[f"SPOT_{ul}"] = series
            log.info("[spot_hist] Kite spot %s rows=%d", ul, len(series["timestamp"]))
    except Exception as exc:
        log.warning("[spot_hist] Kite spot fetch error underlying=%s: %s", ul, exc)

    # VIX candles
    try:
        vix_candles = kite.historical_data(INDIA_VIX_KITE_TOKEN, from_dt, to_dt, "minute")
        vix_series = _kite_candles_to_series(vix_candles)
        if vix_series["timestamp"]:
            result[VIX_TOKEN] = vix_series
            log.info("[spot_hist] Kite VIX rows=%d", len(vix_series["timestamp"]))
    except Exception as exc:
        log.warning("[spot_hist] Kite VIX fetch error: %s", exc)

    return result


def get_spot_historical_data(
    db,
    underlying: str,
    candle: str,
    activation_mode: str = "algo-backtest",
) -> dict:
    """
    Returns { underlying_token: series, "NSE_00": vix_series }
    Both series span 09:15 → candle timestamp on the trade date.

    live / fast-forward → Kite historical_data API (real OHLCV)
    algo-backtest       → DB option_chain_index_spot
    """
    date_part, candle_ts = _parse_candle(candle)

    if str(activation_mode or "").strip() in ("live", "fast-forward"):
        return _fetch_kite_spot_vix(underlying, date_part, candle_ts)

    market_open = f"{date_part}T09:15:00"
    time_q = {"timestamp": {"$gte": market_open, "$lte": candle_ts}}
    col = db._db[SPOT_COL]
    result: dict = {}

    # ── Underlying spot (NIFTY → NSE_01, BANKNIFTY → NSE_02, …) ────────────
    ul = underlying.strip().upper()
    spot_docs = list(
        col.find(
            {**time_q, "underlying": ul, "token": {"$ne": VIX_TOKEN}},
            {"_id": 0, "timestamp": 1, "spot_price": 1, "token": 1},
        ).sort("timestamp", 1)
    )
    if spot_docs:
        token = str(spot_docs[0].get("token") or f"SPOT_{ul}")
        result[token] = _to_series(spot_docs)
        log.info("[spot_hist] %s token=%s rows=%d", ul, token, len(spot_docs))
    else:
        log.warning("[spot_hist] no data for underlying=%s date=%s", ul, date_part)

    # ── India VIX (NSE_00) ──────────────────────────────────────────────────
    vix_docs = list(
        col.find(
            {**time_q, "token": VIX_TOKEN},
            {"_id": 0, "timestamp": 1, "spot_price": 1},
        ).sort("timestamp", 1)
    )
    if vix_docs:
        result[VIX_TOKEN] = _to_series(vix_docs)
        log.info("[spot_hist] VIX rows=%d", len(vix_docs))
    else:
        log.warning("[spot_hist] no VIX data for date=%s", date_part)

    return result
