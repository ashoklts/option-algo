"""
Helpers for shared spot-price lookup and ATM derivation.

This module is intended to be reused across backtest, fast-forward,
execution socket, and live-trade flows.

Live / fast-forward mode NEVER touches option_chain_historical_data.
All option chain data for live comes from Kite instruments API + Kite LTP map.
"""

from __future__ import annotations

import logging
import threading
from bisect import bisect_right
from typing import Any
from pymongo import DESCENDING

from features.backtest_engine import _resolve_expiry, _resolve_strike

log = logging.getLogger(__name__)


def _trace_stdout(message: str) -> None:
    """Print live option-chain traces immediately to the Python terminal."""
    print(message, flush=True)

OPTION_CHAIN_COLLECTION = 'option_chain_historical_data'
INDEX_SPOT_COLLECTION = 'option_chain_index_spot'
MARKET_DATA_CACHE: dict[str, dict] = {}

# ─── Kite instruments daily cache ─────────────────────────────────────────────
# Loaded once per trading day. Keyed by (underlying, expiry, strike, type).
# Value: {'token': int, 'symbol': str, 'exchange': str}
# Used ONLY for live / fast-forward mode (never for backtest).

_kite_inst_lock  = threading.Lock()
_kite_inst_date  = ''                          # date the cache was loaded for
_kite_inst_cache: dict[tuple, dict] = {}       # (underlying, expiry, strike, type) → inst


def _ist_today() -> str:
    from datetime import datetime, timezone, timedelta
    return (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d')


def _load_kite_instruments(force: bool = False) -> dict[tuple, dict]:
    """
    Load NFO instruments from Kite and cache them for the trading day.
    Returns the cache dict (may be empty if Kite not configured).
    Thread-safe.
    """
    global _kite_inst_date, _kite_inst_cache

    today = _ist_today()
    with _kite_inst_lock:
        if not force and _kite_inst_date == today and _kite_inst_cache:
            return _kite_inst_cache

        try:
            from features.kite_broker_ws import get_common_credentials, is_configured  # type: ignore
            if not is_configured():
                return _kite_inst_cache

            from kiteconnect import KiteConnect  # type: ignore
            api_key, access_token = get_common_credentials()
            if not api_key or not access_token:
                return _kite_inst_cache

            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
            instruments = kite.instruments('NFO')

            new_cache: dict[tuple, dict] = {}
            for inst in instruments:
                name      = str(inst.get('name') or '').strip().upper()
                inst_type = str(inst.get('instrument_type') or '').strip().upper()
                exp       = inst.get('expiry')
                stk       = inst.get('strike')
                tok       = inst.get('instrument_token')
                sym       = str(inst.get('tradingsymbol') or '').strip()

                if not (name and inst_type in ('CE', 'PE') and exp and stk is not None and tok):
                    continue

                try:
                    exp_str = exp.strftime('%Y-%m-%d')
                except AttributeError:
                    exp_str = str(exp)[:10]

                key = (name, exp_str, float(stk), inst_type)
                new_cache[key] = {
                    'token':    int(tok),
                    'symbol':   sym,
                    'exchange': str(inst.get('exchange') or 'NFO'),
                }

            _kite_inst_cache = new_cache
            _kite_inst_date  = today
            log.info('[kite_instruments] loaded %d NFO instruments for %s', len(new_cache), today)

        except Exception as exc:
            log.warning('[kite_instruments] load error: %s', exc)

        return _kite_inst_cache


def get_kite_chain_doc(
    underlying: str,
    expiry: str,
    strike: float,
    option_type: str,
) -> dict:
    """
    Build a synthetic option chain doc from Kite instruments cache + live LTP.

    Used ONLY for live / fast-forward mode.
    Returns {} if the instrument is not found or Kite is not configured.
    """
    cache = _load_kite_instruments()
    key   = (
        str(underlying  or '').strip().upper(),
        str(expiry      or '').strip()[:10],
        float(strike),
        str(option_type or '').strip().upper(),
    )
    inst = cache.get(key)
    if not inst:
        _trace_stdout(
            f'[LIVE OPTION CHAIN] underlying={key[0]} expiry={key[1]} '
            f'strike={key[2]} type={key[3]} instrument=NOT_FOUND'
        )
        return {}

    token_int = inst['token']
    token_str = str(token_int)
    ltp       = 0.0

    # 1. REST quote cache — works even before WebSocket subscription
    quotes = fetch_kite_quotes_for_expiry(key[0], key[1])
    ltp    = float(quotes.get(token_str, 0.0))

    # 2. Fallback: WebSocket LTP map (available after token is subscribed)
    if ltp <= 0:
        try:
            from features.kite_broker_ws import get_ltp_map  # type: ignore
            ltp = float(get_ltp_map().get(token_str, 0.0))
        except Exception:
            pass

    _trace_stdout(
        f'[LIVE OPTION CHAIN] underlying={key[0]} expiry={key[1]} '
        f'strike={key[2]} type={key[3]} token={token_str} '
        f'symbol={inst["symbol"]} ltp={ltp if ltp > 0 else "UNAVAILABLE"}'
    )

    return {
        'underlying': key[0],
        'expiry':     key[1],
        'strike':     key[2],
        'type':       key[3],
        'token':      str(token_int),
        'symbol':     inst['symbol'],
        'exchange':   inst['exchange'],
        'close':      ltp,
        'ltp':        ltp,
        'current_price': ltp,
        'price':      ltp,
        'last_price': ltp,
    }


def get_kite_expiries(underlying: str, from_date: str) -> list[str]:
    """
    Return sorted expiry date strings for *underlying* >= from_date
    from the Kite instruments cache.  Used by live / fast-forward mode.
    """
    cache = _load_kite_instruments()
    und   = str(underlying or '').strip().upper()
    expiries: set[str] = set()
    for (name, exp, _strike, _type) in cache:
        if name == und and exp >= from_date:
            expiries.add(exp)
    return sorted(expiries)


def list_kite_option_contracts(underlying: str, expiry: str) -> list[dict]:
    """
    Return all cached Kite option contracts for an underlying + expiry.

    Each item contains:
      {
        'instrument': 'NIFTY',
        'expiry': '2026-04-23',
        'strike': 24500,
        'option_type': 'CE',
        'token': '123456',
        'tokens': '123456',
        'symbol': 'NIFTY26APR24500CE',
        'exchange': 'NFO',
      }
    """
    und = str(underlying or '').strip().upper()
    exp = str(expiry or '').strip()[:10]
    if not und or not exp:
        return []

    cache = _load_kite_instruments()
    contracts: list[dict] = []
    for (name, exp_key, strike, option_type), inst in cache.items():
        if name != und or exp_key != exp:
            continue
        token_value = str(inst.get('token') or '').strip()
        if not token_value:
            continue
        strike_value = int(strike) if float(strike).is_integer() else float(strike)
        contracts.append({
            'instrument': und,
            'expiry': exp,
            'strike': strike_value,
            'option_type': str(option_type or '').strip().upper(),
            'token': token_value,
            'tokens': token_value,
            'symbol': str(inst.get('symbol') or '').strip(),
            'exchange': str(inst.get('exchange') or 'NFO').strip() or 'NFO',
        })

    contracts.sort(key=lambda item: (item['strike'], item['option_type']))
    return contracts


# ─── Kite option-chain quote cache ────────────────────────────────────────────
# At entry time we need LTP for ALL strikes of a specific expiry — but those
# tokens are NOT yet subscribed on the WebSocket.  We solve this by calling
# the Kite REST quote() API once per (underlying, expiry) and caching the
# result for _QUOTE_CACHE_TTL seconds.  This avoids per-token subscriptions
# before entry and works even when ltp_map is empty.
#
# Used ONLY for live / fast-forward mode.

import time as _time

_QUOTE_CACHE_TTL              = 3.0   # seconds
_kite_quote_lock              = threading.Lock()
_kite_quote_cache: dict       = {}    # {(underlying, expiry): {'ts': float, 'data': {str(token): float}}}


def fetch_kite_quotes_for_expiry(underlying: str, expiry: str) -> dict[str, float]:
    """
    Fetch real-time LTP for ALL options of *underlying* + *expiry* via the
    Kite REST quote() API.  Returns {str(instrument_token): float(ltp)}.

    Results are cached for _QUOTE_CACHE_TTL seconds so multiple legs of the
    same strategy share one API call at entry time.

    Live / fast-forward only — never called for backtest.
    """
    und       = str(underlying or '').strip().upper()
    exp       = str(expiry     or '').strip()[:10]
    cache_key = (und, exp)

    with _kite_quote_lock:
        cached = _kite_quote_cache.get(cache_key)
        if cached and (_time.monotonic() - cached['ts']) < _QUOTE_CACHE_TTL:
            return cached['data']

    # Collect all instrument tokens for this underlying + expiry
    inst_cache = _load_kite_instruments()
    tokens: list[int] = [
        inst['token']
        for (name, exp_k, _stk, _typ), inst in inst_cache.items()
        if name == und and exp_k == exp
    ]

    if not tokens:
        return {}

    ltp_data: dict[str, float] = {}
    try:
        from features.kite_broker_ws import get_common_credentials, is_configured  # type: ignore
        if not is_configured():
            return {}
        from kiteconnect import KiteConnect  # type: ignore
        api_key, access_token = get_common_credentials()
        if not api_key or not access_token:
            return {}

        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)

        # Kite quote() accepts up to 500 tokens per call
        for i in range(0, len(tokens), 500):
            batch  = tokens[i:i + 500]
            quotes = kite.quote(batch)
            for _sym, q in quotes.items():
                tok = str(q.get('instrument_token') or '').strip()
                ltp = float(q.get('last_price') or 0.0)
                if tok:
                    ltp_data[tok] = ltp

        log.info(
            '[kite_quotes] fetched %d quotes  underlying=%s  expiry=%s',
            len(ltp_data), und, exp,
        )
        _trace_stdout(
            f'[LIVE OPTION QUOTES] underlying={und} expiry={exp} quotes={len(ltp_data)}'
        )
    except Exception as exc:
        log.warning('[kite_quotes] fetch error underlying=%s expiry=%s: %s', und, exp, exc)
        _trace_stdout(
            f'[LIVE OPTION QUOTES] underlying={und} expiry={exp} fetch_error={exc}'
        )

    with _kite_quote_lock:
        _kite_quote_cache[cache_key] = {'ts': _time.monotonic(), 'data': ltp_data}

    return ltp_data


# Kite numeric instrument tokens for major index underlyings.
# Used by live / fast-forward mode to get real-time spot price from
# kite_broker_ws LTP map instead of querying the DB.
KITE_INDEX_TOKENS: dict[str, int] = {
    'NIFTY':      256265,
    'BANKNIFTY':  260105,
    'SENSEX':     265,
    'FINNIFTY':   257801,
    'MIDCPNIFTY': 288009,
}


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def resolve_atm_price(underlying: str, spot_price: float) -> int:
    normalized_underlying = str(underlying or '').upper()
    step = get_strike_step(normalized_underlying)
    if spot_price <= 0:
        return 0
    return int(round(spot_price / step) * step)


def get_strike_step(underlying: str) -> int:
    normalized_underlying = str(underlying or '').upper()
    return 100 if 'BANK' in normalized_underlying or 'MIDCP' in normalized_underlying else 50


def _normalize_underlyings(underlyings: list[str] | tuple[str, ...] | set[str] | None) -> list[str]:
    return sorted({
        str(item or '').strip().upper()
        for item in (underlyings or [])
        if str(item or '').strip()
    })


def _build_market_cache_key(trade_date: str, underlyings: list[str]) -> str:
    suffix = ','.join(underlyings) if underlyings else '*'
    return f'{str(trade_date or "").strip()}::{suffix}'


def clear_market_data_cache(cache_key: str | None = None) -> None:
    normalized_key = str(cache_key or '').strip()
    if not normalized_key:
        return
    MARKET_DATA_CACHE.pop(normalized_key, None)


def _find_latest_snapshot(items: list[dict], timestamps: list[str], snapshot_ts: str) -> dict:
    if not items or not timestamps:
        return {}
    index = bisect_right(timestamps, snapshot_ts) - 1
    if index < 0:
        return {}
    return items[index] or {}


def preload_market_data_cache(
    db,
    trade_date: str,
    underlyings: list[str] | tuple[str, ...] | set[str] | None = None,
    *,
    force_refresh: bool = False,
) -> dict:
    normalized_date = str(trade_date or '').strip()
    normalized_underlyings = _normalize_underlyings(underlyings)
    cache_key = _build_market_cache_key(normalized_date, normalized_underlyings)
    if not force_refresh and cache_key in MARKET_DATA_CACHE:
        return MARKET_DATA_CACHE[cache_key]

    option_query: dict[str, Any] = {'timestamp': {'$regex': f'^{normalized_date}'}}
    spot_query: dict[str, Any] = {'timestamp': {'$regex': f'^{normalized_date}'}}
    if normalized_underlyings:
        option_query['underlying'] = {'$in': normalized_underlyings}
        spot_query['underlying'] = {'$in': normalized_underlyings}

    chain_docs: dict[tuple[str, str, float, str], list[dict]] = {}
    chain_timestamps: dict[tuple[str, str, float, str], list[str]] = {}
    latest_chain_docs: dict[tuple[str, str, float, str], dict] = {}
    expiries_by_underlying: dict[str, set[str]] = {}

    for item in db._db[OPTION_CHAIN_COLLECTION].find(
        option_query,
        {
            '_id': 0,
            'underlying': 1,
            'expiry': 1,
            'strike': 1,
            'type': 1,
            'timestamp': 1,
            'close': 1,
            'spot_price': 1,
            'symbol': 1,
            'token': 1,
            'oi': 1,
            'iv': 1,
            'delta': 1,
            'gamma': 1,
            'theta': 1,
            'vega': 1,
            'rho': 1,
        },
    ).sort([('underlying', 1), ('expiry', 1), ('strike', 1), ('type', 1), ('timestamp', 1)]):
        underlying = str(item.get('underlying') or '').strip().upper()
        expiry = str(item.get('expiry') or '').strip()
        option_type = str(item.get('type') or '').strip().upper()
        strike = safe_float(item.get('strike'))
        timestamp = str(item.get('timestamp') or '').strip()
        if not underlying or not expiry or not option_type or not timestamp:
            continue
        key = (underlying, expiry, strike, option_type)
        chain_docs.setdefault(key, []).append(item)
        chain_timestamps.setdefault(key, []).append(timestamp)
        latest_chain_docs[key] = item
        expiries_by_underlying.setdefault(underlying, set()).add(expiry)

    spot_docs: dict[str, list[dict]] = {}
    spot_timestamps: dict[str, list[str]] = {}
    latest_spot_docs: dict[str, dict] = {}
    for item in db._db[INDEX_SPOT_COLLECTION].find(
        spot_query,
        {
            '_id': 0,
            'underlying': 1,
            'timestamp': 1,
            'spot_price': 1,
            'token': 1,
            'symbol': 1,
        },
    ).sort([('underlying', 1), ('timestamp', 1)]):
        underlying = str(item.get('underlying') or '').strip().upper()
        timestamp = str(item.get('timestamp') or '').strip()
        if not underlying or not timestamp:
            continue
        spot_docs.setdefault(underlying, []).append(item)
        spot_timestamps.setdefault(underlying, []).append(timestamp)
        latest_spot_docs[underlying] = item

    cache = {
        'cache_key': cache_key,
        'trade_date': normalized_date,
        'underlyings': normalized_underlyings,
        'chain_docs': chain_docs,
        'chain_timestamps': chain_timestamps,
        'latest_chain_docs': latest_chain_docs,
        'spot_docs': spot_docs,
        'spot_timestamps': spot_timestamps,
        'latest_spot_docs': latest_spot_docs,
        'expiries_by_underlying': {
            underlying: sorted(values)
            for underlying, values in expiries_by_underlying.items()
        },
    }
    MARKET_DATA_CACHE[cache_key] = cache
    return cache


def _kite_spot_doc(underlying_norm: str, ts: str) -> dict:
    """
    Return a synthetic spot doc built from live Kite LTP for the index.
    Used only in live / fast-forward mode (no pre-loaded market_cache).
    Returns {} if Kite is not configured or LTP not yet received.
    """
    index_token = KITE_INDEX_TOKENS.get(underlying_norm)
    if not index_token:
        return {}
    try:
        from features.kite_broker_ws import get_ltp_map  # type: ignore
        ltp_map = get_ltp_map()
        price = float(ltp_map.get(str(index_token), 0.0))
        if price > 0:
            return {
                'underlying': underlying_norm,
                'spot_price': price,
                'close':      price,
                'ltp':        price,
                'timestamp':  ts or '',
            }
    except Exception:
        pass
    return {}


def _active_option_token_doc(
    db,
    underlying_norm: str,
    expiry_norm: str,
    strike_val: float,
    opt_norm: str,
    ts: str,
) -> dict:
    """
    Resolve live option contract metadata from active_option_tokens and
    overlay the latest Kite WebSocket LTP.

    This is the live / fast-forward replacement for any option-chain-based
    contract lookup. No REST quote fetch is performed here.
    """
    if db is None:
        return {}

    try:
        doc = db['active_option_tokens'].find_one(
            {
                'instrument': underlying_norm,
                'expiry': expiry_norm,
                'strike': strike_val,
                'option_type': opt_norm,
            },
            {
                '_id': 0,
                'instrument': 1,
                'expiry': 1,
                'strike': 1,
                'option_type': 1,
                'token': 1,
                'tokens': 1,
                'symbol': 1,
                'exchange': 1,
            },
        ) or {}
    except Exception as exc:
        log.warning(
            '[active_option_tokens] lookup error instrument=%s expiry=%s strike=%s type=%s: %s',
            underlying_norm, expiry_norm, strike_val, opt_norm, exc,
        )
        return {}

    token_str = str(doc.get('token') or doc.get('tokens') or '').strip()
    if not doc or not token_str:
        _trace_stdout(
            f'[ACTIVE OPTION TOKEN] instrument={underlying_norm} expiry={expiry_norm} '
            f'strike={strike_val} type={opt_norm} token=NOT_FOUND'
        )
        return {}

    ltp = 0.0
    try:
        from features.kite_broker_ws import get_ltp_map  # type: ignore
        ltp = float(get_ltp_map().get(token_str, 0.0))
    except Exception:
        pass

    _trace_stdout(
        f'[ACTIVE OPTION TOKEN] instrument={underlying_norm} expiry={expiry_norm} '
        f'strike={strike_val} type={opt_norm} token={token_str} '
        f'symbol={str(doc.get("symbol") or "").strip() or "-"} '
        f'ltp={ltp if ltp > 0 else "UNAVAILABLE"}'
    )

    return {
        'underlying': underlying_norm,
        'expiry': expiry_norm,
        'strike': strike_val,
        'type': opt_norm,
        'token': token_str,
        'symbol': str(doc.get('symbol') or '').strip(),
        'exchange': str(doc.get('exchange') or 'NFO').strip() or 'NFO',
        'close': ltp,
        'ltp': ltp,
        'current_price': ltp,
        'price': ltp,
        'last_price': ltp,
        'timestamp': ts or '',
    }


def get_cached_spot_doc(
    db_or_cache,
    underlying: str,
    snapshot_ts: str | None = None,
    *,
    timestamp: str | None = None,
    cache: dict | None = None,
) -> dict:
    """
    Fetch spot price document.

    Modes
    ─────
    • backtest        : first arg is a pre-loaded dict market_cache  →  cache lookup
    • live / fast-fwd : first arg is a pymongo Database              →  kite LTP first,
                        then DB fallback (never uses historical cache)

    ``timestamp`` is an alias for ``snapshot_ts`` (used by trading_core.py).
    ``cache`` is an explicit dict cache (takes priority over db_or_cache).
    """
    ts = snapshot_ts or timestamp

    # Determine the actual dict cache (backtest path)
    actual_cache = cache
    if actual_cache is None and isinstance(db_or_cache, dict):
        actual_cache = db_or_cache

    normalized_underlying = str(underlying or '').strip().upper()
    if not normalized_underlying:
        return {}

    # ── Backtest: use pre-loaded dict cache ───────────────────────────────────
    if actual_cache:
        if not ts:
            return (actual_cache.get('latest_spot_docs') or {}).get(normalized_underlying) or {}
        return _find_latest_snapshot(
            (actual_cache.get('spot_docs') or {}).get(normalized_underlying) or [],
            (actual_cache.get('spot_timestamps') or {}).get(normalized_underlying) or [],
            ts,
        )

    # ── Live / fast-forward: db_or_cache is a pymongo Database ───────────────
    db = db_or_cache
    if db is None:
        return {}

    # 1. Try live Kite LTP for the index token (real-time, most accurate)
    kite_doc = _kite_spot_doc(normalized_underlying, ts or '')
    if kite_doc:
        return kite_doc

    # 2. Fallback: direct DB query (if kite not yet connected / token not subscribed)
    try:
        query: dict = {'underlying': normalized_underlying}
        if ts:
            query['timestamp'] = {'$lte': ts}
        doc = db[INDEX_SPOT_COLLECTION].find_one(query, sort=[('timestamp', -1)])
        return doc or {}
    except Exception:
        return {}


def get_cached_chain_doc(
    db_or_cache,
    underlying: str,
    expiry: str,
    strike: Any,
    option_type: str,
    snapshot_ts: str | None = None,
    *,
    timestamp: str | None = None,
    cache: dict | None = None,
) -> dict:
    """
    Fetch option chain document.

    Modes
    ─────
    • backtest        : first arg is a pre-loaded dict market_cache  →  cache lookup
    • live / fast-fwd : first arg is a pymongo Database              →  DB query for
                        contract metadata (expiry, strike, instrument_token) then
                        overlay the price fields with live Kite LTP so the entry /
                        SL / TP uses real-time tick data, not a stale DB close price.

    ``timestamp`` is an alias for ``snapshot_ts`` (used by trading_core.py).
    ``cache`` is an explicit dict cache (takes priority over db_or_cache).
    """
    ts = snapshot_ts or timestamp

    # Determine the actual dict cache (backtest path)
    actual_cache = cache
    if actual_cache is None and isinstance(db_or_cache, dict):
        actual_cache = db_or_cache

    key = (
        str(underlying or '').strip().upper(),
        str(expiry or '').strip()[:10],
        safe_float(strike),
        str(option_type or '').strip().upper(),
    )

    # ── Backtest: use pre-loaded dict cache ───────────────────────────────────
    if actual_cache:
        if not ts:
            return (actual_cache.get('latest_chain_docs') or {}).get(key) or {}
        return _find_latest_snapshot(
            (actual_cache.get('chain_docs') or {}).get(key) or [],
            (actual_cache.get('chain_timestamps') or {}).get(key) or [],
            ts,
        )

    # ── Live / fast-forward: use active_option_tokens + Kite socket LTP ─────
    # Never query option_chain_historical_data here — that collection is for backtest only.
    underlying_norm, expiry_norm, strike_val, opt_norm = key
    return _active_option_token_doc(
        db_or_cache,
        underlying_norm,
        expiry_norm,
        strike_val,
        opt_norm,
        ts or '',
    )


def build_entry_spot_snapshots(
    db,
    records: list[dict],
    listen_time: str,
    listen_timestamp: str,
    market_cache: dict | None = None,
) -> list[dict]:
    matched_records = [
        record for record in (records or [])
        if _extract_hhmm(record.get('entry_time') or '') == listen_time
    ]
    if not matched_records:
        return []

    underlyings = sorted({
        str(record.get('underlying') or '').strip().upper()
        for record in matched_records
        if str(record.get('underlying') or '').strip()
    })

    spot_map: dict[str, dict] = {}
    expiry_map: dict[str, list[str]] = {}
    resolved_market_cache = market_cache
    if underlyings:
        resolved_market_cache = market_cache or preload_market_data_cache(db, listen_timestamp[:10], underlyings)
        for underlying in underlyings:
            spot_doc = get_cached_spot_doc(resolved_market_cache, underlying, listen_timestamp)
            if spot_doc:
                spot_map[underlying] = spot_doc
            expiry_map[underlying] = list(
                ((resolved_market_cache.get('expiries_by_underlying') or {}).get(underlying) or [])
            )

    snapshots = []
    for record in matched_records:
        underlying = str(record.get('underlying') or '').strip().upper()
        spot_doc = spot_map.get(underlying) or {}
        spot_price = safe_float(spot_doc.get('spot_price'))
        option_chain = build_option_chain_snapshots_for_record(
            db=db,
            record=record,
            underlying=underlying,
            trade_date=listen_timestamp[:10],
            listen_timestamp=listen_timestamp,
            spot_price=spot_price,
            expiries=expiry_map.get(underlying) or [],
            market_cache=resolved_market_cache,
        )
        snapshots.append({
            'group_name': record.get('group_name') or '',
            'strategy_name': record.get('name') or '',
            'entry_time': record.get('entry_time') or '',
            'underlying': underlying,
            'spot_price': spot_price,
            'atm_price': resolve_atm_price(underlying, spot_price),
            'spot_timestamp': spot_doc.get('timestamp') or listen_timestamp,
            'option_chain': option_chain,
        })
    return snapshots


def build_option_chain_snapshots_for_record(
    *,
    db,
    record: dict,
    underlying: str,
    trade_date: str,
    listen_timestamp: str,
    spot_price: float,
    expiries: list[str],
    market_cache: dict | None = None,
) -> list[dict]:
    config = record.get('config') if isinstance(record.get('config'), dict) else {}
    leg_configs = config.get('LegConfigs') if isinstance(config.get('LegConfigs'), dict) else {}
    snapshots: list[dict] = []

    for leg_id, leg_config in leg_configs.items():
        if not isinstance(leg_config, dict):
            continue
        contract = leg_config.get('ContractType') if isinstance(leg_config.get('ContractType'), dict) else {}
        option_type = str(contract.get('Option') or '').strip().upper()
        expiry_kind = str(contract.get('Expiry') or leg_config.get('ExpiryKind') or 'ExpiryType.Weekly').strip()
        entry_kind = str(contract.get('EntryKind') or leg_config.get('EntryType') or 'EntryType.EntryByStrikeType').strip()
        strike_param = contract.get('StrikeParameter')
        expiry = _resolve_expiry(trade_date, expiry_kind, expiries)
        if not expiry:
            continue
        strike = resolve_leg_strike(
            db=db,
            underlying=underlying,
            expiry=expiry,
            option_type=option_type,
            entry_kind=entry_kind,
            strike_param=strike_param,
            spot_price=spot_price,
            listen_timestamp=listen_timestamp,
            market_cache=market_cache,
        )
        if strike is None:
            continue
        chain_doc = get_cached_chain_doc(
            market_cache,
            underlying,
            expiry,
            strike,
            option_type,
            listen_timestamp,
        )
        if not chain_doc:
            chain_doc = db._db[OPTION_CHAIN_COLLECTION].find_one(
                {
                    'underlying': underlying,
                    'expiry': expiry,
                    'strike': float(strike),
                    'type': option_type,
                    'timestamp': {'$lte': listen_timestamp},
                },
                sort=[('timestamp', DESCENDING)],
            ) or {}
        snapshots.append({
            'leg_id': str(leg_id),
            'position': str(leg_config.get('PositionType') or ''),
            'entry_kind': entry_kind,
            'expiry_kind': expiry_kind,
            'expiry': expiry,
            'option_type': option_type,
            'strike': strike,
            'close': safe_float(chain_doc.get('close')),
            'timestamp': chain_doc.get('timestamp') or listen_timestamp,
            'spot_price': safe_float(chain_doc.get('spot_price'), spot_price),
            'symbol': str(chain_doc.get('symbol') or ''),
            'token': str(chain_doc.get('token') or ''),
        })
    return snapshots


def resolve_leg_strike(
    *,
    db,
    underlying: str,
    expiry: str,
    option_type: str,
    entry_kind: str,
    strike_param: Any,
    spot_price: float,
    listen_timestamp: str,
    market_cache: dict | None = None,
) -> int | None:
    step = get_strike_step(underlying)

    if entry_kind == 'EntryType.EntryByPremium':
        target = safe_float(strike_param)
        return resolve_strike_by_premium(
            db=db,
            underlying=underlying,
            expiry=expiry,
            option_type=option_type,
            target_premium=target,
            listen_timestamp=listen_timestamp,
            market_cache=market_cache,
        )

    if entry_kind == 'EntryType.EntryByPremiumRange' and isinstance(strike_param, dict):
        lower = safe_float(strike_param.get('LowerRange'))
        upper = safe_float(strike_param.get('UpperRange'), lower)
        return resolve_strike_by_premium(
            db=db,
            underlying=underlying,
            expiry=expiry,
            option_type=option_type,
            target_premium=(lower + upper) / 2,
            listen_timestamp=listen_timestamp,
            market_cache=market_cache,
        )

    if entry_kind in ('EntryType.EntryByDelta', 'EntryType.EntryByDeltaRange'):
        return _resolve_strike(spot_price, 'StrikeType.ATM', option_type, step)

    if entry_kind == 'EntryType.EntryByAtmMultiplier':
        try:
            scaled_spot = safe_float(spot_price) * safe_float(strike_param, 1.0)
            return resolve_atm_price(underlying, scaled_spot)
        except Exception:
            return _resolve_strike(spot_price, 'StrikeType.ATM', option_type, step)

    if entry_kind in ('EntryType.EntryByStraddlePrice', 'EntryType.EntryByPremiumCloseToStraddle') and isinstance(strike_param, dict):
        strike_kind = str(strike_param.get('StrikeKind') or 'StrikeType.ATM')
        return _resolve_strike(spot_price, strike_kind, option_type, step)

    if isinstance(strike_param, str):
        return _resolve_strike(spot_price, strike_param, option_type, step)

    return _resolve_strike(spot_price, 'StrikeType.ATM', option_type, step)


def resolve_strike_by_premium(
    *,
    db,
    underlying: str,
    expiry: str,
    option_type: str,
    target_premium: float,
    listen_timestamp: str,
    market_cache: dict | None = None,
) -> int | None:
    if target_premium <= 0:
        return None
    best_strike = None
    best_diff = float('inf')
    seen_strikes: set[float] = set()
    if market_cache:
        chain_docs = market_cache.get('chain_docs') or {}
        chain_timestamps = market_cache.get('chain_timestamps') or {}
        for key, items in chain_docs.items():
            cache_underlying, cache_expiry, strike, cache_option_type = key
            if cache_underlying != str(underlying or '').strip().upper():
                continue
            if cache_expiry != str(expiry or '').strip():
                continue
            if cache_option_type != str(option_type or '').strip().upper():
                continue
            item = _find_latest_snapshot(items, chain_timestamps.get(key) or [], listen_timestamp)
            strike = safe_float(item.get('strike'))
            if strike in seen_strikes:
                continue
            seen_strikes.add(strike)
            close_price = safe_float(item.get('close'))
            if close_price <= 0:
                continue
            diff = abs(close_price - target_premium)
            if diff < best_diff:
                best_diff = diff
                best_strike = int(strike)
    else:
        cursor = db._db[OPTION_CHAIN_COLLECTION].find(
            {
                'underlying': underlying,
                'expiry': expiry,
                'type': option_type,
                'timestamp': {'$lte': listen_timestamp},
            },
            {
                '_id': 0,
                'strike': 1,
                'close': 1,
                'timestamp': 1,
            },
        ).sort([('timestamp', DESCENDING)])
        for item in cursor:
            strike = safe_float(item.get('strike'))
            if strike in seen_strikes:
                continue
            seen_strikes.add(strike)
            close_price = safe_float(item.get('close'))
            if close_price <= 0:
                continue
            diff = abs(close_price - target_premium)
            if diff < best_diff:
                best_diff = diff
                best_strike = int(strike)
    return best_strike


def _extract_hhmm(raw_time: str) -> str:
    raw_value = str(raw_time or '').strip()
    if len(raw_value) >= 16:
        return raw_value[11:16]
    return raw_value[:5]
