from __future__ import annotations

import re
import time
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
from bson import ObjectId
from bson.errors import InvalidId
from pymongo import UpdateOne

from features.mongo_data import MongoData
from features.kite_broker import get_kite_instance

DEFAULT_FORMULA = "((70% * 6 Month Volatility) + (20% * 3 Month Performance) + (10% * 1 Year Performance)) / 3 Month Volatility"
LOOKBACK_DAYS = 500
KITE_MARKET_CONFIG_ID = "69e18416c3d234dc8c90e6ca"
TRADING_PERIODS = {
    "1M": 21,
    "3M": 63,
    "6M": 126,
    "9M": 189,
    "1Y": 252,
}
FORMULA_TOKEN_MAP = {
    "1 Month Performance": "perf_1M",
    "3 Month Performance": "perf_3M",
    "6 Month Performance": "perf_6M",
    "9 Month Performance": "perf_9M",
    "1 Year Performance": "perf_1Y",
    "1 Month Volatility": "vol_1M",
    "3 Month Volatility": "vol_3M",
    "6 Month Volatility": "vol_6M",
    "9 Month Volatility": "vol_9M",
    "1 Year Volatility": "vol_1Y",
}
PORTFOLIO_COLLECTION = "scanner_portfolio_settings"
INVESTMENT_COLLECTION = "scanner_investment_portfolio"
STOCKS_COLLECTION = "scanner_stocks_list"
HISTORY_COLLECTION = "scanner_stock_historical_data"
PORTFOLIO_SUMMARY_CACHE_TTL_SECONDS = 10.0

_portfolio_summary_cache_value: list[dict[str, Any]] | None = None
_portfolio_summary_cache_expires_at = 0.0


def _coerce_score_date(raw_value: Any) -> datetime:
    if isinstance(raw_value, datetime):
        return raw_value
    if not raw_value:
        return datetime.now()
    try:
        return datetime.fromisoformat(str(raw_value))
    except Exception:
        return pd.to_datetime(raw_value, errors="coerce").to_pydatetime()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    if np.isnan(numeric) or np.isinf(numeric):
        return default
    return numeric


def _serialize_value(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize_value(item) for key, item in value.items()}
    return value


def _serialize_doc(document: dict[str, Any]) -> dict[str, Any]:
    return {key: _serialize_value(value) for key, value in document.items()}


def _as_object_id(raw_value: str) -> ObjectId:
    try:
        return ObjectId(str(raw_value).strip())
    except (InvalidId, TypeError) as exc:
        raise ValueError("Invalid portfolio id.") from exc


def _normalize_list_payload(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def _normalize_formula(formula: str | None) -> str:
    normalized = str(formula or DEFAULT_FORMULA).strip() or DEFAULT_FORMULA
    normalized = re.sub(r"(\d+(?:\.\d+)?)\s*%", lambda match: str(float(match.group(1)) / 100.0), normalized)
    for label, column in sorted(FORMULA_TOKEN_MAP.items(), key=lambda item: len(item[0]), reverse=True):
        normalized = re.sub(re.escape(label), column, normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"[^A-Za-z0-9_+\-*/(). <>=!&|]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _load_company_map(symbols: list[str]) -> dict[str, str]:
    if not symbols:
        return {}
    db = MongoData()._db
    rows = list(
        db[STOCKS_COLLECTION].find(
            {"symbol": {"$in": symbols}},
            {"_id": 0, "symbol": 1, "company_name": 1},
        )
    )
    company_map: dict[str, str] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").strip()
        if symbol:
            company_map[symbol] = str(row.get("company_name") or symbol).strip()
    return company_map


def _load_stock_meta_map(symbols: list[str]) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    db = MongoData()._db
    normalized_symbols = [str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()]
    rows = list(
        db[STOCKS_COLLECTION].find(
            {"symbol": {"$in": normalized_symbols}},
            {
                "_id": 0,
                "symbol": 1,
                "company_name": 1,
                "token": 1,
                "tokens": 1,
                "instrument_token": 1,
                "exchange_token": 1,
                "code": 1,
            },
        )
    )
    stock_meta_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").strip()
        if symbol:
            stock_meta_map[symbol] = row

    return stock_meta_map


def _load_latest_closes(symbols: list[str]) -> dict[str, float]:
    if not symbols:
        return {}
    db = MongoData()._db
    rows = list(
        db[HISTORY_COLLECTION].find(
            {"h_symbol": {"$in": symbols}},
            {"_id": 0, "h_symbol": 1, "ch_timestamp": 1, "ch_closing_price": 1},
        ).sort([("h_symbol", 1), ("ch_timestamp", -1)])
    )
    close_map: dict[str, float] = {}
    for row in rows:
        symbol = str(row.get("h_symbol") or "").strip()
        if not symbol or symbol in close_map:
            continue
        close_map[symbol] = round(_safe_float(row.get("ch_closing_price")), 2)
    return close_map


def _load_kite_access_token() -> str:
    db = MongoData()._db
    doc = db["kite_market_config"].find_one(
        {"_id": _as_object_id(KITE_MARKET_CONFIG_ID)},
        {"access_token": 1},
    ) or {}
    return str(doc.get("access_token") or "").strip()


def _load_kite_quotes(symbols: list[str]) -> tuple[dict[str, float], dict[str, float]]:
    normalized_symbols = [str(symbol or "").strip().upper() for symbol in symbols if str(symbol or "").strip()]
    if not normalized_symbols:
        return {}, {}

    try:
        access_token = _load_kite_access_token()
        if not access_token:
            return {}, {}

        kite = get_kite_instance(access_token)
        instruments = [f"NSE:{symbol}" for symbol in normalized_symbols]
        quotes = kite.quote(instruments) or {}

        ltp_map: dict[str, float] = {}
        close_map: dict[str, float] = {}
        for instrument, payload in quotes.items():
            symbol = str(instrument).split(":")[-1].strip().upper()
            last_price = _safe_float((payload or {}).get("last_price"))
            ohlc = (payload or {}).get("ohlc") or {}
            prev_close = _safe_float(ohlc.get("close"))
            if last_price > 0:
                ltp_map[symbol] = round(last_price, 2)
            if prev_close > 0:
                close_map[symbol] = round(prev_close, 2)
        return ltp_map, close_map
    except Exception:
        return {}, {}


def _clear_portfolio_summary_cache() -> None:
    global _portfolio_summary_cache_value, _portfolio_summary_cache_expires_at
    _portfolio_summary_cache_value = None
    _portfolio_summary_cache_expires_at = 0.0


def _build_live_price_map(
    investments: list[dict[str, Any]],
    *,
    allow_history_fallback: bool = True,
    prefetched_ltp_map: dict[str, float] | None = None,
    prefetched_close_map: dict[str, float] | None = None,
) -> tuple[dict[str, float], dict[str, float]]:
    active_symbols = sorted(
        {
            str(item.get("symbol") or "").strip()
            for item in investments
            if int(item.get("position_status", 1) or 1) == 1 and str(item.get("symbol") or "").strip()
        }
    )
    if prefetched_ltp_map is None or prefetched_close_map is None:
        kite_ltp_map, kite_close_map = _load_kite_quotes(active_symbols)
        close_map = dict(kite_close_map)
        if allow_history_fallback:
            for symbol, close in _load_latest_closes(active_symbols).items():
                close_map.setdefault(symbol.upper(), close)
        ltp_map: dict[str, float] = dict(kite_ltp_map)
    else:
        close_map = {
            symbol.upper(): round(_safe_float(prefetched_close_map.get(symbol)), 2)
            for symbol in active_symbols
            if _safe_float(prefetched_close_map.get(symbol)) > 0
        }
        ltp_map = {
            symbol.upper(): round(_safe_float(prefetched_ltp_map.get(symbol)), 2)
            for symbol in active_symbols
            if _safe_float(prefetched_ltp_map.get(symbol)) > 0
        }

    fallback_close_map: dict[str, float] = {}
    fallback_ltp_map: dict[str, float] = {}

    for item in investments:
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        fallback_close = _safe_float(item.get("yesterday_close"))
        if fallback_close > 0 and symbol not in fallback_close_map:
            fallback_close_map[symbol] = round(fallback_close, 2)
        if symbol in ltp_map:
            continue
        live_candidates = (
            item.get("ltp"),
            item.get("last_price"),
            item.get("live_price"),
            item.get("cmp"),
        )
        for candidate in live_candidates:
            live_value = _safe_float(candidate)
            if live_value > 0:
                fallback_ltp_map[symbol] = round(live_value, 2)
                break

    for symbol, close in fallback_close_map.items():
        close_map.setdefault(symbol, close)

    for symbol, live_value in fallback_ltp_map.items():
        ltp_map.setdefault(symbol, live_value)

    for symbol in active_symbols:
        symbol = symbol.upper()
        fallback_ltp = fallback_close_map.get(symbol, 0.0)
        ltp_map.setdefault(symbol, close_map.get(symbol, fallback_ltp))

    return ltp_map, close_map


def _build_portfolio_snapshot(
    portfolio_doc: dict[str, Any],
    investments: list[dict[str, Any]],
    *,
    include_investments: bool,
    allow_history_fallback: bool = True,
    prefetched_ltp_map: dict[str, float] | None = None,
    prefetched_close_map: dict[str, float] | None = None,
) -> dict[str, Any]:
    symbols = sorted({str(item.get("symbol") or "").strip() for item in investments if str(item.get("symbol") or "").strip()})
    company_map = _load_company_map(symbols) if include_investments else {}
    stock_meta_map = _load_stock_meta_map(symbols) if include_investments else {}
    ltp_map, close_map = _build_live_price_map(
        investments,
        allow_history_fallback=allow_history_fallback,
        prefetched_ltp_map=prefetched_ltp_map,
        prefetched_close_map=prefetched_close_map,
    )

    current_investment = 0.0
    unrealized_returns = 0.0
    realized_returns = 0.0
    day_returns = 0.0
    active_holdings = 0
    enriched_investments: list[dict[str, Any]] = []

    for item in investments:
        symbol = str(item.get("symbol") or "").strip()
        entry_price = _safe_float(item.get("entry_price"))
        quantity = int(_safe_float(item.get("position_qty") or item.get("qty") or item.get("quantity"), 0))
        position_status = int(item.get("position_status", 1) or 1)
        exit_price = _safe_float(item.get("exit_price"), entry_price)

        if position_status == 1:
            ltp = _safe_float(ltp_map.get(symbol), 0.0)
            if ltp <= 0:
                ltp = entry_price
            close = _safe_float(
                item.get("yesterday_close"),
                close_map.get(symbol, ltp if ltp > 0 else entry_price),
            )
            current_investment += entry_price * quantity
            unrealized_returns += (ltp - entry_price) * quantity
            day_returns += (ltp - close) * quantity
            active_holdings += 1
        else:
            ltp = exit_price if exit_price > 0 else entry_price
            close = ltp
            if position_status == 2:
                realized_returns += (ltp - entry_price) * quantity

        if include_investments:
            overall_pnl_amount = (ltp - entry_price) * quantity
            overall_pnl_percent = ((ltp - entry_price) / entry_price * 100.0) if entry_price else 0.0
            today_pnl = (ltp - close) * quantity if position_status == 1 else 0.0
            today_change = ((ltp - close) / close * 100.0) if close and position_status == 1 else 0.0
            investment_row = _serialize_doc(item)
            stock_meta = stock_meta_map.get(symbol, {})
            investment_row.update(
                {
                    "company_name": company_map.get(symbol, symbol or "N/A"),
                    "kite_token": investment_row.get("kite_token") or stock_meta.get("kite_token") or stock_meta.get("token") or "",
                    "token": investment_row.get("token") or stock_meta.get("token") or stock_meta.get("tokens") or stock_meta.get("instrument_token") or stock_meta.get("exchange_token") or stock_meta.get("code") or "",
                    "tokens": investment_row.get("tokens") or stock_meta.get("tokens") or stock_meta.get("token") or "",
                    "instrument_token": investment_row.get("instrument_token") or stock_meta.get("instrument_token") or stock_meta.get("token") or stock_meta.get("tokens") or "",
                    "exchange_token": investment_row.get("exchange_token") or stock_meta.get("exchange_token") or stock_meta.get("code") or "",
                    "investment_amount": round(entry_price * quantity, 2),
                    "ltp": round(ltp, 2),
                    "overall_pnl_amount": round(overall_pnl_amount, 2),
                    "overall_pnl_percent": round(overall_pnl_percent, 2),
                    "today_pnl": round(today_pnl, 2),
                    "today_change": round(today_change, 2),
                    "yesterday_close": round(close, 2),
                }
            )
            enriched_investments.append(investment_row)

    total_returns = realized_returns + unrealized_returns
    current_value = current_investment + total_returns
    current_value = round(current_value, 2)
    total_returns_pct = (total_returns / current_investment * 100.0) if current_investment else 0.0
    day_returns_pct = (day_returns / current_value * 100.0) if current_value else 0.0

    snapshot = {
        "portfolio_id": str(portfolio_doc.get("_id") or ""),
        "portfolio_name": str(portfolio_doc.get("strategy_name") or portfolio_doc.get("name") or "Unnamed").strip() or "Unnamed",
        "description": str(portfolio_doc.get("description") or "").strip(),
        "investment_value": round(current_investment, 2),
        "current_value": current_value,
        "realized_returns": round(realized_returns, 2),
        "unrealized_returns": round(unrealized_returns, 2),
        "returns": round(total_returns, 2),
        "return_pct": round(total_returns_pct, 2),
        "returns_percent": round(total_returns_pct, 2),
        "day_returns": round(day_returns, 2),
        "day_returns_percent": round(day_returns_pct, 2),
        "combained_portfilio": bool(portfolio_doc.get("combained_portfilio")),
        "holdings": active_holdings,
        "created_at": _serialize_value(portfolio_doc.get("created_at")),
    }

    if include_investments:
        serialized_portfolio = _serialize_doc(portfolio_doc)
        snapshot.update(
            {
                "strategy_id": snapshot["portfolio_id"],
                "strategy_name": snapshot["portfolio_name"],
                "total_stocks": len(investments),
                "current_investment": snapshot["investment_value"],
                "current_value": current_value,
                "realized_returns_value": snapshot["realized_returns"],
                "unrealized_returns_value": snapshot["unrealized_returns"],
                "total_returns_value": snapshot["returns"],
                "total_returns_percent": snapshot["returns_percent"],
                "day_returns_value": snapshot["day_returns"],
                "day_returns_percent": snapshot["day_returns_percent"],
                "investments": enriched_investments,
                "formula": serialized_portfolio.get("formula", ""),
                "strategy_index": serialized_portfolio.get("indexes", ""),
                "sector": serialized_portfolio.get("sectors", ""),
                "starting_capital": _safe_float(serialized_portfolio.get("starting_capital")),
                "entry_rank": serialized_portfolio.get("entry_rank", 0),
                "exit_rank": serialized_portfolio.get("exit_rank", 0),
                "rebalance_frequency": serialized_portfolio.get("rebalance_frequency", ""),
                "rebalance_date": serialized_portfolio.get("rebalance_date", ""),
                "alternative_rebalance_days": serialized_portfolio.get("alternative_rebalance_days", []),
                "position_sizing": serialized_portfolio.get("position_sizing", ""),
                "timestamp": datetime.utcnow().isoformat(),
                "uncorrelated_asset_status": bool(serialized_portfolio.get("uncorrelated_asset_status", False)),
                "uncorrelated_asset_allocation": _safe_float(serialized_portfolio.get("uncorrelated_asset_allocation")),
                "uncorrelated_asset_type": serialized_portfolio.get("uncorrelated_asset_type", "gold_bees"),
                "stocks_ltp": {key: round(_safe_float(value), 2) for key, value in ltp_map.items()},
                "portfolio": serialized_portfolio,
            }
        )

    return snapshot


def get_indexes() -> list[dict[str, str]]:
    db = MongoData()._db
    rows = list(
        db["scanner_index_stocks"].find(
            {},
            {"_id": 0, "name": 1, "filter_symbol": 1},
        ).sort("name", 1)
    )
    return [
        {
            "label": str(row.get("name") or row.get("filter_symbol") or "").strip(),
            "value": str(row.get("filter_symbol") or "").strip(),
        }
        for row in rows
        if str(row.get("filter_symbol") or "").strip()
    ]


UNIVERSE_STOCK_LIST_COLLECTION = "scanner_universe_stock_list"
_INITIAL_START_DATE = "01-01-1999"


def get_previous_universe_stocks(filter_symbol: str) -> dict[str, Any]:
    db = MongoData()._db
    filter_symbol = filter_symbol.strip()

    # Most recent closed record (end_date is not empty), sorted by end_date desc
    record = db[UNIVERSE_STOCK_LIST_COLLECTION].find_one(
        {"filter_symbol": filter_symbol, "universe_period_end_date": {"$ne": ""}},
        sort=[("universe_period_end_date", -1)],
    )

    is_current = False
    if not record:
        # No closed record — fall back to current active record
        record = db[UNIVERSE_STOCK_LIST_COLLECTION].find_one(
            {"filter_symbol": filter_symbol, "universe_period_end_date": ""},
        )
        is_current = True

    if not record:
        return {
            "status": "no_data",
            "message": f"No record found for '{filter_symbol}'.",
        }

    return {
        "status": "success",
        "is_current_active": is_current,
        "filter_symbol": filter_symbol,
        "universe_period_start_date": record.get("universe_period_start_date", ""),
        "universe_period_end_date": record.get("universe_period_end_date", ""),
        "stock_count": len(record.get("stocks") or []),
        "stocks": sorted(record.get("stocks") or []),
    }


def sync_universe_stock_list() -> dict[str, Any]:
    # Deprecated — use kite_sync_universe_stock_list instead.
    # scanner_universe_stock_list is now the single source of truth.
    return kite_sync_universe_stock_list()


def remove_universe_field_from_stocks_list() -> dict[str, Any]:
    db = MongoData()._db
    result = db[STOCKS_COLLECTION].update_many(
        {"universe": {"$exists": True}},
        {"$unset": {"universe": ""}},
    )
    return {
        "status": "success",
        "matched": result.matched_count,
        "modified": result.modified_count,
    }


def kite_sync_universe_stock_list() -> dict[str, Any]:
    db = MongoData()._db
    now = datetime.utcnow()
    today = now.strftime("%d-%m-%Y")
    yesterday = (now - timedelta(days=1)).strftime("%d-%m-%Y")

    # Fetch all current NSE EQ tradingsymbols from Kite
    access_token = _load_kite_access_token()
    if not access_token:
        raise ValueError("Kite access token not configured.")

    kite = get_kite_instance(access_token)
    raw_instruments = kite.instruments("NSE") or []

    kite_eq_symbols: set[str] = set()
    for inst in raw_instruments:
        sym = str(inst.get("tradingsymbol") or "").strip().upper()
        if sym and str(inst.get("instrument_type") or "").strip().upper() == "EQ":
            kite_eq_symbols.add(sym)

    # Source: scanner_universe_stock_list active records (not stocks_list.universe)
    active_records = list(db[UNIVERSE_STOCK_LIST_COLLECTION].find(
        {"universe_period_end_date": ""},
        {"_id": 1, "filter_symbol": 1, "stocks": 1, "universe_id": 1},
    ))

    synced = []
    for active in active_records:
        filter_symbol = str(active.get("filter_symbol") or "").strip()
        universe_id = str(active.get("universe_id") or "").strip()
        if not filter_symbol:
            continue

        old_symbols = sorted(
            str(s or "").strip().upper()
            for s in (active.get("stocks") or [])
            if str(s or "").strip()
        )

        # Validate: keep only stocks still tradeable in Kite NSE EQ (detect delistings)
        new_symbols = sorted(s for s in old_symbols if s in kite_eq_symbols)
        missing_from_kite = [s for s in old_symbols if s not in kite_eq_symbols]

        if old_symbols == new_symbols:
            db[UNIVERSE_STOCK_LIST_COLLECTION].update_one(
                {"_id": active["_id"]},
                {"$set": {"updated_at": now}},
            )
            synced.append({
                "filter_symbol": filter_symbol,
                "action": "no_change",
                "stock_count": len(new_symbols),
                "missing_from_kite": missing_from_kite,
            })
            continue

        # Some stocks delisted — close old record, open new one
        db[UNIVERSE_STOCK_LIST_COLLECTION].update_one(
            {"_id": active["_id"]},
            {"$set": {"universe_period_end_date": yesterday, "updated_at": now}},
        )
        db[UNIVERSE_STOCK_LIST_COLLECTION].insert_one({
            "universe_id": universe_id,
            "filter_symbol": filter_symbol,
            "stocks": new_symbols,
            "universe_period_start_date": today,
            "universe_period_end_date": "",
            "created_at": now,
            "updated_at": now,
        })
        synced.append({
            "filter_symbol": filter_symbol,
            "action": "updated",
            "stock_count": len(new_symbols),
            "missing_from_kite": missing_from_kite,
        })

    return {
        "status": "success",
        "synced": synced,
        "total": len(synced),
    }


def backfill_stocks_list_kite_tokens() -> dict[str, Any]:
    db = MongoData()._db
    now = datetime.utcnow()

    access_token = _load_kite_access_token()
    if not access_token:
        raise ValueError("Kite access token not configured.")

    kite = get_kite_instance(access_token)
    raw_instruments = kite.instruments("NSE") or []

    # Build map: tradingsymbol → instrument data (EQ only)
    kite_map: dict[str, dict] = {}
    for inst in raw_instruments:
        sym = str(inst.get("tradingsymbol") or "").strip().upper()
        if sym and str(inst.get("instrument_type") or "").strip().upper() == "EQ":
            kite_map[sym] = inst

    # ── Step 1: Update kite tokens for existing stocks_list symbols ──────────
    stocks = list(db[STOCKS_COLLECTION].find({}, {"_id": 1, "symbol": 1}))
    existing_symbols: set[str] = set()

    updated = 0
    not_found_in_kite: list[str] = []

    for row in stocks:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        existing_symbols.add(symbol)

        inst = kite_map.get(symbol)
        if not inst:
            not_found_in_kite.append(symbol)
            continue

        db[STOCKS_COLLECTION].update_one(
            {"_id": row["_id"]},
            {"$set": {
                "kite_token": str(inst.get("instrument_token") or "").strip(),
                "instrument_token": str(inst.get("instrument_token") or "").strip(),
                "exchange_token": str(inst.get("exchange_token") or "").strip(),
                "tradingsymbol": str(inst.get("tradingsymbol") or symbol).strip(),
            }},
        )
        updated += 1

    # ── Step 2: Check universe stocks — find missing from stocks_list ─────────
    # Get all active universe records
    universe_records = list(db[UNIVERSE_STOCK_LIST_COLLECTION].find(
        {"universe_period_end_date": ""},
        {"_id": 0, "filter_symbol": 1, "stocks": 1},
    ))

    missing_from_stocks_list: list[dict[str, Any]] = []
    newly_added: list[str] = []

    for rec in universe_records:
        filter_symbol = str(rec.get("filter_symbol") or "").strip()
        for sym in (rec.get("stocks") or []):
            sym = str(sym or "").strip().upper()
            if not sym or sym in existing_symbols:
                continue

            # Symbol is in universe but missing from stocks_list — add it
            inst = kite_map.get(sym)
            db[STOCKS_COLLECTION].insert_one({
                "symbol": sym,
                "tradingsymbol": sym,
                "exchange": "NSE",
                "kite_token": str(inst.get("instrument_token") or "").strip() if inst else "",
                "instrument_token": str(inst.get("instrument_token") or "").strip() if inst else "",
                "exchange_token": str(inst.get("exchange_token") or "").strip() if inst else "",
                "created_at": now,
            })
            existing_symbols.add(sym)
            newly_added.append(sym)
            missing_from_stocks_list.append({"symbol": sym, "universe": filter_symbol})

    return {
        "status": "success",
        "stocks_list_total": len(stocks),
        "kite_token_updated": updated,
        "not_found_in_kite": not_found_in_kite,
        "not_found_in_kite_count": len(not_found_in_kite),
        "missing_from_stocks_list_added": missing_from_stocks_list,
        "missing_added_count": len(newly_added),
    }


def get_sectors() -> list[dict[str, str]]:
    db = MongoData()._db
    values = db[STOCKS_COLLECTION].distinct("industry")
    sectors = sorted(str(item).strip() for item in values if str(item).strip())
    return [{"label": sector, "value": sector} for sector in sectors]


def _get_symbols_from_universe_history(index_names: list[str], score_date: datetime) -> list[str]:
    """Return stock symbols from scanner_universe_stock_list valid for score_date."""
    db = MongoData()._db
    score_day = score_date.date()

    def _parse_ddmmyyyy(s: str):
        try:
            return datetime.strptime(s.strip(), "%d-%m-%Y").date()
        except Exception:
            return None

    symbols: list[str] = []
    for index_name in index_names:
        records = list(
            db[UNIVERSE_STOCK_LIST_COLLECTION].find(
                {"filter_symbol": index_name},
                {"_id": 0, "stocks": 1, "universe_period_start_date": 1, "universe_period_end_date": 1},
            )
        )
        for rec in records:
            start = _parse_ddmmyyyy(str(rec.get("universe_period_start_date") or ""))
            end_str = str(rec.get("universe_period_end_date") or "").strip()
            end = _parse_ddmmyyyy(end_str) if end_str else None

            if start is None:
                continue
            if start > score_day:
                continue
            if end is not None and end < score_day:
                continue

            symbols.extend(str(s).strip().upper() for s in (rec.get("stocks") or []) if str(s).strip())
            break  # found the matching period for this index

    return sorted(set(symbols))


def _load_meta(
    index_names: list[str],
    sectors: list[str],
    symbols_override: list[str] | None = None,
) -> pd.DataFrame:
    """
    Always use symbols_override (from scanner_universe_stock_list) as the universe source.
    stocks_list is queried only for metadata (sector/industry) — never by universe field.
    """
    db = MongoData()._db

    if not symbols_override:
        return pd.DataFrame()

    projection = {"_id": 0, "symbol": 1, "industry": 1}
    meta_rows = list(db[STOCKS_COLLECTION].find(
        {"symbol": {"$in": symbols_override}},
        projection,
    ))

    # Symbols from universe that have no entry in stocks_list still need to appear
    meta_symbol_set = {str(r.get("symbol") or "").strip() for r in meta_rows}
    for sym in symbols_override:
        if sym not in meta_symbol_set:
            meta_rows.append({"symbol": sym, "industry": ""})

    meta = pd.DataFrame(meta_rows)
    meta["symbol"] = meta["symbol"].astype(str).str.strip()
    meta["sector"] = meta["industry"].fillna("").astype(str).str.strip()
    meta["universe"] = ",".join(sorted(index_names)) if index_names else ""

    meta = meta[meta["symbol"] != ""]
    if sectors:
        meta = meta[meta["sector"].isin(sectors)]
    meta = meta.drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)
    return meta[["symbol", "sector", "universe"]]


def _load_history(symbols: list[str], hist_start: datetime, hist_end: datetime) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    db = MongoData()._db
    query = {
        "h_symbol": {"$in": symbols},
        "ch_timestamp": {
            "$gte": hist_start.strftime("%Y-%m-%d"),
            "$lte": hist_end.strftime("%Y-%m-%d 23:59:59"),
        },
    }
    projection = {"_id": 0, "h_symbol": 1, "ch_timestamp": 1, "ch_closing_price": 1}
    rows = list(db[HISTORY_COLLECTION].find(query, projection))
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["h_symbol"] = df["h_symbol"].astype(str).str.strip()
    df["ch_timestamp"] = pd.to_datetime(df["ch_timestamp"], errors="coerce")
    df["ch_closing_price"] = pd.to_numeric(df["ch_closing_price"], errors="coerce")
    df = df.dropna(subset=["h_symbol", "ch_timestamp", "ch_closing_price"])
    df = df.sort_values(["h_symbol", "ch_timestamp"]).reset_index(drop=True)
    return df


def _compute_metrics(history: pd.DataFrame) -> pd.DataFrame:
    df = history.copy()
    df["ret"] = df.groupby("h_symbol")["ch_closing_price"].pct_change()

    for suffix, periods in TRADING_PERIODS.items():
        perf_col = f"perf_{suffix}"
        vol_col = f"vol_{suffix}"
        df[perf_col] = df.groupby("h_symbol")["ch_closing_price"].pct_change(periods=periods) * 100.0
        df[vol_col] = df.groupby("h_symbol")["ret"].transform(
            lambda series: series.rolling(periods, min_periods=5).std() * 100.0
        )

    metric_columns = [column for column in df.columns if column.startswith(("perf_", "vol_"))]
    df[metric_columns] = df[metric_columns].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return df


def _evaluate_scores(df: pd.DataFrame, formula: str) -> pd.Series:
    expression = _normalize_formula(formula)
    try:
        return df.eval(expression, engine="python")
    except Exception as exc:
        raise ValueError(f"Invalid scoring formula: {formula}. Parsed: {expression}") from exc


def _attach_kite_tokens(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return rows

    stock_meta_map = _load_stock_meta_map(
        [str(row.get("symbol") or "").strip() for row in rows if str(row.get("symbol") or "").strip()]
    )

    enriched_rows: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").strip()
        stock_meta = stock_meta_map.get(symbol, {})
        resolved_token = (
            row.get("kite_token")
            or row.get("token")
            or row.get("tokens")
            or row.get("instrument_token")
            or row.get("exchange_token")
            or row.get("code")
            or stock_meta.get("kite_token")
            or stock_meta.get("token")
            or stock_meta.get("tokens")
            or stock_meta.get("instrument_token")
            or stock_meta.get("exchange_token")
            or stock_meta.get("code")
            or ""
        )
        enriched_rows.append(
            {
                **row,
                "kite_token": resolved_token,
                "symbol_token": row.get("symbol_token") or resolved_token,
                "token": row.get("token") or resolved_token,
                "tokens": row.get("tokens") or resolved_token,
                "instrument_token": row.get("instrument_token") or resolved_token,
            }
        )
    return enriched_rows


def _build_portfolio(score_rows: pd.DataFrame, total_capital: float, top_n: int) -> tuple[list[dict[str, Any]], dict[str, float]]:
    top_n = max(1, int(top_n or 1))
    equal_investment = float(total_capital) / float(top_n)
    portfolio_rows: list[dict[str, Any]] = []
    used_capital = 0.0

    for row in score_rows.sort_values("rank").head(top_n).to_dict(orient="records"):
        last_price = float(row.get("last_price") or 0.0)
        qty = int(equal_investment // last_price) if last_price > 0 else 0
        amount = round(qty * last_price, 2)
        used_capital += amount
        portfolio_rows.append(
            {
                **row,
                "qty": qty,
                "amount": amount,
                "Investment": round(equal_investment, 2),
            }
        )

    portfolio_rows = _attach_kite_tokens(portfolio_rows)

    summary = {
        "total_capital": round(float(total_capital), 2),
        "used_capital": round(used_capital, 2),
        "remaining_capital": round(float(total_capital) - used_capital, 2),
    }
    return portfolio_rows, summary


def generate_stock_scores(payload: dict[str, Any]) -> dict[str, Any]:
    index_names = _normalize_list_payload(payload.get("index_name") or payload.get("index_names"))
    sectors = _normalize_list_payload(payload.get("sectors"))
    min_price = payload.get("min_price")
    max_price = payload.get("max_price")
    top_n = int(payload.get("top_n", 12) or 12)
    total_capital = float(payload.get("total_capital", 1_000_000) or 1_000_000)
    formula = str(payload.get("formula") or DEFAULT_FORMULA)
    score_date = _coerce_score_date(payload.get("score_date"))
    if pd.isna(score_date):
        score_date = datetime.now()

    if index_names:
        symbols_override = _get_symbols_from_universe_history(index_names, score_date)
        if not symbols_override:
            return {
                "status": "no_data",
                "message": f"No universe stock list found for {index_names} on {score_date.date()}. "
                           "Please run /scanner/kite_sync_universe_stocks first.",
            }
    else:
        symbols_override = []

    meta = _load_meta(index_names, sectors, symbols_override=symbols_override)
    if meta.empty:
        return {
            "status": "no_data",
            "message": "No stocks found for the selected filters.",
        }

    hist_start = score_date - timedelta(days=LOOKBACK_DAYS)
    hist_end = score_date - timedelta(days=1)
    history = _load_history(meta["symbol"].tolist(), hist_start, hist_end)
    if history.empty:
        return {
            "status": "no_data",
            "message": "No historical data found for the selected filters.",
        }

    history = _compute_metrics(history)
    history["score"] = _evaluate_scores(history, formula).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    latest = history.groupby("h_symbol", as_index=False).tail(1).copy()
    latest = latest.merge(meta, left_on="h_symbol", right_on="symbol", how="inner")
    latest["last_price"] = latest["ch_closing_price"].round(2)

    if min_price not in (None, "", 0, "0"):
        latest = latest[latest["last_price"] >= float(min_price)]
    if max_price not in (None, "", 0, "0"):
        latest = latest[latest["last_price"] <= float(max_price)]

    if latest.empty:
        return {
            "status": "no_data",
            "message": "No stocks matched the selected price range.",
        }

    latest["rank"] = latest["score"].rank(ascending=False, method="first").astype(int)
    latest = latest.sort_values(["rank", "symbol"]).reset_index(drop=True)

    ordered_columns = [
        "rank",
        "symbol",
        "sector",
        "universe",
        "score",
        "last_price",
        "perf_1M",
        "perf_3M",
        "perf_6M",
        "perf_9M",
        "perf_1Y",
        "vol_1M",
        "vol_3M",
        "vol_6M",
        "vol_9M",
        "vol_1Y",
    ]
    for column in ordered_columns:
        if column not in latest.columns:
            latest[column] = 0.0

    snapshot = latest[ordered_columns].replace([np.inf, -np.inf], np.nan).fillna(0.0).round(6)
    snapshot_rows = _attach_kite_tokens(snapshot.to_dict(orient="records"))
    investment_portfolio, summary = _build_portfolio(pd.DataFrame(snapshot_rows), total_capital, top_n)

    return {
        "status": "success",
        "meta": {
            "index_names": index_names,
            "sectors": sectors,
            "min_price": min_price,
            "max_price": max_price,
            "top_n": top_n,
            "total_capital": total_capital,
            "formula_used": formula,
            "score_date": score_date.strftime("%Y-%m-%d"),
            "lookback_start": hist_start.strftime("%Y-%m-%d"),
            "lookback_end": hist_end.strftime("%Y-%m-%d"),
        },
        "stocks_scored": snapshot_rows,
        "investment_portfolio": investment_portfolio,
        "summary": summary,
    }


def get_portfolio_summary() -> list[dict[str, Any]]:
    global _portfolio_summary_cache_value, _portfolio_summary_cache_expires_at
    now = time.monotonic()
    if _portfolio_summary_cache_value is not None and now < _portfolio_summary_cache_expires_at:
        return _portfolio_summary_cache_value

    db = MongoData()._db
    portfolios = list(
        db[PORTFOLIO_COLLECTION]
        .find(
            {},
            {
                "_id": 1,
                "strategy_name": 1,
                "name": 1,
                "description": 1,
                "combained_portfilio": 1,
                "created_at": 1,
            },
        )
        .sort("created_at", -1)
    )
    if not portfolios:
        return []

    portfolio_ids = [item.get("_id") for item in portfolios if item.get("_id") is not None]
    investments = list(
        db[INVESTMENT_COLLECTION].find(
            {"strategy_id": {"$in": portfolio_ids}},
            {
                "_id": 0,
                "strategy_id": 1,
                "symbol": 1,
                "entry_price": 1,
                "position_qty": 1,
                "qty": 1,
                "quantity": 1,
                "position_status": 1,
                "exit_price": 1,
                "yesterday_close": 1,
                "ltp": 1,
                "last_price": 1,
                "live_price": 1,
                "cmp": 1,
            },
        )
    )

    investments_by_strategy: dict[ObjectId, list[dict[str, Any]]] = {}
    all_active_symbols: set[str] = set()
    for item in investments:
        strategy_id = item.get("strategy_id")
        if isinstance(strategy_id, ObjectId):
            investments_by_strategy.setdefault(strategy_id, []).append(item)
        if int(item.get("position_status", 1) or 1) == 1:
            symbol = str(item.get("symbol") or "").strip().upper()
            if symbol:
                all_active_symbols.add(symbol)

    prefetched_ltp_map, prefetched_close_map = _load_kite_quotes(sorted(all_active_symbols))
    for symbol, close in _load_latest_closes(sorted(all_active_symbols)).items():
        prefetched_close_map.setdefault(symbol.upper(), close)

    snapshots: list[dict[str, Any]] = []
    for portfolio_doc in portfolios:
        strategy_id = portfolio_doc.get("_id")
        if not isinstance(strategy_id, ObjectId):
            continue
        strategy_investments = investments_by_strategy.get(strategy_id, [])
        if not strategy_investments:
            continue
        snapshots.append(
            _build_portfolio_snapshot(
                portfolio_doc,
                strategy_investments,
                include_investments=False,
                allow_history_fallback=False,
                prefetched_ltp_map=prefetched_ltp_map,
                prefetched_close_map=prefetched_close_map,
            )
        )
    _portfolio_summary_cache_value = snapshots
    _portfolio_summary_cache_expires_at = now + PORTFOLIO_SUMMARY_CACHE_TTL_SECONDS
    return snapshots


def get_portfolio_detail(strategy_id: str) -> dict[str, Any]:
    object_id = _as_object_id(strategy_id)
    db = MongoData()._db
    portfolio_doc = db[PORTFOLIO_COLLECTION].find_one({"_id": object_id})
    if not portfolio_doc:
        raise ValueError("Portfolio not found.")

    investments = list(db[INVESTMENT_COLLECTION].find({"strategy_id": object_id}))
    if not investments:
        raise ValueError("No investments found for this portfolio.")

    return _build_portfolio_snapshot(
        portfolio_doc,
        investments,
        include_investments=True,
        allow_history_fallback=True,
    )


def get_combained_portfolio_detail(strategy_id: str) -> dict[str, Any]:
    object_id = _as_object_id(strategy_id)
    db = MongoData()._db
    portfolio_doc = db[PORTFOLIO_COLLECTION].find_one({"_id": object_id})
    if not portfolio_doc:
        raise ValueError("Portfolio not found.")

    investments = list(db[INVESTMENT_COLLECTION].find({"strategy_id": object_id}))
    if not investments:
        raise ValueError("No investments found for this portfolio.")

    return _build_combained_portfolio_snapshot(portfolio_doc, investments)


def _build_combained_portfolio_snapshot(portfolio_doc: dict[str, Any], investments: list[dict[str, Any]]) -> dict[str, Any]:
    symbols = sorted({str(item.get("symbol") or "").strip() for item in investments if str(item.get("symbol") or "").strip()})
    company_map = _load_company_map(symbols)
    ltp_map, close_map = _build_live_price_map(investments, allow_history_fallback=True)

    active_investment = 0.0
    realized_returns = 0.0
    unrealized_returns = 0.0
    day_returns = 0.0
    active_holdings = 0
    enriched_investments: list[dict[str, Any]] = []

    for item in investments:
        symbol = str(item.get("symbol") or "").strip()
        entry_price = _safe_float(item.get("entry_price"))
        base_qty = int(_safe_float(item.get("position_qty") or item.get("qty") or item.get("quantity"), 0))
        position_status = int(item.get("position_status", 1) or 1)
        primary_qty = int(_safe_float(item.get("primary_investment_qty"), 0))
        secondary_qty = int(_safe_float(item.get("secondary_investment_qty"), 0))
        if primary_qty <= 0 and secondary_qty <= 0:
          primary_qty = base_qty
        exit_price = _safe_float(item.get("exit_price"), entry_price)

        if position_status == 1:
            ltp = _safe_float(ltp_map.get(symbol), entry_price)
            close = _safe_float(item.get("yesterday_close"), close_map.get(symbol, ltp))
            if not item.get("primary_exit_date"):
                qty = primary_qty or 0
                active_investment += entry_price * qty
                unrealized_returns += (ltp - entry_price) * qty
            elif item.get("primary_exit_price") is not None:
                exit_qty = int(_safe_float(item.get("primary_exit_qty"), primary_qty or base_qty))
                realized_returns += (_safe_float(item.get("primary_exit_price"), ltp) - entry_price) * exit_qty

            if secondary_qty > 0:
                if not item.get("secondary_exit_date"):
                    active_investment += entry_price * secondary_qty
                    unrealized_returns += (ltp - entry_price) * secondary_qty
                elif item.get("secondary_exit_price") is not None:
                    exit_qty = int(_safe_float(item.get("secondary_exit_qty"), secondary_qty))
                    realized_returns += (_safe_float(item.get("secondary_exit_price"), ltp) - entry_price) * exit_qty

            day_returns += (ltp - close) * base_qty
            active_holdings += 1
        else:
            ltp = exit_price if exit_price > 0 else entry_price
            close = ltp
            exit_qty = int(_safe_float(item.get("exit_qty"), base_qty))
            realized_returns += (ltp - entry_price) * exit_qty

        overall_pnl_amount = (ltp - entry_price) * base_qty
        overall_pnl_percent = ((ltp - entry_price) / entry_price * 100.0) if entry_price else 0.0
        today_pnl = (ltp - close) * base_qty if position_status == 1 else 0.0
        today_change = ((ltp - close) / close * 100.0) if close and position_status == 1 else 0.0

        investment_row = _serialize_doc(item)
        investment_row.update(
            {
                "_id": str(item.get("_id") or ""),
                "company_name": company_map.get(symbol, symbol or "N/A"),
                "investment_amount": round(entry_price * base_qty, 2),
                "ltp": round(ltp, 2),
                "overall_pnl_amount": round(overall_pnl_amount, 2),
                "overall_pnl_percent": round(overall_pnl_percent, 2),
                "today_pnl": round(today_pnl, 2),
                "today_change": round(today_change, 2),
                "yesterday_close": round(close, 2),
                "exit_type": investment_row.get("indentification"),
                "entry_type": investment_row.get("indentification"),
            }
        )

        if item.get("secondary_exit_date") and not item.get("primary_exit_date"):
            exit_qty = int(_safe_float(item.get("secondary_exit_qty"), secondary_qty))
            secondary_exit_price = _safe_float(item.get("secondary_exit_price"), ltp)
            investment_row.update(
                {
                    "secondary_exit_date": _serialize_value(item.get("secondary_exit_date")),
                    "secondary_exit_price": round(secondary_exit_price, 2),
                    "secondary_exit_qty": exit_qty,
                    "secondary_exit_rank": item.get("secondary_exit_rank"),
                    "exit_type": "Secondary",
                    "exit_date": _serialize_value(item.get("secondary_exit_date")),
                    "exit_price": round(secondary_exit_price, 2),
                    "exit_qty": exit_qty,
                    "exit_overall_pnl_amount": round((secondary_exit_price - entry_price) * exit_qty, 2),
                    "exit_overall_pnl_percent": round(((secondary_exit_price - entry_price) / entry_price * 100.0) if entry_price else 0.0, 2),
                    "entry_type": "Primary" if position_status == 1 else investment_row.get("entry_type"),
                }
            )

        if item.get("primary_exit_date") and not item.get("secondary_exit_date"):
            exit_qty = int(_safe_float(item.get("primary_exit_qty"), primary_qty or base_qty))
            primary_exit_price = _safe_float(item.get("primary_exit_price"), ltp)
            investment_row.update(
                {
                    "primary_exit_date": _serialize_value(item.get("primary_exit_date")),
                    "primary_exit_price": round(primary_exit_price, 2),
                    "primary_exit_qty": exit_qty,
                    "primary_exit_rank": item.get("primary_exit_rank"),
                    "exit_type": "Primary",
                    "exit_date": _serialize_value(item.get("primary_exit_date")),
                    "exit_price": round(primary_exit_price, 2),
                    "exit_qty": exit_qty,
                    "exit_overall_pnl_amount": round((primary_exit_price - entry_price) * exit_qty, 2),
                    "exit_overall_pnl_percent": round(((primary_exit_price - entry_price) / entry_price * 100.0) if entry_price else 0.0, 2),
                    "entry_type": "Secondary" if position_status == 1 and secondary_qty > 0 else investment_row.get("entry_type"),
                }
            )

        if item.get("primary_exit_date") and item.get("secondary_exit_date"):
            exit_qty = int(_safe_float(item.get("exit_qty"), base_qty))
            investment_row.update(
                {
                    "entry_type": "Primary & Secondary",
                    "exit_overall_pnl_amount": round((ltp - entry_price) * exit_qty, 2),
                    "exit_overall_pnl_percent": round(((ltp - entry_price) / entry_price * 100.0) if entry_price else 0.0, 2),
                }
            )

        enriched_investments.append(investment_row)

    total_returns = realized_returns + unrealized_returns
    current_value = active_investment + total_returns
    total_returns_pct = (total_returns / active_investment * 100.0) if active_investment else 0.0
    day_returns_pct = (day_returns / current_value * 100.0) if current_value else 0.0

    serialized_portfolio = _serialize_doc(portfolio_doc)
    return {
        "portfolio_id": str(portfolio_doc.get("_id") or ""),
        "portfolio_name": str(portfolio_doc.get("strategy_name") or portfolio_doc.get("name") or "Unnamed").strip() or "Unnamed",
        "strategy_id": str(portfolio_doc.get("_id") or ""),
        "strategy_name": str(portfolio_doc.get("strategy_name") or portfolio_doc.get("name") or "Unnamed").strip() or "Unnamed",
        "description": str(portfolio_doc.get("description") or "").strip(),
        "total_stocks": len(investments),
        "holdings": active_holdings,
        "current_investment": round(active_investment, 2),
        "investment_value": round(active_investment, 2),
        "current_value": round(current_value, 2),
        "realized_returns_value": round(realized_returns, 2),
        "unrealized_returns_value": round(unrealized_returns, 2),
        "total_returns_value": round(total_returns, 2),
        "total_returns_percent": round(total_returns_pct, 2),
        "day_returns_value": round(day_returns, 2),
        "day_returns_percent": round(day_returns_pct, 2),
        "investments": enriched_investments,
        "formula": serialized_portfolio.get("formula", ""),
        "strategy_index": serialized_portfolio.get("indexes", ""),
        "sector": serialized_portfolio.get("sectors", ""),
        "starting_capital": _safe_float(serialized_portfolio.get("starting_capital")),
        "entry_rank": serialized_portfolio.get("entry_rank", 0),
        "exit_rank": serialized_portfolio.get("exit_rank", 0),
        "rebalance_frequency": serialized_portfolio.get("rebalance_frequency", ""),
        "rebalance_date": serialized_portfolio.get("rebalance_date", ""),
        "entry_rank_1": serialized_portfolio.get("entry_rank_1", 0),
        "exit_rank_1": serialized_portfolio.get("exit_rank_1", 0),
        "rebalance_frequency_1": serialized_portfolio.get("rebalance_frequency_1", ""),
        "rebalance_date_1": serialized_portfolio.get("rebalance_date_1", ""),
        "position_sizing": serialized_portfolio.get("position_sizing", ""),
        "formula_1": serialized_portfolio.get("formula_1", ""),
        "alternative_rebalance_days": serialized_portfolio.get("alternative_rebalance_days", []),
        "uncorrelated_asset_status": bool(serialized_portfolio.get("uncorrelated_asset_status", False)),
        "uncorrelated_asset_allocation": _safe_float(serialized_portfolio.get("uncorrelated_asset_allocation")),
        "uncorrelated_asset_type": serialized_portfolio.get("uncorrelated_asset_type", "gold_bees"),
        "created_at": _serialize_value(portfolio_doc.get("created_at")),
        "timestamp": datetime.utcnow().isoformat(),
        "stocks_ltp": {key: round(_safe_float(value), 2) for key, value in ltp_map.items()},
        "portfolio": serialized_portfolio,
    }


def generate_combained_stock_scores(payload: dict[str, Any]) -> dict[str, Any]:
    first = generate_stock_scores(payload)
    if first.get("status") != "success":
        return first

    second_payload = {
        "index_name": payload.get("index_name_1") or payload.get("index_names_1"),
        "sectors": payload.get("sectors_1"),
        "min_price": payload.get("min_price_1"),
        "max_price": payload.get("max_price_1"),
        "top_n": payload.get("top_n_1", payload.get("top_n", 12)),
        "total_capital": payload.get("total_capital_1", payload.get("total_capital", 1_000_000)),
        "score_date": payload.get("score_date_1", payload.get("score_date")),
        "formula": payload.get("formula_1"),
    }
    second = generate_stock_scores(second_payload)
    if second.get("status") != "success":
        return {
            **first,
            "stocks_scored_1": [],
        }

    response = dict(first)
    response["stocks_scored_1"] = second.get("stocks_scored", [])
    return response


def save_investment_portfolio(invest_stocks: list[dict[str, Any]], strategy_id: str) -> int:
    strategy_oid = _as_object_id(strategy_id)
    db = MongoData()._db
    stock_meta_map = _load_stock_meta_map(
        [str(stock.get("symbol") or "").strip() for stock in invest_stocks if str(stock.get("symbol") or "").strip()]
    )
    investment_docs = []
    for stock in invest_stocks:
        symbol = str(stock.get("symbol") or "").strip()
        stock_meta = stock_meta_map.get(symbol, {})
        resolved_token = (
            stock.get("kite_token")
            or stock.get("symbol_token")
            or stock.get("token")
            or stock.get("tokens")
            or stock.get("instrument_token")
            or stock.get("exchange_token")
            or stock.get("code")
            or stock_meta.get("kite_token")
            or stock_meta.get("symbol_token")
            or stock_meta.get("token")
            or stock_meta.get("tokens")
            or stock_meta.get("instrument_token")
            or stock_meta.get("exchange_token")
            or stock_meta.get("code")
        )
        print(
            "[SCANNER SAVE DEBUG] incoming_stock",
            {
                "symbol": symbol,
                "symbol_token": stock.get("symbol_token"),
                "kite_token": stock.get("kite_token"),
                "token": stock.get("token"),
                "tokens": stock.get("tokens"),
                "instrument_token": stock.get("instrument_token"),
                "exchange_token": stock.get("exchange_token"),
                "code": stock.get("code"),
            },
        )
        print(
            "[SCANNER SAVE DEBUG] resolved_token",
            {
                "symbol": symbol,
                "resolved_token": resolved_token,
                "stock_meta": {
                    "kite_token": stock_meta.get("kite_token"),
                    "symbol_token": stock_meta.get("symbol_token"),
                    "token": stock_meta.get("token"),
                    "tokens": stock_meta.get("tokens"),
                    "instrument_token": stock_meta.get("instrument_token"),
                    "exchange_token": stock_meta.get("exchange_token"),
                    "code": stock_meta.get("code"),
                },
            },
        )
        investment_doc = {
            "entry_price": _safe_float(stock.get("last_price")),
            "position_qty": int(_safe_float(stock.get("qty"), 0)),
            "rank": int(_safe_float(stock.get("rank"), 0)),
            "universe": stock.get("universe"),
            "sector": stock.get("sector"),
            "score": _safe_float(stock.get("score")),
            "symbol": stock.get("symbol"),
            "investment_amount": _safe_float(stock.get("amount")),
            "perf_1M": _safe_float(stock.get("perf_1M")),
            "perf_3M": _safe_float(stock.get("perf_3M")),
            "perf_6M": _safe_float(stock.get("perf_6M")),
            "perf_1Y": _safe_float(stock.get("perf_1Y")),
            "vol_1M": _safe_float(stock.get("vol_1M")),
            "vol_3M": _safe_float(stock.get("vol_3M")),
            "vol_6M": _safe_float(stock.get("vol_6M")),
            "vol_1Y": _safe_float(stock.get("vol_1Y")),
            "entry_date": datetime.utcnow(),
            "strategy_id": strategy_oid,
            "position_status": 1,
            "exited_qty": 0,
            "symbol_type": "equity",
            "kite_token": resolved_token,
            "symbol_token": resolved_token,
            "token": resolved_token,
            "tokens": resolved_token,
            "instrument_token": resolved_token,
            "exchange_token": stock.get("exchange_token") or stock_meta.get("exchange_token") or stock.get("code") or stock_meta.get("code"),
            "indentification": stock.get("indentification"),
            "primary_investment_amount": stock.get("primary_investment_amount"),
            "primary_investment_investment": stock.get("primary_investment_investment"),
            "primary_investment_qty": stock.get("primary_investment_qty"),
            "primary_investment_rank": stock.get("primary_investment_rank"),
            "secondary_investment_amount": stock.get("secondary_investment_amount"),
            "secondary_investment_allocation": stock.get("secondary_investment_allocation"),
            "secondary_investment_qty": stock.get("secondary_investment_qty"),
            "secondary_investment_rank": stock.get("secondary_investment_rank"),
        }
        print("[SCANNER SAVE DEBUG] investment_doc_before_insert", investment_doc)
        investment_docs.append(investment_doc)
    if not investment_docs:
        return 0
    insert_result = db[INVESTMENT_COLLECTION].insert_many(investment_docs)
    inserted_ids = list(insert_result.inserted_ids or [])
    post_insert_updates: list[UpdateOne] = []
    for inserted_id, investment_doc in zip(inserted_ids, investment_docs):
        resolved_token = investment_doc.get("symbol_token") or investment_doc.get("kite_token") or investment_doc.get("token")
        if resolved_token in (None, ""):
            continue
        post_insert_updates.append(
            UpdateOne(
                {"_id": inserted_id},
                {
                    "$set": {
                        "symbol_token": resolved_token,
                        "kite_token": resolved_token,
                        "token": resolved_token,
                        "tokens": resolved_token,
                        "instrument_token": resolved_token,
                    }
                },
            )
        )
    if post_insert_updates:
        db[INVESTMENT_COLLECTION].bulk_write(post_insert_updates, ordered=False)
    _clear_portfolio_summary_cache()
    return len(investment_docs)


def save_portfolio_settings(portfolio_data: dict[str, Any]) -> str:
    if not isinstance(portfolio_data, dict) or not portfolio_data:
        raise ValueError("portfolio_settings is required.")

    db = MongoData()._db
    portfolio_doc = dict(portfolio_data)
    portfolio_doc["created_at"] = datetime.utcnow()
    result = db[PORTFOLIO_COLLECTION].insert_one(portfolio_doc)
    _clear_portfolio_summary_cache()
    return str(result.inserted_id)


def save_portfolio(portfolio_settings: dict[str, Any], invest_stock_data: list[dict[str, Any]]) -> dict[str, Any]:
    if not portfolio_settings:
        raise ValueError("portfolio_settings is required.")
    if not invest_stock_data:
        raise ValueError("invest_stock_data is required.")

    portfolio_strategy_id = save_portfolio_settings(portfolio_settings)
    inserted_count = save_investment_portfolio(invest_stock_data, portfolio_strategy_id)
    return {
        "status": "success",
        "portfolio_strategy_id": portfolio_strategy_id,
        "inserted_investments": inserted_count,
    }


def update_portfolio_investments(portfolio_strategy_id: str, invest_stock_data: list[dict[str, Any]]) -> dict[str, Any]:
    inserted_count = save_investment_portfolio(invest_stock_data, portfolio_strategy_id)
    _clear_portfolio_summary_cache()
    return {
        "status": "success",
        "portfolio_strategy_id": portfolio_strategy_id,
        "inserted_investments": inserted_count,
    }


def get_live_portfolio(strategy_id: str) -> dict[str, Any]:
    return get_portfolio_detail(strategy_id)


def get_live_combained_portfolio(strategy_id: str) -> dict[str, Any]:
    return get_combained_portfolio_detail(strategy_id)


def _update_position_as_exited(investment_id: ObjectId, exit_price: float, exit_qty: int, exit_rank: int = 0) -> bool:
    db = MongoData()._db
    result = db[INVESTMENT_COLLECTION].update_one(
        {"_id": investment_id},
        {
            "$set": {
                "exit_price": round(exit_price, 2),
                "exit_date": datetime.utcnow(),
                "exit_qty": exit_qty,
                "exit_rank": exit_rank,
                "position_status": 2,
            }
        },
    )
    if result.modified_count > 0:
        _clear_portfolio_summary_cache()
    return result.modified_count > 0


def rebalance_portfolio(strategy_id: str) -> dict[str, Any]:
    detail = get_portfolio_detail(strategy_id)
    score_request = {
        "index_name": detail.get("strategy_index"),
        "sectors": detail.get("sector"),
        "min_price": None,
        "max_price": None,
        "top_n": detail.get("entry_rank"),
        "total_capital": detail.get("starting_capital"),
        "score_date": datetime.now().strftime("%Y-%m-%d"),
        "formula": detail.get("formula"),
    }
    score_result = generate_stock_scores(score_request)
    stocks_scored = score_result.get("stocks_scored", [])
    score_map = {str(item.get("symbol")): item for item in stocks_scored}
    active_positions = [row for row in detail.get("investments", []) if int(row.get("position_status", 1) or 1) == 1]

    total_investment_amount = sum(_safe_float(stock.get("ltp")) * int(_safe_float(stock.get("position_qty"), 0)) for stock in active_positions)
    remaining_payment = _safe_float(detail.get("starting_capital")) - total_investment_amount

    exited_stocks: list[dict[str, Any]] = []
    update_status: list[dict[str, Any]] = []
    insert_status: list[dict[str, Any]] = []
    exit_stock_investment_amount = 0.0

    exit_rank_threshold = int(_safe_float(detail.get("exit_rank"), 0))
    entry_rank_threshold = int(_safe_float(detail.get("entry_rank"), 0))
    for stock in active_positions:
        symbol = str(stock.get("symbol") or "").strip()
        current_rank = int(_safe_float((score_map.get(symbol) or {}).get("rank"), 999999))
        if symbol and current_rank > exit_rank_threshold:
            ltp = _safe_float(stock.get("ltp"), _safe_float(stock.get("entry_price")))
            qty = int(_safe_float(stock.get("position_qty"), 0))
            exit_value = qty * ltp
            ok = _update_position_as_exited(_as_object_id(stock.get("_id")), ltp, qty, current_rank)
            update_status.append({"symbol": symbol, "status": "Updated" if ok else "Failed"})
            exited_stocks.append(
                {
                    "symbol": symbol,
                    "exit_price": round(ltp, 2),
                    "exit_date": datetime.utcnow().isoformat(),
                    "exit_qty": qty,
                    "exit_value": round(exit_value, 2),
                }
            )
            exit_stock_investment_amount += exit_value

    remaining_payment += exit_stock_investment_amount
    active_symbols = {str(p.get("symbol") or "").strip() for p in active_positions if int(p.get("position_status", 1) or 1) == 1}
    new_stocks = [row for row in stocks_scored if int(_safe_float(row.get("rank"), 999999)) <= entry_rank_threshold and str(row.get("symbol") or "").strip() not in active_symbols][: len(exited_stocks)]
    capital_per_stock = (remaining_payment / len(new_stocks)) if new_stocks else 0.0
    db = MongoData()._db
    for stock in new_stocks:
        ltp = _safe_float(stock.get("last_price"))
        qty = int(capital_per_stock // ltp) if ltp > 0 else 0
        if qty <= 0:
            continue
        investment_amount = qty * ltp
        db[INVESTMENT_COLLECTION].insert_one(
            {
                "symbol": stock.get("symbol"),
                "entry_price": ltp,
                "entry_date": datetime.utcnow(),
                "strategy_id": _as_object_id(strategy_id),
                "position_status": 1,
                "position_qty": qty,
                "exited_qty": 0,
                "symbol_type": "equity",
                "sector": stock.get("sector"),
                "universe": stock.get("universe"),
                "score": stock.get("score"),
                "rank": stock.get("rank"),
                "investment_amount": round(investment_amount, 2),
            }
        )
        insert_status.append({"symbol": stock.get("symbol"), "status": "Inserted"})
        remaining_payment -= investment_amount
        _clear_portfolio_summary_cache()

    return {
        "total_investment_before": round(total_investment_amount, 2),
        "exit_stock_value": round(exit_stock_investment_amount, 2),
        "remaining_capital_after": round(remaining_payment, 2),
        "exited_count": len(exited_stocks),
        "new_buy_count": len(insert_status),
        "exited_stocks": exited_stocks,
        "update_status": update_status,
        "insert_status": insert_status,
    }


def rebalance_combained_portfolio(strategy_id: str) -> dict[str, Any]:
    return rebalance_portfolio(strategy_id)


def invest_goldbees_strategy(strategy_id: str) -> dict[str, Any]:
    detail = get_portfolio_detail(strategy_id)
    active_positions = [row for row in detail.get("investments", []) if int(row.get("position_status", 1) or 1) == 1]
    exited_stocks: list[dict[str, Any]] = []
    update_status: list[dict[str, Any]] = []
    db = MongoData()._db

    total_investment_amount = sum(_safe_float(stock.get("entry_price")) * int(_safe_float(stock.get("position_qty"), 0)) for stock in active_positions)
    current_investment_amount = sum(_safe_float(stock.get("ltp")) * int(_safe_float(stock.get("position_qty"), 0)) for stock in active_positions)
    current_remaining_amount = _safe_float(detail.get("starting_capital")) - total_investment_amount
    remaining_payment = current_remaining_amount + current_investment_amount

    for stock in active_positions:
        symbol = str(stock.get("symbol") or "").strip()
        ltp = _safe_float(stock.get("ltp"), _safe_float(stock.get("entry_price")))
        qty = int(_safe_float(stock.get("position_qty"), 0))
        ok = _update_position_as_exited(_as_object_id(stock.get("_id")), ltp, qty, 0)
        update_status.append({"symbol": symbol, "status": "Updated" if ok else "Failed"})
        exited_stocks.append(
            {
                "symbol": symbol,
                "exit_price": round(ltp, 2),
                "exit_date": datetime.utcnow().isoformat(),
                "exit_qty": qty,
                "exit_value": round(qty * ltp, 2),
            }
        )

    gold_cash = max(0.0, (remaining_payment - 1000.0) * (_safe_float(detail.get("uncorrelated_asset_allocation")) / 100.0))
    gold_ltp = _build_live_price_map([{"symbol": "GOLDBEES", "position_status": 1}], allow_history_fallback=True)[0].get("GOLDBEES")
    if not gold_ltp:
        gold_ltp = _load_latest_closes(["GOLDBEES"]).get("GOLDBEES", 0.0)
    if not gold_ltp:
        gold_ltp = 125.4
    gold_qty = int(gold_cash // gold_ltp) if gold_ltp > 0 else 0
    investment_amount = gold_qty * gold_ltp
    insert_status: list[dict[str, Any]] = []
    if gold_qty > 0:
        db[INVESTMENT_COLLECTION].insert_one(
            {
                "symbol": "GOLDBEES",
                "entry_price": round(gold_ltp, 2),
                "position_qty": gold_qty,
                "rank": 0,
                "universe": "nifty_500",
                "sector": "Gold",
                "score": 0,
                "investment_amount": round(investment_amount, 2),
                "perf_1M": 0,
                "perf_3M": 0,
                "perf_6M": 0,
                "perf_1Y": 0,
                "vol_1M": 0,
                "vol_3M": 0,
                "vol_6M": 0,
                "vol_1Y": 0,
                "entry_date": datetime.utcnow(),
                "strategy_id": _as_object_id(strategy_id),
                "position_status": 1,
                "exited_qty": 0,
                "symbol_type": "equity",
            }
        )
        insert_status.append({"symbol": "GOLDBEES", "status": "Inserted"})
        _clear_portfolio_summary_cache()

    return {
        "total_investment_before": round(total_investment_amount, 2),
        "exit_stock_value": round(current_investment_amount, 2),
        "remaining_capital_after": round(remaining_payment - investment_amount, 2),
        "exited_count": len(exited_stocks),
        "new_buy_count": len(insert_status),
        "exited_stocks": exited_stocks,
        "update_status": update_status,
        "insert_status": insert_status,
    }


def exit_goldbees_strategy(strategy_id: str) -> dict[str, Any]:
    detail = get_portfolio_detail(strategy_id)
    gold_rows = [
        row
        for row in detail.get("investments", [])
        if int(row.get("position_status", 1) or 1) == 1 and str(row.get("symbol") or "").strip().upper() == "GOLDBEES"
    ]
    if not gold_rows:
        raise ValueError("No active GOLDBEES position found.")

    exited_stocks = []
    update_status = []
    for stock in gold_rows:
        symbol = str(stock.get("symbol") or "").strip()
        ltp = _safe_float(stock.get("ltp"), _safe_float(stock.get("entry_price")))
        qty = int(_safe_float(stock.get("position_qty"), 0))
        ok = _update_position_as_exited(_as_object_id(stock.get("_id")), ltp, qty, 0)
        update_status.append({"symbol": symbol, "status": "Updated" if ok else "Failed"})
        exited_stocks.append(
            {
                "symbol": symbol,
                "exit_price": round(ltp, 2),
                "exit_date": datetime.utcnow().isoformat(),
                "exit_qty": qty,
                "exit_value": round(qty * ltp, 2),
            }
        )

    return {
        "status": "success",
        "exited_count": len(exited_stocks),
        "new_buy_count": 0,
        "exited_stocks": exited_stocks,
        "update_status": update_status,
    }
