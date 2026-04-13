"""
Helpers for shared spot-price lookup and ATM derivation.

This module is intended to be reused across backtest, fast-forward,
execution socket, and live-trade flows.
"""

from __future__ import annotations

from bisect import bisect_right
from typing import Any
from pymongo import DESCENDING

from features.backtest_engine import _resolve_expiry, _resolve_strike

OPTION_CHAIN_COLLECTION = 'option_chain_historical_data'
INDEX_SPOT_COLLECTION = 'option_chain_index_spot'
MARKET_DATA_CACHE: dict[str, dict] = {}


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


def get_cached_spot_doc(market_cache: dict | None, underlying: str, snapshot_ts: str | None = None) -> dict:
    if not market_cache:
        return {}
    normalized_underlying = str(underlying or '').strip().upper()
    if not normalized_underlying:
        return {}
    if not snapshot_ts:
        return (market_cache.get('latest_spot_docs') or {}).get(normalized_underlying) or {}
    return _find_latest_snapshot(
        (market_cache.get('spot_docs') or {}).get(normalized_underlying) or [],
        (market_cache.get('spot_timestamps') or {}).get(normalized_underlying) or [],
        snapshot_ts,
    )


def get_cached_chain_doc(
    market_cache: dict | None,
    underlying: str,
    expiry: str,
    strike: Any,
    option_type: str,
    snapshot_ts: str | None = None,
) -> dict:
    if not market_cache:
        return {}
    key = (
        str(underlying or '').strip().upper(),
        str(expiry or '').strip()[:10],
        safe_float(strike),
        str(option_type or '').strip().upper(),
    )
    if not snapshot_ts:
        return (market_cache.get('latest_chain_docs') or {}).get(key) or {}
    return _find_latest_snapshot(
        (market_cache.get('chain_docs') or {}).get(key) or [],
        (market_cache.get('chain_timestamps') or {}).get(key) or [],
        snapshot_ts,
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
