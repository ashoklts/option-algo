"""
execution_socket.py
────────────────────
WebSocket handler for two modes:

1. BACKTEST SIMULATION  (action: start_listening)
   Countdown-based market replay using option_chain snapshots.

2. LIVE MONITOR  (action: start_live_monitor)
   Ticks every 60 s. Reads running strategies from algo_trades,
   fetches latest option_chain data per open leg, checks SL/TP/entry-time,
   executes exits + re-entries + lazy-leg activations directly on the DB,
   and broadcasts the updated position snapshot via the socket.
"""

import asyncio
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any

from bson import ObjectId
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pymongo import DESCENDING

from features.mongo_data import MongoData
from features.spot_atm_utils import (
    build_entry_spot_snapshots,
    clear_market_data_cache,
    get_cached_chain_doc,
    get_cached_spot_doc,
    preload_market_data_cache,
)

log = logging.getLogger(__name__)

socket_router = APIRouter(prefix='/algo')
MARKET_START_MINUTES = (9 * 60) + 15
MARKET_CLOSE_MINUTES = (15 * 60) + 30   # 15:30 IST
RUNNING_STATUS = 'StrategyStatus.Live_Running'
BACKTEST_IMPORT_STATUS = 'StrategyStatus.Import'
OPEN_LEG_STATUS = 1
CLOSED_LEG_STATUS = 0
LIVE_TICK_SECONDS = 60          # check interval for live mode
ENABLE_BACKTEST_DEBUG_LOGS = False
OPTION_CHAIN_COLLECTION = 'option_chain_historical_data'
SLOW_BACKTEST_WARN_MS = 250
DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
LISTENING_STRATEGY_SNAPSHOTS: dict[str, list[dict[str, Any]]] = {}
LISTENING_TIME_STATE: dict[str, dict[str, Any]] = {}
PENDING_EXECUTE_ORDER_GROUP_STARTS: dict[str, list[dict[str, Any]]] = {}
CONNECTED_CHANNEL_WEBSOCKETS: dict[str, list[WebSocket]] = {
    'executions': [],
    'execute-orders': [],
    'update': [],
}
OVERALL_FEATURE_LEG_ID = '__overall__'


# ─── Time helpers ─────────────────────────────────────────────────────────────

def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _now_iso() -> str:
    return _now_utc().strftime('%Y-%m-%dT%H:%M:%SZ')


def _now_ist_str() -> str:
    """Return current IST time as HH:MM for comparing with entry_time / exit_time."""
    # IST = UTC + 5:30
    now = _now_utc()
    total_minutes = now.hour * 60 + now.minute + 330  # +330 for IST offset
    total_minutes %= 1440
    return f"{total_minutes // 60:02d}:{total_minutes % 60:02d}"


def _today_ist() -> str:
    """Return today's date in IST as YYYY-MM-DD."""
    now = _now_utc()
    # Add 5:30 hours for IST
    total_minutes = now.hour * 60 + now.minute + 330
    extra_days = total_minutes // 1440
    from datetime import timedelta
    d = now.date() + timedelta(days=extra_days)
    return d.strftime('%Y-%m-%d')


def _parse_listen_timestamp(value: Any) -> datetime | None:
    raw_value = str(value or '').strip()
    if not raw_value:
        return None
    normalized_value = raw_value.replace(' ', 'T')
    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M'):
        try:
            return datetime.strptime(normalized_value, fmt)
        except ValueError:
            continue
    return None


def _format_listen_timestamp(value: datetime) -> str:
    return value.strftime('%Y-%m-%dT%H:%M:%S')


def queue_execute_order_group_start(group_id: str, records: list[dict[str, Any]] | None) -> None:
    normalized_group_id = str(group_id or '').strip()
    if not normalized_group_id:
        return
    normalized_records = [dict(item) for item in (records or []) if isinstance(item, dict)]
    if normalized_records:
        PENDING_EXECUTE_ORDER_GROUP_STARTS[normalized_group_id] = normalized_records
        return
    PENDING_EXECUTE_ORDER_GROUP_STARTS.pop(normalized_group_id, None)


def _consume_execute_order_group_start(group_id: str) -> list[dict[str, Any]]:
    normalized_group_id = str(group_id or '').strip()
    if not normalized_group_id:
        return []
    return PENDING_EXECUTE_ORDER_GROUP_STARTS.pop(normalized_group_id, [])


def _consume_all_execute_order_group_starts() -> list[dict[str, Any]]:
    if not PENDING_EXECUTE_ORDER_GROUP_STARTS:
        return []
    records: list[dict[str, Any]] = []
    for group_records in PENDING_EXECUTE_ORDER_GROUP_STARTS.values():
        records.extend([dict(item) for item in (group_records or []) if isinstance(item, dict)])
    PENDING_EXECUTE_ORDER_GROUP_STARTS.clear()
    return records


def _register_channel_websocket(channel: str, websocket: WebSocket) -> None:
    channel_key = str(channel or '').strip()
    if not channel_key:
        return
    bucket = CONNECTED_CHANNEL_WEBSOCKETS.setdefault(channel_key, [])
    if websocket not in bucket:
        bucket.append(websocket)


def _unregister_channel_websocket(channel: str, websocket: WebSocket) -> None:
    channel_key = str(channel or '').strip()
    bucket = CONNECTED_CHANNEL_WEBSOCKETS.get(channel_key) or []
    if websocket in bucket:
        bucket.remove(websocket)


async def _broadcast_channel_message(channel: str, message: str) -> int:
    channel_key = str(channel or '').strip()
    if not channel_key or not message:
        return 0
    sockets = list(CONNECTED_CHANNEL_WEBSOCKETS.get(channel_key) or [])
    delivered = 0
    for socket in sockets:
        try:
            await socket.send_text(message)
            delivered += 1
        except Exception:
            _unregister_channel_websocket(channel_key, socket)
    return delivered


def _build_execute_order_socket_payload(
    records: list[dict[str, Any]] | None,
    *,
    trigger: str,
    group_id: str = '',
) -> dict[str, Any]:
    normalized_records = [dict(item) for item in (records or []) if isinstance(item, dict)]
    resolved_group_id = str(group_id or '').strip()
    if normalized_records:
        first_portfolio = normalized_records[0].get('portfolio') if isinstance(normalized_records[0].get('portfolio'), dict) else {}
        resolved_group_id = str(first_portfolio.get('group_id') or '').strip() or resolved_group_id
    return {
        'group_id': resolved_group_id,
        'records': [
            dict(item, position_refresh_trigger=trigger)
            for item in normalized_records
        ],
    }


def _collect_group_records_for_execute_order(records: list[dict], changed_records: list[dict]) -> list[dict]:
    changed_group_ids = []
    seen_group_ids: set[str] = set()

    for record in changed_records or []:
        group_id = str((((record.get('portfolio') or {}).get('group_id')) or '')).strip()
        if not group_id or group_id in seen_group_ids:
            continue
        seen_group_ids.add(group_id)
        changed_group_ids.append(group_id)

    if not changed_group_ids:
        return [dict(item) for item in (changed_records or []) if isinstance(item, dict)]

    grouped_records: list[dict] = []
    for group_id in changed_group_ids:
        grouped_records.extend([
            dict(record) for record in (records or [])
            if str((((record.get('portfolio') or {}).get('group_id')) or '')).strip() == group_id
        ])
    return grouped_records


def _collect_records_for_group(records: list[dict], group_id: str) -> list[dict]:
    normalized_group_id = str(group_id or '').strip()
    if not normalized_group_id:
        return [dict(record) for record in (records or []) if isinstance(record, dict)]
    return [
        dict(record) for record in (records or [])
        if str((((record.get('portfolio') or {}).get('group_id')) or '')).strip() == normalized_group_id
    ]


# ─── Simulation helpers ───────────────────────────────────────────────────────

def _format_remaining_minutes(total_minutes: int) -> str:
    normalized = max(0, int(total_minutes or 0))
    hours = normalized // 60
    minutes = normalized % 60
    return f'{hours:02d}:{minutes:02d}'


def _format_market_time(remaining_minutes: int) -> str:
    simulated_minutes = max(0, MARKET_START_MINUTES - max(0, int(remaining_minutes or 0)))
    hours = (simulated_minutes // 60) % 24
    minutes = simulated_minutes % 60
    return f'{hours:02d}:{minutes:02d}'


def _format_market_time_from_minutes(total_market_minutes: int) -> str:
    normalized = max(0, int(total_market_minutes or 0))
    hours = (normalized // 60) % 24
    minutes = normalized % 60
    return f'{hours:02d}:{minutes:02d}'


def _build_payload(selected_date: str, remaining_minutes: int, behind_time: int | None = None) -> dict:
    payload = {
        'selected_date': selected_date,
        'remaining_minutes': max(0, int(remaining_minutes or 0)),
        'remaining_time': _format_remaining_minutes(remaining_minutes),
        'market_time': _format_market_time(remaining_minutes),
    }
    if behind_time is not None:
        payload['behind_time'] = max(0, int(behind_time or 0))
    return payload


def _build_message(message_type: str, message: str, data=None) -> str:
    payload = {
        'type': message_type,
        'message': message,
        'server_time': _now_iso(),
    }
    if data is not None:
        payload['data'] = data
    return json.dumps(payload)


def _print_position_refresh_status(
    stage: str,
    *,
    trade_date: str = '',
    activation_mode: str = '',
    reason: str = '',
    count: int | None = None,
) -> None:
    parts = [
        f"stage={str(stage or '').strip() or '-'}",
        f"trade_date={str(trade_date or '').strip() or '-'}",
        f"activation_mode={str(activation_mode or '').strip() or '-'}",
        f"reason={str(reason or '').strip() or '-'}",
    ]
    if count is not None:
        parts.append(f'count={int(count)}')
    print('[POSITION REFRESH STATUS] ' + ' '.join(parts))


def _print_open_position_groups(open_positions: list[dict] | None, *, trade_date: str = '') -> None:
    positions = [item for item in (open_positions or []) if isinstance(item, dict)]
    if not positions:
        print(
            '[OPEN POSITION STATUS] '
            f"trade_date={str(trade_date or '').strip() or '-'} "
            'count=0'
        )
        return
    for item in positions:
        print(
            '[OPEN POSITION STATUS] '
            f"trade_date={str(trade_date or '').strip() or '-'} "
            f"group_name={str(item.get('group_name') or '').strip() or '-'} "
            f"strategy_name={str(item.get('strategy_name') or '').strip() or '-'}"
        )


def _print_running_trade_record_groups(
    records: list[dict] | None,
    *,
    trade_date: str = '',
    activation_mode: str = '',
    listen_time: str = '',
) -> None:
    matched_records = [item for item in (records or []) if isinstance(item, dict)]
    if not matched_records:
        print(
            '[RUNNING TRADE STATUS] '
            f"trade_date={str(trade_date or '').strip() or '-'} "
            f"activation_mode={str(activation_mode or '').strip() or '-'} "
            'count=0'
        )
        return
    listen_hhmm = str(listen_time or '').strip()[:5]
    for item in matched_records:
        entry_time_raw = str(item.get('entry_time') or '').strip()
        entry_time_hhmm = entry_time_raw[11:16] if len(entry_time_raw) >= 16 else entry_time_raw[:5]
        # Determine entry status when listen_time is available
        if listen_hhmm:
            legs = item.get('legs') if isinstance(item.get('legs'), list) else []
            has_entry = any(
                isinstance(leg, dict) and isinstance(leg.get('entry_trade'), dict) and leg.get('entry_trade')
                for leg in legs
            )
            if has_entry:
                entry_status = 'position_taking'
            elif listen_hhmm == entry_time_hhmm:
                entry_status = 'checking_position'
            elif entry_time_hhmm and listen_hhmm < entry_time_hhmm:
                entry_status = 'waiting_for_entry'
            else:
                entry_status = 'pending_entry'
            status_suffix = f' status={entry_status}'
        else:
            status_suffix = ''
        print(
            '[RUNNING TRADE STATUS] '
            f"trade_date={str(trade_date or '').strip() or '-'} "
            f"activation_mode={str(activation_mode or '').strip() or '-'} "
            f"group_name={str(((item.get('portfolio') or {}).get('group_name') or item.get('group_name') or '')).strip() or '-'} "
            f"strategy_name={str(item.get('name') or item.get('strategy_name') or '').strip() or '-'} "
            f"entry_time={entry_time_hhmm or '-'}"
            f"{status_suffix}"
        )


def _print_trade_query_debug(
    query: dict | None,
    *,
    trade_date: str = '',
    activation_mode: str = '',
) -> None:
    print(
        '[RUNNING TRADE QUERY] '
        f"trade_date={str(trade_date or '').strip() or '-'} "
        f"activation_mode={str(activation_mode or '').strip() or '-'} "
        f"query={json.dumps(query or {}, default=str, sort_keys=True)}"
    )


def _print_open_strategy_fetch_flow(
    stage: str,
    *,
    trade_date: str = '',
    activation_mode: str = '',
    count: int | None = None,
) -> None:
    parts = [
        f"stage={str(stage or '').strip() or '-'}",
        f"trade_date={str(trade_date or '').strip() or '-'}",
        f"activation_mode={str(activation_mode or '').strip() or '-'}",
    ]
    if count is not None:
        parts.append(f'count={int(count)}')
    print('[OPEN STRATEGY FLOW] ' + ' '.join(parts))


def _print_first_open_strategy_payload(
    records: list[dict] | None,
    *,
    trade_date: str = '',
    activation_mode: str = '',
) -> None:
    matched_records = [item for item in (records or []) if isinstance(item, dict)]
    if not matched_records:
        print(
            '[FIRST OPEN STRATEGY PAYLOAD] '
            f"trade_date={str(trade_date or '').strip() or '-'} "
            f"activation_mode={str(activation_mode or '').strip() or '-'} "
            'payload=null'
        )
        return
    print(
        '[FIRST OPEN STRATEGY PAYLOAD] '
        f"trade_date={str(trade_date or '').strip() or '-'} "
        f"activation_mode={str(activation_mode or '').strip() or '-'} "
        f"payload={json.dumps(matched_records[0], default=str)}"
    )


def _normalize_expiry_datetime(expiry_value: Any) -> str | None:
    raw = str(expiry_value or '').strip()
    if not raw:
        return None
    if ' ' in raw:
        return raw
    return f'{raw} 15:30:00'


def _backtest_debug_log(message: str) -> None:
    if ENABLE_BACKTEST_DEBUG_LOGS:
        print(message)


def _slow_backtest_log(stage: str, elapsed_ms: float, **context: Any) -> None:
    if elapsed_ms < SLOW_BACKTEST_WARN_MS:
        return
    suffix = ' '.join(f'{key}={value}' for key, value in context.items())
    log.warning('BACKTEST_SLOW stage=%s elapsed_ms=%.2f %s', stage, elapsed_ms, suffix)


# ─── Type helpers ─────────────────────────────────────────────────────────────

def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _is_valid_trade_date(value: Any) -> bool:
    return bool(DATE_RE.fullmatch(str(value or '').strip()))


def _build_listening_snapshot_key(trade_date: str, activation_mode: str | None = None) -> str:
    normalized_date = str(trade_date or '').strip()
    normalized_mode = str(activation_mode or 'algo-backtest').strip() or 'algo-backtest'
    return f'{normalized_mode}::{normalized_date}'


def _set_listening_time_state(
    trade_date: str,
    *,
    activation_mode: str | None = None,
    listen_time: str = '',
    listen_timestamp: str = '',
) -> None:
    if not _is_valid_trade_date(trade_date):
        return
    snapshot_key = _build_listening_snapshot_key(trade_date, activation_mode)
    LISTENING_TIME_STATE[snapshot_key] = {
        'trade_date': trade_date,
        'activation_mode': str(activation_mode or 'algo-backtest').strip() or 'algo-backtest',
        'listen_time': str(listen_time or '').strip(),
        'listen_timestamp': str(listen_timestamp or '').strip(),
    }


def _get_listening_time_state(
    trade_date: str,
    *,
    activation_mode: str | None = None,
) -> dict[str, Any]:
    if not _is_valid_trade_date(trade_date):
        return {}
    snapshot_key = _build_listening_snapshot_key(trade_date, activation_mode)
    state = LISTENING_TIME_STATE.get(snapshot_key) or {}
    return dict(state) if isinstance(state, dict) else {}


def _set_listening_strategy_snapshot(
    trade_date: str,
    records: list[dict] | None,
    *,
    activation_mode: str | None = None,
) -> None:
    if not _is_valid_trade_date(trade_date):
        return
    snapshot_key = _build_listening_snapshot_key(trade_date, activation_mode)
    normalized_records: list[dict[str, Any]] = []
    for record in records or []:
        if isinstance(record, dict):
            normalized_records.append(dict(record))
    LISTENING_STRATEGY_SNAPSHOTS[snapshot_key] = normalized_records


def _get_listening_strategy_snapshot(
    trade_date: str,
    *,
    activation_mode: str | None = None,
) -> list[dict[str, Any]]:
    if not _is_valid_trade_date(trade_date):
        return []
    snapshot_key = _build_listening_snapshot_key(trade_date, activation_mode)
    records = LISTENING_STRATEGY_SNAPSHOTS.get(snapshot_key) or []
    return [dict(record) for record in records if isinstance(record, dict)]


def _clear_listening_strategy_snapshot(
    trade_date: str | None = None,
    *,
    activation_mode: str | None = None,
) -> None:
    if trade_date is None:
        LISTENING_STRATEGY_SNAPSHOTS.clear()
        return
    if not _is_valid_trade_date(trade_date):
        return
    snapshot_key = _build_listening_snapshot_key(trade_date, activation_mode)
    LISTENING_STRATEGY_SNAPSHOTS.pop(snapshot_key, None)
    LISTENING_TIME_STATE.pop(snapshot_key, None)


def _build_trade_query(
    trade_date: str,
    *,
    activation_mode: str | None = None,
    statuses: list[str] | tuple[str, ...] | None = None,
) -> dict:
    query: dict[str, Any] = {'trade_status': 1}
    normalized_date = str(trade_date or '').strip()
    if normalized_date:
        query['creation_ts'] = {'$regex': f'^{re.escape(normalized_date)}'}
    if activation_mode:
        query['activation_mode'] = str(activation_mode).strip()
    normalized_statuses = [str(status or '').strip() for status in (statuses or []) if str(status or '').strip()]
    if len(normalized_statuses) == 1:
        query['status'] = normalized_statuses[0]
    elif normalized_statuses:
        query['status'] = {'$in': normalized_statuses}
    return query


# ─── Token generation ─────────────────────────────────────────────────────────

def make_token(underlying: str, expiry: str, strike: Any, option_type: str) -> str:
    """
    Generate a composite token for an option contract.
    Example: make_token('NIFTY', '2025-11-04', 24500, 'CE')
             → 'NIFTY_2025-11-04_24500_CE'
    """
    strike_str = str(int(float(strike))) if strike is not None else '0'
    return f"{underlying}_{expiry}_{strike_str}_{option_type}"


# ─── Option chain lookup ───────────────────────────────────────────────────────

def get_latest_chain_doc(
    chain_col,
    underlying: str,
    expiry: str,
    strike: Any,
    option_type: str,
    trade_date: str,
    market_cache: dict | None = None,
) -> dict:
    """
    Fetch the latest option_chain record for the given contract on trade_date.
    Returns {} if not found.
    """
    try:
        cached_doc = get_cached_chain_doc(market_cache, underlying, expiry, strike, option_type)
        if cached_doc:
            return cached_doc
        doc = chain_col.find_one(
            {
                'underlying': underlying,
                'expiry': expiry,
                'strike': float(strike) if strike is not None else None,
                'type': option_type,
                'timestamp': {'$regex': f'^{trade_date}'},
            },
            sort=[('timestamp', DESCENDING)],
        )
        return doc or {}
    except Exception as exc:
        log.warning('get_latest_chain_doc error: %s', exc)
        return {}


def get_chain_at_time(
    chain_col,
    underlying: str,
    expiry: str,
    strike: Any,
    option_type: str,
    snapshot_ts: str,
    market_cache: dict | None = None,
) -> dict:
    """
    Fetch the option_chain record closest to snapshot_ts (for simulation mode).
    """
    try:
        cached_doc = get_cached_chain_doc(market_cache, underlying, expiry, strike, option_type, snapshot_ts)
        if cached_doc:
            return cached_doc
        base_query = {
            'underlying': underlying,
            'expiry': expiry,
            'strike': float(strike) if strike is not None else None,
            'type': option_type,
        }
        doc = chain_col.find_one({**base_query, 'timestamp': snapshot_ts})
        if not doc:
            doc = chain_col.find_one(
                {**base_query, 'timestamp': {'$lte': snapshot_ts}},
                sort=[('timestamp', DESCENDING)],
            )
        return doc or {}
    except Exception as exc:
        log.warning('get_chain_at_time error: %s', exc)
        return {}


def get_chain_by_token_at_time(
    chain_col,
    token: str,
    snapshot_ts: str,
) -> dict:
    """
    Fetch the option_chain record for a specific token at snapshot_ts,
    falling back to the latest record at or before that timestamp.
    """
    normalized_token = str(token or '').strip()
    normalized_snapshot_ts = str(snapshot_ts or '').strip()
    if not normalized_token or not normalized_snapshot_ts:
        return {}
    try:
        timestamp_variants = []
        for candidate in [
            normalized_snapshot_ts,
            normalized_snapshot_ts.replace('T', ' ').rstrip('Z'),
            normalized_snapshot_ts.replace(' ', 'T').rstrip('Z'),
        ]:
            cleaned_candidate = str(candidate or '').strip()
            if cleaned_candidate and cleaned_candidate not in timestamp_variants:
                timestamp_variants.append(cleaned_candidate)

        doc = None
        for ts_value in timestamp_variants:
            doc = chain_col.find_one({'token': normalized_token, 'timestamp': ts_value})
            if doc:
                return doc

        if not doc:
            for ts_value in timestamp_variants:
                doc = chain_col.find_one(
                    {'token': normalized_token, 'timestamp': {'$lte': ts_value}},
                    sort=[('timestamp', DESCENDING)],
                )
                if doc:
                    return doc

        if not doc:
            minute_prefixes = []
            for ts_value in timestamp_variants:
                minute_prefix = ts_value[:16]
                if minute_prefix and minute_prefix not in minute_prefixes:
                    minute_prefixes.append(minute_prefix)
            for minute_prefix in minute_prefixes:
                doc = chain_col.find_one(
                    {'token': normalized_token, 'timestamp': {'$regex': '^' + re.escape(minute_prefix)}},
                    sort=[('timestamp', DESCENDING)],
                )
                if doc:
                    return doc
        return doc or {}
    except Exception as exc:
        log.warning('get_chain_by_token_at_time error token=%s ts=%s: %s', normalized_token, normalized_snapshot_ts, exc)
        return {}


def get_index_spot_at_time(
    index_spot_col,
    underlying: str,
    snapshot_ts: str,
    market_cache: dict | None = None,
) -> dict:
    """Fetch the latest instrument spot from option_chain_index_spot up to snapshot_ts."""
    try:
        cached_doc = get_cached_spot_doc(market_cache, underlying, snapshot_ts)
        if cached_doc:
            return cached_doc
        doc = index_spot_col.find_one({'underlying': underlying, 'timestamp': snapshot_ts})
        if not doc:
            doc = index_spot_col.find_one(
                {
                    'underlying': underlying,
                    'timestamp': {'$lte': snapshot_ts},
                },
                sort=[('timestamp', DESCENDING)],
            )
        return doc or {}
    except Exception as exc:
        log.warning('get_index_spot_at_time error: %s', exc)
        return {}


# ─── SL / TP calculations ──────────────────────────────────────────────────────

def _is_sell(position_str: str) -> bool:
    return 'sell' in str(position_str or '').lower()


from features.position_manager import (
    calc_sl_price,
    calc_tp_price,
    is_sl_hit,
    is_tp_hit,
    update_trail_sl,
    get_trail_config,
    get_reentry_sl_config,
    get_reentry_tp_config,
    parse_overall_sl,
    parse_overall_tgt,
    check_overall_sl,
    check_overall_tgt,
    parse_overall_trail_sl,
    update_overall_trail_sl,
    parse_lock_and_trail,
    check_lock_and_trail,
    parse_overall_reentry_sl,
    parse_overall_reentry_tgt,
    build_reentry_action,
    check_leg_exit,
    LockAndTrailConfig,
    ReentryAction,
    LegCheckResult,
)


# ─── DB operations ────────────────────────────────────────────────────────────

def close_leg_in_db(db: MongoData, trade_id: str, leg_index: int,
                    exit_price: float, exit_reason: str, now_ts: str,
                    leg_id: str = '') -> None:
    """Mark a leg as closed in algo_trades and update position history exit_trade."""
    exit_trade_payload = {
        'trigger_timestamp': now_ts,
        'trigger_price': exit_price,
        'price': exit_price,
        'traded_timestamp': now_ts,
        'exchange_timestamp': now_ts,
        'exit_reason': exit_reason,
    }
    try:
        db._db['algo_trades'].update_one(
            {'_id': trade_id},
            {'$set': {
                f'legs.{leg_index}.status': CLOSED_LEG_STATUS,
                f'legs.{leg_index}.exit_reason': exit_reason,
                f'legs.{leg_index}.exit_trade': exit_trade_payload,
                f'legs.{leg_index}.last_saw_price': exit_price,
            }},
        )
    except Exception as exc:
        log.error('close_leg_in_db error trade=%s leg=%d: %s', trade_id, leg_index, exc)
    if leg_id:
        _update_position_history_exit(db, trade_id, leg_id, exit_price, exit_reason, now_ts)


def update_leg_sl_in_db(db: MongoData, trade_id: str, leg_index: int,
                         new_sl: float, last_price: float, leg_id: str = '') -> None:
    """Update running SL fields only when the stoploss level actually changes."""
    try:
        db._db['algo_trades'].update_one(
            {'_id': trade_id},
            {'$set': {
                f'legs.{leg_index}.current_sl_price': new_sl,
                f'legs.{leg_index}.display_sl_value': new_sl,
                f'legs.{leg_index}.last_saw_price': last_price,
            }},
        )
    except Exception as exc:
        log.error('update_leg_sl_in_db error: %s', exc)
    if leg_id:
        try:
            db._db['algo_trade_positions_history'].update_one(
                {'trade_id': trade_id, 'leg_id': leg_id, 'exit_trade': None},
                {'$set': {
                    'current_sl_price': new_sl,
                    'display_sl_value': new_sl,
                    'last_saw_price': last_price,
                }},
            )
        except Exception as exc:
            log.error('update_leg_sl_in_db history sync error trade=%s leg=%s: %s', trade_id, leg_id, exc)


def push_new_leg_in_db(db: MongoData, trade_id: str, new_leg: dict) -> None:
    """Append a new leg entry to algo_trades.legs."""
    try:
        db._db['algo_trades'].update_one(
            {'_id': trade_id},
            {'$push': {'legs': new_leg}},
        )
    except Exception as exc:
        log.error('push_new_leg_in_db error: %s', exc)


def _mark_trade_squared_off_at_exit_time(db: MongoData, trade_id: str) -> None:
    normalized_trade_id = str(trade_id or '').strip()
    if not normalized_trade_id:
        return
    try:
        db._db['algo_trades'].update_one(
            {'_id': normalized_trade_id},
            {'$set': {
                'active_on_server': False,
                'status': 'StrategyStatus.SquaredOff',
                'trade_status': 2,
            }},
        )
    except Exception as exc:
        log.error('_mark_trade_squared_off_at_exit_time error trade=%s: %s', normalized_trade_id, exc)


def _resolve_overall_cycle_value(base_value: float, completed_reentries: int) -> float:
    normalized_base = _safe_float(base_value)
    if normalized_base <= 0:
        return 0.0
    cycle_index = max(0, int(completed_reentries or 0)) + 1
    return round(normalized_base * cycle_index, 2)


def _resolve_next_overall_cycle_value(base_value: float, completed_reentries: int) -> float:
    normalized_base = _safe_float(base_value)
    if normalized_base <= 0:
        return 0.0
    next_cycle_index = max(0, int(completed_reentries or 0)) + 2
    return round(normalized_base * next_cycle_index, 2)


def _set_overall_feature_status_state(
    db: MongoData,
    trade_id: str,
    feature: str,
    *,
    enabled: bool,
    status: str,
    timestamp: str,
    current_mtm: float | None = None,
    disabled_reason: str | None = None,
) -> None:
    normalized_trade_id = str(trade_id or '').strip()
    normalized_feature = str(feature or '').strip()
    if not normalized_trade_id or not normalized_feature:
        return
    update_fields: dict[str, Any] = {
        'enabled': enabled,
        'status': status,
        'updated_at': timestamp,
    }
    if current_mtm is not None:
        update_fields['current_mtm'] = round(_safe_float(current_mtm), 2)
    if not enabled:
        update_fields['disabled_at'] = timestamp
        update_fields['disabled_reason'] = str(disabled_reason or status or 'disabled')
    try:
        db._db['algo_leg_feature_status'].update_many(
            {
                'trade_id': normalized_trade_id,
                'leg_id': OVERALL_FEATURE_LEG_ID,
                'feature': normalized_feature,
                'enabled': True,
            },
            {'$set': update_fields},
        )
    except Exception as exc:
        log.warning('_set_overall_feature_status_state error trade=%s feature=%s: %s', normalized_trade_id, normalized_feature, exc)


def _sync_overall_strategy_feature_status(
    db: MongoData,
    trade: dict,
    timestamp: str,
    *,
    current_mtm: float = 0.0,
    overall_sl_done: int | None = None,
    overall_tgt_done: int | None = None,
) -> None:
    trade_id = str(trade.get('_id') or '').strip()
    if not trade_id:
        return

    strategy_cfg = trade.get('strategy') or trade.get('config') or {}
    osl_type, osl_val = parse_overall_sl(strategy_cfg)
    otgt_type, otgt_val = parse_overall_tgt(strategy_cfg)
    ore_type, ore_count = parse_overall_reentry_sl(strategy_cfg)
    ort_type, ort_count = parse_overall_reentry_tgt(strategy_cfg)
    ore_done = int(overall_sl_done if overall_sl_done is not None else (trade.get('overall_sl_reentry_done') or 0))
    ort_done = int(overall_tgt_done if overall_tgt_done is not None else (trade.get('overall_tgt_reentry_done') or 0))
    meta_strategy_id = str(trade.get('strategy_id') or '')
    meta_strategy_name = str(trade.get('name') or '')
    meta_ticker = str(
        trade.get('ticker')
        or ((trade.get('config') or {}).get('Ticker') or '')
        or ((trade.get('strategy') or {}).get('Ticker') or '')
    )
    today = str(timestamp or '').replace('T', ' ')[:10]
    feature_col = db._db['algo_leg_feature_status']

    def _upsert_row(feature: str, base_type: str, base_value: float, done_count: int, re_type: str, re_count: int) -> None:
        if base_type == 'None' or _safe_float(base_value) <= 0:
            _set_overall_feature_status_state(
                db, trade_id, feature,
                enabled=False,
                status='disabled',
                timestamp=timestamp,
                current_mtm=current_mtm,
                disabled_reason='feature_not_configured',
            )
            return

        cycle_number = max(0, int(done_count or 0)) + 1
        current_value = _resolve_overall_cycle_value(base_value, done_count)
        next_value = _resolve_next_overall_cycle_value(base_value, done_count)
        description_label = 'Overall SL' if feature == 'overall_sl' else 'Overall Target'
        description = (
            f'{description_label} active for cycle {cycle_number}. '
            f'Current threshold: ₹{current_value:.2f}. '
            f'Re-entry type: {re_type or "None"}. '
            f'Re-entry used: {done_count}/{re_count}. '
            f'Next cycle threshold: ₹{next_value:.2f}.'
        )
        query = {
            'trade_id': trade_id,
            'leg_id': OVERALL_FEATURE_LEG_ID,
            'feature': feature,
            'enabled': True,
        }
        update = {
            '$set': {
                'strategy_id': meta_strategy_id,
                'strategy_name': meta_strategy_name,
                'ticker': meta_ticker,
                'trade_date': today,
                'feature': feature,
                'feature_scope': 'strategy',
                'leg_id': OVERALL_FEATURE_LEG_ID,
                'enabled': True,
                'status': 'active',
                'trigger_type': base_type,
                'trigger_value': current_value,
                'base_trigger_value': round(_safe_float(base_value), 2),
                'next_trigger_value': next_value,
                'reentry_type': re_type,
                'reentry_count': int(re_count or 0),
                'reentry_done': int(done_count or 0),
                'cycle_number': cycle_number,
                'current_mtm': round(_safe_float(current_mtm), 2),
                'trigger_description': description,
                'updated_at': timestamp,
                'disabled_at': None,
                'disabled_reason': None,
            },
            '$setOnInsert': {
                'created_at': timestamp,
            },
        }
        try:
            feature_col.update_one(query, update, upsert=True)
        except Exception as exc:
            log.warning('_sync_overall_strategy_feature_status upsert error trade=%s feature=%s: %s', trade_id, feature, exc)

    _upsert_row('overall_sl', osl_type, osl_val, ore_done, ore_type, ore_count)
    _upsert_row('overall_target', otgt_type, otgt_val, ort_done, ort_type, ort_count)


def _disable_trade_feature_rows_for_new_cycle(
    db: MongoData,
    trade_id: str,
    *,
    reason: str,
    timestamp: str,
) -> None:
    normalized_trade_id = str(trade_id or '').strip()
    if not normalized_trade_id:
        return
    feature_col = db._db['algo_leg_feature_status']
    now = str(timestamp or '').strip()
    base_set = {
        'enabled': False,
        'disabled_at': now,
        'disabled_reason': str(reason or 'cycle_completed'),
        'updated_at': now,
    }
    try:
        feature_col.update_many(
            {
                'trade_id': normalized_trade_id,
                'enabled': True,
                'status': {'$in': ['pending', 'active']},
            },
            {'$set': dict(base_set, status='disabled')},
        )
        feature_col.update_many(
            {
                'trade_id': normalized_trade_id,
                'enabled': True,
                'status': {'$nin': ['pending', 'active']},
            },
            {'$set': base_set},
        )
    except Exception as exc:
        log.warning('_disable_trade_feature_rows_for_new_cycle error trade=%s: %s', normalized_trade_id, exc)


def _compute_strategy_mtm_snapshot(
    db: MongoData,
    trade_id: str,
    now_ts: str,
    open_positions: list[dict] | None = None,
) -> tuple[float, list[dict]]:
    normalized_trade_id = str(trade_id or '').strip()
    if not normalized_trade_id:
        return 0.0, []

    open_positions_by_leg_id: dict[str, dict] = {}
    for position in (open_positions or []):
        leg_id = str((position or {}).get('leg_id') or '').strip()
        if leg_id:
            open_positions_by_leg_id[leg_id] = dict(position)

    history_docs = list(
        db._db['algo_trade_positions_history'].find(
            {'trade_id': normalized_trade_id},
            {
                '_id': 1,
                'leg_id': 1,
                'id': 1,
                'position': 1,
                'quantity': 1,
                'lot_size': 1,
                'entry_trade': 1,
                'exit_trade': 1,
                'last_saw_price': 1,
            },
        )
    )

    total_mtm = 0.0
    legs_pnl_snapshot: list[dict] = []
    now_dt = _parse_listen_timestamp(now_ts)

    for doc in history_docs:
        entry_trade = doc.get('entry_trade') if isinstance(doc.get('entry_trade'), dict) else {}
        if not entry_trade:
            continue
        leg_id = str(doc.get('leg_id') or doc.get('id') or doc.get('_id') or '').strip()
        entry_price = _safe_float(entry_trade.get('price') or entry_trade.get('trigger_price'))
        lot_size = _safe_int(doc.get('lot_size'), 1)
        lots = _safe_int(doc.get('quantity') or entry_trade.get('quantity'))
        effective_quantity = max(0, lots) * max(1, lot_size)
        if entry_price <= 0 or effective_quantity <= 0 or not leg_id:
            continue

        is_sell = _is_sell(str(doc.get('position') or ''))
        exit_trade = doc.get('exit_trade') if isinstance(doc.get('exit_trade'), dict) else None
        exit_ts = str((exit_trade or {}).get('traded_timestamp') or (exit_trade or {}).get('trigger_timestamp') or '').strip()
        exit_dt = _parse_listen_timestamp(exit_ts)

        if exit_trade and (not now_dt or not exit_dt or exit_dt <= now_dt):
            exit_price = _safe_float(exit_trade.get('price') or exit_trade.get('trigger_price'))
            pnl = (
                (entry_price - exit_price) * effective_quantity
                if is_sell else
                (exit_price - entry_price) * effective_quantity
            )
        else:
            open_position = open_positions_by_leg_id.get(leg_id) or {}
            current_price = _safe_float(
                open_position.get('current_price')
                or open_position.get('ltp')
                or doc.get('last_saw_price')
            )
            pnl = (
                (entry_price - current_price) * effective_quantity
                if is_sell else
                (current_price - entry_price) * effective_quantity
            )

        rounded_pnl = round(pnl, 2)
        total_mtm += rounded_pnl
        legs_pnl_snapshot.append({'leg_id': leg_id, 'pnl': rounded_pnl, 'quantity': effective_quantity})

    return round(total_mtm, 2), legs_pnl_snapshot


def _is_reentered_leg(leg: dict, trade: dict) -> bool:
    """
    Returns True only for reentry / lazy legs.
    Parent legs (original ListOfLegConfigs entries) return False.
    """
    if leg.get('is_lazy'):
        return True
    if leg.get('triggered_by'):
        return True
    leg_id = str(leg.get('id') or '')
    # Build original parent leg id set: config.LegConfigs first, then strategy.ListOfLegConfigs
    original_leg_configs: dict = ((trade.get('config') or {}).get('LegConfigs') or {})
    if not original_leg_configs:
        strategy = trade.get('strategy') or {}
        list_of_legs = strategy.get('ListOfLegConfigs') or []
        original_leg_configs = {
            str(lc.get('id') or str(i)): lc
            for i, lc in enumerate(list_of_legs)
            if isinstance(lc, dict)
        }
    if leg_id and original_leg_configs and leg_id not in original_leg_configs:
        return True
    return False


def _build_position_history_doc(trade: dict, leg: dict) -> dict | None:
    entry_trade = leg.get('entry_trade') or {}
    entry_timestamp = str(entry_trade.get('traded_timestamp') or entry_trade.get('trigger_timestamp') or '').strip()
    if not entry_timestamp:
        return None
    history_leg = {
        'id': str(leg.get('id') or ''),
        'status': int(leg.get('status') or 0),
        'token': leg.get('token'),
        'symbol': leg.get('symbol'),
        'quantity': _safe_int(leg.get('quantity')),
        'lot_size': _safe_int(leg.get('lot_size') or leg.get('quantity')),
        'position': leg.get('position'),
        'option': leg.get('option'),
        'expiry_date': _normalize_expiry_datetime(leg.get('expiry_date')),
        'strike': leg.get('strike'),
        'last_saw_price': _safe_float(leg.get('last_saw_price')),
        'entry_trade': entry_trade,
        'exit_trade': leg.get('exit_trade') if leg.get('exit_trade') is not None else None,
        'is_reentered_leg': _is_reentered_leg(leg, trade),
        'transactions': leg.get('transactions') or {},
        'current_transaction_id': leg.get('current_transaction_id'),
        'initial_sl_value': leg.get('initial_sl_value', leg.get('current_sl_price')),
        'display_sl_value': leg.get('display_sl_value', leg.get('current_sl_price')),
        'display_target_value': leg.get('display_target_value'),
        'leg_type': str(leg.get('leg_type') or ''),
        'lot_config_value': _safe_int(leg.get('lot_config_value') or 1),
        'momentum_base_price': _safe_float(leg.get('momentum_base_price')) or None,
        'momentum_target_price': _safe_float(leg.get('momentum_target_price')) or None,
        'momentum_reference_set_at': str(leg.get('momentum_reference_set_at') or ''),
        'momentum_triggered_at': str(leg.get('momentum_triggered_at') or ''),
    }
    ticker = str(
        (trade.get('strategy') or {}).get('Ticker')
        or (trade.get('config') or {}).get('Ticker')
        or trade.get('ticker')
        or ''
    )
    return {
        **history_leg,
        'trade_id': str(trade.get('_id') or ''),
        'strategy_id': str(trade.get('strategy_id') or ''),
        'strategy_name': str(trade.get('name') or ''),
        'group_name': str(((trade.get('portfolio') or {}).get('group_name') or '')),
        'ticker': ticker,
        'creation_ts': str(trade.get('creation_ts') or ''),
        'entry_timestamp': entry_timestamp,
        'leg_id': history_leg['id'],
        'history_type': 'position_entry',
        'created_at': _now_iso(),
    }


def _store_position_history(
    db: MongoData, trade: dict, leg: dict,
    override_leg_cfg: dict | None = None,
) -> tuple[bool, dict | None]:
    history_doc = _build_position_history_doc(trade, leg)
    if not history_doc:
        return False, None
    query = {
        'trade_id': history_doc['trade_id'],
        'leg_id': history_doc['leg_id'],
        'entry_timestamp': history_doc['entry_timestamp'],
    }
    history_col = db._db['algo_trade_positions_history']
    existing = history_col.find_one(query, {'_id': 1})
    if existing:
        return False, history_doc
    result = history_col.insert_one(history_doc)
    inserted_id = str(result.inserted_id)
    history_doc['_id'] = inserted_id
    history_doc['id'] = inserted_id
    try:
        history_col.update_one(
            {'_id': result.inserted_id},
            {'$set': {'id': inserted_id}},
        )
    except Exception as exc:
        log.error('_store_position_history id sync error trade=%s leg=%s: %s', history_doc['trade_id'], history_doc['leg_id'], exc)
    # Replace the original dict leg (same leg_id) with the history string _id.
    # This keeps legs as pure string IDs — no mixed dict/string state.
    leg_id_val = history_doc.get('leg_id', '')
    try:
        result_pull = db._db['algo_trades'].update_one(
            {'_id': history_doc['trade_id']},
            {'$pull': {'legs': {'id': leg_id_val}}},
        )
        if result_pull.modified_count == 0:
            # Leg dict not found (already removed or never added) — just push the id
            pass
    except Exception as exc:
        log.error('_store_position_history legs pull error trade=%s leg=%s: %s', history_doc['trade_id'], leg_id_val, exc)
    try:
        db._db['algo_trades'].update_one(
            {'_id': history_doc['trade_id']},
            {'$push': {'legs': inserted_id}},
        )
    except Exception as exc:
        log.error('_store_position_history legs push error trade=%s: %s', history_doc['trade_id'], exc)
    print(
        f"[POSITION ENTRY] trade={history_doc['trade_id']} "
        f"leg={history_doc['leg_id']} "
        f"entry={history_doc['entry_timestamp']} "
        f"history_id={inserted_id}"
    )

    # ── Resolve leg_cfg once — used by both notification calls below ──────────
    resolved_leg_cfg: dict = override_leg_cfg or {}
    if not resolved_leg_cfg:
        try:
            all_leg_cfgs = _resolve_trade_leg_configs(trade)
            resolved_leg_cfg = _resolve_leg_cfg(str(leg.get('id') or ''), leg, all_leg_cfgs)
        except Exception as exc:
            log.warning('_store_position_history leg_cfg lookup error: %s', exc)

    # ── Notification: entry_taken ──────────────────────────────────────────
    try:
        from features.notification_manager import record_entry_taken
        from features.position_manager import parse_overall_sl, parse_overall_tgt
        strategy_cfg = trade.get('strategy') or trade.get('config') or {}
        osl_type, osl_val = parse_overall_sl(strategy_cfg)
        otgt_type, otgt_val = parse_overall_tgt(strategy_cfg)
        record_entry_taken(
            db._db, trade, leg, resolved_leg_cfg,
            timestamp=history_doc.get('entry_timestamp') or inserted_id,
            overall_sl_type=osl_type, overall_sl_value=osl_val,
            overall_tgt_type=otgt_type, overall_tgt_value=otgt_val,
        )
    except Exception as _ne:
        log.warning('notification entry_taken error: %s', _ne)

    # ── Leg feature status: SL / Target / TrailSL records ────────────────────
    try:
        from features.notification_manager import record_leg_features_at_entry
        record_leg_features_at_entry(
            db._db, trade, leg, resolved_leg_cfg,
            timestamp=history_doc.get('entry_timestamp') or inserted_id,
            feature_leg_id=inserted_id,
        )
    except Exception as _lfe:
        log.warning('leg_feature_status entry error: %s', _lfe)

    try:
        from features.notification_manager import upsert_simple_momentum_feature_status
        momentum_cfg = resolved_leg_cfg.get('LegMomentum') or {}
        momentum_type = str(momentum_cfg.get('Type') or 'None')
        momentum_value = _safe_float(momentum_cfg.get('Value'))
        momentum_base_price = _safe_float(leg.get('momentum_base_price'))
        momentum_target_price = _safe_float(leg.get('momentum_target_price'))
        if (
            'None' not in momentum_type
            and momentum_type
            and momentum_value > 0
            and momentum_base_price > 0
            and momentum_target_price > 0
        ):
            feature_leg = dict(leg)
            feature_leg['_id'] = inserted_id
            upsert_simple_momentum_feature_status(
                db._db,
                trade,
                feature_leg,
                resolved_leg_cfg,
                timestamp=history_doc.get('entry_timestamp') or inserted_id,
                base_price=momentum_base_price,
                target_price=momentum_target_price,
                current_price=_safe_float((leg.get('entry_trade') or {}).get('price')),
                status='triggered' if str(leg.get('momentum_triggered_at') or '').strip() else 'pending',
                enabled=False if str(leg.get('momentum_triggered_at') or '').strip() else True,
            )
        print(
            '[ENTRY FEATURE SEED]',
            {
                'trade_id': history_doc.get('trade_id'),
                'leg_id': history_doc.get('leg_id'),
                'history_leg_id': inserted_id,
                'sl_enabled': bool(resolved_leg_cfg.get('LegStopLoss')),
                'target_enabled': bool(resolved_leg_cfg.get('LegTarget')),
                'trail_enabled': bool(resolved_leg_cfg.get('LegTrailSL')),
                'momentum_type': str((resolved_leg_cfg.get('LegMomentum') or {}).get('Type') or 'None'),
            },
        )
    except Exception as _mfe:
        log.warning('simple_momentum feature seed error: %s', _mfe)

    return True, history_doc


def _validate_trade_leg_storage(db: MongoData, trade_id: str) -> bool:
    normalized_trade_id = str(trade_id or '').strip()
    if not normalized_trade_id:
        return False
    trade = db._db['algo_trades'].find_one({'_id': normalized_trade_id}, {'legs': 1}) or {}
    legs = trade.get('legs') if isinstance(trade.get('legs'), list) else []
    invalid_legs = []
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else None
        exit_trade = leg.get('exit_trade') if isinstance(leg.get('exit_trade'), dict) else None
        if entry_trade or exit_trade:
            invalid_legs.append({
                'id': str(leg.get('id') or '').strip(),
                'has_entry_trade': bool(entry_trade),
                'has_exit_trade': bool(exit_trade),
            })
    if invalid_legs:
        print(
            '[LEGS STORAGE ERROR]',
            {
                'trade_id': normalized_trade_id,
                'message': 'Entered/closed legs must be stored as string history ids only',
                'invalid_legs': invalid_legs,
            },
        )
        return False
    return True


def _update_position_history_exit(
    db: MongoData,
    trade_id: str,
    leg_id: str,
    exit_price: float,
    exit_reason: str,
    now_ts: str,
) -> None:
    """Update exit_trade in algo_trade_positions_history when a leg is closed."""
    exit_trade_payload = {
        'trigger_timestamp': now_ts,
        'trigger_price': exit_price,
        'price': exit_price,
        'traded_timestamp': now_ts,
        'exchange_timestamp': now_ts,
        'exit_reason': exit_reason,
    }
    history_matchers = [
        {'leg_id': leg_id},
        {'id': leg_id},
    ]
    try:
        history_matchers.append({'_id': ObjectId(str(leg_id))})
    except Exception:
        pass
    try:
        result = db._db['algo_trade_positions_history'].update_one(
            {'trade_id': trade_id, 'exit_trade': None, '$or': history_matchers},
            {'$set': {
                'exit_trade':     exit_trade_payload,
                'history_type':   'position_closed',
                'status':         2,   # 1=open, 2=closed
                'exit_timestamp': now_ts,
                'last_saw_price': exit_price,
            }},
        )
        if result.modified_count > 0:
            print(
                f"[POSITION EXIT] trade={trade_id} "
                f"leg={leg_id} "
                f"exit={now_ts} "
                f"price={exit_price} "
                f"reason={exit_reason}"
            )
    except Exception as exc:
        log.error('_update_position_history_exit error trade=%s leg=%s: %s', trade_id, leg_id, exc)


def _resolve_chain_market_price(chain_doc: dict | None) -> float:
    if not isinstance(chain_doc, dict):
        return 0.0
    for field_name in ('close', 'ltp', 'current_price', 'price', 'last_price'):
        resolved_price = _safe_float(chain_doc.get(field_name))
        if resolved_price > 0:
            return resolved_price
    return 0.0


def _normalize_chain_market_fields(chain_doc: dict | None) -> dict:
    if not isinstance(chain_doc, dict):
        return {}
    normalized_doc = dict(chain_doc)
    close_price = _safe_float(normalized_doc.get('close'))
    if close_price <= 0:
        return normalized_doc
    for field_name in ('ltp', 'current_price', 'price', 'last_price'):
        if _safe_float(normalized_doc.get(field_name)) <= 0:
            normalized_doc[field_name] = close_price
    return normalized_doc


def _resolve_simple_momentum_target(base_price: float, momentum_type: str, momentum_value: float) -> float:
    base = _safe_float(base_price)
    value = _safe_float(momentum_value)
    if base <= 0 or value <= 0:
        return 0.0
    if 'Percentage' in str(momentum_type or ''):
        delta = base * (value / 100.0)
    else:
        delta = value
    if 'Down' in str(momentum_type or ''):
        return round(base - delta, 2)
    return round(base + delta, 2)


def _is_simple_momentum_triggered(current_price: float, target_price: float, momentum_type: str) -> bool:
    current = _safe_float(current_price)
    target = _safe_float(target_price)
    if current <= 0 or target <= 0:
        return False
    if 'Down' in str(momentum_type or ''):
        return current <= target
    return current >= target


# ─── Re-entry / lazy-leg helpers ──────────────────────────────────────────────

def _build_pending_leg(leg_id: str, leg_config: dict, trade: dict,
                        now_ts: str, triggered_by: str,
                        leg_type: str = '') -> dict:
    """
    Build a new leg dict (status=OPEN_LEG_STATUS, no entry_trade yet) from a leg config.
    Supports both config.LegConfigs (ContractType-nested) and strategy.ListOfLegConfigs (flat) format.
    The next live tick will try to fill the entry price from option_chain.
    """
    contract = leg_config.get('ContractType') or {}

    # option: ContractType.Option OR extract from InstrumentKind ("LegType.CE" → "CE")
    option_raw = contract.get('Option') or ''
    if not option_raw:
        instrument = str(leg_config.get('InstrumentKind') or '')
        option_raw = instrument.split('.')[-1] if '.' in instrument else instrument

    # expiry_kind: ContractType.Expiry OR ExpiryKind
    expiry_kind = contract.get('Expiry') or str(leg_config.get('ExpiryKind') or '')

    # strike_parameter: ContractType.StrikeParameter OR StrikeParameter
    strike_parameter = contract.get('StrikeParameter') or str(leg_config.get('StrikeParameter') or 0)

    # entry_kind: ContractType.EntryKind OR EntryType
    entry_kind = contract.get('EntryKind') or str(leg_config.get('EntryType') or '')

    # lot_config_value: how many lots (multiplier on lot_size)
    lot_config_value = max(1, int((leg_config.get('LotConfig') or {}).get('Value') or 1))

    return {
        'id': leg_id,
        'status': OPEN_LEG_STATUS,
        'position': str(leg_config.get('PositionType') or ''),
        'option': option_raw,
        'expiry_kind': expiry_kind,
        'strike_parameter': strike_parameter,
        'entry_kind': entry_kind,
        'entry_trade': None,
        'quantity': 0,                # will be lot_size * lot_config_value on actual entry
        'lot_config_value': lot_config_value,
        'last_saw_price': 0.0,
        'current_sl_price': None,
        'strike': None,
        'expiry_date': None,
        'token': None,
        'symbol': None,
        'triggered_by': triggered_by,
        'queued_at': now_ts,
        'is_lazy': True,
        'leg_type': leg_type,
        'momentum_base_price': None,
        'momentum_target_price': None,
        'momentum_reference_set_at': None,
        'momentum_triggered_at': None,
        'momentum_armed_notified_at': None,
        'momentum_triggered_notified_at': None,
    }


def _queue_original_legs_if_needed(db: MongoData, trade: dict, now_ts: str) -> tuple[bool, dict]:
    """
    Ensure original parent legs from config exist in algo_trades.legs.
    Supports both config.LegConfigs (dict keyed by id) and strategy.ListOfLegConfigs (list).
    Returns (created_any, refreshed_trade).
    """
    trade_id = str(trade.get('_id') or '')

    # Build leg_configs dict: prefer config.LegConfigs, fallback to strategy.ListOfLegConfigs
    leg_configs: dict = ((trade.get('config') or {}).get('LegConfigs') or {})
    if not leg_configs:
        strategy = trade.get('strategy') or {}
        list_of_legs = strategy.get('ListOfLegConfigs') or []
        leg_configs = {
            str(lc.get('id') or str(i)): lc
            for i, lc in enumerate(list_of_legs)
            if isinstance(lc, dict)
        }

    # Dict legs still present (not yet synced to history)
    existing_ids = {
        str(leg.get('id') or '')
        for leg in (trade.get('legs') or [])
        if isinstance(leg, dict)
    }
    # Also include leg_ids already stored in algo_trade_positions_history for this trade
    # so that socket restarts don't re-queue legs that were already entered
    try:
        history_col = db._db['algo_trade_positions_history']
        history_leg_ids = {
            str(doc.get('leg_id') or '')
            for doc in history_col.find({'trade_id': trade_id}, {'leg_id': 1})
        }
        existing_ids |= history_leg_ids
    except Exception as exc:
        log.warning('_queue_original_legs_if_needed history check error trade=%s: %s', trade_id, exc)

    created_any = False

    for idx, (leg_id, leg_config) in enumerate(leg_configs.items()):
        if leg_id in existing_ids:
            continue
        leg_type = f'leg_{idx + 1}'
        new_leg = _build_pending_leg(str(leg_id), leg_config, trade, now_ts, '', leg_type=leg_type)
        new_leg['is_lazy'] = False
        push_new_leg_in_db(db, trade_id, new_leg)
        print(
            f'[LEG QUEUED] trade={trade_id} leg_id={leg_id} leg_type={leg_type} '
            f'option={new_leg.get("option")} expiry_kind={new_leg.get("expiry_kind")} '
            f'position={new_leg.get("position")} lot_config_value={new_leg.get("lot_config_value")}'
        )
        created_any = True

    if not created_any:
        return False, trade
    refreshed_trade = db._db['algo_trades'].find_one({'_id': trade_id}) or trade
    return True, refreshed_trade


def _get_parent_leg_type(trade: dict, triggered_by: str) -> str:
    """Find the leg_type of the parent leg by its id. Falls back to triggered_by id."""
    for leg in (trade.get('legs') or []):
        if isinstance(leg, dict) and str(leg.get('id') or '') == triggered_by:
            return str(leg.get('leg_type') or triggered_by or '')
    return triggered_by or ''


def _resolve_leg_cfg(leg_id: str, leg: dict, all_leg_configs: dict) -> dict:
    """
    Resolve leg config for SL / Target / TrailSL / NextLeg checks.

    Lazy legs entered via momentum have a composite id such as
    'putLeg1-ttx2772l' while their config is stored under 'putLeg1' in
    IdleLegConfigs.  Standard dict-get misses these, causing tsl_enabled=False
    and NextLeg chaining to silently skip.

    Resolution priority:
      1. Exact id match  (original legs, non-momentum lazy legs)
      2. lazy_leg_ref field  (momentum lazy legs created after the fix was applied)
      3. Longest prefix match  (momentum lazy legs from existing/pre-fix history docs)
    """
    # 1. Exact match
    cfg = all_leg_configs.get(leg_id)
    if cfg:
        return cfg

    # 2. Stored lazy_leg_ref  (set by _process_momentum_pending_feature_legs)
    lazy_ref = str(leg.get('lazy_leg_ref') or '').strip()
    if lazy_ref:
        cfg = all_leg_configs.get(lazy_ref)
        if cfg:
            return cfg

    # 3. Longest-prefix match — handles existing history docs without lazy_leg_ref.
    #    'putLeg1-ttx2772l'.startswith('putLeg1-') → True → use putLeg1 config.
    #    Longest key wins so 'callLeg1Extra' beats 'callLeg1' for 'callLeg1Extra-xyz'.
    best_key = ''
    for k in all_leg_configs:
        if leg_id.startswith(k + '-') and len(k) > len(best_key):
            best_key = k
    if best_key:
        return all_leg_configs[best_key]

    return {}


def _count_child_legs(trade: dict, triggered_by: str, is_lazy: bool) -> int:
    """Count how many reentry or lazy legs already exist for a parent."""
    return sum(
        1 for leg in (trade.get('legs') or [])
        if isinstance(leg, dict)
        and str(leg.get('triggered_by') or '') == triggered_by
        and bool(leg.get('is_lazy')) == is_lazy
    )


def _has_momentum_config(leg_cfg: dict) -> bool:
    """Return True if the leg config has a non-None momentum type with value > 0."""
    mom = leg_cfg.get('LegMomentum') or {}
    mom_type = str(mom.get('Type') or 'None')
    mom_val = _safe_float(mom.get('Value'))
    return 'None' not in mom_type and mom_val > 0


def _build_momentum_feature_leg_id(lazy_ref: str, triggered_by: str) -> str:
    lazy_leg_ref = str(lazy_ref or '').strip()
    parent_leg_id = str(triggered_by or '').strip()
    if lazy_leg_ref and parent_leg_id:
        return f'{lazy_leg_ref}-{parent_leg_id}'
    return lazy_leg_ref or parent_leg_id


def _queue_lazy_momentum_to_feature_status(
    db: MongoData, trade_id: str, lazy_ref: str, lazy_cfg: dict,
    triggered_by: str, leg_type: str, now_ts: str
) -> bool:
    """
    Queue a lazy leg with momentum into algo_leg_feature_status (status='active')
    instead of pushing it directly into the legs array.
    Returns True if successfully queued, False if already exists.
    """
    feature_col = db._db['algo_leg_feature_status']
    queued_leg_id = _build_momentum_feature_leg_id(lazy_ref, triggered_by)
    # Duplicate check — skip if already active for this trade+leg
    try:
        existing = feature_col.find_one({
            'trade_id': trade_id,
            'leg_id': queued_leg_id,
            'feature': 'momentum_pending',
            'status': 'active',
        })
        if existing:
            log.info('momentum_pending already active for trade=%s leg=%s — skipping', trade_id, queued_leg_id)
            return False
    except Exception as exc:
        log.warning('_queue_lazy_momentum_to_feature_status duplicate check error: %s', exc)

    mom = lazy_cfg.get('LegMomentum') or {}
    option_raw = str(lazy_cfg.get('InstrumentKind') or '')
    option_type = option_raw.split('.')[-1] if '.' in option_raw else option_raw

    doc = {
        'trade_id': trade_id,
        'leg_id': queued_leg_id,
        'lazy_leg_ref': lazy_ref,
        'parent_leg_id': triggered_by,
        'feature': 'momentum_pending',
        'enabled': True,
        'status': 'active',
        'option': option_type,
        'expiry_kind': str(lazy_cfg.get('ExpiryKind') or 'ExpiryType.Weekly'),
        'strike_parameter': str(lazy_cfg.get('StrikeParameter') or 'StrikeType.ATM'),
        'entry_kind': str(lazy_cfg.get('EntryType') or 'EntryType.EntryByStrikeType'),
        'position': str(lazy_cfg.get('PositionType') or ''),
        'lot_config_value': int((lazy_cfg.get('LotConfig') or {}).get('Value') or 1),
        'triggered_by': triggered_by,
        'leg_type': leg_type,
        'momentum_type': str(mom.get('Type') or 'None'),
        'momentum_value': _safe_float(mom.get('Value')),
        'momentum_base_price': None,
        'momentum_target_price': None,
        'strike': None,
        'expiry_date': None,
        'token': None,
        'symbol': None,
        'queued_at': now_ts,
        'armed_at': None,
        'triggered_at': None,
    }
    try:
        feature_col.insert_one(doc)
        print(
            f'[MOMENTUM QUEUED] trade={trade_id} leg={queued_leg_id} parent={triggered_by} '
            f'type={doc["momentum_type"]} value={doc["momentum_value"]} option={option_type} leg_type={leg_type}'
        )
        return True
    except Exception as exc:
        log.error('_queue_lazy_momentum_to_feature_status insert error: %s', exc)
        return False


def _process_momentum_pending_feature_legs(
    db: MongoData, trade: dict, chain_col, trade_date: str, now_ts: str,
    lot_size: int, index_spot_doc: dict | None = None, market_cache: dict | None = None
) -> list[str]:
    """
    Every tick: check algo_leg_feature_status for active momentum_pending legs.
    - If momentum target not yet reached: arm (set base/target price) and wait.
    - If momentum triggered: push full entry leg dict into algo_trades.legs.
    Returns list of leg_ids that were entered.
    """
    trade_id = str(trade.get('_id') or '')
    feature_col = db._db['algo_leg_feature_status']
    entered_ids: list[str] = []

    try:
        active_docs = list(feature_col.find({
            'trade_id': trade_id,
            'feature': 'momentum_pending',
            'status': 'active',
        }))
    except Exception as exc:
        log.warning('_process_momentum_pending_feature_legs fetch error trade=%s: %s', trade_id, exc)
        return entered_ids

    if not active_docs:
        return entered_ids

    underlying = str(
        (trade.get('strategy') or {}).get('Ticker')
        or (trade.get('config') or {}).get('Ticker')
        or trade.get('ticker') or ''
    )
    if not underlying:
        return entered_ids

    from features.strike_selector import resolve_expiry, resolve_strike as _resolve_strike_selector

    index_spot_doc = index_spot_doc or {}
    if not index_spot_doc:
        index_spot_col = db._db['option_chain_index_spot']
        index_spot_doc = get_index_spot_at_time(
            index_spot_col, underlying, now_ts, market_cache=market_cache
        )
    spot_price = _safe_float(index_spot_doc.get('spot_price'))

    for feat_doc in active_docs:
        feat_id = str(feat_doc.get('_id') or '')
        leg_id = str(feat_doc.get('leg_id') or '')
        lazy_leg_ref = str(feat_doc.get('lazy_leg_ref') or leg_id or '')
        option_type = str(feat_doc.get('option') or 'CE')
        expiry_kind = str(feat_doc.get('expiry_kind') or 'ExpiryType.Weekly')
        entry_kind = str(feat_doc.get('entry_kind') or '')
        strike_param_raw = feat_doc.get('strike_parameter')
        position_str = str(feat_doc.get('position') or 'PositionType.Sell')
        lot_config_value = max(1, int(feat_doc.get('lot_config_value') or 1))
        momentum_type = str(feat_doc.get('momentum_type') or 'None')
        momentum_value = _safe_float(feat_doc.get('momentum_value'))
        triggered_by = str(feat_doc.get('triggered_by') or '')
        leg_type_str = str(feat_doc.get('leg_type') or '')

        # ── Resolve expiry / strike if not yet done ──────────────────────────
        expiry = str(feat_doc.get('expiry_date') or '')
        strike = feat_doc.get('strike')
        token = str(feat_doc.get('token') or '')
        symbol = str(feat_doc.get('symbol') or '')

        if not expiry or strike in (None, ''):
            try:
                expiry, expiry_err = resolve_expiry(chain_col, underlying, option_type, trade_date, now_ts)
                if expiry_err:
                    print(f'[MOMENTUM PENDING] leg={leg_id} expiry resolve error: {expiry_err}')
                    continue
                sel = _resolve_strike_selector(
                    chain_col=chain_col,
                    underlying=underlying,
                    option_type=option_type,
                    entry_kind=entry_kind,
                    strike_param_raw=strike_param_raw,
                    position=position_str,
                    spot_price=spot_price,
                    expiry=expiry,
                    trade_date=trade_date,
                    snapshot_timestamp=now_ts,
                    market_cache=market_cache,
                    leg_id=leg_id,
                )
                if sel.error:
                    print(f'[MOMENTUM PENDING] leg={leg_id} strike resolve error: {sel.error}')
                    continue
                strike = sel.strike
                resolved_doc = _normalize_chain_market_fields(sel.chain_doc or {})
                token = str(resolved_doc.get('token') or token or make_token(underlying, expiry, strike, option_type))
                symbol = str(resolved_doc.get('symbol') or symbol or token)
                feature_col.update_one(
                    {'_id': feat_doc['_id']},
                    {'$set': {'expiry_date': expiry, 'strike': strike, 'token': token, 'symbol': symbol}},
                )
            except Exception as exc:
                log.warning('momentum_pending strike resolve error leg=%s: %s', leg_id, exc)
                continue

        # ── Get current option price at this specific snapshot timestamp ────────
        try:
            current_chain_doc = _normalize_chain_market_fields(
                get_chain_at_time(chain_col, underlying, expiry, strike, option_type, now_ts, market_cache=market_cache)
            )
        except Exception as exc:
            log.warning('momentum_pending chain fetch error leg=%s: %s', leg_id, exc)
            continue
        current_price = _resolve_chain_market_price(current_chain_doc)

        base_price = _safe_float(feat_doc.get('momentum_base_price'))
        target_price = _safe_float(feat_doc.get('momentum_target_price'))

        # ── Arm: set base/target price on first tick ─────────────────────────
        if base_price <= 0 or target_price <= 0:
            if current_price <= 0:
                print(f'[MOMENTUM PENDING] leg={leg_id} waiting for price data')
                continue
            base_price = current_price
            target_price = _resolve_simple_momentum_target(base_price, momentum_type, momentum_value)
            armed_now = now_ts
            try:
                feature_col.update_one(
                    {'_id': feat_doc['_id']},
                    {'$set': {
                        'momentum_base_price': base_price,
                        'momentum_target_price': target_price,
                        'armed_at': armed_now,
                        'strike': strike,
                        'expiry_date': expiry,
                        'token': token,
                        'symbol': symbol,
                    }},
                )
            except Exception as exc:
                log.warning('momentum_pending arm error leg=%s: %s', leg_id, exc)
            print(
                f'[MOMENTUM ARMED] leg={leg_id} type={momentum_type} value={momentum_value} '
                f'base={base_price} target={target_price} strike={strike} option={option_type}'
            )
            continue

        # ── Check if momentum target is reached ──────────────────────────────
        if not _is_simple_momentum_triggered(current_price, target_price, momentum_type):
            print(
                f'[MOMENTUM WAIT] leg={leg_id} type={momentum_type} value={momentum_value} '
                f'base={base_price} target={target_price} current={current_price} strike={strike}'
            )
            continue

        # ── Momentum triggered — execute entry ───────────────────────────────
        print(
            f'[MOMENTUM OK] leg={leg_id} type={momentum_type} value={momentum_value} '
            f'base={base_price} target={target_price} current={current_price} — entering'
        )
        entry_price = current_price
        is_sell_pos = _is_sell(position_str)
        all_leg_configs = _resolve_trade_leg_configs(trade)
        leg_cfg = all_leg_configs.get(lazy_leg_ref) or all_leg_configs.get(leg_id) or {}
        sl_config = leg_cfg.get('LegStopLoss') or {}
        sl_price = calc_sl_price(entry_price, is_sell_pos, sl_config)
        actual_quantity = lot_config_value

        exchange_ts = now_ts.replace('T', ' ')[:19] if 'T' in now_ts else now_ts[:19]
        entry_trade_payload = {
            'trigger_timestamp': exchange_ts,
            'trigger_price': entry_price,
            'underlying_trigger_price': spot_price,
            'price': entry_price,
            'quantity': actual_quantity,
            'underlying_at_trade': spot_price,
            'traded_timestamp': exchange_ts,
            'exchange_timestamp': exchange_ts,
        }

        new_leg = _build_pending_leg(leg_id, leg_cfg or {
            'PositionType': position_str,
            'InstrumentKind': f'LegType.{option_type}',
            'ExpiryKind': expiry_kind,
            'StrikeParameter': strike_param_raw or 'StrikeType.ATM',
            'EntryType': entry_kind,
            'LotConfig': {'Value': lot_config_value},
        }, trade, now_ts, triggered_by, leg_type=leg_type_str)
        new_leg.update({
            'strike': strike,
            'expiry_date': _normalize_expiry_datetime(expiry),
            'token': token,
            'symbol': symbol,
            'quantity': actual_quantity,
            'lot_size': lot_size,
            'lot_config_value': lot_config_value,
            'current_sl_price': sl_price,
            'initial_sl_value': sl_price,
            'display_sl_value': sl_price,
            'last_saw_price': entry_price,
            'is_lazy': False,
            # Store the original IdleLegConfigs key so SL/Target/TrailSL/NextLeg
            # config can be resolved even when leg_id is a composite key
            # (e.g. 'putLeg1-callLeg1' vs the config key 'putLeg1').
            'lazy_leg_ref': lazy_leg_ref,
            'momentum_base_price': base_price,
            'momentum_target_price': target_price,
            'momentum_reference_set_at': str(feat_doc.get('armed_at') or feat_doc.get('queued_at') or now_ts),
            'momentum_triggered_at': now_ts,
            'entry_trade': entry_trade_payload,
            'exit_trade': None,
            'spot_at_queue': _safe_float(feat_doc.get('spot_at_queue')),
        })

        try:
            db._db['algo_trades'].update_one(
                {'_id': trade_id},
                {'$push': {'legs': new_leg}},
            )
            feature_col.update_one(
                {'_id': feat_doc['_id']},
                {'$set': {'status': 'triggered', 'triggered_at': now_ts}},
            )
            print(
                f'[MOMENTUM ENTRY] trade={trade_id} leg={leg_id} entry_price={entry_price} '
                f'sl={sl_price} strike={strike} option={option_type}'
            )
            # Store position history — pass leg_cfg explicitly so SL/TrailSL
            # feature records are created correctly for momentum idle legs.
            refreshed_trade = db._db['algo_trades'].find_one({'_id': trade_id}) or trade
            refreshed_legs = [l for l in (refreshed_trade.get('legs') or []) if isinstance(l, dict)]
            entered_leg = next((l for l in refreshed_legs if str(l.get('id') or '') == leg_id), None)
            if entered_leg:
                _store_position_history(db, refreshed_trade, entered_leg, override_leg_cfg=leg_cfg)
            entered_ids.append(leg_id)
        except Exception as exc:
            log.error('momentum_pending entry error leg=%s: %s', leg_id, exc)

    return entered_ids


def _handle_reentry(db: MongoData, trade: dict, leg_config: dict,
                     reentry_config: dict, triggered_by: str, now_ts: str) -> str | None:
    """
    Process a re-entry or lazy-leg trigger.
    Returns a string describing what was queued, or None.
    """
    if not reentry_config:
        return None

    reentry_type = str(reentry_config.get('Type') or '')
    reentry_value = reentry_config.get('Value')

    trade_id = str(trade.get('_id') or '')

    # IdleLegConfigs: prefer strategy.IdleLegConfigs, fallback to config.IdleLegConfigs
    strategy = trade.get('strategy') or {}
    idle_configs = strategy.get('IdleLegConfigs') or {}
    if not idle_configs:
        idle_configs = (trade.get('config') or {}).get('IdleLegConfigs') or {}

    parent_leg_type = _get_parent_leg_type(trade, triggered_by)

    # ── NextLeg → activate an idle (lazy) leg ─────────────────────────────────
    if 'NextLeg' in reentry_type:
        lazy_ref = str((reentry_value or {}).get('NextLegRef') or reentry_value or '')
        lazy_cfg = idle_configs.get(lazy_ref)
        if not lazy_cfg:
            log.warning('Lazy leg ref %s not found in IdleLegConfigs for trade %s', lazy_ref, trade_id)
            return None
        existing_ids = {str(l.get('id') or '') for l in (trade.get('legs') or []) if isinstance(l, dict)}
        if lazy_ref in existing_ids:
            log.info('Lazy leg %s already exists for trade %s — skipping', lazy_ref, trade_id)
            return None
        lazy_count = _count_child_legs(trade, triggered_by, is_lazy=True)
        leg_type = f'{parent_leg_type}-lazyleg_{lazy_count + 1}' if parent_leg_type else f'lazyleg_{lazy_count + 1}'
        # If lazy leg has momentum, queue it to algo_leg_feature_status instead of legs array
        if _has_momentum_config(lazy_cfg):
            queued = _queue_lazy_momentum_to_feature_status(
                db, trade_id, lazy_ref, lazy_cfg, triggered_by, leg_type, now_ts
            )
            if queued:
                return f'LazyLeg:{lazy_ref} momentum_pending queued to feature_status leg_type={leg_type}'
            return None
        new_leg = _build_pending_leg(lazy_ref, lazy_cfg, trade, now_ts, triggered_by, leg_type=leg_type)
        push_new_leg_in_db(db, trade_id, new_leg)
        return f'LazyLeg:{lazy_ref} queued leg_type={leg_type}'

    # ── Immediate → re-enter same leg ASAP ────────────────────────────────────
    if 'Immediate' in reentry_type:
        count = _safe_int((reentry_value or {}).get('ReentryCount') if isinstance(reentry_value, dict) else reentry_value)
        if count <= 0:
            return None
        orig_id = str(leg_config.get('id') or triggered_by)
        reentry_id = f'{orig_id}_re_{now_ts.replace(":", "").replace("T", "")}'
        reentry_count = _count_child_legs(trade, triggered_by, is_lazy=False)
        leg_type = f'{parent_leg_type}-reentry_{reentry_count + 1}' if parent_leg_type else f'reentry_{reentry_count + 1}'
        new_leg = _build_pending_leg(reentry_id, leg_config, trade, now_ts, triggered_by, leg_type=leg_type)
        new_leg['reentry_count_remaining'] = count - 1
        new_leg['reentry_type'] = 'Immediate'
        push_new_leg_in_db(db, trade_id, new_leg)
        return f'Reentry:Immediate queued ({count}x) leg_type={leg_type}'

    # ── AtCost → wait for price to come back to entry cost ────────────────────
    if 'AtCost' in reentry_type:
        orig_id = str(leg_config.get('id') or triggered_by)
        reentry_id = f'{orig_id}_re_{now_ts.replace(":", "").replace("T", "")}'
        reentry_count = _count_child_legs(trade, triggered_by, is_lazy=False)
        leg_type = f'{parent_leg_type}-reentry_{reentry_count + 1}' if parent_leg_type else f'reentry_{reentry_count + 1}'
        new_leg = _build_pending_leg(reentry_id, leg_config, trade, now_ts, triggered_by, leg_type=leg_type)
        new_leg['reentry_type'] = 'AtCost'
        new_leg['reentry_count_remaining'] = _safe_int(reentry_value)
        push_new_leg_in_db(db, trade_id, new_leg)
        return f'Reentry:AtCost queued leg_type={leg_type}'

    # ── LikeOriginal (Momentum) ────────────────────────────────────────────────
    if 'LikeOriginal' in reentry_type:
        count = _safe_int(reentry_value)
        if count <= 0:
            return None
        orig_id = str(leg_config.get('id') or triggered_by)
        reentry_id = f'{orig_id}_re_{now_ts.replace(":", "").replace("T", "")}'
        reentry_count = _count_child_legs(trade, triggered_by, is_lazy=False)
        leg_type = f'{parent_leg_type}-reentry_{reentry_count + 1}' if parent_leg_type else f'reentry_{reentry_count + 1}'
        new_leg = _build_pending_leg(reentry_id, leg_config, trade, now_ts, triggered_by, leg_type=leg_type)
        new_leg['reentry_type'] = 'LikeOriginal'
        new_leg['reentry_count_remaining'] = count - 1
        push_new_leg_in_db(db, trade_id, new_leg)
        return f'Reentry:LikeOriginal queued leg_type={leg_type}'

    return None


# ─── Entry resolver for pending legs ──────────────────────────────────────────

def _try_enter_pending_leg(db: MongoData, trade: dict, leg: dict,
                            leg_index: int, chain_col, trade_date: str,
                            now_ts: str, lot_size: int,
                            snapshot_timestamp: str | None = None,
                            index_spot_doc: dict | None = None,
                            market_cache: dict | None = None) -> tuple[bool, str | None]:
    """
    Try to fill a pending (is_lazy=True, entry_trade=None) leg.
    Resolves strike/expiry from option_chain ATM and sets entry_trade.
    Returns True if entered.
    """
    underlying = str(
        (trade.get('strategy') or {}).get('Ticker')
        or (trade.get('config') or {}).get('Ticker')
        or trade.get('ticker')
        or ''
    )
    option_type = str(leg.get('option') or 'CE')
    entry_kind = str(leg.get('entry_kind') or '')
    leg_id_log = str(leg.get('id') or '')
    strike_param_raw = leg.get('strike_parameter')

    if not underlying:
        print(f'[ENTRY MISS] leg={leg_id_log} reason=no_underlying trade_id={trade.get("_id")}')
        return False, 'no_underlying'

    index_spot_doc = index_spot_doc or {}
    if not index_spot_doc:
        index_spot_col = db._db['option_chain_index_spot']
        index_spot_doc = get_index_spot_at_time(
            index_spot_col,
            underlying,
            snapshot_timestamp or now_ts,
            market_cache=market_cache,
        )

    spot_price = _safe_float(index_spot_doc.get('spot_price'))
    print(f'[STRIKE CALC] leg={leg_id_log} type={option_type} spot_price={spot_price}')

    if spot_price <= 0:
        print(f'[ENTRY MISS] leg={leg_id_log} underlying={underlying} snapshot={snapshot_timestamp} reason=spot_price_missing_or_zero')
        return False, 'spot_missing'

    # ── Resolve leg config early (needed for momentum gate) ──────────────────
    _leg_id_str_early = str(leg.get('id') or '')
    _all_leg_cfgs_early = _resolve_trade_leg_configs(trade)
    _leg_cfg_early = _resolve_leg_cfg(_leg_id_str_early, leg, _all_leg_cfgs_early)

    # ── LegMomentum gate ─────────────────────────────────────────────────────
    _momentum_cfg   = _leg_cfg_early.get('LegMomentum') or {}
    _momentum_type  = str(_momentum_cfg.get('Type') or 'None')
    _momentum_value = _safe_float(_momentum_cfg.get('Value'))
    # Resolve expiry / strike selector early so momentum can be armed on a fixed contract.
    from features.strike_selector import resolve_expiry, resolve_strike as _resolve_strike_selector
    expiry = str(leg.get('expiry_date') or '')
    strike = leg.get('strike')
    token = str(leg.get('token') or '')
    symbol = str(leg.get('symbol') or '')
    resolved_chain_doc: dict = {}

    if not expiry or strike in (None, ''):
        expiry, expiry_err = resolve_expiry(chain_col, underlying, option_type, trade_date, snapshot_timestamp)
        if expiry_err:
            print(f'[ENTRY MISS] leg={leg_id_log} underlying={underlying} option={option_type} trade_date={trade_date} reason={expiry_err}')
            return False, expiry_err
        print(f'[STRIKE CALC] leg={leg_id_log} type={option_type} expiry_resolved={expiry}')

        sel = _resolve_strike_selector(
            chain_col=chain_col,
            underlying=underlying,
            option_type=option_type,
            entry_kind=entry_kind,
            strike_param_raw=strike_param_raw,
            position=str(leg.get('position') or ''),
            spot_price=spot_price,
            expiry=expiry,
            trade_date=trade_date,
            snapshot_timestamp=snapshot_timestamp,
            market_cache=market_cache,
            leg_id=leg_id_log,
        )
        if sel.error:
            print(f'[ENTRY MISS] leg={leg_id_log} underlying={underlying} option={option_type} expiry={expiry} reason={sel.error}')
            return False, sel.error
        strike = sel.strike
        resolved_chain_doc = _normalize_chain_market_fields(sel.chain_doc or {})
        token = str(resolved_chain_doc.get('token') or token or make_token(underlying, expiry, strike, option_type))
        symbol = str(resolved_chain_doc.get('symbol') or symbol or token)
    else:
        expiry = expiry[:10] if ' ' in expiry else expiry

    if 'None' not in _momentum_type and _momentum_value > 0:
        current_chain_doc = resolved_chain_doc
        if not current_chain_doc:
            if snapshot_timestamp:
                current_chain_doc = _normalize_chain_market_fields(
                    get_chain_at_time(chain_col, underlying, expiry, strike, option_type, snapshot_timestamp, market_cache=market_cache)
                )
            else:
                current_chain_doc = _normalize_chain_market_fields(
                    get_latest_chain_doc(chain_col, underlying, expiry, strike, option_type, trade_date)
                )

        base_price = _safe_float(leg.get('momentum_base_price'))
        target_price = _safe_float(leg.get('momentum_target_price'))
        current_option_price = _resolve_chain_market_price(current_chain_doc)
        token = str(current_chain_doc.get('token') or token or make_token(underlying, expiry, strike, option_type))
        symbol = str(current_chain_doc.get('symbol') or symbol or token)

        if base_price <= 0 or target_price <= 0:
            if current_option_price <= 0:
                print(f'[MOMENTUM WAIT] leg={leg_id_log} type={_momentum_type} reason=base_price_missing')
                return False, 'momentum_wait'
            base_price = current_option_price
            target_price = _resolve_simple_momentum_target(base_price, _momentum_type, _momentum_value)
            try:
                db._db['algo_trades'].update_one(
                    {'_id': str(trade.get('_id') or '')},
                    {'$set': {
                        'legs.$[elem].strike': strike,
                        'legs.$[elem].expiry_date': _normalize_expiry_datetime(expiry),
                        'legs.$[elem].token': token,
                        'legs.$[elem].symbol': symbol,
                        'legs.$[elem].spot_at_queue': spot_price,
                        'legs.$[elem].momentum_base_price': base_price,
                        'legs.$[elem].momentum_target_price': target_price,
                        'legs.$[elem].momentum_reference_set_at': now_ts,
                        'legs.$[elem].momentum_armed_notified_at': now_ts,
                    }},
                    array_filters=[{'elem.id': leg_id_log}],
                )
                leg['strike'] = strike
                leg['expiry_date'] = _normalize_expiry_datetime(expiry)
                leg['token'] = token
                leg['symbol'] = symbol
                leg['spot_at_queue'] = spot_price
                leg['momentum_base_price'] = base_price
                leg['momentum_target_price'] = target_price
                leg['momentum_reference_set_at'] = now_ts
                leg['momentum_armed_notified_at'] = now_ts
            except Exception as exc:
                log.warning('momentum arm state save error leg=%s: %s', leg_id_log, exc)
            try:
                from features.notification_manager import record_simple_momentum_armed
                record_simple_momentum_armed(
                    db._db, trade, leg, _leg_cfg_early, now_ts,
                    base_price=base_price, target_price=target_price, spot_price=spot_price,
                )
            except Exception as exc:
                log.warning('simple momentum armed notification error leg=%s: %s', leg_id_log, exc)
            print(
                f'[MOMENTUM ARMED] leg={leg_id_log} type={_momentum_type} '
                f'value={_momentum_value} base={base_price} target={target_price} '
                f'strike={strike} option={option_type}'
            )
            return False, 'momentum_wait'

        if not _is_simple_momentum_triggered(current_option_price, target_price, _momentum_type):
            print(
                f'[MOMENTUM WAIT] leg={leg_id_log} type={_momentum_type} '
                f'value={_momentum_value} base={base_price} target={target_price} '
                f'current={current_option_price} strike={strike}'
            )
            return False, 'momentum_wait'

        print(
            f'[MOMENTUM OK] leg={leg_id_log} type={_momentum_type} value={_momentum_value} '
            f'base={base_price} target={target_price} current={current_option_price} — proceeding to entry'
        )
        entry_price = current_option_price
        resolved_chain_doc = current_chain_doc
        try:
            if not str(leg.get('momentum_triggered_notified_at') or '').strip():
                from features.notification_manager import record_simple_momentum_triggered
                record_simple_momentum_triggered(
                    db._db, trade, leg, _leg_cfg_early, now_ts,
                    base_price=base_price,
                    target_price=target_price,
                    current_price=current_option_price,
                    spot_price=spot_price,
                )
            db._db['algo_trades'].update_one(
                {'_id': str(trade.get('_id') or '')},
                {'$set': {
                    'legs.$[elem].momentum_triggered_at': now_ts,
                    'legs.$[elem].momentum_triggered_notified_at': now_ts,
                }},
                array_filters=[{'elem.id': leg_id_log}],
            )
            leg['momentum_triggered_at'] = now_ts
            leg['momentum_triggered_notified_at'] = now_ts
        except Exception as exc:
            log.warning('simple momentum trigger state error leg=%s: %s', leg_id_log, exc)
    else:
        # Resolve strike on the current tick when no simple momentum is configured.
        if not resolved_chain_doc:
            expiry, expiry_err = resolve_expiry(chain_col, underlying, option_type, trade_date, snapshot_timestamp)
            if expiry_err:
                print(f'[ENTRY MISS] leg={leg_id_log} underlying={underlying} option={option_type} trade_date={trade_date} reason={expiry_err}')
                return False, expiry_err
            print(f'[STRIKE CALC] leg={leg_id_log} type={option_type} expiry_resolved={expiry}')

            sel = _resolve_strike_selector(
                chain_col=chain_col,
                underlying=underlying,
                option_type=option_type,
                entry_kind=entry_kind,
                strike_param_raw=strike_param_raw,
                position=str(leg.get('position') or ''),
                spot_price=spot_price,
                expiry=expiry,
                trade_date=trade_date,
                snapshot_timestamp=snapshot_timestamp,
                market_cache=market_cache,
                leg_id=leg_id_log,
            )
            if sel.error:
                print(f'[ENTRY MISS] leg={leg_id_log} underlying={underlying} option={option_type} expiry={expiry} reason={sel.error}')
                return False, sel.error
            strike = sel.strike
            entry_price = sel.entry_price
            resolved_chain_doc = _normalize_chain_market_fields(sel.chain_doc or {})
            token = str(resolved_chain_doc.get('token') or token or make_token(underlying, expiry, strike, option_type))
            symbol = str(resolved_chain_doc.get('symbol') or symbol or token)
        else:
            entry_price = _resolve_chain_market_price(resolved_chain_doc)
    spot_price = _safe_float(index_spot_doc.get('spot_price'))
    position_str = str(leg.get('position') or 'PositionType.Sell')
    is_sell_pos = _is_sell(position_str)

    # Compute SL price from config — check strategy.ListOfLegConfigs + IdleLegConfigs first
    leg_id_str = str(leg.get('id') or '')
    all_leg_configs = _resolve_trade_leg_configs(trade)
    leg_cfg = _resolve_leg_cfg(leg_id_str, leg, all_leg_configs)
    sl_config = leg_cfg.get('LegStopLoss') or {}
    sl_price = calc_sl_price(entry_price, is_sell_pos, sl_config)

    # quantity = LotConfig.Value directly (not multiplied by lot_size)
    lot_config_value = max(1, int(
        (leg_cfg.get('LotConfig') or {}).get('Value')
        or leg.get('lot_config_value')
        or 1
    ))
    actual_quantity = lot_config_value

    trade_date_str = str(trade_date or '')
    exchange_ts = now_ts.replace('T', ' ')[:19] if 'T' in now_ts else now_ts[:19]
    entry_trade_payload = {
        'trigger_timestamp': exchange_ts,
        'trigger_price': entry_price,
        'underlying_trigger_price': spot_price,
        'price': entry_price,
        'quantity': actual_quantity,
        'underlying_at_trade': spot_price,
        'traded_timestamp': exchange_ts,
        'exchange_timestamp': exchange_ts,
    }
    _backtest_debug_log(
        f"[BACKTEST ENTRY SPOT] timestamp={exchange_ts} underlying={underlying} "
        f"spot_price={spot_price} atm_strike={strike} entry_price={entry_price} "
        f"lot_config_value={lot_config_value} actual_quantity={actual_quantity}"
    )
    leg_is_reentered = _is_reentered_leg(leg, trade)

    try:
        db._db['algo_trades'].update_one(
            {'_id': str(trade.get('_id') or '')},
            {'$set': {
                'legs.$[elem].strike': strike,
                'legs.$[elem].expiry_date': _normalize_expiry_datetime(expiry),
                'legs.$[elem].token': token,
                'legs.$[elem].symbol': symbol,
                'legs.$[elem].quantity': actual_quantity,
                'legs.$[elem].lot_size': lot_size,
                'legs.$[elem].lot_config_value': lot_config_value,
                'legs.$[elem].current_sl_price': sl_price,
                'legs.$[elem].initial_sl_value': sl_price,
                'legs.$[elem].display_sl_value': sl_price,
                'legs.$[elem].display_target_value': leg.get('display_target_value'),
                'legs.$[elem].last_saw_price': entry_price,
                'legs.$[elem].is_lazy': False,
                'legs.$[elem].is_reentered_leg': leg_is_reentered,
                'legs.$[elem].momentum_base_price': _safe_float(leg.get('momentum_base_price')) or None,
                'legs.$[elem].momentum_target_price': _safe_float(leg.get('momentum_target_price')) or None,
                'legs.$[elem].momentum_reference_set_at': leg.get('momentum_reference_set_at'),
                'legs.$[elem].momentum_triggered_at': leg.get('momentum_triggered_at') or now_ts,
                'legs.$[elem].momentum_triggered_notified_at': leg.get('momentum_triggered_notified_at'),
                'legs.$[elem].transactions': leg.get('transactions') or {},
                'legs.$[elem].current_transaction_id': leg.get('current_transaction_id'),
                'legs.$[elem].exit_trade': None,
                'legs.$[elem].entry_trade': entry_trade_payload,
            }},
            array_filters=[{'elem.id': leg_id_str}],
        )
        refreshed_trade = db._db['algo_trades'].find_one({'_id': str(trade.get('_id') or '')}) or trade
        refreshed_legs = [item for item in (refreshed_trade.get('legs') or []) if isinstance(item, dict)]
        refreshed_leg = next((item for item in refreshed_legs if str(item.get('id') or '') == leg_id_str), None)
        if refreshed_leg:
            inserted, history_doc = _store_position_history(db, refreshed_trade, refreshed_leg)
            if inserted and history_doc:
                print(
                    f"[LAZY LEG ENTRY SYNC] trade={history_doc['trade_id']} "
                    f"leg={history_doc['leg_id']} "
                    f"history_id={history_doc.get('_id') or '-'}"
                )
        return True, None
    except Exception as exc:
        log.error('_try_enter_pending_leg error: %s', exc)
        return False, None


# ─── Core: simulate-mode position extractor ───────────────────────────────────

def _build_snapshot_timestamp(selected_date: str, market_time: str) -> str:
    return f'{selected_date}T{market_time}:00'


def _extract_hhmm(raw_time: str) -> str:
    raw_value = str(raw_time or '').strip()
    if len(raw_value) >= 16:
        return raw_value[11:16]
    return raw_value[:5]


def _serialize_trade_record(item: dict) -> dict:
    return {
        '_id': str(item.get('_id') or ''),
        'strategy_id': str(item.get('strategy_id') or ''),
        'name': str(item.get('name') or ''),
        'status': str(item.get('status') or ''),
        'trade_status': _safe_int(item.get('trade_status')),
        'active_on_server': bool(item.get('active_on_server')),
        'activation_mode': str(item.get('activation_mode') or ''),
        'broker_type': str(item.get('broker_type') or ''),
        'ticker': str(item.get('ticker') or ((item.get('config') or {}).get('Ticker') or '')),
        'legs': item.get('legs') if isinstance(item.get('legs'), list) else [],
        'creation_ts': str(item.get('creation_ts') or ''),
        'entry_time': str(item.get('entry_time') or ''),
        'exit_time': str(item.get('exit_time') or ''),
        'config': item.get('config') if isinstance(item.get('config'), dict) else {},
        'portfolio': item.get('portfolio') if isinstance(item.get('portfolio'), dict) else {},
    }


def _load_running_trade_records(db: MongoData, trade_date: str, activation_mode: str = 'algo-backtest') -> list[dict]:
    query = _build_trade_query(
        trade_date,
        activation_mode=activation_mode,
        statuses=[RUNNING_STATUS],
    )
    query['active_on_server'] = True
    cursor = db._db['algo_trades'].find(query).sort('creation_ts', 1)
    return [_serialize_trade_record(item) for item in cursor]


def _trade_emit_signature(record: dict) -> str:
    legs = record.get('legs') if isinstance(record.get('legs'), list) else []
    parts = [str(record.get('_id') or ''), str(record.get('status') or ''), str(record.get('active_on_server') or '')]
    for leg in legs:
        if not isinstance(leg, dict):
            # string history _id reference — include it in signature as-is
            parts.append(str(leg))
            continue
        entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
        exit_trade = leg.get('exit_trade') if isinstance(leg.get('exit_trade'), dict) else {}
        parts.append(
            '|'.join([
                str(leg.get('id') or ''),
                str(_safe_int(leg.get('status'))),
                str(entry_trade.get('traded_timestamp') or entry_trade.get('trigger_timestamp') or ''),
                str(exit_trade.get('traded_timestamp') or exit_trade.get('trigger_timestamp') or ''),
                str(leg.get('current_sl_price') or ''),
                str(leg.get('display_sl_value') or ''),
            ])
        )
    return '::'.join(parts)


def _has_opened_order(record: dict) -> bool:
    for leg in (record.get('legs') if isinstance(record.get('legs'), list) else []):
        if not isinstance(leg, dict):
            # String ID in legs = history record exists = order was entered
            if str(leg).strip():
                return True
            continue
        entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
        if entry_trade.get('traded_timestamp') or entry_trade.get('trigger_timestamp'):
            return True
    return False


def _resolve_trade_leg_configs(trade: dict) -> dict:
    """
    Build leg config lookup for a trade.

    Priority:
      1. trade.strategy.ListOfLegConfigs / IdleLegConfigs
      2. trade.config.LegConfigs / IdleLegConfigs
    """
    all_leg_configs: dict = {}
    strategy_cfg = trade.get('strategy') or {}
    for lc in (strategy_cfg.get('ListOfLegConfigs') or []):
        if isinstance(lc, dict):
            all_leg_configs[str(lc.get('id') or '')] = lc
    idle_cfg = strategy_cfg.get('IdleLegConfigs') or {}
    if isinstance(idle_cfg, dict):
        all_leg_configs.update(idle_cfg)

    cfg = trade.get('config') or {}
    all_leg_configs.update(cfg.get('LegConfigs') or {})
    all_leg_configs.update(cfg.get('IdleLegConfigs') or {})
    return all_leg_configs


def _build_execute_order_events(
    records: list[dict],
    seen_signatures: dict[str, str],
) -> list[dict]:
    events: list[dict] = []
    for record in records:
        trade_id = str(record.get('_id') or '')
        signature = _trade_emit_signature(record)
        previous_signature = seen_signatures.get(trade_id)
        seen_signatures[trade_id] = signature
        if previous_signature == signature:
            continue
        if not _has_opened_order(record):
            continue
        events.append(record)
    return events


def _seed_execute_order_signatures(
    records: list[dict] | None,
    seen_signatures: dict[str, str],
) -> None:
    for record in (records or []):
        if not isinstance(record, dict):
            continue
        trade_id = str(record.get('_id') or '').strip()
        if not trade_id:
            continue
        seen_signatures[trade_id] = _trade_emit_signature(record)


def _populate_legs_from_history(db: 'MongoData', records: list[dict]) -> list[dict]:
    """Enrich records with full leg objects from algo_trade_positions_history (same as API)."""
    trade_ids = [str(r.get('_id') or '') for r in records if r.get('_id')]
    if not trade_ids:
        return records
    try:
        history_col = db._db['algo_trade_positions_history']
        history_by_trade: dict[str, list] = {tid: [] for tid in trade_ids}
        for doc in history_col.find({'trade_id': {'$in': trade_ids}}):
            doc['_id'] = str(doc.get('_id') or '')
            tid = str(doc.get('trade_id') or '')
            if tid in history_by_trade:
                history_by_trade[tid].append(doc)
    except Exception as exc:
        log.warning('_populate_legs_from_history error: %s', exc)
        return records
    enriched = []
    for rec in records:
        trade_id = str(rec.get('_id') or '')
        history_legs = history_by_trade.get(trade_id) or []
        new_rec = dict(rec)
        new_rec['legs'] = history_legs
        new_rec['open_legs_count'] = sum(1 for l in history_legs if int(l.get('status') or 0) == 1)
        new_rec['closed_legs_count'] = sum(1 for l in history_legs if int(l.get('status') or 0) == 2)
        new_rec['pending_legs_count'] = sum(1 for l in history_legs if int(l.get('status') or 0) == 0)
        enriched.append(new_rec)
    return _attach_leg_feature_statuses(db, enriched)


def _attach_leg_feature_statuses(db: 'MongoData', records: list[dict]) -> list[dict]:
    def _format_feature_status_timestamp(value: Any) -> str:
        raw_value = str(value or '').strip()
        if not raw_value:
            return ''
        return raw_value.replace('T', ' ')

    def _format_feature_status_price(value: Any) -> str:
        numeric = _safe_float(value)
        if numeric <= 0:
            return '-'
        return f'₹{numeric:.2f}'

    def _describe_feature_status_row(row: dict) -> str:
        if not isinstance(row, dict):
            return ''

        description = str(row.get('trigger_description') or '').strip()
        if description:
            return description

        feature_key = str(row.get('feature') or '').strip()
        if feature_key in {'overall_sl', 'overall_target'}:
            label = 'Overall SL' if feature_key == 'overall_sl' else 'Overall Target'
            cycle_number = int(row.get('cycle_number') or 1)
            trigger_value = _format_feature_status_price(row.get('trigger_value'))
            next_value = _format_feature_status_price(row.get('next_trigger_value'))
            reentry_type = str(row.get('reentry_type') or 'None')
            reentry_count = int(row.get('reentry_count') or 0)
            reentry_done = int(row.get('reentry_done') or 0)
            return (
                f'{label} active for cycle {cycle_number}. '
                f'Current threshold {trigger_value}. '
                f'Re-entry {reentry_type} used {reentry_done}/{reentry_count}. '
                f'Next cycle threshold {next_value}.'
            )
        if feature_key != 'momentum_pending':
            return ''

        status = str(row.get('status') or '').strip().lower()
        option = str(row.get('option') or '').strip().upper() or '-'
        position = str(row.get('position') or '').split('.')[-1].strip() or 'Position'
        strike = str(row.get('strike') or '').strip() or '-'
        momentum_type = str(row.get('momentum_type') or '').split('.')[-1].strip() or 'Momentum'
        momentum_value = _safe_float(row.get('momentum_value'))
        base_price = _format_feature_status_price(row.get('momentum_base_price'))
        target_price = _format_feature_status_price(row.get('momentum_target_price'))
        queued_at = _format_feature_status_timestamp(row.get('queued_at'))
        armed_at = _format_feature_status_timestamp(row.get('armed_at'))

        if status == 'triggered':
            triggered_at = _format_feature_status_timestamp(row.get('triggered_at'))
            return f'Momentum triggered for {strike} {option} {position} leg at {triggered_at or "-"}.'

        if _safe_float(row.get('momentum_base_price')) > 0 and _safe_float(row.get('momentum_target_price')) > 0:
            return (
                f'Momentum waiting for {strike} {option} {position} leg. '
                f'{momentum_type} {momentum_value:g} armed at {armed_at or queued_at or "-"} '
                f'with base {base_price} and target {target_price}.'
            )

        return (
            f'Momentum queue active for {strike} {option} {position} leg since {queued_at or "-"}. '
            f'Waiting to arm {momentum_type} {momentum_value:g}.'
        )

    def _build_pending_feature_leg(row: dict) -> dict:
        row_copy = dict(row)
        description = _describe_feature_status_row(row_copy)
        if description:
            row_copy['trigger_description'] = description

        feature_map: dict[str, dict] = {}
        feature_key = str(row_copy.get('feature') or '').strip()
        if feature_key:
            feature_map[feature_key] = row_copy

        return {
            'id': str(row_copy.get('leg_id') or ''),
            'leg_id': str(row_copy.get('leg_id') or ''),
            'status': 0,
            'position': row_copy.get('position'),
            'option': row_copy.get('option'),
            'strike': row_copy.get('strike'),
            'expiry_date': row_copy.get('expiry_date'),
            'token': row_copy.get('token'),
            'symbol': row_copy.get('symbol'),
            'quantity': 0,
            'lot_config_value': int(row_copy.get('lot_config_value') or 1),
            'entry_trade': None,
            'exit_trade': None,
            'last_saw_price': row_copy.get('momentum_base_price'),
            'is_lazy': True,
            'is_pending_feature_leg': True,
            'queued_at': row_copy.get('queued_at'),
            'armed_at': row_copy.get('armed_at'),
            'triggered_at': row_copy.get('triggered_at'),
            'leg_type': row_copy.get('leg_type'),
            'momentum_base_price': row_copy.get('momentum_base_price'),
            'momentum_target_price': row_copy.get('momentum_target_price'),
            'feature_status_rows': [row_copy],
            'feature_status_map': feature_map,
            'active_trigger_descriptions': [description] if description else [],
        }

    if not records:
        return records

    trade_ids = [str(rec.get('_id') or '') for rec in records if rec.get('_id')]
    if not trade_ids:
        return records

    feature_rows_by_key: dict[tuple[str, str], list[dict]] = {}
    try:
        feature_col = db._db['algo_leg_feature_status']
        for doc in feature_col.find({'trade_id': {'$in': trade_ids}, 'enabled': True}):
            trade_id = str(doc.get('trade_id') or '')
            leg_id = str(doc.get('leg_id') or '')
            if not trade_id or not leg_id:
                continue
            doc['_id'] = str(doc.get('_id') or '')
            feature_rows_by_key.setdefault((trade_id, leg_id), []).append(doc)
    except Exception as exc:
        log.warning('_attach_leg_feature_statuses error: %s', exc)
        return records

    enriched_records: list[dict] = []
    for rec in records:
        trade_id = str(rec.get('_id') or '')
        legs = rec.get('legs') if isinstance(rec.get('legs'), list) else []
        existing_leg_ids: set[str] = set()
        enriched_legs: list[dict] = []
        for leg in legs:
            if not isinstance(leg, dict):
                continue
            leg_id = str(leg.get('_id') or leg.get('leg_id') or leg.get('id') or '')
            if leg_id:
                existing_leg_ids.add(leg_id)
            feature_rows = feature_rows_by_key.get((trade_id, leg_id), [])
            leg_copy = dict(leg)
            leg_copy['feature_status_rows'] = feature_rows
            feature_map: dict[str, dict] = {}
            active_descriptions: list[str] = []
            for row in feature_rows:
                feature_key = str(row.get('feature') or '').strip()
                if not feature_key:
                    continue
                row_copy = dict(row)
                description = _describe_feature_status_row(row_copy)
                if description:
                    row_copy['trigger_description'] = description
                feature_map[feature_key] = row_copy
                if description:
                    active_descriptions.append(description)
            leg_copy['feature_status_map'] = feature_map
            leg_copy['feature_status_rows'] = list(feature_map.values()) if feature_map else feature_rows
            leg_copy['active_trigger_descriptions'] = active_descriptions
            enriched_legs.append(leg_copy)

        pending_feature_legs: list[dict] = []
        strategy_feature_rows: list[dict] = []
        for (feature_trade_id, feature_leg_id), feature_rows in feature_rows_by_key.items():
            if feature_trade_id != trade_id or not feature_leg_id or feature_leg_id in existing_leg_ids:
                continue
            if feature_leg_id == OVERALL_FEATURE_LEG_ID:
                for row in feature_rows:
                    row_copy = dict(row)
                    description = _describe_feature_status_row(row_copy)
                    if description:
                        row_copy['trigger_description'] = description
                    strategy_feature_rows.append(row_copy)
                continue
            for row in feature_rows:
                if str(row.get('feature') or '').strip() != 'momentum_pending':
                    continue
                if str(row.get('status') or '').strip().lower() != 'active':
                    continue
                pending_feature_legs.append(_build_pending_feature_leg(row))
        new_rec = dict(rec)
        new_rec['legs'] = enriched_legs
        new_rec['pending_feature_legs'] = pending_feature_legs
        new_rec['strategy_feature_status_rows'] = strategy_feature_rows
        enriched_records.append(new_rec)
    return enriched_records


def _extract_market_underlyings(records: list[dict]) -> list[str]:
    return sorted({
        str(
            record.get('ticker')
            or ((record.get('config') or {}).get('Ticker') or '')
        ).strip().upper()
        for record in (records or [])
        if str(
            record.get('ticker')
            or ((record.get('config') or {}).get('Ticker') or '')
        ).strip()
    })


def _preload_trade_date_market_cache(
    db: MongoData,
    trade_date: str,
    records: list[dict] | None = None,
) -> dict | None:
    normalized_date = str(trade_date or '').strip()
    if not normalized_date:
        return None
    underlyings = _extract_market_underlyings(records or [])
    try:
        return preload_market_data_cache(db, normalized_date, underlyings)
    except Exception as exc:
        log.warning('market cache preload error trade_date=%s underlyings=%s error=%s', normalized_date, underlyings, exc)
        return None


def _clear_market_cache_snapshot(market_cache: dict | None) -> None:
    cache_key = str((market_cache or {}).get('cache_key') or '').strip()
    if cache_key:
        clear_market_data_cache(cache_key)


def _is_algo_backtest_status(value: Any) -> bool:
    return str(value or '').strip().lower() == 'algo-backtest'


def _prefetch_backtest_market_data_once(db: MongoData, trade_date: str, records: list[dict] | None = None) -> None:
    market_cache = _preload_trade_date_market_cache(db, trade_date, records or [])
    if not market_cache:
        return
    underlyings = list((market_cache or {}).get('underlyings') or [])
    print(
        '[BACKTEST MARKET DATA READY] '
        f'trade_date={trade_date} '
        f'underlyings={",".join(underlyings) if underlyings else "-"} '
        f'option_chain_and_spot_loaded=true'
    )
    _clear_market_cache_snapshot(market_cache)


def _build_open_position_updates(
    records: list[dict],
    db: MongoData,
    trade_date: str,
    market_cache: dict | None = None,
) -> list[dict]:
    chain_col = db._db[OPTION_CHAIN_COLLECTION]
    open_positions: list[dict] = []
    for record in records:
        underlying = str(record.get('ticker') or ((record.get('config') or {}).get('Ticker') or ''))
        for leg in (record.get('legs') if isinstance(record.get('legs'), list) else []):
            if not isinstance(leg, dict):
                continue
            if _safe_int(leg.get('status')) != OPEN_LEG_STATUS:
                continue
            entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
            if not entry_trade:
                continue
            strike = leg.get('strike')
            expiry_date = str(leg.get('expiry_date') or '')[:10]
            option_type = str(leg.get('option') or '')
            chain_doc = get_latest_chain_doc(
                chain_col,
                underlying,
                expiry_date,
                strike,
                option_type,
                trade_date,
                market_cache=market_cache,
            )
            ltp = _safe_float(chain_doc.get('close'), _safe_float(leg.get('last_saw_price')))
            open_positions.append({
                'trade_id': str(record.get('_id') or ''),
                'strategy_id': str(record.get('strategy_id') or ''),
                'strategy_name': str(record.get('name') or ''),
                'group_name': str(((record.get('portfolio') or {}).get('group_name') or '')),
                'ticker': underlying,
                'underlying': underlying,
                'leg_id': str(leg.get('id') or ''),
                'position': str(leg.get('position') or ''),
                'option': option_type,
                'strike': strike,
                'expiry_date': str(leg.get('expiry_date') or ''),
                'entry_price': _safe_float(entry_trade.get('price')),
                'ltp': ltp,
                'spot_price': _safe_float(chain_doc.get('spot_price')),
                'quantity': _safe_int(leg.get('quantity') or entry_trade.get('quantity')),
                'timestamp': str(chain_doc.get('timestamp') or ''),
                'symbol': str(leg.get('symbol') or chain_doc.get('symbol') or ''),
                'token': str(leg.get('token') or chain_doc.get('token') or ''),
            })
    return open_positions


def _build_strategy_spot_subscribe_tokens(
    records: list[dict],
    db: MongoData,
    trade_date: str,
    market_cache: dict | None = None,
) -> list[dict]:
    index_spot_col = db._db['option_chain_index_spot']
    snapshot_ts = f'{str(trade_date or "").strip()} 23:59:59.999999'
    token_map: dict[str, dict] = {}
    for record in (records or []):
        underlying = str(
            record.get('ticker')
            or ((record.get('config') or {}).get('Ticker') or '')
        ).strip().upper()
        if not underlying:
            continue
        spot_doc = get_cached_spot_doc(market_cache, underlying, snapshot_ts) if market_cache else {}
        if not spot_doc:
            spot_doc = get_index_spot_at_time(index_spot_col, underlying, snapshot_ts)
        token = str((spot_doc or {}).get('token') or '').strip()
        if not token or token in token_map:
            continue
        token_map[token] = {
            'token': token,
            'underlying': underlying,
            'symbol': str((spot_doc or {}).get('symbol') or underlying).strip(),
            'expiry_date': '',
            'strike': None,
            'option': 'SPOT',
        }
    return list(token_map.values())


def _build_subscribe_tokens(
    open_positions: list[dict],
    strategy_spot_tokens: list[dict] | None = None,
) -> list[dict]:
    token_map: dict[str, dict] = {}
    for item in (strategy_spot_tokens or []):
        token = str(item.get('token') or '').strip()
        if not token or token in token_map:
            continue
        token_map[token] = {
            'token': token,
            'underlying': str(item.get('underlying') or '').strip(),
            'symbol': str(item.get('symbol') or '').strip(),
            'expiry_date': str(item.get('expiry_date') or '').strip(),
            'strike': item.get('strike'),
            'option': str(item.get('option') or '').strip(),
        }
    for item in (open_positions or []):
        token = str(item.get('token') or '').strip()
        if not token:
            continue
        if token in token_map:
            continue
        token_map[token] = {
            'token': token,
            'underlying': str(item.get('underlying') or item.get('ticker') or '').strip(),
            'symbol': str(item.get('symbol') or '').strip(),
            'expiry_date': str(item.get('expiry_date') or '').strip(),
            'strike': item.get('strike'),
            'option': str(item.get('option') or '').strip(),
        }
    return list(token_map.values())


def _build_subscribed_token_market_data(
    open_positions: list[dict],
    strategy_spot_tokens: list[dict] | None = None,
    db: MongoData | None = None,
    trade_date: str = '',
    market_cache: dict | None = None,
) -> list[dict]:
    token_data_map: dict[str, dict] = {}
    if db and strategy_spot_tokens:
        index_spot_col = db._db['option_chain_index_spot']
        snapshot_ts = f'{str(trade_date or "").strip()} 23:59:59.999999'
        for item in strategy_spot_tokens:
            token = str(item.get('token') or '').strip()
            underlying = str(item.get('underlying') or '').strip().upper()
            if not token or not underlying:
                continue
            spot_doc = get_cached_spot_doc(market_cache, underlying, snapshot_ts) if market_cache else {}
            if not spot_doc:
                spot_doc = get_index_spot_at_time(index_spot_col, underlying, snapshot_ts)
            token_data_map[token] = {
                'token': token,
                'underlying': underlying,
                'symbol': str((spot_doc or {}).get('symbol') or item.get('symbol') or underlying).strip(),
                'option': 'SPOT',
                'strike': None,
                'expiry_date': '',
                'ltp': _safe_float((spot_doc or {}).get('spot_price')),
                'spot_price': _safe_float((spot_doc or {}).get('spot_price')),
                'timestamp': str((spot_doc or {}).get('timestamp') or '').strip(),
            }
    for item in (open_positions or []):
        token = str(item.get('token') or '').strip()
        if not token:
            continue
        token_data_map[token] = {
            'token': token,
            'underlying': str(item.get('underlying') or item.get('ticker') or '').strip(),
            'symbol': str(item.get('symbol') or '').strip(),
            'option': str(item.get('option') or '').strip(),
            'strike': item.get('strike'),
            'expiry_date': str(item.get('expiry_date') or '').strip(),
            'ltp': _safe_float(item.get('ltp')),
            'spot_price': _safe_float(item.get('spot_price')),
            'timestamp': str(item.get('timestamp') or '').strip(),
        }
    return list(token_data_map.values())


def _build_leg_token(leg: dict, underlying: str) -> str:
    return str(
        leg.get('token')
        or make_token(
            underlying,
            str(leg.get('expiry_date') or ''),
            leg.get('strike'),
            str(leg.get('option') or ''),
        )
    )


def _build_active_leg_contract(leg: dict, trade: dict) -> dict | None:
    entry_trade = leg.get('entry_trade') or {}
    if not entry_trade:
        return None
    underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
    return {
        'trade_id': str(trade.get('_id') or ''),
        'strategy_id': str(trade.get('strategy_id') or ''),
        'strategy_name': str(trade.get('name') or ''),
        'leg_id': str(leg.get('id') or ''),
        'token': _build_leg_token(leg, underlying),
        'symbol': str(leg.get('symbol') or ''),
        'underlying': underlying,
        'expiry_date': str(leg.get('expiry_date') or ''),
        'strike': leg.get('strike'),
        'option': str(leg.get('option') or ''),
        'entry_timestamp': str(entry_trade.get('traded_timestamp') or entry_trade.get('trigger_timestamp') or ''),
    }


def _fetch_active_leg_ticks(
    chain_col,
    active_contracts: list[dict],
    snapshot_timestamp: str,
    market_cache: dict | None = None,
) -> list[dict]:
    token_ticks: list[dict] = []
    for contract in active_contracts:
        chain_doc = get_chain_at_time(
            chain_col,
            contract.get('underlying') or '',
            contract.get('expiry_date') or '',
            contract.get('strike'),
            contract.get('option') or '',
            snapshot_timestamp,
            market_cache=market_cache,
        )
        if not chain_doc:
            continue
        # Prefer the actual token from option_chain DB doc (e.g. "NSE_2025110406910")
        chain_token = str(chain_doc.get('token') or '').strip()
        token_ticks.append({
            'trade_id': contract.get('trade_id'),
            'strategy_id': contract.get('strategy_id'),
            'strategy_name': contract.get('strategy_name'),
            'group_name': contract.get('group_name'),
            'leg_id': contract.get('leg_id'),
            'token': chain_token or contract.get('token'),
            'symbol': str(chain_doc.get('symbol') or contract.get('symbol') or ''),
            'underlying': str(chain_doc.get('underlying') or contract.get('underlying') or ''),
            'expiry_date': str(chain_doc.get('expiry') or contract.get('expiry_date') or ''),
            'strike': chain_doc.get('strike') or contract.get('strike'),
            'option': str(chain_doc.get('type') or contract.get('option') or ''),
            'entry_timestamp': contract.get('entry_timestamp'),
            'snapshot': {
                'token': chain_token or contract.get('token'),
                'timestamp': chain_doc.get('timestamp') or snapshot_timestamp,
                'underlying': chain_doc.get('underlying') or contract.get('underlying'),
                'expiry': chain_doc.get('expiry') or contract.get('expiry_date'),
                'strike': chain_doc.get('strike', contract.get('strike')),
                'type': chain_doc.get('type') or contract.get('option'),
                'close': _safe_float(chain_doc.get('close')),
                'oi': chain_doc.get('oi'),
                'iv': chain_doc.get('iv'),
                'delta': chain_doc.get('delta'),
                'gamma': chain_doc.get('gamma'),
                'theta': chain_doc.get('theta'),
                'vega': chain_doc.get('vega'),
                'rho': chain_doc.get('rho'),
                'spot_price': chain_doc.get('spot_price'),
                'bb_qty': chain_doc.get('bb_qty') or chain_doc.get('bid_qty') or 0,
                'bb_price': chain_doc.get('bb_price') or chain_doc.get('bid_price') or 0,
                'ba_qty': chain_doc.get('ba_qty') or chain_doc.get('ask_qty') or 0,
                'ba_price': chain_doc.get('ba_price') or chain_doc.get('ask_price') or 0,
                'vol_in_day': chain_doc.get('vol_in_day') or chain_doc.get('volume') or 0,
            },
        })
    return token_ticks


def _build_active_contracts_from_records(
    records: list[dict],
    db: MongoData | None = None,
    trade_date: str = '',
    market_cache: dict | None = None,
) -> list[dict]:
    """
    Collect all open option legs + underlying spot tokens as active contracts.
    Prefers algo_trade_positions_history (status=1) when db is provided.
    Adds one SPOT contract per unique underlying that has open legs — no duplicates.
    Spot tokens are active from entry_time to exit_time of the strategy.
    """
    contracts: list[dict] = []
    trade_ids = [str(r.get('_id') or '') for r in (records or []) if isinstance(r, dict) and r.get('_id')]
    trade_meta: dict[str, dict] = {
        str(r.get('_id') or ''): r for r in (records or []) if isinstance(r, dict)
    }

    # Primary: query history for open legs (status=1)
    if db and trade_ids:
        try:
            history_col = db._db['algo_trade_positions_history']
            for doc in history_col.find({'trade_id': {'$in': trade_ids}, 'status': OPEN_LEG_STATUS}):
                tid = str(doc.get('trade_id') or '')
                rec = trade_meta.get(tid) or {}
                underlying = str(doc.get('ticker') or (rec.get('config') or {}).get('Ticker') or rec.get('ticker') or '')
                entry_trade = doc.get('entry_trade') if isinstance(doc.get('entry_trade'), dict) else None
                if not entry_trade:
                    continue
                contracts.append({
                    'trade_id': tid,
                    'strategy_id': str(doc.get('strategy_id') or rec.get('strategy_id') or ''),
                    'strategy_name': str(doc.get('strategy_name') or rec.get('name') or ''),
                    'group_name': str(doc.get('group_name') or ''),
                    'leg_id': str(doc.get('leg_id') or ''),
                    'token': _build_leg_token(doc, underlying),
                    'symbol': str(doc.get('symbol') or ''),
                    'underlying': underlying,
                    # _normalize_expiry_datetime stores '2025-11-06 15:30:00'; chain
                    # collection uses plain '2025-11-06' — strip the time component.
                    'expiry_date': str(doc.get('expiry_date') or '')[:10],
                    'strike': doc.get('strike'),
                    'option': str(doc.get('option') or ''),
                    'entry_timestamp': str(
                        entry_trade.get('traded_timestamp') or entry_trade.get('trigger_timestamp') or ''
                    ),
                })
        except Exception as exc:
            log.warning('_build_active_contracts_from_records history query error: %s', exc)

    # Fallback: dict legs not yet synced to history
    history_leg_ids = {c['leg_id'] for c in contracts}
    for record in (records or []):
        if not isinstance(record, dict):
            continue
        underlying = str(record.get('underlying') or (record.get('config') or {}).get('Ticker') or record.get('ticker') or '')
        for leg in (record.get('legs') or []):
            if not isinstance(leg, dict):
                continue
            if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
                continue
            entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else None
            if not entry_trade:
                continue
            leg_id = str(leg.get('id') or '')
            if leg_id in history_leg_ids:
                continue
            contracts.append({
                'trade_id': str(record.get('_id') or ''),
                'strategy_id': str(record.get('strategy_id') or ''),
                'strategy_name': str(record.get('name') or ''),
                'group_name': str(record.get('group_name') or ''),
                'leg_id': leg_id,
                'token': _build_leg_token(leg, underlying),
                'symbol': str(leg.get('symbol') or ''),
                'underlying': underlying,
                'expiry_date': str(leg.get('expiry_date') or ''),
                'strike': leg.get('strike'),
                'option': str(leg.get('option') or ''),
                'entry_timestamp': str(
                    entry_trade.get('traded_timestamp') or entry_trade.get('trigger_timestamp') or ''
                ),
            })

    # Add underlying SPOT token for each unique underlying that has open legs
    # Deduplicated — one SPOT contract per underlying, no matter how many strategies share it
    active_underlyings = {c['underlying'].strip().upper() for c in contracts if c.get('underlying')}
    if active_underlyings and db:
        index_spot_col = db._db['option_chain_index_spot']
        # ISO format required — collection stores timestamps as "2025-11-03T15:29:00"
        snapshot_ts = f'{str(trade_date or "").strip()}T23:59:59' if trade_date else ''
        spot_token_set: set[str] = set()
        for underlying in sorted(active_underlyings):
            try:
                spot_doc = (
                    get_cached_spot_doc(market_cache, underlying, snapshot_ts)
                    if market_cache and snapshot_ts else {}
                )
                if not spot_doc:
                    spot_doc = get_index_spot_at_time(index_spot_col, underlying, snapshot_ts) if snapshot_ts else {}
                if not spot_doc:
                    # fallback: latest available spot doc for this underlying
                    spot_doc = index_spot_col.find_one(
                        {'underlying': underlying},
                        sort=[('timestamp', DESCENDING)],
                    ) or {}
                spot_token = str((spot_doc or {}).get('token') or '').strip()
                if not spot_token or spot_token in spot_token_set:
                    continue
                spot_token_set.add(spot_token)
                contracts.append({
                    'trade_id': '',
                    'strategy_id': '',
                    'strategy_name': '',
                    'group_name': '',
                    'leg_id': f'spot_{underlying.lower()}',
                    'token': spot_token,
                    'symbol': str((spot_doc or {}).get('symbol') or underlying).strip(),
                    'underlying': underlying,
                    'expiry_date': '',
                    'strike': None,
                    'option': 'SPOT',
                    'entry_timestamp': '',
                })
                print(f'[SPOT SUBSCRIBE] underlying={underlying} token={spot_token}')
            except Exception as exc:
                log.warning('_build_active_contracts_from_records spot token error underlying=%s: %s', underlying, exc)

    # Deduplicate option leg tokens — multiple strategies can share the same strike/token
    seen_tokens: set[str] = set()
    deduped: list[dict] = []
    for c in contracts:
        tok = str(c.get('token') or '')
        if tok and tok in seen_tokens:
            continue
        if tok:
            seen_tokens.add(tok)
        deduped.append(c)

    return deduped


def _build_momentum_pending_contracts(
    db: MongoData,
    records: list[dict],
) -> list[dict]:
    """
    Return contract dicts for legs that are actively monitoring momentum
    (feature='momentum_pending', status='active' in algo_leg_feature_status).
    Only includes docs where strike/expiry_date/token have been resolved
    (after the first tick), so _fetch_active_leg_ticks can look up LTP data.
    These contracts are appended to active_contracts so the frontend receives
    real-time LTP updates while a lazy leg waits for its momentum trigger price.
    """
    contracts: list[dict] = []
    trade_ids = [
        str(r.get('_id') or '')
        for r in (records or [])
        if isinstance(r, dict) and r.get('_id')
    ]
    if not trade_ids or not db:
        return contracts

    trade_meta: dict[str, dict] = {
        str(r.get('_id') or ''): r for r in (records or []) if isinstance(r, dict)
    }

    try:
        feature_col = db._db['algo_leg_feature_status']
        pending_docs = list(feature_col.find({
            'trade_id': {'$in': trade_ids},
            'feature': 'momentum_pending',
            'status': 'active',
            'token': {'$nin': [None, '']},
            'strike': {'$ne': None},
            'expiry_date': {'$nin': [None, '']},
        }))
    except Exception as exc:
        log.warning('_build_momentum_pending_contracts query error: %s', exc)
        return contracts

    for doc in pending_docs:
        token = str(doc.get('token') or '').strip()
        strike = doc.get('strike')
        expiry_date = str(doc.get('expiry_date') or '')[:10]
        option = str(doc.get('option') or '')
        if not token or strike is None or not expiry_date or not option:
            continue

        tid = str(doc.get('trade_id') or '')
        rec = trade_meta.get(tid) or {}
        underlying = str(
            doc.get('underlying') or
            (rec.get('strategy') or {}).get('Ticker') or
            (rec.get('config') or {}).get('Ticker') or
            rec.get('ticker') or ''
        )
        if not underlying:
            continue

        contracts.append({
            'trade_id': tid,
            'strategy_id': str(rec.get('strategy_id') or ''),
            'strategy_name': str(rec.get('name') or ''),
            'group_name': str(rec.get('group_name') or ''),
            'leg_id': str(doc.get('leg_id') or ''),
            'token': token,
            'symbol': str(doc.get('symbol') or ''),
            'underlying': underlying,
            'expiry_date': expiry_date,
            'strike': strike,
            'option': option,
            'entry_timestamp': '',
            'is_momentum_pending': True,
        })

    return contracts


def _append_momentum_pending_to_contracts(
    active_contracts: list[dict],
    db: MongoData | None,
    records: list[dict],
) -> list[dict]:
    """
    Merge momentum-pending contracts into active_contracts (token-deduped).
    Returns the extended list (same object, mutated in-place for efficiency).
    """
    if not db or not records:
        return active_contracts
    pending = _build_momentum_pending_contracts(db, records)
    if not pending:
        return active_contracts
    existing_tokens = {str(c.get('token') or '') for c in active_contracts if c.get('token')}
    for mc in pending:
        tok = str(mc.get('token') or '')
        if tok and tok not in existing_tokens:
            active_contracts.append(mc)
            existing_tokens.add(tok)
            print(
                f'[MOMENTUM PENDING SUBSCRIBE] leg={mc.get("leg_id")} '
                f'token={tok} strike={mc.get("strike")} option={mc.get("option")} '
                f'expiry={mc.get("expiry_date")}'
            )
    return active_contracts


def _format_chain_tick_as_ltp(tick: dict) -> dict:
    """Convert a _fetch_active_leg_ticks entry to the ltp wire format.
    Token is taken from the chain_doc (e.g. 'NSE_2025110406910'), not composite key.
    """
    snapshot = tick.get('snapshot') or {}
    # Use chain_doc token stored in snapshot['token'], fallback to tick token
    token = str(snapshot.get('token') or tick.get('token') or '')
    return {
        'token': token,
        'timestamp': str(snapshot.get('timestamp') or ''),
        'ltp': _safe_float(snapshot.get('close')),
        'bb_qty': int(snapshot.get('bb_qty') or 0),
        'bb_price': _safe_float(snapshot.get('bb_price')),
        'ba_qty': int(snapshot.get('ba_qty') or 0),
        'ba_price': _safe_float(snapshot.get('ba_price')),
        'vol_in_day': int(snapshot.get('vol_in_day') or snapshot.get('oi') or 0),
        'expiry': str(snapshot.get('expiry') or tick.get('expiry_date') or ''),
        'strike': snapshot.get('strike') or tick.get('strike'),
        'option_type': str(snapshot.get('type') or tick.get('option') or ''),
    }


def _fetch_spot_ticks(
    index_spot_col,
    spot_contracts: list[dict],
    snapshot_timestamp: str,
    market_cache: dict | None = None,
) -> list[dict]:
    """Fetch LTP for SPOT (underlying index) contracts from option_chain_index_spot."""
    ticks: list[dict] = []
    for contract in spot_contracts:
        underlying = str(contract.get('underlying') or '').strip().upper()
        if not underlying:
            continue
        try:
            spot_doc = get_index_spot_at_time(
                index_spot_col, underlying, snapshot_timestamp, market_cache=market_cache
            )
            if not spot_doc:
                continue
            token = str(spot_doc.get('token') or contract.get('token') or '').strip()
            ticks.append({
                'token': token,
                'timestamp': str(spot_doc.get('timestamp') or snapshot_timestamp),
                'ltp': _safe_float(spot_doc.get('spot_price')),
                'bb_qty': 0,
                'bb_price': 0.0,
                'ba_qty': 0,
                'ba_price': 0.0,
                'vol_in_day': 0,
                'underlying': underlying,
                'option_type': 'SPOT',
            })
        except Exception as exc:
            log.warning('_fetch_spot_ticks error underlying=%s: %s', underlying, exc)
    return ticks


def _is_original_leg(leg: dict, trade: dict) -> bool:
    leg_id = str(leg.get('id') or '')
    original_leg_configs = ((trade.get('config') or {}).get('LegConfigs') or {})
    return bool(leg_id and leg_id in original_leg_configs)


def _print_backtest_strategy_status(trade: dict, listening_time: str, market_time: str, entry_time_hhmm: str, state: str) -> None:
    trade_id = str(trade.get('_id') or '')
    strategy_name = str(trade.get('name') or '')
    group_name = str(((trade.get('portfolio') or {}).get('group_name') or 'No Group'))
    _backtest_debug_log(
        f"[BACKTEST CHECK] listening_time={listening_time} trade={trade_id} group={group_name} strategy={strategy_name} "
        f"entry_time={entry_time_hhmm or '--:--'} market_time={market_time} state={state}"
    )


def _extract_running_positions(
    db: MongoData,
    selected_date: str,
    market_time: str,
    include_position_snapshots: bool = True,
    running_trades: list[dict] | None = None,
    market_cache: dict | None = None,
) -> dict:
    """
    Used by backtest simulation mode.
    Fetches open legs from algo_trades and queries option_chain by composite key.
    Also evaluates pending leg entry eligibility on every simulated minute.
    """
    extract_started_at = perf_counter()
    algo_trades_col = db._db['algo_trades']
    chain_col = db._db[OPTION_CHAIN_COLLECTION]
    index_spot_col = db._db['option_chain_index_spot']

    trade_query = _build_trade_query(
        selected_date,
        activation_mode='algo-backtest',
        statuses=[BACKTEST_IMPORT_STATUS, RUNNING_STATUS],
    )

    if running_trades is None:
        trade_load_started_at = perf_counter()
        running_trades = list(algo_trades_col.find(trade_query))
        _slow_backtest_log(
            'load_running_trades',
            (perf_counter() - trade_load_started_at) * 1000,
            date=selected_date,
            total=len(running_trades),
        )
    snapshot_timestamp = _build_snapshot_timestamp(selected_date, market_time)
    listening_time = _now_iso()
    _backtest_debug_log(f"[BACKTEST LISTENING] listening_time={listening_time} snapshot={snapshot_timestamp}")
    open_positions = []
    strategy_map: dict[str, dict] = {}
    position_entry_events: list[dict] = []
    active_leg_contracts: list[dict] = []
    wait_for_spot_data = False
    blocked_snapshot = None
    market_hhmm = _extract_hhmm(market_time)
    lot_size_cache: dict[str, int] = {}
    index_spot_cache: dict[tuple[str, str], dict] = {}

    for trade_index, trade in enumerate(running_trades):
        trade_started_at = perf_counter()
        trade_id = str(trade.get('_id') or '')
        underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
        all_leg_configs: dict = {}
        cfg = (trade.get('config') or {})
        all_leg_configs.update(cfg.get('LegConfigs') or {})
        all_leg_configs.update(cfg.get('IdleLegConfigs') or {})

        raw_entry_time = str(trade.get('entry_time') or '')
        entry_time_hhmm = _extract_hhmm(raw_entry_time)
        before_entry = bool(entry_time_hhmm and market_hhmm < entry_time_hhmm)
        entry_snapshot_timestamp = _build_snapshot_timestamp(selected_date, entry_time_hhmm) if entry_time_hhmm else snapshot_timestamp
        _print_backtest_strategy_status(
            trade,
            listening_time,
            snapshot_timestamp,
            entry_time_hhmm,
            'waiting_for_entry' if before_entry else 'checking_positions',
        )

        if underlying not in lot_size_cache:
            try:
                lot_size_cache[underlying] = db.get_lot_size(selected_date, underlying)
            except Exception:
                lot_size_cache[underlying] = 75
        default_lot_size = lot_size_cache[underlying]

        refreshed_trade = trade
        if not before_entry:
            created_original_legs, refreshed_trade = _queue_original_legs_if_needed(
                db, refreshed_trade, entry_snapshot_timestamp
            )
            if refreshed_trade is not trade:
                running_trades[trade_index] = refreshed_trade
            if created_original_legs:
                _backtest_debug_log(
                    f"[BACKTEST ORIGINAL LEGS] listening_time={listening_time} trade={trade_id} "
                    f"strategy={str(refreshed_trade.get('name') or '')} entry_snapshot={entry_snapshot_timestamp}"
                )
            # ── Process momentum-pending legs from algo_leg_feature_status ────
            spot_cache_key_mt = (underlying, snapshot_timestamp)
            if spot_cache_key_mt not in index_spot_cache:
                index_spot_cache[spot_cache_key_mt] = get_index_spot_at_time(
                    index_spot_col, underlying, snapshot_timestamp, market_cache=market_cache
                )
            _entered_mt = _process_momentum_pending_feature_legs(
                db, refreshed_trade, chain_col, selected_date, snapshot_timestamp,
                default_lot_size,
                index_spot_doc=index_spot_cache[spot_cache_key_mt],
                market_cache=market_cache,
            )
            if _entered_mt:
                refreshed_trade = algo_trades_col.find_one({'_id': trade_id}) or refreshed_trade
                running_trades[trade_index] = refreshed_trade
                _backtest_debug_log(
                    f"[BACKTEST MOMENTUM ENTERED] trade={trade_id} legs={_entered_mt} snapshot={snapshot_timestamp}"
                )
        refreshed_legs = [l for l in (refreshed_trade.get('legs') or []) if isinstance(l, dict)]
        for leg_index, leg in enumerate(refreshed_legs):
            leg_started_at = perf_counter()
            if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
                continue

            is_pending = bool(leg.get('is_lazy') or leg.get('entry_trade') is None)
            if is_pending:
                if before_entry:
                    _backtest_debug_log(
                        f"[BACKTEST ENTRY WAIT] listening_time={listening_time} trade={trade_id} leg={str(leg.get('id') or '')} "
                        f"entry_time={entry_time_hhmm or '--:--'} market_time={snapshot_timestamp}"
                    )
                    continue
                entry_lookup_snapshot = (
                    entry_snapshot_timestamp
                    if _is_original_leg(leg, refreshed_trade)
                    else snapshot_timestamp
                )
                spot_cache_key = (underlying, entry_lookup_snapshot)
                if spot_cache_key not in index_spot_cache:
                    index_spot_cache[spot_cache_key] = get_index_spot_at_time(
                        index_spot_col, underlying, entry_lookup_snapshot, market_cache=market_cache
                    )
                lot_size = _safe_int(leg.get('quantity')) or default_lot_size
                entered, wait_reason = _try_enter_pending_leg(
                    db, refreshed_trade, leg, leg_index, chain_col, selected_date,
                    entry_lookup_snapshot, lot_size, snapshot_timestamp=entry_lookup_snapshot,
                    index_spot_doc=index_spot_cache[spot_cache_key], market_cache=market_cache,
                )
                if wait_reason == 'spot_missing':
                    wait_for_spot_data = True
                    blocked_snapshot = entry_lookup_snapshot
                    _backtest_debug_log(
                        f"[BACKTEST SPOT WAIT] listening_time={listening_time} trade={trade_id} "
                        f"leg={str(leg.get('id') or '')} wait_snapshot={entry_lookup_snapshot}"
                    )
                    continue
                if not entered:
                    _slow_backtest_log(
                        'pending_leg_not_entered',
                        (perf_counter() - leg_started_at) * 1000,
                        trade_id=trade_id,
                        leg_id=str(leg.get('id') or ''),
                    )
                    _backtest_debug_log(
                        f"[BACKTEST ENTRY MISS] listening_time={listening_time} trade={trade_id} leg={str(leg.get('id') or '')} "
                        f"entry_time={entry_time_hhmm or '--:--'} lookup_snapshot={entry_lookup_snapshot} market_time={snapshot_timestamp}"
                    )
                    continue
                _backtest_debug_log(
                    f"[BACKTEST ENTRY OPENED] listening_time={listening_time} trade={trade_id} leg={str(leg.get('id') or '')} "
                    f"entry_time={entry_time_hhmm or '--:--'} lookup_snapshot={entry_lookup_snapshot} market_time={snapshot_timestamp}"
                )
                refreshed_trade = algo_trades_col.find_one({'_id': trade_id}) or refreshed_trade
                running_trades[trade_index] = refreshed_trade
                refreshed_legs = [l for l in (refreshed_trade.get('legs') or []) if isinstance(l, dict)]
                _leg_id_to_find = str(leg.get('id') or '')
                leg = next((l for l in refreshed_legs if str(l.get('id') or '') == _leg_id_to_find), None)
                if leg is None:
                    continue

            entry_trade = leg.get('entry_trade') or {}
            if not entry_trade:
                _backtest_debug_log(
                    f"[BACKTEST ENTRY EMPTY] listening_time={listening_time} trade={trade_id} leg={str(leg.get('id') or '')} "
                    f"entry_time={entry_time_hhmm or '--:--'} market_time={snapshot_timestamp}"
                )
                continue

            inserted, history_doc = _store_position_history(db, refreshed_trade, leg)
            if inserted and history_doc:
                position_entry_events.append({
                    'trade_id': history_doc['trade_id'],
                    'leg_id': history_doc['leg_id'],
                    'entry_timestamp': history_doc['entry_timestamp'],
                    'message': 'Position entry stored successfully',
                    'history': history_doc,
                })

            active_contract = _build_active_leg_contract(leg, refreshed_trade)
            if active_contract:
                active_leg_contracts.append(active_contract)

            if not include_position_snapshots:
                continue

            entry_price = _safe_float(entry_trade.get('price'))
            quantity = _safe_int(leg.get('quantity') or entry_trade.get('quantity'))
            lot_size = _safe_int(leg.get('lot_size'), 1)
            effective_quantity = max(0, quantity) * max(1, lot_size)
            strike = leg.get('strike')
            expiry_date = str(leg.get('expiry_date') or '')
            option_type = str(leg.get('option') or '')
            position_str = str(leg.get('position') or '')
            is_sell_pos = _is_sell(position_str)

            chain_doc = get_chain_at_time(
                chain_col, underlying, expiry_date, strike, option_type, snapshot_timestamp, market_cache=market_cache
            )
            current_price = _safe_float(chain_doc.get('close'), leg.get('last_saw_price', 0.0))
            pnl = (
                (entry_price - current_price) * effective_quantity
                if is_sell_pos else
                (current_price - entry_price) * effective_quantity
            )

            token = make_token(underlying, expiry_date, strike, option_type)
            leg_id = str(leg.get('id') or '')
            leg_cfg = all_leg_configs.get(leg_id) or {}
            entry_timestamp = str(entry_trade.get('traded_timestamp') or entry_trade.get('trigger_timestamp') or '')

            _backtest_debug_log(f"[BACKTEST POSITION] listening_time={listening_time} trade={trade_id} leg={leg_id} entry_time={entry_timestamp} market_time={snapshot_timestamp}")

            position_payload = {
                'strategy_id': str(refreshed_trade.get('strategy_id') or ''),
                'strategy_name': str(refreshed_trade.get('name') or ''),
                'trade_id': trade_id,
                'leg_id': leg_id,
                'symbol': str(leg.get('symbol') or ''),
                'token': token,
                'position': position_str,
                'option': option_type,
                'strike': strike,
                'expiry_date': expiry_date,
                'entry_price': entry_price,
                'current_price': current_price,
                'quantity': effective_quantity,
                'lots': quantity,
                'lot_size': lot_size,
                'pnl': round(pnl, 2),
                'entry_timestamp': entry_timestamp,
                'sl_price': calc_sl_price(entry_price, is_sell_pos, leg_cfg.get('LegStopLoss') or {}),
                'tp_price': calc_tp_price(entry_price, is_sell_pos, leg_cfg.get('LegTarget') or {}),
                'snapshot': {
                    'timestamp': chain_doc.get('timestamp') or snapshot_timestamp,
                    'underlying': chain_doc.get('underlying') or underlying,
                    'expiry': chain_doc.get('expiry') or expiry_date,
                    'strike': chain_doc.get('strike', strike),
                    'type': chain_doc.get('type') or option_type,
                    'close': current_price,
                    'oi': chain_doc.get('oi'),
                    'iv': chain_doc.get('iv'),
                    'delta': chain_doc.get('delta'),
                    'gamma': chain_doc.get('gamma'),
                    'theta': chain_doc.get('theta'),
                    'vega': chain_doc.get('vega'),
                    'rho': chain_doc.get('rho'),
                    'spot_price': chain_doc.get('spot_price'),
                },
            }
            open_positions.append(position_payload)

            strat_entry = strategy_map.setdefault(trade_id, {
                'trade_id': trade_id,
                'strategy_id': str(refreshed_trade.get('strategy_id') or ''),
                'strategy_name': str(refreshed_trade.get('name') or ''),
                'entry_time': entry_time_hhmm,
                'open_positions': [],
                'total_pnl': 0.0,
            })
            strat_entry['open_positions'].append(position_payload)
            strat_entry['total_pnl'] = round(strat_entry['total_pnl'] + position_payload['pnl'], 2)
            _slow_backtest_log(
                'process_leg',
                (perf_counter() - leg_started_at) * 1000,
                trade_id=trade_id,
                leg_id=leg_id,
            )

        _slow_backtest_log(
            'process_trade',
            (perf_counter() - trade_started_at) * 1000,
            trade_id=trade_id,
            underlying=underlying,
        )

    result = {
        'snapshot_timestamp': snapshot_timestamp,
        'running_strategies': list(strategy_map.values()),
        'open_positions': open_positions,
        'active_leg_contracts': active_leg_contracts,
        'active_leg_tokens': [contract.get('token') for contract in active_leg_contracts if contract.get('token')],
        'active_leg_ticks': _fetch_active_leg_ticks(
            chain_col,
            active_leg_contracts,
            snapshot_timestamp,
            market_cache=market_cache,
        ),
        'position_entry_events': position_entry_events,
        'wait_for_spot_data': wait_for_spot_data,
        'blocked_snapshot': blocked_snapshot,
    }
    _slow_backtest_log(
        'extract_running_positions_total',
        (perf_counter() - extract_started_at) * 1000,
        date=selected_date,
        market_time=market_time,
        total_running=len(running_trades),
        total_open=len(open_positions),
    )
    return result


# ─── Core: backtest SL/TP/exit tick ───────────────────────────────────────────

def _backtest_minute_tick(
    db: MongoData,
    trade_date: str,
    listen_timestamp: str,
    activation_mode: str = 'algo-backtest',
    market_cache: dict | None = None,
    running_trades: list[dict] | None = None,
) -> dict:
    """
    Called every simulated minute for backtest / forward-test strategies.
    Mirrors _live_minute_tick but uses historical chain data at listen_timestamp.
    Checks SL/TP/exit_time/overall SL-target for every open leg and records
    notifications in algo_trade_notification.

    Pass running_trades to avoid an extra DB load (caller already has them).
    """
    algo_trades_col = db._db['algo_trades']
    chain_col       = db._db[OPTION_CHAIN_COLLECTION]
    now_ts          = listen_timestamp
    now_time        = listen_timestamp[11:16] if len(listen_timestamp) >= 16 else ''  # HH:MM

    if running_trades is None:
        trade_query = _build_trade_query(
            trade_date,
            activation_mode=activation_mode,
            statuses=[RUNNING_STATUS, BACKTEST_IMPORT_STATUS],
        )
        running_trades = list(algo_trades_col.find(trade_query))

    open_positions     = []
    strategy_map: dict[str, dict] = {}
    actions_taken: list[str]      = []

    for trade in running_trades:
        trade_id   = str(trade.get('_id') or '')
        underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
        cfg        = trade.get('config') or {}
        all_leg_configs = _resolve_trade_leg_configs(trade)

        raw_exit_time  = str(trade.get('exit_time') or '')
        exit_time_hhmm = raw_exit_time[11:16] if len(raw_exit_time) >= 16 else raw_exit_time[:5]
        past_exit = bool(exit_time_hhmm and now_time and now_time >= exit_time_hhmm)

        # Dict legs (pending / not yet synced to history)
        legs = [leg for leg in (trade.get('legs') or []) if isinstance(leg, dict)]
        _dict_leg_ids = {str(l.get('id') or '') for l in legs if l.get('id')}

        # String refs = legs already stored in algo_trade_positions_history.
        # Re-load them so SL/TP/trail checks can run.
        _has_str_refs = any(isinstance(item, str) for item in (trade.get('legs') or []))
        if _has_str_refs:
            _hist_col = db._db['algo_trade_positions_history']
            for _hdoc in _hist_col.find({'trade_id': trade_id, 'status': OPEN_LEG_STATUS}):
                _hleg_id = str(_hdoc.get('leg_id') or _hdoc.get('id') or '')
                if not _hleg_id or _hleg_id in _dict_leg_ids:
                    continue  # already in legs or invalid
                # Normalize expiry_date: history stores '2025-11-06 15:30:00',
                # chain collection uses plain '2025-11-06'
                _raw_exp = str(_hdoc.get('expiry_date') or '')
                _hdoc['expiry_date'] = _raw_exp[:10] if ' ' in _raw_exp else _raw_exp
                _hdoc['id'] = _hleg_id  # ensure 'id' key is set
                legs.append(_hdoc)

        for leg_index, leg in enumerate(legs):
            leg_status = int(leg.get('status') or 0)
            if leg_status != OPEN_LEG_STATUS:
                continue
            entry_trade = leg.get('entry_trade') or {}
            if not entry_trade:
                continue  # pending leg, not yet entered

            leg_id      = str(leg.get('id') or '')
            leg_cfg     = _resolve_leg_cfg(leg_id, leg, all_leg_configs)
            entry_price = _safe_float(entry_trade.get('price'))
            quantity     = _safe_int(leg.get('quantity') or entry_trade.get('quantity'))
            lot_size     = _safe_int(leg.get('lot_size'), 1)
            effective_quantity = max(0, quantity) * max(1, lot_size)
            strike       = leg.get('strike')
            expiry_date  = str(leg.get('expiry_date') or '')
            option_type  = str(leg.get('option') or '')
            position_str = str(leg.get('position') or '')
            is_sell_pos  = _is_sell(position_str)

            # ── Force-exit at exit_time ────────────────────────────────────
            if past_exit:
                chain_doc  = get_chain_at_time(chain_col, underlying, expiry_date, strike, option_type, now_ts, market_cache=market_cache)
                exit_price = _safe_float(chain_doc.get('close'), leg.get('last_saw_price', 0.0))
                close_leg_in_db(db, trade_id, leg_index, exit_price, 'exit_time', now_ts, leg_id=leg_id)
                actions_taken.append(f'{trade_id}/{leg_id}: force-exit at {exit_price} (exit_time)')
                try:
                    from features.notification_manager import record_force_exit, disable_leg_features
                    record_force_exit(db._db, trade, leg, now_ts, exit_price, exit_reason='exit_time')
                    disable_leg_features(db._db, trade_id, leg_id, reason='force_exit', timestamp=now_ts)
                except Exception as _e:
                    log.warning('bt notification force_exit error: %s', _e)
                continue

            # ── Fetch historical price ─────────────────────────────────────
            chain_doc     = get_chain_at_time(chain_col, underlying, expiry_date, strike, option_type, now_ts, market_cache=market_cache)
            current_price = _safe_float(chain_doc.get('close'), _safe_float(leg.get('last_saw_price')))
            if current_price <= 0:
                continue  # no data at this timestamp

            sl_config    = leg_cfg.get('LegStopLoss') or {}
            tp_config    = leg_cfg.get('LegTarget') or {}
            trail_config = get_trail_config(leg_cfg)

            initial_sl   = _safe_float(leg.get('initial_sl_value')) or calc_sl_price(entry_price, is_sell_pos, sl_config)
            stored_sl    = _safe_float(leg.get('current_sl_price')) or None
            sl_price     = stored_sl or initial_sl
            if sl_price and trail_config:
                sl_price = update_trail_sl(
                    entry_price,
                    current_price,
                    sl_price,
                    is_sell_pos,
                    trail_config,
                    initial_sl=initial_sl,
                )

            tp_price = calc_tp_price(entry_price, is_sell_pos, tp_config)
            pnl      = (
                (entry_price - current_price) * effective_quantity
                if is_sell_pos else
                (current_price - entry_price) * effective_quantity
            )

            print(
                '[FEATURE CHECK]',
                f'listen_time={now_ts}',
                f'trade_id={trade_id}',
                f'leg_id={leg_id}',
                f'strategy_name={str(trade.get("name") or "-")}',
                f'option={option_type}',
                f'position={position_str}',
                f'entry_price={entry_price}',
                f'ltp={current_price}',
                f'stored_sl={stored_sl}',
                f'next_sl={sl_price}',
                f'tp={tp_price}',
                f'sl_enabled={bool(sl_price)}',
                f'tg_enabled={bool(tp_price)}',
                f'tsl_enabled={bool(trail_config)}',
            )

            from features.notification_manager import (
                record_sl_hit, record_target_hit,
                record_trail_sl_changed, record_reentry_queued, record_force_exit,
                trigger_leg_feature, disable_leg_features, rotate_trail_sl_record,
            )

            # ── SL hit ────────────────────────────────────────────────────
            if is_sl_hit(current_price, sl_price, is_sell_pos):
                close_leg_in_db(db, trade_id, leg_index, current_price, 'stoploss', now_ts, leg_id=leg_id)
                actions_taken.append(f'{trade_id}/{leg_id}: SL hit @ {current_price}')
                record_sl_hit(db._db, trade, leg, now_ts, current_price, sl_price or 0.0)
                # Feature status: mark sl=triggered, disable target+trailSL
                trigger_leg_feature(db._db, trade_id, leg_id, 'sl', current_price, now_ts)
                disable_leg_features(db._db, trade_id, leg_id, except_feature='sl', reason='sl_triggered', timestamp=now_ts)
                reentry_cfg = get_reentry_sl_config(leg_cfg)
                if reentry_cfg:
                    leg_cfg_with_id = dict(leg_cfg, id=leg_id)
                    result = _handle_reentry(db, trade, leg_cfg_with_id, reentry_cfg, leg_id, now_ts)
                    if result:
                        actions_taken.append(f'{trade_id}/{leg_id}: {result}')
                        re_type = str(reentry_cfg.get('Type') or '')
                        re_kind = ('lazy' if 'NextLeg' in re_type else
                                   'immediate' if 'Immediate' in re_type else
                                   'at_cost' if 'AtCost' in re_type else 'like_original')
                        new_leg_id = str((reentry_cfg.get('Value') or {}).get('NextLegRef') or '')
                        record_reentry_queued(db._db, trade, now_ts, leg_id, re_kind, new_leg_id, re_type, reason='sl')
                continue

            # ── TP hit ────────────────────────────────────────────────────
            if is_tp_hit(current_price, tp_price, is_sell_pos):
                close_leg_in_db(db, trade_id, leg_index, current_price, 'target', now_ts, leg_id=leg_id)
                actions_taken.append(f'{trade_id}/{leg_id}: TP hit @ {current_price}')
                record_target_hit(db._db, trade, leg, now_ts, current_price, tp_price or 0.0)
                # Feature status: mark target=triggered, disable sl+trailSL
                trigger_leg_feature(db._db, trade_id, leg_id, 'target', current_price, now_ts)
                disable_leg_features(db._db, trade_id, leg_id, except_feature='target', reason='target_triggered', timestamp=now_ts)
                reentry_cfg = get_reentry_tp_config(leg_cfg)
                if reentry_cfg:
                    leg_cfg_with_id = dict(leg_cfg, id=leg_id)
                    result = _handle_reentry(db, trade, leg_cfg_with_id, reentry_cfg, leg_id, now_ts)
                    if result:
                        actions_taken.append(f'{trade_id}/{leg_id}: {result}')
                        re_type = str(reentry_cfg.get('Type') or '')
                        re_kind = ('lazy' if 'NextLeg' in re_type else
                                   'immediate' if 'Immediate' in re_type else
                                   'at_cost' if 'AtCost' in re_type else 'like_original')
                        new_leg_id = str((reentry_cfg.get('Value') or {}).get('NextLegRef') or '')
                        record_reentry_queued(db._db, trade, now_ts, leg_id, re_kind, new_leg_id, re_type, reason='target')
                continue

            # ── Trail SL moved — rotate feature records ───────────────────
            if sl_price and sl_price != stored_sl:
                update_leg_sl_in_db(db, trade_id, leg_index, sl_price, current_price, leg_id=leg_id)
                if stored_sl and sl_price != stored_sl:
                    record_trail_sl_changed(
                        db._db, trade, leg, now_ts, stored_sl, sl_price, current_price,
                        trail_config=trail_config,
                    )
                    # Feature status: disable old SL+trailSL records, insert fresh
                    # pending records with the updated SL price (step-by-step audit trail)
                    rotate_trail_sl_record(
                        db._db, trade_id, leg_id,
                        old_sl_price=stored_sl,
                        new_sl_price=sl_price,
                        current_option_price=current_price,
                        trail_config=trail_config,
                        timestamp=now_ts,
                    )

            # ── Snapshot ──────────────────────────────────────────────────
            strat_entry = strategy_map.setdefault(trade_id, {
                'trade_id': trade_id,
                'strategy_id': str(trade.get('strategy_id') or ''),
                'strategy_name': str(trade.get('name') or ''),
                'ticker': underlying,
                'open_positions': [],
                'total_pnl': 0.0,
            })
            pos_payload = {
                'leg_id': leg_id,
                'pnl': round(pnl, 2),
                'current_price': current_price,
                'ltp': current_price,
                'entry_price': entry_price,
                'quantity': effective_quantity,
                'lots': quantity,
                'lot_size': lot_size,
            }
            strat_entry['open_positions'].append(pos_payload)
            strat_entry['total_pnl'] = round(strat_entry['total_pnl'] + pnl, 2)
            open_positions.append(pos_payload)

        # ── Overall SL / Target / Trail / Lock checks ──────────────────────
        if not past_exit:
            trade_strat_entry   = strategy_map.get(trade_id)
            strategy_cfg        = trade.get('strategy') or cfg
            osl_type, osl_val   = parse_overall_sl(strategy_cfg)
            otgt_type, otgt_val = parse_overall_tgt(strategy_cfg)
            current_trade_mtm, legs_pnl_snapshot = _compute_strategy_mtm_snapshot(
                db,
                trade_id,
                now_ts,
                (trade_strat_entry or {}).get('open_positions', []),
            )
            open_leg_ids = {p['leg_id'] for p in (trade_strat_entry or {}).get('open_positions', [])}

            def _bt_close_remaining(exit_reason: str) -> None:
                for _leg in legs:
                    if str(_leg.get('id') or '') not in open_leg_ids:
                        continue
                    if int(_leg.get('status') or 0) != OPEN_LEG_STATUS:
                        continue
                    _lid = str(_leg.get('id') or '')
                    _idx = next((i for i, l in enumerate(legs) if str(l.get('id') or '') == _lid), 0)
                    _cd  = get_chain_at_time(chain_col, underlying, str(_leg.get('expiry_date') or ''), _leg.get('strike'), str(_leg.get('option') or ''), now_ts, market_cache=market_cache)
                    _ep  = _safe_float(_cd.get('close'), _leg.get('last_saw_price', 0.0))
                    close_leg_in_db(db, trade_id, _idx, _ep, exit_reason, now_ts, leg_id=_lid)
                    try:
                        from features.notification_manager import record_force_exit, disable_leg_features
                        record_force_exit(db._db, trade, _leg, now_ts, _ep, exit_reason=exit_reason)
                        disable_leg_features(db._db, trade_id, _lid, reason=exit_reason, timestamp=now_ts)
                    except Exception as _e:
                        log.warning('bt notification force_exit(%s) error: %s', exit_reason, _e)

            _peak_mtm = _safe_float(trade.get('peak_mtm'), current_trade_mtm)
            _new_peak  = max(_peak_mtm, current_trade_mtm)
            _ore_type, _ore_count = parse_overall_reentry_sl(strategy_cfg)
            _ort_type, _ort_count = parse_overall_reentry_tgt(strategy_cfg)
            _ore_done = int(trade.get('overall_sl_reentry_done') or 0)
            _ort_done = int(trade.get('overall_tgt_reentry_done') or 0)
            _effective_sl = _resolve_overall_cycle_value(osl_val, _ore_done)
            _effective_tgt = _resolve_overall_cycle_value(otgt_val, _ort_done)
            _dyn_sl    = _safe_float(trade.get('current_overall_sl_threshold'), _effective_sl or osl_val)
            _sync_overall_strategy_feature_status(
                db, trade, now_ts,
                current_mtm=current_trade_mtm,
                overall_sl_done=_ore_done,
                overall_tgt_done=_ort_done,
            )
            print(
                '[OVERALL CHECK]',
                {
                    'mode': 'backtest',
                    'trade_id': trade_id,
                    'strategy_name': str(trade.get('name') or ''),
                    'timestamp': now_ts,
                    'current_mtm': round(current_trade_mtm, 2),
                    'overall_sl_type': osl_type,
                    'overall_sl_base': round(_safe_float(osl_val), 2),
                    'overall_sl_current': _effective_sl,
                    'overall_sl_reentry_type': _ore_type,
                    'overall_sl_reentry_done': _ore_done,
                    'overall_sl_reentry_count': _ore_count,
                    'overall_tgt_type': otgt_type,
                    'overall_tgt_base': round(_safe_float(otgt_val), 2),
                    'overall_tgt_current': _effective_tgt,
                    'overall_tgt_reentry_type': _ort_type,
                    'overall_tgt_reentry_done': _ort_done,
                    'overall_tgt_reentry_count': _ort_count,
                    'dynamic_sl_threshold': round(_safe_float(_dyn_sl), 2),
                },
            )

            _osl_hit = (
                osl_type != 'None'
                and _safe_float(_effective_sl) > 0
                and current_trade_mtm <= -_safe_float(_effective_sl)
            ) or (
                _dyn_sl > 0 and current_trade_mtm <= -_dyn_sl
            )
            # Guard: skip if no open legs (prevents duplicate events on next ticks)
            if _osl_hit and not open_leg_ids:
                _osl_hit = False

            if _osl_hit:
                _set_overall_feature_status_state(
                    db, trade_id, 'overall_sl',
                    enabled=False, status='triggered', timestamp=now_ts,
                    current_mtm=current_trade_mtm, disabled_reason='overall_sl_hit',
                )
                _set_overall_feature_status_state(
                    db, trade_id, 'overall_target',
                    enabled=False, status='disabled', timestamp=now_ts,
                    current_mtm=current_trade_mtm, disabled_reason='overall_sl_hit',
                )
                _disable_trade_feature_rows_for_new_cycle(
                    db,
                    trade_id,
                    reason='overall_sl_cycle_completed',
                    timestamp=now_ts,
                )
                _bt_close_remaining('overall_sl')
                actions_taken.append(f'{trade_id}: overall SL hit mtm={current_trade_mtm}')
                try:
                    from features.notification_manager import record_overall_sl_hit
                    record_overall_sl_hit(
                        db._db, trade, now_ts, osl_type, _effective_sl, current_trade_mtm, legs_pnl_snapshot,
                        cycle_number=_ore_done + 1,
                        configured_reentry_count=_ore_count,
                    )
                except Exception as _e:
                    log.warning('bt notification overall_sl_hit error: %s', _e)
                if _ore_type != 'None' and _ore_count > 0 and _ore_done < _ore_count:
                    _new_ids = _requeue_original_legs_bt(db, trade, trade_id, strategy_cfg, now_ts)
                    if _new_ids:
                        _next_cycle_sl = _resolve_overall_cycle_value(osl_val, _ore_done + 1)
                        db._db['algo_trades'].update_one(
                            {'_id': trade_id},
                            {'$set': {
                                'overall_sl_reentry_done': _ore_done + 1,
                                'peak_mtm': 0.0,
                                'current_overall_sl_threshold': _next_cycle_sl,
                            }},
                        )
                        _sync_overall_strategy_feature_status(
                            db, trade, now_ts,
                            current_mtm=current_trade_mtm,
                            overall_sl_done=_ore_done + 1,
                            overall_tgt_done=_ort_done,
                        )
                        print(
                            '[OVERALL REENTRY QUEUED]',
                            {
                                'mode': 'backtest',
                                'trade_id': trade_id,
                                'reason': 'overall_sl',
                                'reentry_type': _ore_type,
                                'reentry_done': _ore_done + 1,
                                'reentry_count': _ore_count,
                                'next_cycle_number': _ore_done + 2,
                                'next_overall_sl_value': _resolve_next_overall_cycle_value(osl_val, _ore_done),
                                'next_overall_tgt_value': _effective_tgt,
                                'legs_queued': _new_ids,
                            },
                        )
                        try:
                            from features.notification_manager import record_overall_reentry_queued
                            record_overall_reentry_queued(
                                db._db, trade, now_ts, 'overall_sl', _ore_type, _ore_count, _new_ids,
                                completed_reentries=_ore_done + 1,
                                next_cycle_number=_ore_done + 2,
                                next_overall_sl_value=_resolve_next_overall_cycle_value(osl_val, _ore_done),
                                next_overall_tgt_value=_effective_tgt,
                            )
                        except Exception as _e:
                            log.warning('bt notification overall_reentry_sl error: %s', _e)

            elif (
                open_leg_ids
                and otgt_type != 'None'
                and _safe_float(_effective_tgt) > 0
                and current_trade_mtm >= _safe_float(_effective_tgt)
            ):
                _set_overall_feature_status_state(
                    db, trade_id, 'overall_target',
                    enabled=False, status='triggered', timestamp=now_ts,
                    current_mtm=current_trade_mtm, disabled_reason='overall_target_hit',
                )
                _set_overall_feature_status_state(
                    db, trade_id, 'overall_sl',
                    enabled=False, status='disabled', timestamp=now_ts,
                    current_mtm=current_trade_mtm, disabled_reason='overall_target_hit',
                )
                _disable_trade_feature_rows_for_new_cycle(
                    db,
                    trade_id,
                    reason='overall_target_cycle_completed',
                    timestamp=now_ts,
                )
                _bt_close_remaining('overall_target')
                actions_taken.append(f'{trade_id}: overall Target hit mtm={current_trade_mtm}')
                try:
                    from features.notification_manager import record_overall_target_hit
                    record_overall_target_hit(
                        db._db, trade, now_ts, otgt_type, _effective_tgt, current_trade_mtm, legs_pnl_snapshot,
                        cycle_number=_ort_done + 1,
                        configured_reentry_count=_ort_count,
                    )
                except Exception as _e:
                    log.warning('bt notification overall_tgt_hit error: %s', _e)
                if _ort_type != 'None' and _ort_count > 0 and _ort_done < _ort_count:
                    _new_ids = _requeue_original_legs_bt(db, trade, trade_id, strategy_cfg, now_ts)
                    if _new_ids:
                        _reset_sl_threshold = _resolve_overall_cycle_value(osl_val, _ore_done)
                        db._db['algo_trades'].update_one(
                            {'_id': trade_id},
                            {'$set': {
                                'overall_tgt_reentry_done': _ort_done + 1,
                                'peak_mtm': 0.0,
                                'current_overall_sl_threshold': _reset_sl_threshold,
                            }},
                        )
                        _sync_overall_strategy_feature_status(
                            db, trade, now_ts,
                            current_mtm=current_trade_mtm,
                            overall_sl_done=_ore_done,
                            overall_tgt_done=_ort_done + 1,
                        )
                        print(
                            '[OVERALL REENTRY QUEUED]',
                            {
                                'mode': 'backtest',
                                'trade_id': trade_id,
                                'reason': 'overall_target',
                                'reentry_type': _ort_type,
                                'reentry_done': _ort_done + 1,
                                'reentry_count': _ort_count,
                                'next_cycle_number': _ort_done + 2,
                                'next_overall_sl_value': _effective_sl,
                                'next_overall_tgt_value': _resolve_next_overall_cycle_value(otgt_val, _ort_done),
                                'legs_queued': _new_ids,
                            },
                        )
                        try:
                            from features.notification_manager import record_overall_reentry_queued
                            record_overall_reentry_queued(
                                db._db, trade, now_ts, 'overall_target', _ort_type, _ort_count, _new_ids,
                                completed_reentries=_ort_done + 1,
                                next_cycle_number=_ort_done + 2,
                                next_overall_sl_value=_effective_sl,
                                next_overall_tgt_value=_resolve_next_overall_cycle_value(otgt_val, _ort_done),
                            )
                        except Exception as _e:
                            log.warning('bt notification overall_reentry_tgt error: %s', _e)
            elif open_leg_ids:
                _lock_cfg  = parse_lock_and_trail(strategy_cfg)
                _lock_exit, _lock_floor = check_lock_and_trail(_lock_cfg, current_trade_mtm, _new_peak)
                if _lock_exit:
                    _bt_close_remaining('lock_and_trail')
                    actions_taken.append(f'{trade_id}: LockAndTrail exit mtm={current_trade_mtm}')
                    try:
                        from features.notification_manager import record_lock_and_trail_exit
                        record_lock_and_trail_exit(db._db, trade, now_ts, _lock_floor, current_trade_mtm, legs_pnl_snapshot)
                    except Exception as _e:
                        log.warning('bt notification lock_and_trail error: %s', _e)
                else:
                    trail_type, for_every, trail_by = parse_overall_trail_sl(strategy_cfg)
                    if trail_type != 'None' and for_every > 0:
                        _old_thresh = _safe_float(trade.get('current_overall_sl_threshold'), osl_val)
                        _new_thresh = update_overall_trail_sl(for_every, trail_by, osl_val, _new_peak)
                        if _new_thresh < _old_thresh:
                            db._db['algo_trades'].update_one({'_id': trade_id}, {'$set': {'peak_mtm': _new_peak, 'current_overall_sl_threshold': _new_thresh}})
                            actions_taken.append(f'{trade_id}: overall trail SL {_old_thresh}->{_new_thresh}')
                            try:
                                from features.notification_manager import record_overall_trail_sl_changed
                                record_overall_trail_sl_changed(db._db, trade, now_ts, _old_thresh, _new_thresh, _new_peak, current_trade_mtm)
                            except Exception as _e:
                                log.warning('bt notification overall_trail_sl error: %s', _e)
                        elif _new_peak > _peak_mtm:
                            db._db['algo_trades'].update_one({'_id': trade_id}, {'$set': {'peak_mtm': _new_peak}})
                    elif _new_peak > _peak_mtm:
                        db._db['algo_trades'].update_one({'_id': trade_id}, {'$set': {'peak_mtm': _new_peak}})
        else:
            _mark_trade_squared_off_at_exit_time(db, trade_id)

    return {'actions_taken': actions_taken, 'checked_at': now_ts, 'total_open': len(open_positions)}


def _requeue_original_legs_bt(db, trade: dict, trade_id: str, strategy_cfg: dict, now_ts: str) -> list[str]:
    """Re-queue original legs for overall reentry in backtest mode."""
    original_cfgs = strategy_cfg.get('ListOfLegConfigs') or []
    if not original_cfgs:
        original_cfgs = list((_resolve_trade_leg_configs(trade) or {}).values())
    new_ids: list[str] = []
    existing_ids = {str(l.get('id') or '') for l in (trade.get('legs') or []) if isinstance(l, dict)}
    ts_sfx = now_ts.replace(':', '').replace('T', '').replace('-', '')[:14]
    for lc in original_cfgs:
        if not isinstance(lc, dict):
            continue
        orig_id = str(lc.get('id') or '')
        if not orig_id:
            continue
        new_id  = f'{orig_id}-ore-{ts_sfx}'
        if new_id in existing_ids:
            continue
        new_leg = _build_pending_leg(
            new_id,
            lc,
            trade,
            now_ts,
            f'overall_{orig_id}',
            leg_type=f'{orig_id}-overall_reentry',
        )
        new_leg.update({
            'is_lazy': True,
            'is_reentered_leg': True,
            'lazy_leg_ref': orig_id,
            'parent_leg_id': orig_id,
        })
        try:
            db._db['algo_trades'].update_one({'_id': trade_id}, {'$push': {'legs': new_leg}})
            new_ids.append(new_id)
        except Exception as _e:
            log.warning('bt requeue leg error: %s', _e)
    return new_ids


# ─── Core: live-mode minute tick ──────────────────────────────────────────────

def _live_minute_tick(db: MongoData, trade_date: str) -> dict:
    """
    Called every 60 s in live mode.
    1. Loads running strategies from algo_trades.
    2. For pending legs: attempts entry from latest option_chain.
    3. For open legs: fetches latest option_chain, checks SL/TP/exit_time.
    4. Executes exits + queues re-entries / lazy legs in the DB.
    5. Returns the full position snapshot (only open positions with chain data).
    """
    algo_trades_col = db._db['algo_trades']
    chain_col = db._db[OPTION_CHAIN_COLLECTION]
    now_ts = _now_iso()
    now_time = _now_ist_str()   # HH:MM for comparing with entry/exit times

    trade_query = _build_trade_query(
        trade_date,
        activation_mode='live',
        statuses=[RUNNING_STATUS],
    )
    running_trades = list(algo_trades_col.find(trade_query))

    open_positions = []
    strategy_map: dict[str, dict] = {}
    actions_taken: list[str] = []
    position_entry_events: list[dict] = []

    for trade in running_trades:
        trade_id = str(trade.get('_id') or '')
        underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
        cfg = trade.get('config') or {}

        # Merge all leg configs (original + idle)
        all_leg_configs = _resolve_trade_leg_configs(trade)

        # Parse entry/exit time bounds  (format: "YYYY-MM-DD HH:MM:SS" or "HH:MM:SS")
        raw_entry_time = str(trade.get('entry_time') or '')
        raw_exit_time = str(trade.get('exit_time') or '')
        entry_time_hhmm = raw_entry_time[11:16] if len(raw_entry_time) >= 16 else raw_entry_time[:5]
        exit_time_hhmm = raw_exit_time[11:16] if len(raw_exit_time) >= 16 else raw_exit_time[:5]

        past_exit = bool(exit_time_hhmm and now_time >= exit_time_hhmm)
        before_entry = bool(entry_time_hhmm and now_time < entry_time_hhmm)

        try:
            lot_size = db.get_lot_size(trade_date, underlying)
        except Exception:
            lot_size = 75

        refreshed_trade = trade
        created_original_legs, refreshed_trade = _queue_original_legs_if_needed(db, refreshed_trade, now_ts)
        if created_original_legs:
            trade = refreshed_trade

        # ── Process momentum-pending legs from algo_leg_feature_status ────────
        if not before_entry:
            _entered_momentum_ids = _process_momentum_pending_feature_legs(
                db, trade, chain_col, trade_date, now_ts, lot_size
            )
            if _entered_momentum_ids:
                trade = db._db['algo_trades'].find_one({'_id': trade_id}) or trade
                actions_taken.extend(
                    f'{trade_id}/{lid}: momentum_pending entered' for lid in _entered_momentum_ids
                )

        legs = [leg for leg in (trade.get('legs') or []) if isinstance(leg, dict)]
        _dict_leg_ids_live = {str(l.get('id') or '') for l in legs if l.get('id')}
        _has_str_refs_live = any(isinstance(item, str) for item in (trade.get('legs') or []))
        if _has_str_refs_live:
            _hist_col_live = db._db['algo_trade_positions_history']
            for _hdoc_live in _hist_col_live.find({'trade_id': trade_id, 'status': OPEN_LEG_STATUS}):
                _hleg_id_live = str(_hdoc_live.get('leg_id') or _hdoc_live.get('id') or '')
                if not _hleg_id_live or _hleg_id_live in _dict_leg_ids_live:
                    continue
                _raw_exp_live = str(_hdoc_live.get('expiry_date') or '')
                _hdoc_live['expiry_date'] = _raw_exp_live[:10] if ' ' in _raw_exp_live else _raw_exp_live
                _hdoc_live['id'] = _hleg_id_live
                legs.append(_hdoc_live)

        for leg_index, leg in enumerate(legs):
            leg_status = int(leg.get('status') or 0)
            leg_id  = str(leg.get('id') or '')
            leg_cfg = _resolve_leg_cfg(leg_id, leg, all_leg_configs)

            # ── Force-exit all open legs if past exit_time ─────────────────
            if past_exit and leg_status == OPEN_LEG_STATUS:
                # Fetch last price for the exit
                strike = leg.get('strike')
                expiry_date = str(leg.get('expiry_date') or '')
                option_type = str(leg.get('option') or '')
                chain_doc = get_latest_chain_doc(chain_col, underlying, expiry_date, strike, option_type, trade_date)
                exit_price = _safe_float(chain_doc.get('close'), leg.get('last_saw_price', 0.0))
                close_leg_in_db(db, trade_id, leg_index, exit_price, 'exit_time', now_ts, leg_id=leg_id)
                actions_taken.append(f'{trade_id}/{leg_id}: force-exit at {exit_price} (exit_time)')
                try:
                    from features.notification_manager import record_force_exit, disable_leg_features
                    record_force_exit(db._db, trade, leg, now_ts, exit_price, exit_reason='exit_time')
                    disable_leg_features(db._db, trade_id, leg_id, reason='force_exit', timestamp=now_ts)
                except Exception as _nfe:
                    log.warning('notification force_exit error: %s', _nfe)
                continue

            # ── Skip pending legs before entry_time ───────────────────────
            if before_entry:
                continue

            # ── Try to enter pending (lazy/re-entry) legs ─────────────────
            is_pending = (leg_status == OPEN_LEG_STATUS
                          and (leg.get('is_lazy') or leg.get('entry_trade') is None))
            if is_pending:
                entered, _ = _try_enter_pending_leg(
                    db, trade, leg, leg_index, chain_col, trade_date, now_ts, lot_size
                )
                if entered:
                    actions_taken.append(f'{trade_id}/{leg_id}: lazy/re-entry entered')
                    # Re-fetch the updated leg for PnL snapshot
                    refreshed = algo_trades_col.find_one({'_id': trade_id})
                    if refreshed:
                        trade = refreshed
                        _refreshed_legs = [l for l in (refreshed.get('legs') or []) if isinstance(l, dict)]
                        _leg_id_to_find = str(leg.get('id') or '')
                        leg = next((l for l in _refreshed_legs if str(l.get('id') or '') == _leg_id_to_find), leg)
                        inserted, history_doc = _store_position_history(db, refreshed, leg)
                        if inserted and history_doc:
                            position_entry_events.append({
                                'trade_id': history_doc['trade_id'],
                                'leg_id': history_doc['leg_id'],
                                'entry_timestamp': history_doc['entry_timestamp'],
                                'message': 'Position entry stored successfully',
                                'history': history_doc,
                            })
                else:
                    continue

            # ── Active open legs: SL/TP check ─────────────────────────────
            if leg_status != OPEN_LEG_STATUS or not (leg.get('entry_trade')):
                continue

            entry_trade = leg.get('entry_trade') or {}
            entry_price = _safe_float(entry_trade.get('price'))
            quantity = _safe_int(leg.get('quantity') or entry_trade.get('quantity'))
            lot_size = _safe_int(leg.get('lot_size'), 1)
            effective_quantity = max(0, quantity) * max(1, lot_size)
            strike = leg.get('strike')
            expiry_date = str(leg.get('expiry_date') or '')
            option_type = str(leg.get('option') or '')
            position_str = str(leg.get('position') or '')
            is_sell_pos = _is_sell(position_str)

            # Fetch latest option_chain
            chain_doc = get_latest_chain_doc(chain_col, underlying, expiry_date, strike, option_type, trade_date)
            current_price = _safe_float(chain_doc.get('close'), _safe_float(leg.get('last_saw_price')))
            spot_price = _safe_float(chain_doc.get('spot_price'))

            sl_config = leg_cfg.get('LegStopLoss') or {}
            tp_config = leg_cfg.get('LegTarget') or {}
            trail_config = sl_config.get('Trail') or {}

            # Compute / update SL price
            initial_sl = _safe_float(leg.get('initial_sl_value')) or calc_sl_price(entry_price, is_sell_pos, sl_config)
            stored_sl = _safe_float(leg.get('current_sl_price')) or None
            sl_price = stored_sl or initial_sl
            if sl_price and trail_config:
                sl_price = update_trail_sl(
                    entry_price,
                    current_price,
                    sl_price,
                    is_sell_pos,
                    trail_config,
                    initial_sl=initial_sl,
                )

            tp_price = calc_tp_price(entry_price, is_sell_pos, tp_config)
            pnl = (
                (entry_price - current_price) * effective_quantity
                if is_sell_pos else
                (current_price - entry_price) * effective_quantity
            )

            token = make_token(underlying, expiry_date, strike, option_type)

            print(
                '[FEATURE CHECK]',
                f'listen_time={now_ts}',
                f'trade_id={trade_id}',
                f'leg_id={leg_id}',
                f'strategy_name={str(trade.get("name") or "-")}',
                f'option={option_type}',
                f'position={position_str}',
                f'entry_price={entry_price}',
                f'ltp={current_price}',
                f'stored_sl={stored_sl}',
                f'next_sl={sl_price}',
                f'tp={tp_price}',
                f'sl_enabled={bool(sl_price)}',
                f'tg_enabled={bool(tp_price)}',
                f'tsl_enabled={bool(trail_config)}',
            )

            from features.notification_manager import (
                record_sl_hit, record_target_hit,
                record_trail_sl_changed, record_reentry_queued, record_force_exit,
                trigger_leg_feature, disable_leg_features, rotate_trail_sl_record,
            )

            # ── SL hit ────────────────────────────────────────────────────
            if is_sl_hit(current_price, sl_price, is_sell_pos):
                close_leg_in_db(db, trade_id, leg_index, current_price, 'stoploss', now_ts, leg_id=leg_id)
                actions_taken.append(f'{trade_id}/{leg_id}: SL hit @ {current_price} (sl={sl_price})')
                record_sl_hit(db._db, trade, leg, now_ts, current_price, sl_price or 0.0)
                # Feature status: mark sl=triggered, disable target+trailSL
                trigger_leg_feature(db._db, trade_id, leg_id, 'sl', current_price, now_ts)
                disable_leg_features(db._db, trade_id, leg_id, except_feature='sl', reason='sl_triggered', timestamp=now_ts)
                reentry_cfg = get_reentry_sl_config(leg_cfg)
                if reentry_cfg:
                    leg_cfg_with_id = dict(leg_cfg)
                    leg_cfg_with_id['id'] = leg_id
                    result = _handle_reentry(db, trade, leg_cfg_with_id, reentry_cfg, leg_id, now_ts)
                    if result:
                        actions_taken.append(f'{trade_id}/{leg_id}: {result}')
                        re_type = str(reentry_cfg.get('Type') or '')
                        re_kind = ('lazy' if 'NextLeg' in re_type else
                                   'immediate' if 'Immediate' in re_type else
                                   'at_cost' if 'AtCost' in re_type else 'like_original')
                        new_leg_id = str((reentry_cfg.get('Value') or {}).get('NextLegRef') or '')
                        record_reentry_queued(db._db, trade, now_ts, leg_id, re_kind, new_leg_id, re_type, reason='sl')
                continue  # leg is now closed

            # ── TP hit ────────────────────────────────────────────────────
            if is_tp_hit(current_price, tp_price, is_sell_pos):
                close_leg_in_db(db, trade_id, leg_index, current_price, 'target', now_ts, leg_id=leg_id)
                actions_taken.append(f'{trade_id}/{leg_id}: TP hit @ {current_price} (tp={tp_price})')
                record_target_hit(db._db, trade, leg, now_ts, current_price, tp_price or 0.0)
                # Feature status: mark target=triggered, disable sl+trailSL
                trigger_leg_feature(db._db, trade_id, leg_id, 'target', current_price, now_ts)
                disable_leg_features(db._db, trade_id, leg_id, except_feature='target', reason='target_triggered', timestamp=now_ts)
                reentry_cfg = get_reentry_tp_config(leg_cfg)
                if reentry_cfg:
                    leg_cfg_with_id = dict(leg_cfg)
                    leg_cfg_with_id['id'] = leg_id
                    result = _handle_reentry(db, trade, leg_cfg_with_id, reentry_cfg, leg_id, now_ts)
                    if result:
                        actions_taken.append(f'{trade_id}/{leg_id}: {result}')
                        re_type = str(reentry_cfg.get('Type') or '')
                        re_kind = ('lazy' if 'NextLeg' in re_type else
                                   'immediate' if 'Immediate' in re_type else
                                   'at_cost' if 'AtCost' in re_type else 'like_original')
                        new_leg_id = str((reentry_cfg.get('Value') or {}).get('NextLegRef') or '')
                        record_reentry_queued(db._db, trade, now_ts, leg_id, re_kind, new_leg_id, re_type, reason='target')
                continue  # leg is now closed

            # ── Trail SL moved — rotate feature records ───────────────────
            if sl_price and sl_price != stored_sl:
                update_leg_sl_in_db(db, trade_id, leg_index, sl_price, current_price, leg_id=leg_id)
                if stored_sl and sl_price != stored_sl:
                    record_trail_sl_changed(
                        db._db, trade, leg, now_ts, stored_sl, sl_price, current_price,
                        trail_config=trail_config,
                    )
                    # Feature status: disable old SL+trailSL records, insert fresh
                    # pending records with the updated SL price (step-by-step audit trail)
                    rotate_trail_sl_record(
                        db._db, trade_id, leg_id,
                        old_sl_price=stored_sl,
                        new_sl_price=sl_price,
                        current_option_price=current_price,
                        trail_config=trail_config,
                        timestamp=now_ts,
                    )

            # ── Build position snapshot ────────────────────────────────────
            position_payload = {
                'strategy_id': str(trade.get('strategy_id') or ''),
                'strategy_name': str(trade.get('name') or ''),
                'trade_id': trade_id,
                'leg_id': leg_id,
                'symbol': str(leg.get('symbol') or token),
                'token': token,
                'position': position_str,
                'option': option_type,
                'strike': strike,
                'expiry_date': expiry_date,
                'entry_price': entry_price,
                'current_price': current_price,
                'quantity': effective_quantity,
                'lots': quantity,
                'lot_size': lot_size,
                'pnl': round(pnl, 2),
                'sl_price': sl_price,
                'tp_price': tp_price,
                'entry_timestamp': str(entry_trade.get('traded_timestamp') or entry_trade.get('trigger_timestamp') or ''),
                'snapshot': {
                    'timestamp': chain_doc.get('timestamp') or now_ts,
                    'underlying': chain_doc.get('underlying') or underlying,
                    'expiry': chain_doc.get('expiry') or expiry_date,
                    'strike': chain_doc.get('strike', strike),
                    'type': chain_doc.get('type') or option_type,
                    'close': current_price,
                    'oi': chain_doc.get('oi'),
                    'iv': chain_doc.get('iv'),
                    'delta': chain_doc.get('delta'),
                    'gamma': chain_doc.get('gamma'),
                    'theta': chain_doc.get('theta'),
                    'vega': chain_doc.get('vega'),
                    'rho': chain_doc.get('rho'),
                    'spot_price': spot_price,
                },
            }
            open_positions.append(position_payload)

            strat_entry = strategy_map.setdefault(trade_id, {
                'trade_id': trade_id,
                'strategy_id': str(trade.get('strategy_id') or ''),
                'strategy_name': str(trade.get('name') or ''),
                'ticker': underlying,
                'entry_time': entry_time_hhmm,
                'exit_time': exit_time_hhmm,
                'open_positions': [],
                'total_pnl': 0.0,
            })
            strat_entry['open_positions'].append(position_payload)
            strat_entry['total_pnl'] = round(strat_entry['total_pnl'] + pnl, 2)

        # ── Overall SL / Target / Trail SL check (per strategy) ───────────
        if not past_exit:
            trade_strat_entry = strategy_map.get(trade_id)
            strategy_cfg = trade.get('strategy') or cfg
            osl_type, osl_val = parse_overall_sl(strategy_cfg)
            otgt_type, otgt_val = parse_overall_tgt(strategy_cfg)

            current_trade_mtm, legs_pnl_snapshot = _compute_strategy_mtm_snapshot(
                db,
                trade_id,
                now_ts,
                (trade_strat_entry or {}).get('open_positions', []),
            )
            # Leg IDs still open (not yet closed by individual SL/TP above)
            open_leg_ids = {p['leg_id'] for p in (trade_strat_entry or {}).get('open_positions', [])}

            def _close_remaining_open_legs(exit_reason: str) -> None:
                for _leg in legs:
                    if str(_leg.get('id') or '') not in open_leg_ids:
                        continue
                    if int(_leg.get('status') or 0) != OPEN_LEG_STATUS:
                        continue
                    _leg_id = str(_leg.get('id') or '')
                    _leg_idx = next(
                        (i for i, l in enumerate(legs) if str(l.get('id') or '') == _leg_id), 0
                    )
                    _cdoc = get_latest_chain_doc(
                        chain_col, underlying,
                        str(_leg.get('expiry_date') or ''),
                        _leg.get('strike'),
                        str(_leg.get('option') or ''),
                        trade_date,
                    )
                    _ep = _safe_float(_cdoc.get('close'), _leg.get('last_saw_price', 0.0))
                    close_leg_in_db(db, trade_id, _leg_idx, _ep, exit_reason, now_ts, leg_id=_leg_id)
                    try:
                        from features.notification_manager import record_force_exit, disable_leg_features
                        record_force_exit(db._db, trade, _leg, now_ts, _ep, exit_reason=exit_reason)
                        disable_leg_features(db._db, trade_id, _leg_id, reason=exit_reason, timestamp=now_ts)
                    except Exception as _nfe:
                        log.warning('notification force_exit(%s) error: %s', exit_reason, _nfe)

            # Track peak MTM for trail/lock features
            _peak_mtm = _safe_float(trade.get('peak_mtm'), current_trade_mtm)
            _new_peak = max(_peak_mtm, current_trade_mtm)

            # ── Helper: re-queue all original legs (OverallReentry) ───────────
            def _requeue_original_legs(reason: str) -> list[str]:
                """Push original leg configs back as pending legs. Returns new leg IDs."""
                _original_cfgs = strategy_cfg.get('ListOfLegConfigs') or []
                if not _original_cfgs:
                    _original_cfgs = list((_resolve_trade_leg_configs(trade) or {}).values())
                _new_ids: list[str] = []
                _existing_ids = {
                    str(l.get('id') or '') for l in (trade.get('legs') or []) if isinstance(l, dict)
                }
                _ts_sfx = now_ts.replace(':', '').replace('T', '').replace('-', '')[:14]
                for _lc in _original_cfgs:
                    if not isinstance(_lc, dict):
                        continue
                    _orig_id = str(_lc.get('id') or '')
                    if not _orig_id:
                        continue
                    _new_id  = f'{_orig_id}-ore-{_ts_sfx}'
                    if _new_id in _existing_ids:
                        continue
                    _new_leg = _build_pending_leg(
                        _new_id,
                        _lc,
                        trade,
                        now_ts,
                        f'overall_{reason}_{_orig_id}',
                        leg_type=f'{_orig_id}-overall_reentry',
                    )
                    _new_leg.update({
                        'is_lazy': True,
                        'is_reentered_leg': True,
                        'lazy_leg_ref': _orig_id,
                        'parent_leg_id': _orig_id,
                    })
                    try:
                        db._db['algo_trades'].update_one(
                            {'_id': trade_id}, {'$push': {'legs': _new_leg}}
                        )
                        _new_ids.append(_new_id)
                    except Exception as _pe:
                        log.warning('overall reentry push error: %s', _pe)
                return _new_ids

            # ── Check overall SL ──────────────────────────────────────────────
            _ore_type, _ore_count = parse_overall_reentry_sl(strategy_cfg)
            _ort_type, _ort_count = parse_overall_reentry_tgt(strategy_cfg)
            _ore_done = int(trade.get('overall_sl_reentry_done') or 0)
            _ort_done = int(trade.get('overall_tgt_reentry_done') or 0)
            _effective_sl = _resolve_overall_cycle_value(osl_val, _ore_done)
            _effective_tgt = _resolve_overall_cycle_value(otgt_val, _ort_done)
            _osl_triggered = (
                osl_type != 'None'
                and _safe_float(_effective_sl) > 0
                and current_trade_mtm <= -_safe_float(_effective_sl)
            )

            # Also check dynamic SL threshold from overall trail SL
            _dyn_sl_threshold = _safe_float(
                trade.get('current_overall_sl_threshold'),
                _effective_sl or osl_val,
            )
            _sync_overall_strategy_feature_status(
                db, trade, now_ts,
                current_mtm=current_trade_mtm,
                overall_sl_done=_ore_done,
                overall_tgt_done=_ort_done,
            )
            print(
                '[OVERALL CHECK]',
                {
                    'mode': 'live',
                    'trade_id': trade_id,
                    'strategy_name': str(trade.get('name') or ''),
                    'timestamp': now_ts,
                    'current_mtm': round(current_trade_mtm, 2),
                    'overall_sl_type': osl_type,
                    'overall_sl_base': round(_safe_float(osl_val), 2),
                    'overall_sl_current': _effective_sl,
                    'overall_sl_reentry_type': _ore_type,
                    'overall_sl_reentry_done': _ore_done,
                    'overall_sl_reentry_count': _ore_count,
                    'overall_tgt_type': otgt_type,
                    'overall_tgt_base': round(_safe_float(otgt_val), 2),
                    'overall_tgt_current': _effective_tgt,
                    'overall_tgt_reentry_type': _ort_type,
                    'overall_tgt_reentry_done': _ort_done,
                    'overall_tgt_reentry_count': _ort_count,
                    'dynamic_sl_threshold': round(_safe_float(_dyn_sl_threshold), 2),
                },
            )
            if not _osl_triggered and _dyn_sl_threshold > 0 and current_trade_mtm <= -_dyn_sl_threshold:
                _osl_triggered = True

            # Guard: if no legs are open, these events have already fired — skip.
            if _osl_triggered and not open_leg_ids:
                _osl_triggered = False

            if _osl_triggered:
                _set_overall_feature_status_state(
                    db, trade_id, 'overall_sl',
                    enabled=False, status='triggered', timestamp=now_ts,
                    current_mtm=current_trade_mtm, disabled_reason='overall_sl_hit',
                )
                _set_overall_feature_status_state(
                    db, trade_id, 'overall_target',
                    enabled=False, status='disabled', timestamp=now_ts,
                    current_mtm=current_trade_mtm, disabled_reason='overall_sl_hit',
                )
                _disable_trade_feature_rows_for_new_cycle(
                    db,
                    trade_id,
                    reason='overall_sl_cycle_completed',
                    timestamp=now_ts,
                )
                _close_remaining_open_legs('overall_sl')
                actions_taken.append(f'{trade_id}: overall SL hit mtm={current_trade_mtm}')
                try:
                    from features.notification_manager import record_overall_sl_hit
                    record_overall_sl_hit(
                        db._db, trade, now_ts,
                        osl_type, _effective_sl, current_trade_mtm, legs_pnl_snapshot,
                        cycle_number=_ore_done + 1,
                        configured_reentry_count=_ore_count,
                    )
                except Exception as _e:
                    log.warning('notification overall_sl_hit error: %s', _e)
                # ── OverallReentrySL ─────────────────────────────────────────
                if _ore_type != 'None' and _ore_count > 0 and _ore_done < _ore_count:
                    _new_leg_ids = _requeue_original_legs('sl')
                    if _new_leg_ids:
                        _next_cycle_sl = _resolve_overall_cycle_value(osl_val, _ore_done + 1)
                        db._db['algo_trades'].update_one(
                            {'_id': trade_id},
                            {'$set': {
                                'overall_sl_reentry_done': _ore_done + 1,
                                'peak_mtm': 0.0,
                                'current_overall_sl_threshold': _next_cycle_sl,
                            }},
                        )
                        _sync_overall_strategy_feature_status(
                            db, trade, now_ts,
                            current_mtm=current_trade_mtm,
                            overall_sl_done=_ore_done + 1,
                            overall_tgt_done=_ort_done,
                        )
                        actions_taken.append(
                            f'{trade_id}: OverallReentrySL queued legs={_new_leg_ids}'
                        )
                        print(
                            '[OVERALL REENTRY QUEUED]',
                            {
                                'mode': 'live',
                                'trade_id': trade_id,
                                'reason': 'overall_sl',
                                'reentry_type': _ore_type,
                                'reentry_done': _ore_done + 1,
                                'reentry_count': _ore_count,
                                'next_cycle_number': _ore_done + 2,
                                'next_overall_sl_value': _resolve_next_overall_cycle_value(osl_val, _ore_done),
                                'next_overall_tgt_value': _effective_tgt,
                                'legs_queued': _new_leg_ids,
                            },
                        )
                        try:
                            from features.notification_manager import record_overall_reentry_queued
                            record_overall_reentry_queued(
                                db._db, trade, now_ts, reason='overall_sl',
                                reentry_type=_ore_type, count=_ore_count,
                                legs_queued=_new_leg_ids,
                                completed_reentries=_ore_done + 1,
                                next_cycle_number=_ore_done + 2,
                                next_overall_sl_value=_resolve_next_overall_cycle_value(osl_val, _ore_done),
                                next_overall_tgt_value=_effective_tgt,
                            )
                        except Exception as _e:
                            log.warning('notification overall_reentry_queued error: %s', _e)

            elif (
                open_leg_ids
                and otgt_type != 'None'
                and _safe_float(_effective_tgt) > 0
                and current_trade_mtm >= _safe_float(_effective_tgt)
            ):
                _set_overall_feature_status_state(
                    db, trade_id, 'overall_target',
                    enabled=False, status='triggered', timestamp=now_ts,
                    current_mtm=current_trade_mtm, disabled_reason='overall_target_hit',
                )
                _set_overall_feature_status_state(
                    db, trade_id, 'overall_sl',
                    enabled=False, status='disabled', timestamp=now_ts,
                    current_mtm=current_trade_mtm, disabled_reason='overall_target_hit',
                )
                _disable_trade_feature_rows_for_new_cycle(
                    db,
                    trade_id,
                    reason='overall_target_cycle_completed',
                    timestamp=now_ts,
                )
                _close_remaining_open_legs('overall_target')
                actions_taken.append(f'{trade_id}: overall Target hit mtm={current_trade_mtm}')
                try:
                    from features.notification_manager import record_overall_target_hit
                    record_overall_target_hit(
                        db._db, trade, now_ts,
                        otgt_type, _effective_tgt, current_trade_mtm, legs_pnl_snapshot,
                        cycle_number=_ort_done + 1,
                        configured_reentry_count=_ort_count,
                    )
                except Exception as _e:
                    log.warning('notification overall_tgt_hit error: %s', _e)
                # ── OverallReentryTgt ────────────────────────────────────────
                if _ort_type != 'None' and _ort_count > 0 and _ort_done < _ort_count:
                    _new_leg_ids = _requeue_original_legs('target')
                    if _new_leg_ids:
                        _reset_sl_threshold = _resolve_overall_cycle_value(osl_val, _ore_done)
                        db._db['algo_trades'].update_one(
                            {'_id': trade_id},
                            {'$set': {
                                'overall_tgt_reentry_done': _ort_done + 1,
                                'peak_mtm': 0.0,
                                'current_overall_sl_threshold': _reset_sl_threshold,
                            }},
                        )
                        _sync_overall_strategy_feature_status(
                            db, trade, now_ts,
                            current_mtm=current_trade_mtm,
                            overall_sl_done=_ore_done,
                            overall_tgt_done=_ort_done + 1,
                        )
                        actions_taken.append(
                            f'{trade_id}: OverallReentryTgt queued legs={_new_leg_ids}'
                        )
                        print(
                            '[OVERALL REENTRY QUEUED]',
                            {
                                'mode': 'live',
                                'trade_id': trade_id,
                                'reason': 'overall_target',
                                'reentry_type': _ort_type,
                                'reentry_done': _ort_done + 1,
                                'reentry_count': _ort_count,
                                'next_cycle_number': _ort_done + 2,
                                'next_overall_sl_value': _effective_sl,
                                'next_overall_tgt_value': _resolve_next_overall_cycle_value(otgt_val, _ort_done),
                                'legs_queued': _new_leg_ids,
                            },
                        )
                        try:
                            from features.notification_manager import record_overall_reentry_queued
                            record_overall_reentry_queued(
                                db._db, trade, now_ts, reason='overall_target',
                                reentry_type=_ort_type, count=_ort_count,
                                legs_queued=_new_leg_ids,
                                completed_reentries=_ort_done + 1,
                                next_cycle_number=_ort_done + 2,
                                next_overall_sl_value=_effective_sl,
                                next_overall_tgt_value=_resolve_next_overall_cycle_value(otgt_val, _ort_done),
                            )
                        except Exception as _e:
                            log.warning('notification overall_reentry_tgt_queued error: %s', _e)

            elif open_leg_ids:
                # ── LockAndTrail ─────────────────────────────────────────────
                _lock_cfg = parse_lock_and_trail(strategy_cfg)
                _lock_exit, _lock_floor = check_lock_and_trail(_lock_cfg, current_trade_mtm, _new_peak)
                if _lock_exit:
                    _close_remaining_open_legs('lock_and_trail')
                    actions_taken.append(
                        f'{trade_id}: LockAndTrail exit mtm={current_trade_mtm} floor={_lock_floor}'
                    )
                    try:
                        from features.notification_manager import record_lock_and_trail_exit
                        record_lock_and_trail_exit(
                            db._db, trade, now_ts,
                            floor=_lock_floor, current_mtm=current_trade_mtm,
                            legs_pnl=legs_pnl_snapshot,
                        )
                    except Exception as _e:
                        log.warning('notification lock_and_trail_exit error: %s', _e)

                else:
                    # ── Overall Trail SL ──────────────────────────────────────
                    trail_type, for_every, trail_by = parse_overall_trail_sl(strategy_cfg)
                    if trail_type != 'None' and for_every > 0:
                        initial_osl = osl_val
                        old_sl_threshold = _safe_float(
                            trade.get('current_overall_sl_threshold'), initial_osl
                        )
                        new_sl_threshold = update_overall_trail_sl(
                            for_every, trail_by, initial_osl, _new_peak
                        )
                        if new_sl_threshold < old_sl_threshold:
                            db._db['algo_trades'].update_one(
                                {'_id': trade_id},
                                {'$set': {
                                    'peak_mtm': _new_peak,
                                    'current_overall_sl_threshold': new_sl_threshold,
                                }},
                            )
                            actions_taken.append(
                                f'{trade_id}: overall trail SL {old_sl_threshold}->{new_sl_threshold}'
                            )
                            try:
                                from features.notification_manager import record_overall_trail_sl_changed
                                record_overall_trail_sl_changed(
                                    db._db, trade, now_ts,
                                    old_sl_threshold, new_sl_threshold, _new_peak, current_trade_mtm,
                                )
                            except Exception as _e:
                                log.warning('notification overall_trail_sl_changed error: %s', _e)
                        elif _new_peak > _peak_mtm:
                            db._db['algo_trades'].update_one(
                                {'_id': trade_id},
                                {'$set': {'peak_mtm': _new_peak}},
                            )
                    elif _new_peak > _peak_mtm:
                        # Update peak even without trail SL (needed for LockAndTrail)
                        db._db['algo_trades'].update_one(
                            {'_id': trade_id},
                            {'$set': {'peak_mtm': _new_peak}},
                        )
        else:
            _mark_trade_squared_off_at_exit_time(db, trade_id)

    return {
        'trade_date': trade_date,
        'checked_at': now_ts,
        'current_ist_time': now_time,
        'running_strategies': list(strategy_map.values()),
        'open_positions': open_positions,
        'actions_taken': actions_taken,
        'position_entry_events': position_entry_events,
        'total_running': len(running_trades),
        'total_open_legs': len(open_positions),
    }


# ─── Sync already-entered legs to history ────────────────────────────────────

def _sync_entered_legs_to_history(db: MongoData, records: list[dict]) -> list[dict]:
    """
    For trades whose legs already have entry_trade data (imported/pre-filled),
    store each entered leg to algo_trade_positions_history and push _id to algo_trades.legs.
    Idempotent — duplicate entries are skipped by _store_position_history.
    Returns list of synced entries.
    """
    synced: list[dict] = []
    for record in (records or []):
        trade_id = str(record.get('_id') or '')
        if not trade_id:
            continue
        trade = db._db['algo_trades'].find_one({'_id': trade_id})
        if not trade:
            continue
        legs = [leg for leg in (trade.get('legs') or []) if isinstance(leg, dict)]
        for leg in legs:
            entry_trade = leg.get('entry_trade')
            if not isinstance(entry_trade, dict) or not entry_trade:
                continue  # not yet entered
            leg_id_val = str(leg.get('id') or '')
            inserted, history_doc = _store_position_history(db, trade, leg)
            # Always remove the original dict leg — replaced by string history _id
            if leg_id_val:
                try:
                    db._db['algo_trades'].update_one(
                        {'_id': trade_id},
                        {'$pull': {'legs': {'id': leg_id_val}}},
                    )
                except Exception as exc:
                    log.error('_sync_entered_legs_to_history pull dict leg error trade=%s leg=%s: %s', trade_id, leg_id_val, exc)
            if inserted and history_doc:
                print(
                    f'[HISTORY SYNC] trade={trade_id} '
                    f'leg={history_doc["leg_id"]} '
                    f'leg_type={leg.get("leg_type") or ""} '
                    f'entry={history_doc["entry_timestamp"]} '
                    f'history_id={history_doc["_id"]}'
                )
                synced.append({
                    'trade_id': trade_id,
                    'leg_id': history_doc['leg_id'],
                    'history_id': history_doc['_id'],
                    'entry_timestamp': history_doc['entry_timestamp'],
                })
        _validate_trade_leg_storage(db, trade_id)
    return synced


# ─── Backtest entry executor ──────────────────────────────────────────────────

def _execute_backtest_entries(
    db: MongoData,
    records: list[dict],
    listen_time: str,
    listen_timestamp: str,
    market_cache: dict | None = None,
) -> list[dict]:
    """
    For each strategy whose entry_time matches listen_time, ensure legs are
    queued and attempt to fill any pending (not yet entered) legs.
    Returns a list of dicts describing each entry action taken.
    """
    trade_date = listen_timestamp[:10] if listen_timestamp and len(listen_timestamp) >= 10 else ''
    chain_col = db._db[OPTION_CHAIN_COLLECTION]
    entries_executed: list[dict] = []

    for record in (records or []):
        record_entry_time = _extract_hhmm(record.get('entry_time') or '')
        trade_id = str(record.get('_id') or '')
        if not trade_id:
            continue

        entry_time_match = (record_entry_time == listen_time)

        # Catch-up: entry time already passed and trade has never been entered
        # (e.g. strategy activated while listen_time is ahead of entry_time)
        entry_time_past = (
            record_entry_time
            and listen_time
            and listen_time > record_entry_time
        )
        if entry_time_past and not entry_time_match:
            # Check if any legs have been entered for this trade in history
            try:
                history_col = db._db['algo_trade_positions_history']
                already_entered = history_col.count_documents({'trade_id': trade_id, 'status': OPEN_LEG_STATUS}) > 0
            except Exception:
                already_entered = True  # safe: don't double-enter on error
            if already_entered:
                continue
            # No legs entered yet but listen_time > entry_time → instant catch-up entry
            catch_up = True
        else:
            catch_up = False

        if not entry_time_match and not catch_up:
            continue

        print(
            f'[BACKTEST ENTRY TRIGGER] trade_id={trade_id} '
            f'group_name={record.get("group_name") or ""} '
            f'strategy_name={record.get("name") or ""} '
            f'entry_time={record_entry_time or "--:--"} '
            f'listen_time={listen_time} '
            f'triggering_previous_entry_execution=true'
            + (' catch_up=true' if catch_up else '')
        )

        # Re-fetch latest trade state from DB
        trade = db._db['algo_trades'].find_one({'_id': trade_id})
        if not trade:
            continue

        # Ensure original legs exist in DB
        created, trade = _queue_original_legs_if_needed(db, trade, listen_timestamp)
        if created:
            print(
                f'[BACKTEST ENTRY] trade_id={trade_id} '
                f'strategy={record.get("name") or ""} '
                f'entry_time={record_entry_time or "--:--"} '
                'original_legs_queued=true'
            )

        underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
        try:
            lot_size = db.get_lot_size(trade_date, underlying)
        except Exception:
            lot_size = 75

        legs = [leg for leg in (trade.get('legs') or []) if isinstance(leg, dict)]
        for leg_index, leg in enumerate(legs):
            leg_status = int(leg.get('status') or 0)
            if leg_status != OPEN_LEG_STATUS:
                continue
            # Pending = open leg with no entry_trade yet
            entry_trade = leg.get('entry_trade')
            if entry_trade is not None:
                continue

            entered, reason = _try_enter_pending_leg(
                db,
                trade,
                leg,
                leg_index,
                chain_col,
                trade_date,
                listen_timestamp,
                lot_size,
                snapshot_timestamp=listen_timestamp,
                market_cache=market_cache,
            )
            leg_id = str(leg.get('id') or str(leg_index))
            print(
                f'[BACKTEST ENTRY] trade_id={trade_id} '
                f'strategy={record.get("name") or ""} '
                f'entry_time={record_entry_time or "--:--"} '
                f'leg_id={leg_id} '
                f'entered={entered} '
                f'reason={reason or "-"} '
                f'listen_time={listen_time}'
            )
            if not entered:
                import json as _json
                underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
                option_type = str(leg.get('option') or 'CE')
                print(f'[ENTRY FAILED DEBUG] ── Failed Leg Object ──')
                print(_json.dumps(leg, indent=2, default=str))
                print(f'[ENTRY FAILED DEBUG] ── Trade Legs entry_trade details ──')
                for _i, _l in enumerate(trade.get('legs') or []):
                    if isinstance(_l, dict):
                        print(f'  leg[{_i}] id={_l.get("id")} option={_l.get("option")} strike={_l.get("strike")} entry_trade={_json.dumps(_l.get("entry_trade"), default=str)}')
                    else:
                        print(f'  leg[{_i}] => history_ref (string): {_l}')
                print(f'[ENTRY FAILED DEBUG] ── Option Chain at timestamp={listen_timestamp} underlying={underlying} option={option_type} ──')
                try:
                    _chain_docs = list(chain_col.find(
                        {'underlying': underlying, 'type': option_type, 'timestamp': listen_timestamp},
                        {'_id': 0, 'strike': 1, 'expiry': 1, 'close': 1, 'open': 1, 'timestamp': 1, 'token': 1}
                    ).sort('strike', 1).limit(20))
                    if _chain_docs:
                        for _cd in _chain_docs:
                            print(f'  chain: strike={_cd.get("strike")} expiry={_cd.get("expiry")} close={_cd.get("close")} token={_cd.get("token")}')
                    else:
                        print(f'  [NO CHAIN DATA FOUND] underlying={underlying} option={option_type} timestamp={listen_timestamp}')
                        _nearby = chain_col.find_one(
                            {'underlying': underlying, 'type': option_type, 'timestamp': {'$lte': listen_timestamp}},
                            sort=[('timestamp', -1)]
                        )
                        print(f'  nearest available chain doc: {_json.dumps(_nearby, indent=2, default=str) if _nearby else "None"}')
                except Exception as _ce:
                    print(f'  [CHAIN QUERY ERROR] {_ce}')
            entries_executed.append({
                'trade_id': trade_id,
                'strategy_name': record.get('name') or '',
                'group_name': record.get('group_name') or '',
                'entry_time': record_entry_time or '',
                'leg_id': leg_id,
                'entered': entered,
                'reason': reason or '',
                'listen_time': listen_time,
                'listen_timestamp': listen_timestamp,
            })

    return entries_executed


def run_backtest_simulation_step(
    db: MongoData,
    listen_timestamp: str,
    activation_mode: str = 'algo-backtest',
) -> dict:
    """
    Run one backtest simulation step for a fixed listen timestamp.

    Reuses the same backend conditions as websocket autorun:
      - original leg queue
      - pending leg entry / simple momentum / lazy leg activation
      - SL / target / trail SL / overall checks
      - active leg LTP snapshot
      - open position snapshot
    """
    normalized_timestamp = str(listen_timestamp or '').strip()
    if not normalized_timestamp:
        return {
            'listen_timestamp': '',
            'listen_time': '',
            'trade_date': '',
            'records': [],
            'entry_snapshots': [],
            'entries_executed': [],
            'actions_taken': [],
            'ltp': [],
            'open_positions': [],
            'active_leg_tokens': [],
            'count': 0,
            'open_positions_count': 0,
        }

    trade_date = normalized_timestamp[:10] if len(normalized_timestamp) >= 10 else ''
    listen_time = normalized_timestamp[11:19] if len(normalized_timestamp) >= 19 else normalized_timestamp[11:16]
    listen_hhmm = listen_time[:5]

    # ── Load trades + warm market cache ────────────────────────────────────────
    # Market cache is intentionally NOT cleared at the end of this function.
    # Consecutive calls for the same date hit the in-memory cache (milliseconds).
    # The cache is keyed by (date, underlyings) inside preload_market_data_cache
    # so switching dates automatically gets a fresh key.
    records = _load_running_trade_records(db, trade_date, activation_mode=activation_mode)
    market_cache = _preload_trade_date_market_cache(db, trade_date, records)

    entry_snapshots = build_entry_spot_snapshots(
        db,
        records,
        listen_hhmm,
        normalized_timestamp,
        market_cache=market_cache,
    )

    # ── Entry execution ─────────────────────────────────────────────────────────
    entries_executed: list[dict] = []
    if records:
        entries_executed = _execute_backtest_entries(
            db,
            records,
            listen_hhmm,
            normalized_timestamp,
            market_cache=market_cache,
        )
        if entries_executed:
            synced_trade_ids: dict[str, dict] = {}
            for executed_entry in entries_executed:
                if not executed_entry.get('entered'):
                    continue
                synced_trade_id = str(executed_entry.get('trade_id') or '').strip()
                if synced_trade_id:
                    synced_trade_ids[synced_trade_id] = {'_id': synced_trade_id}
            if synced_trade_ids:
                _sync_entered_legs_to_history(db, list(synced_trade_ids.values()))
                for synced_trade_id in synced_trade_ids:
                    _validate_trade_leg_storage(db, synced_trade_id)
            # Reload only when entries actually happened
            records = _load_running_trade_records(db, trade_date, activation_mode=activation_mode)

    _set_listening_strategy_snapshot(trade_date, records, activation_mode=activation_mode)
    _set_listening_time_state(
        trade_date,
        activation_mode=activation_mode,
        listen_time=listen_time,
        listen_timestamp=normalized_timestamp,
    )

    # ── SL / Target / TrailSL / Overall checks + simple momentum pending ────────
    # _backtest_minute_tick and _extract_running_positions must load their own
    # trades from DB.  _load_running_trade_records filters to RUNNING_STATUS +
    # active_on_server=True only; those functions need BOTH RUNNING_STATUS and
    # BACKTEST_IMPORT_STATUS trades.  Passing the wrong subset caused SL/TG/TSL
    # and momentum checks to silently skip backtest-import trades.
    # The market cache is already warm at this point so all chain/spot lookups
    # inside these functions are O(1) memory hits — no extra MongoDB round-trips.
    bt_tick_result = _backtest_minute_tick(
        db,
        trade_date,
        normalized_timestamp,
        activation_mode=activation_mode,
        market_cache=market_cache,
        # running_trades intentionally NOT passed — let it load with the correct
        # status filter ([RUNNING_STATUS, BACKTEST_IMPORT_STATUS]).
    )

    # ── Reload after tick (SL/Target may have closed/changed legs) ─────────────
    records = _load_running_trade_records(db, trade_date, activation_mode=activation_mode)
    _set_listening_strategy_snapshot(trade_date, records, activation_mode=activation_mode)

    # ── LTP for active + momentum-pending contracts ─────────────────────────────
    active_contracts = _build_active_contracts_from_records(
        records,
        db=db,
        trade_date=trade_date,
        market_cache=market_cache,
    )
    _append_momentum_pending_to_contracts(active_contracts, db, records)

    chain_col = db._db[OPTION_CHAIN_COLLECTION]
    index_spot_col = db._db['option_chain_index_spot']
    option_contracts = [c for c in active_contracts if c.get('option') != 'SPOT']
    spot_contracts   = [c for c in active_contracts if c.get('option') == 'SPOT']

    option_ticks = _fetch_active_leg_ticks(
        chain_col, option_contracts, normalized_timestamp, market_cache=market_cache,
    )
    option_ltp = [_format_chain_tick_as_ltp(t) for t in option_ticks]
    spot_ltp   = _fetch_spot_ticks(
        index_spot_col, spot_contracts, normalized_timestamp, market_cache=market_cache,
    )

    # ── Open position snapshot + simple momentum pending check ─────────────────
    # _extract_running_positions calls _process_momentum_pending_feature_legs for
    # every trade (simple momentum entry check).  It must also use its own DB load
    # so that BACKTEST_IMPORT_STATUS trades are included.
    position_snapshot = _extract_running_positions(
        db,
        trade_date,
        listen_hhmm,
        include_position_snapshots=True,
        running_trades=None,    # load with correct status filter inside
        market_cache=market_cache,
    )
    # NOTE: market_cache is intentionally kept alive here for the next call.

    return {
        'listen_timestamp': normalized_timestamp,
        'listen_time': listen_time,
        'trade_date': trade_date,
        'records': records,
        'entry_snapshots': entry_snapshots,
        'entries_executed': entries_executed,
        'actions_taken': list(bt_tick_result.get('actions_taken') or []),
        'ltp': option_ltp + spot_ltp,
        'open_positions': list(position_snapshot.get('open_positions') or []),
        'active_leg_tokens': list(position_snapshot.get('active_leg_tokens') or []),
        'count': len(records),
        'open_positions_count': len(position_snapshot.get('open_positions') or []),
    }


def build_backtest_simulation_socket_messages(
    db: MongoData,
    result: dict,
) -> dict[str, str]:
    """
    Build all WebSocket messages for one backtest simulation step.
    Called exactly once per step (the result is passed to broadcast_backtest_simulation_step
    to avoid a second, redundant _populate_legs_from_history call).
    """
    normalized_records = _populate_legs_from_history(db, result.get('records') or [])
    selected_date    = str(result.get('trade_date') or '')
    listen_time      = str(result.get('listen_time') or '')
    listen_timestamp = str(result.get('listen_timestamp') or '')
    entries_executed = result.get('entries_executed') or []

    # ── execute-orders: full strategy records + executed order details ──────────
    execute_order_payload = _build_execute_order_socket_payload(
        normalized_records,
        trigger='manual-simulator',
    )
    # Attach entries_executed (the actual order fills) so the frontend gets
    # entry_price, quantity, timestamps etc. for every order placed this tick.
    execute_order_payload['entries_executed'] = entries_executed
    execute_order_payload['entries_count']    = len(entries_executed)

    return {
        'executions': _build_message(
            'countdown_update',
            'Execution listening time updated',
            {
                'selected_date': selected_date,
                'listen_time': listen_time,
                'listen_timestamp': listen_timestamp,
                'records': normalized_records,
                'count': len(normalized_records),
                'entry_snapshots': result.get('entry_snapshots') or [],
                'entries_executed': entries_executed,
                'mode': 'backtest',
            },
        ),
        'execute-orders': _build_message(
            'execute_order',
            'Strategy execution refreshed',
            execute_order_payload,
        ),
        'update': _build_message(
            'update',
            'Open position snapshot refreshed',
            {
                'trade_date': selected_date,
                'activation_mode': 'algo-backtest',
                'status': 'algo-backtest',
                'trigger_reason': 'manual_simulator_run',
                'refresh_status': 'completed',
                'open_positions': result.get('open_positions') or [],
                'count': result.get('open_positions_count') or 0,
            },
        ),
        # LTP goes to the update channel so the frontend receives it alongside
        # open-position data.  Kept as a separate message type so the frontend
        # can route it independently (ltp_update handler).
        'ltp_update': _build_message(
            'ltp_update',
            'LTP update for active positions',
            {
                'trade_date': selected_date,
                'listen_time': listen_time,
                'listen_timestamp': listen_timestamp,
                'ltp': result.get('ltp') or [],
            },
        ),
    }


async def broadcast_backtest_simulation_step(
    db: MongoData,
    result: dict,
    prebuilt_messages: dict[str, str] | None = None,
) -> dict[str, int]:
    """
    Broadcast all simulation step messages to connected WebSocket clients.

    Pass prebuilt_messages (from build_backtest_simulation_socket_messages) to
    avoid rebuilding them a second time — _populate_legs_from_history is expensive.
    """
    messages = prebuilt_messages or build_backtest_simulation_socket_messages(db, result)
    delivered_execute_orders = await _broadcast_channel_message('execute-orders', messages.get('execute-orders') or '')
    delivered_update         = await _broadcast_channel_message('update',         messages.get('update') or '')
    delivered_ltp            = await _broadcast_channel_message('update',         messages.get('ltp_update') or '')
    delivered_executions     = await _broadcast_channel_message('executions',     messages.get('executions') or '')
    return {
        'execute-orders': delivered_execute_orders,
        'update': delivered_update + delivered_ltp,
        'executions': delivered_executions,
    }


# ─── WebSocket handler ────────────────────────────────────────────────────────

@socket_router.websocket('/ws/executions')
async def executions_socket(websocket: WebSocket):
    await websocket.accept()
    _register_channel_websocket('executions', websocket)
    await websocket.send_text(_build_message(
        'connection_established',
        'Algo execution websocket connected',
        {'channel': 'executions'},
    ))
    listening_active = False
    selected_date = ''
    simulated_minutes = MARKET_START_MINUTES
    listening_records: list[dict] = []
    market_cache: dict | None = None
    db = MongoData()

    try:
        while True:
            try:
                raw_message = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
            except asyncio.TimeoutError:
                if listening_active:
                    listen_time = _format_market_time_from_minutes(simulated_minutes)
                    listen_timestamp = (
                        f'{selected_date}T{listen_time}:00'
                        if selected_date
                        else listen_time
                    )
                    _set_listening_time_state(
                        selected_date,
                        activation_mode='algo-backtest',
                        listen_time=listen_time,
                        listen_timestamp=listen_timestamp,
                    )

                    # ── Market close at 15:30 → stop listening ────────────────
                    if simulated_minutes >= MARKET_CLOSE_MINUTES:
                        listening_active = False
                        _clear_listening_strategy_snapshot(selected_date, activation_mode='algo-backtest')
                        await websocket.send_text(_build_message(
                            'listening_completed',
                            'Market closed at 15:30 — backtest listening ended',
                            {
                                'selected_date': selected_date,
                                'listen_time': listen_time,
                                'listen_timestamp': listen_timestamp,
                                'mode': 'backtest',
                            },
                        ))
                        continue

                    print(f"[EXECUTION SOCKET] current_time={listen_timestamp}")
                    for record in listening_records:
                        rec_entry_raw = str(record.get('entry_time') or '').strip()
                        rec_entry_hhmm = rec_entry_raw[11:16] if len(rec_entry_raw) >= 16 else rec_entry_raw[:5]
                        # Determine per-strategy status
                        legs = record.get('legs') if isinstance(record.get('legs'), list) else []
                        has_entry = any(
                            isinstance(leg, dict) and isinstance(leg.get('entry_trade'), dict) and leg.get('entry_trade')
                            for leg in legs
                        )
                        if has_entry:
                            entry_status = 'position_taking'
                        elif listen_time == rec_entry_hhmm:
                            entry_status = 'checking_position'
                        elif rec_entry_hhmm and listen_time < rec_entry_hhmm:
                            entry_status = 'waiting_for_entry'
                        else:
                            entry_status = 'pending_entry'
                        print(
                            "[EXECUTION SOCKET] "
                            f"group_name={record.get('group_name') or ''} "
                            f"ticker={record.get('underlying') or ''} "
                            f"strategy_name={record.get('name') or ''} "
                            f"entry_time={rec_entry_hhmm or '-'} "
                            f"status={entry_status}"
                        )
                        await websocket.send_text(_build_message(
                            'strategy_tick',
                            'Strategy tick update',
                            {
                                'group_name':    record.get('group_name') or '',
                                'ticker':        record.get('underlying') or '',
                                'strategy_name': record.get('name') or '',
                                'entry_time':    rec_entry_hhmm or '',
                                'listen_time':   listen_time,
                                'entry_status':  entry_status,
                            },
                        ))

                    # ── Build entry snapshots (strategy entry_time == listen_time) ──
                    entry_snapshots = build_entry_spot_snapshots(
                        db,
                        listening_records,
                        listen_time,
                        listen_timestamp,
                        market_cache=market_cache,
                    )
                    for snapshot in entry_snapshots:
                        print(
                            "[EXECUTION SOCKET] "
                            f"group_name={snapshot['group_name']} "
                            f"strategy_name={snapshot['strategy_name']} "
                            f"entry_time={snapshot['entry_time']} "
                            f"underlying={snapshot['underlying']} "
                            f"spot_price={snapshot['spot_price']} "
                            f"atm_price={snapshot['atm_price']}"
                        )
                        for option_chain in snapshot.get('option_chain') or []:
                            print(
                                "[EXECUTION SOCKET] "
                                f"leg_id={option_chain.get('leg_id') or ''} "
                                f"position={option_chain.get('position') or ''} "
                                f"expiry_kind={option_chain.get('expiry_kind') or ''} "
                                f"expiry={option_chain.get('expiry') or ''} "
                                f"option_type={option_chain.get('option_type') or ''} "
                                f"strike={option_chain.get('strike') or 0} "
                                f"close={option_chain.get('close') or 0} "
                                f"timestamp={option_chain.get('timestamp') or ''}"
                            )

                    # ── Execute entries for matched strategies ─────────────────
                    entries_executed: list[dict] = []
                    if entry_snapshots and selected_date:
                        entries_executed = _execute_backtest_entries(
                            db,
                            listening_records,
                            listen_time,
                            listen_timestamp,
                            market_cache=market_cache,
                        )
                        if entries_executed:
                            synced_trade_ids = {}
                            for executed_entry in entries_executed:
                                if not executed_entry.get('entered'):
                                    continue
                                synced_trade_ids[str(executed_entry.get('trade_id') or '').strip()] = {
                                    '_id': str(executed_entry.get('trade_id') or '').strip(),
                                }
                            if synced_trade_ids:
                                _sync_entered_legs_to_history(db, list(synced_trade_ids.values()))
                                for synced_trade_id in synced_trade_ids:
                                    _validate_trade_leg_storage(db, synced_trade_id)
                            # Refresh listening_records from DB after entry writes
                            refreshed_cursor = db._db['algo_trades'].find({
                                'activation_mode': 'algo-backtest',
                                'active_on_server': True,
                                'trade_status': 1,
                                'status': RUNNING_STATUS,
                                'creation_ts': {'$regex': f'^{re.escape(selected_date)}'},
                            }).sort('creation_ts', 1)
                            refreshed_records = []
                            for item in refreshed_cursor:
                                refreshed_records.append({
                                    '_id': str(item.get('_id') or ''),
                                    'strategy_id': str(item.get('strategy_id') or ''),
                                    'name': str(item.get('name') or ''),
                                    'status': str(item.get('status') or ''),
                                    'trade_status': int(item.get('trade_status') or 0),
                                    'active_on_server': bool(item.get('active_on_server')),
                                    'creation_ts': str(item.get('creation_ts') or ''),
                                    'entry_time': str(item.get('entry_time') or ''),
                                    'exit_time': str(item.get('exit_time') or ''),
                                    'group_name': str(((item.get('portfolio') or {}).get('group_name') or '')),
                                    'underlying': str(
                                        ((item.get('config') or {}).get('Ticker'))
                                        or item.get('ticker')
                                        or ''
                                    ),
                                    'config': item.get('config') if isinstance(item.get('config'), dict) else {},
                                    'portfolio': item.get('portfolio') if isinstance(item.get('portfolio'), dict) else {},
                                    'legs': item.get('legs') if isinstance(item.get('legs'), list) else [],
                                })
                            listening_records = refreshed_records
                            _set_listening_strategy_snapshot(selected_date, refreshed_records)

                    _set_listening_strategy_snapshot(selected_date, listening_records)
                    await websocket.send_text(_build_message(
                        'countdown_update',
                        'Execution listening time updated',
                        {
                            'selected_date': selected_date,
                            'listen_time': listen_time,
                            'listen_timestamp': listen_timestamp,
                            'records': listening_records,
                            'count': len(listening_records),
                            'entry_snapshots': entry_snapshots,
                            'entries_executed': entries_executed,
                            'mode': 'backtest',
                        },
                    ))
                    simulated_minutes += 1
                else:
                    print(f"[EXECUTION SOCKET] current_time={_now_iso()}")
                continue

            try:
                parsed_message = json.loads(raw_message)
            except json.JSONDecodeError:
                parsed_message = {'raw': raw_message}

            action = str((parsed_message or {}).get('action') or '').strip().lower()

            # ── subscribe_executions ───────────────────────────────────────
            if action == 'subscribe_executions':
                await websocket.send_text(_build_message(
                    'subscription_ack',
                    'Execution websocket subscription confirmed',
                    parsed_message,
                ))
                continue

            if action == 'start_live_monitor':
                _clear_market_cache_snapshot(market_cache)
                market_cache = None
                snapshot = {
                    'listen_time': _now_iso(),
                    'trade_date': str((parsed_message or {}).get('trade_date') or '').strip(),
                    'mode': 'live',
                }
                await websocket.send_text(_build_message(
                    'live_monitor_started',
                    'Live monitor started',
                    snapshot,
                ))
                continue

            if action == 'stop_live_monitor':
                await websocket.send_text(_build_message(
                    'live_monitor_stopped',
                    'Live monitor stopped',
                    {'listen_time': _now_iso(), 'mode': 'live'},
                ))
                continue

            if action == 'start_listening':
                _clear_market_cache_snapshot(market_cache)
                selected_date = str((parsed_message or {}).get('selected_date') or '').strip()
                status_tag = str((parsed_message or {}).get('status') or '').strip()
                behind_time = max(0, _safe_int((parsed_message or {}).get('behind_time')))
                simulated_minutes = max(0, MARKET_START_MINUTES - behind_time)
                listen_time = _format_market_time_from_minutes(simulated_minutes)
                listen_timestamp = (
                    f'{selected_date}T{listen_time}:00'
                    if selected_date
                    else listen_time
                )
                listening_active = True
                initial_records = []
                if selected_date:
                    cursor = db._db['algo_trades'].find({
                        'activation_mode': 'algo-backtest',
                        'active_on_server': True,
                        'trade_status': 1,
                        'status': RUNNING_STATUS,
                        'creation_ts': {'$regex': f'^{re.escape(selected_date)}'},
                    }).sort('creation_ts', 1)
                    for item in cursor:
                        initial_records.append({
                            '_id': str(item.get('_id') or ''),
                            'strategy_id': str(item.get('strategy_id') or ''),
                            'name': str(item.get('name') or ''),
                            'status': str(item.get('status') or ''),
                            'trade_status': int(item.get('trade_status') or 0),
                            'active_on_server': bool(item.get('active_on_server')),
                            'creation_ts': str(item.get('creation_ts') or ''),
                            'entry_time': str(item.get('entry_time') or ''),
                            'exit_time': str(item.get('exit_time') or ''),
                            'group_name': str(((item.get('portfolio') or {}).get('group_name') or '')),
                            'underlying': str(
                                ((item.get('config') or {}).get('Ticker'))
                                or item.get('ticker')
                                or ''
                            ),
                            'config': item.get('config') if isinstance(item.get('config'), dict) else {},
                            'portfolio': item.get('portfolio') if isinstance(item.get('portfolio'), dict) else {},
                            'legs': item.get('legs') if isinstance(item.get('legs'), list) else [],
                        })
                listening_records = initial_records
                _set_listening_strategy_snapshot(selected_date, initial_records)
                if _is_algo_backtest_status(status_tag):
                    market_cache = _preload_trade_date_market_cache(db, selected_date, initial_records)
                    if market_cache:
                        print(
                            '[BACKTEST MARKET DATA READY] '
                            f'trade_date={selected_date} '
                            f'underlyings={",".join((market_cache or {}).get("underlyings") or []) or "-"} '
                            'option_chain_and_spot_loaded=true'
                        )
                else:
                    market_cache = None
                payload = {
                    'selected_date': selected_date,
                    'behind_time': behind_time,
                    'remaining_minutes': (parsed_message or {}).get('remaining_minutes'),
                    'listen_time': listen_time,
                    'listen_timestamp': listen_timestamp,
                    'records': initial_records,
                    'count': len(initial_records),
                    'mode': 'backtest',
                }
                _set_listening_time_state(
                    selected_date,
                    activation_mode='algo-backtest',
                    listen_time=listen_time,
                    listen_timestamp=listen_timestamp,
                )
                await websocket.send_text(_build_message(
                    'listening_started',
                    'Execution listening started',
                    payload,
                ))
                continue

            if action == 'stop_listening':
                listening_active = False
                listening_records = []
                _clear_listening_strategy_snapshot(selected_date)
                _clear_market_cache_snapshot(market_cache)
                market_cache = None
                await websocket.send_text(_build_message(
                    'listening_stopped',
                    'Execution listening stopped',
                    {
                        'selected_date': selected_date,
                        'listen_time': _format_market_time_from_minutes(simulated_minutes),
                        'listen_timestamp': (
                            f'{selected_date}T{_format_market_time_from_minutes(simulated_minutes)}:00'
                            if selected_date
                            else _format_market_time_from_minutes(simulated_minutes)
                        ),
                        'mode': 'backtest',
                    },
                ))
                _clear_listening_strategy_snapshot(selected_date, activation_mode='algo-backtest')
                continue

            await websocket.send_text(_build_message(
                'test_response',
                'Execution websocket test response from backend',
                parsed_message,
            ))

    except WebSocketDisconnect:
        return
    finally:
        _unregister_channel_websocket('executions', websocket)
        _clear_listening_strategy_snapshot(selected_date)
        _clear_market_cache_snapshot(market_cache)
        db.close()


@socket_router.websocket('/ws/execute-orders')
async def execute_orders_socket(websocket: WebSocket):
    await websocket.accept()
    _register_channel_websocket('execute-orders', websocket)
    await websocket.send_text(_build_message(
        'connection_established',
        'Execute orders websocket connected',
        {'channel': 'execute-orders'},
    ))
    db = MongoData()
    trade_date = _today_ist()
    activation_mode = 'algo-backtest'
    subscribed_group_id = ''
    autorun_enabled = False
    seen_signatures: dict[str, str] = {}

    try:
        while True:
            try:
                raw_message = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                try:
                    parsed_message = json.loads(raw_message)
                except json.JSONDecodeError:
                    parsed_message = {}
                message_type = str((parsed_message or {}).get('type') or '').strip()
                requested_date = str((parsed_message or {}).get('trade_date') or '').strip()
                requested_mode = str((parsed_message or {}).get('activation_mode') or '').strip()
                requested_group_id = str((parsed_message or {}).get('group_id') or '').strip()
                requested_autoload = (parsed_message or {}).get('autoload')
                if requested_date:
                    trade_date = requested_date
                    seen_signatures = {}
                if requested_mode:
                    activation_mode = requested_mode
                if isinstance(requested_autoload, bool):
                    autorun_enabled = requested_autoload
                elif str(requested_autoload).strip().lower() in {'true', 'false'}:
                    autorun_enabled = str(requested_autoload).strip().lower() == 'true'
                if requested_group_id != subscribed_group_id:
                    seen_signatures = {}
                subscribed_group_id = requested_group_id
                if message_type == 'cancel-deployment':
                    cancel_strategy_id = str((parsed_message or {}).get('strategy_id') or '').strip()
                    cancel_group_id = str((parsed_message or {}).get('group_id') or '').strip()
                    print(
                        f'[CANCEL DEPLOYMENT] strategy_id={cancel_strategy_id!r} '
                        f'group_id={cancel_group_id!r} trade_date={trade_date}'
                    )
                    # Build query to find the records to cancel
                    cancel_query: dict[str, Any] = _build_trade_query(
                        trade_date, activation_mode=activation_mode, statuses=[RUNNING_STATUS]
                    )
                    cancel_query['active_on_server'] = True
                    if cancel_strategy_id:
                        cancel_query['_id'] = cancel_strategy_id
                    elif cancel_group_id:
                        cancel_query['portfolio.group_id'] = cancel_group_id
                    algo_trades_col = db._db['algo_trades']
                    cancelled_records: list[dict] = []
                    try:
                        trade_records = list(algo_trades_col.find(cancel_query))
                        for trade_rec in trade_records:
                            t_id = str(trade_rec.get('_id') or '')
                            algo_trades_col.update_one(
                                {'_id': t_id},
                                {'$set': {'active_on_server': False, 'status': 'StrategyStatus.SquaredOff'}},
                            )
                            print(f'[CANCEL DEPLOYMENT] set active_on_server=False trade_id={t_id}')
                        # Reload the affected group including cancelled records (active_on_server=False)
                        affected_group_id = cancel_group_id or str(
                            (((trade_records[0].get('portfolio') or {}).get('group_id')) or '') if trade_records else ''
                        )
                        # Query without status filter — include SquaredOff records too
                        group_reload_query = _build_trade_query(trade_date, activation_mode=activation_mode)
                        if cancel_strategy_id:
                            # Single strategy cancel — return only that strategy
                            group_reload_query['_id'] = cancel_strategy_id
                        elif affected_group_id:
                            # Group cancel — return all strategies in the group
                            group_reload_query['portfolio.group_id'] = affected_group_id
                        raw_group_records = list(algo_trades_col.find(group_reload_query))
                        cancelled_records = [_serialize_trade_record(r) for r in raw_group_records]
                    except Exception as exc:
                        log.error('cancel-deployment error: %s', exc)
                        print(f'[CANCEL DEPLOYMENT] error: {exc}')
                    enriched_cancelled = _populate_legs_from_history(db, cancelled_records)
                    await websocket.send_text(_build_message(
                        'execute_order',
                        'Deployment cancelled',
                        _build_execute_order_socket_payload(
                            enriched_cancelled,
                            trigger='cancel-deployment',
                            group_id=cancel_group_id,
                        ),
                    ))
                    continue

                if message_type == 'squared-off':
                    sq_strategy_id = str((parsed_message or {}).get('strategy_id') or '').strip()
                    sq_group_id = str((parsed_message or {}).get('group_id') or '').strip()
                    print(
                        '[SQUARE OFF RECEIVED]',
                        {
                            'type': message_type,
                            'strategy_id': sq_strategy_id,
                            'group_id': sq_group_id,
                            'listen_timestamp': str((parsed_message or {}).get('listen_timestamp') or '').strip(),
                            'trade_date': trade_date,
                            'activation_mode': activation_mode,
                        },
                    )
                    print(
                        f'[SQUARE OFF] strategy_id={sq_strategy_id!r} '
                        f'group_id={sq_group_id!r} trade_date={trade_date}'
                    )
                    # Use listen_timestamp sent by frontend (latestListenTimestamp = backtest time)
                    exit_timestamp = str((parsed_message or {}).get('listen_timestamp') or '').strip()
                    # Normalize to "YYYY-MM-DD HH:MM:SS" format if needed
                    if exit_timestamp:
                        exit_timestamp = exit_timestamp.replace('T', ' ').rstrip('Z')
                    # Derive sq_trade_date from exit_timestamp or fall back via LISTENING_TIME_STATE
                    sq_trade_date = exit_timestamp[:10] if len(exit_timestamp) >= 10 else ''
                    if not exit_timestamp:
                        for _k, _v in LISTENING_TIME_STATE.items():
                            _ts = str(_v.get('listen_timestamp') or '').strip()
                            if _ts:
                                exit_timestamp = _ts.replace('T', ' ').rstrip('Z')
                                sq_trade_date = exit_timestamp[:10]
                                break
                    if not exit_timestamp:
                        exit_timestamp = _now_iso()
                        sq_trade_date = trade_date
                    print(
                        f'[SQUARE OFF] exit_timestamp={exit_timestamp!r} sq_trade_date={sq_trade_date!r}'
                    )
                    chain_col = db._db[OPTION_CHAIN_COLLECTION]
                    algo_trades_col = db._db['algo_trades']
                    history_col = db._db['algo_trade_positions_history']

                    # Build query to find trade records to square off
                    sq_query: dict[str, Any] = _build_trade_query(
                        sq_trade_date, activation_mode=activation_mode, statuses=[RUNNING_STATUS]
                    )
                    sq_query['active_on_server'] = True
                    if sq_strategy_id:
                        sq_query['$or'] = [{'_id': sq_strategy_id}, {'strategy_id': sq_strategy_id}]
                    elif sq_group_id:
                        sq_query['portfolio.group_id'] = sq_group_id

                    sq_records: list[dict] = []
                    try:
                        trade_records = list(algo_trades_col.find(sq_query))
                        for trade_rec in trade_records:
                            t_id = str(trade_rec.get('_id') or '')
                            underlying = str(
                                trade_rec.get('ticker')
                                or ((trade_rec.get('config') or {}).get('Ticker') or '')
                            ).strip()
                            trade_legs = [leg for leg in (trade_rec.get('legs') or []) if isinstance(leg, dict)]
                            trade_leg_index_by_id = {
                                str(leg.get('id') or '').strip(): leg_index
                                for leg_index, leg in enumerate(trade_legs)
                                if str(leg.get('id') or '').strip()
                            }
                            all_open_legs_closed = True
                            history_open_legs_by_id = {}
                            for history_leg in history_col.find({'trade_id': t_id, 'status': OPEN_LEG_STATUS}):
                                history_leg_id = str(history_leg.get('leg_id') or history_leg.get('id') or '').strip()
                                if history_leg_id:
                                    history_open_legs_by_id[history_leg_id] = history_leg

                            legs_to_close = []
                            for leg_index, leg in enumerate(trade_legs):
                                leg_exit_trade = leg.get('exit_trade') if isinstance(leg.get('exit_trade'), dict) else None
                                leg_status = int(leg.get('status') or 0)
                                if leg_status == OPEN_LEG_STATUS or leg_exit_trade is None:
                                    legs_to_close.append((leg_index, leg))
                            if not legs_to_close:
                                legs_to_close = [(-1, leg) for leg in history_open_legs_by_id.values()]

                            for leg_index, open_leg in legs_to_close:
                                leg_id = str(open_leg.get('id') or open_leg.get('leg_id') or '').strip()
                                history_open_leg = history_open_legs_by_id.get(leg_id) or {}
                                leg_token = str(
                                    open_leg.get('token')
                                    or history_open_leg.get('token')
                                    or ''
                                ).strip()
                                strike = open_leg.get('strike')
                                expiry_date = str(open_leg.get('expiry_date') or '')[:10]
                                option_type = str(open_leg.get('option') or '')
                                # Fetch exact token close at current listen time (backtest timestamp)
                                chain_doc = (
                                    get_chain_by_token_at_time(chain_col, leg_token, exit_timestamp)
                                    if (leg_token and exit_timestamp)
                                    else {}
                                )
                                if not chain_doc and exit_timestamp:
                                    chain_doc = get_chain_at_time(
                                        chain_col,
                                        underlying,
                                        expiry_date,
                                        strike,
                                        option_type,
                                        exit_timestamp,
                                    )
                                chain_doc = _normalize_chain_market_fields(chain_doc)
                                print(
                                    '[SQUARE OFF TOKEN LTP]',
                                    {
                                        'strategy_id': sq_strategy_id,
                                        'group_id': sq_group_id,
                                        'trade_id': t_id,
                                        'leg_id': leg_id,
                                        'token': leg_token,
                                        'listen_timestamp': exit_timestamp,
                                        'option_chain': {
                                            'timestamp': chain_doc.get('timestamp'),
                                            'close': chain_doc.get('close'),
                                            'ltp': chain_doc.get('ltp'),
                                            'current_price': chain_doc.get('current_price'),
                                            'price': chain_doc.get('price'),
                                            'last_price': chain_doc.get('last_price'),
                                        },
                                    },
                                )
                                print(
                                    f'[SQUARE OFF] chain lookup token={leg_token or "-"} underlying={underlying} '
                                    f'expiry={expiry_date} strike={strike} option={option_type} '
                                    f'ts={exit_timestamp!r} chain_doc_ts={chain_doc.get("timestamp")!r} '
                                    f'close={chain_doc.get("close")!r} '
                                    f'ltp={chain_doc.get("ltp")!r} '
                                    f'current_price={chain_doc.get("current_price")!r} '
                                    f'price={chain_doc.get("price")!r} '
                                    f'last_price={chain_doc.get("last_price")!r}'
                                )
                                if not chain_doc:
                                    print(
                                        f'[SQUARE OFF] missing chain doc for token={leg_token or "-"} '
                                        f'ts={exit_timestamp!r}; skipping stale fallback prices'
                                    )
                                    all_open_legs_closed = False
                                    continue
                                exit_price = _resolve_chain_market_price(chain_doc)
                                if exit_price <= 0:
                                    print(
                                        f'[SQUARE OFF] no valid market price for token={leg_token or "-"} '
                                        f'ts={exit_timestamp!r}; skipping stale fallback prices'
                                    )
                                    all_open_legs_closed = False
                                    continue
                                resolved_leg_index = leg_index if leg_index >= 0 else trade_leg_index_by_id.get(leg_id, -1)
                                if resolved_leg_index >= 0:
                                    close_leg_in_db(
                                        db, t_id, resolved_leg_index, exit_price, 'squared_off', exit_timestamp, leg_id=leg_id
                                    )
                                else:
                                    _update_position_history_exit(
                                        db, t_id, leg_id, exit_price, 'squared_off', exit_timestamp
                                    )
                                print(
                                    f'[SQUARE OFF] closed leg trade={t_id} leg={leg_id} '
                                    f'price={exit_price} ts={exit_timestamp}'
                                )
                            # Mark trade as squared off
                            refreshed_trade = algo_trades_col.find_one({'_id': t_id}) or {}
                            refreshed_legs = [leg for leg in (refreshed_trade.get('legs') or []) if isinstance(leg, dict)]
                            remaining_history_open_legs = list(history_col.find({'trade_id': t_id, 'status': OPEN_LEG_STATUS}))
                            pending_exit_legs = [
                                leg for leg in refreshed_legs
                                if int(leg.get('status') or 0) == OPEN_LEG_STATUS
                                or not isinstance(leg.get('exit_trade'), dict)
                            ]
                            if all_open_legs_closed and not pending_exit_legs and not remaining_history_open_legs:
                                algo_trades_col.update_one(
                                    {'_id': t_id},
                                    {'$set': {
                                        'active_on_server': False,
                                        'status': 'StrategyStatus.SquaredOff',
                                        'trade_status': 2,
                                    }},
                                )
                                print(f'[SQUARE OFF] trade={t_id} marked SquaredOff')
                            else:
                                print(
                                    f'[SQUARE OFF] trade={t_id} not marked SquaredOff because one or more legs were not closed '
                                    f'pending_exit_legs={len(pending_exit_legs)} '
                                    f'remaining_history_open_legs={len(remaining_history_open_legs)}'
                                )

                        # Reload affected group and emit
                        affected_sq_group_id = sq_group_id or str(
                            (((trade_records[0].get('portfolio') or {}).get('group_id')) or '') if trade_records else ''
                        )
                        sq_reload_query = _build_trade_query(sq_trade_date, activation_mode=activation_mode)
                        sq_reload_query.pop('trade_status', None)
                        sq_reload_query.pop('status', None)
                        if sq_strategy_id:
                            sq_reload_query['$or'] = [{'_id': sq_strategy_id}, {'strategy_id': sq_strategy_id}]
                        elif affected_sq_group_id:
                            sq_reload_query['portfolio.group_id'] = affected_sq_group_id
                        raw_sq_records = list(algo_trades_col.find(sq_reload_query))
                        sq_records = [_serialize_trade_record(r) for r in raw_sq_records]
                    except Exception as exc:
                        log.error('squared-off error: %s', exc)
                        print(f'[SQUARE OFF] error: {exc}')

                    enriched_sq = _populate_legs_from_history(db, sq_records)
                    await websocket.send_text(_build_message(
                        'execute_order',
                        'Strategy squared off',
                        _build_execute_order_socket_payload(
                            enriched_sq,
                            trigger='squared-off',
                            group_id=sq_group_id,
                        ),
                    ))
                    continue

                if message_type in {'get_trades', 'get_orders'}:
                    trade_query = _build_trade_query(
                        trade_date,
                        activation_mode=activation_mode,
                        statuses=[RUNNING_STATUS],
                    )
                    trade_query['active_on_server'] = True
                    _print_open_strategy_fetch_flow(
                        'getting_open_strategy',
                        trade_date=trade_date,
                        activation_mode=activation_mode,
                    )
                    _print_trade_query_debug(
                        trade_query,
                        trade_date=trade_date,
                        activation_mode=activation_mode,
                    )
                    current_records = _load_running_trade_records(db, trade_date, activation_mode=activation_mode)
                    current_records = _collect_records_for_group(current_records, subscribed_group_id)
                    _print_open_strategy_fetch_flow(
                        'open_strategy_taken',
                        trade_date=trade_date,
                        activation_mode=activation_mode,
                        count=len(current_records),
                    )
                    _print_running_trade_record_groups(
                        current_records,
                        trade_date=trade_date,
                        activation_mode=activation_mode,
                    )
                    enriched_current_records = _populate_legs_from_history(db, current_records)
                    _seed_execute_order_signatures(enriched_current_records, seen_signatures)
                    if subscribed_group_id:
                        _consume_execute_order_group_start(subscribed_group_id)
                    else:
                        _consume_all_execute_order_group_starts()
                    await websocket.send_text(_build_message(
                        'execute_order',
                        'Strategy execution refreshed',
                        _build_execute_order_socket_payload(
                            enriched_current_records,
                            trigger=message_type,
                            group_id=subscribed_group_id,
                        ),
                    ))
                    continue
                await websocket.send_text(_build_message(
                    'subscription_ack',
                    'Execute orders websocket subscription confirmed',
                    {
                        'trade_date': trade_date,
                        'activation_mode': activation_mode,
                        'group_id': subscribed_group_id,
                    },
                ))
                continue
            except asyncio.TimeoutError:
                if not autorun_enabled:
                    continue
                pass

            # Load active records + any recently-cancelled records in the subscribed group
            records = _load_running_trade_records(db, trade_date, activation_mode=activation_mode)
            if subscribed_group_id:
                records = [
                    record for record in records
                    if str((((record.get('portfolio') or {}).get('group_id')) or '')).strip() == subscribed_group_id
                ]
                # Also include cancelled (active_on_server=False) records for this group
                # so status changes (e.g. SquaredOff) are detected by the signature check
                cancelled_query = _build_trade_query(trade_date, activation_mode=activation_mode, statuses=[RUNNING_STATUS])
                cancelled_query['portfolio.group_id'] = subscribed_group_id
                cancelled_query['active_on_server'] = False
                active_ids = {str(r.get('_id') or '') for r in records}
                for rec in db._db['algo_trades'].find(cancelled_query):
                    if str(rec.get('_id') or '') not in active_ids:
                        records.append(_serialize_trade_record(rec))
            events = _build_execute_order_events(records, seen_signatures)
            if events:
                grouped_event_records = _collect_group_records_for_execute_order(records, events)
                enriched_events = _populate_legs_from_history(db, grouped_event_records)
                _print_position_refresh_status(
                    'backend_trigger_ready',
                    trade_date=trade_date,
                    activation_mode=activation_mode,
                    reason='execute_order',
                    count=len(enriched_events),
                )
                await websocket.send_text(_build_message(
                    'execute_order',
                    'Strategy execution updated',
                    _build_execute_order_socket_payload(
                        enriched_events,
                        trigger='execute_order',
                        group_id=subscribed_group_id,
                    ),
                ))

    except WebSocketDisconnect:
        return
    finally:
        _unregister_channel_websocket('execute-orders', websocket)
        db.close()


@socket_router.websocket('/ws/update')
async def update_socket(websocket: WebSocket):
    await websocket.accept()
    _register_channel_websocket('update', websocket)
    await websocket.send_text(_build_message(
        'connection_established',
        'Update websocket connected',
        {'channel': 'update'},
    ))
    db = MongoData()
    trade_date = _today_ist()
    activation_mode = 'algo-backtest'
    subscription_status = ''
    subscribe_tokens: list[dict] = []
    market_cache: dict | None = None
    open_positions: list[dict] = []
    token_market_data: list[dict] = []
    last_running_trade_records: list[dict] = []
    first_open_strategy_payload_printed = False
    simulated_listen_at: datetime | None = None
    autorun_enabled = False
    prev_subscribed_token_set: set[str] = set()
    current_listen_hhmm: str = ''
    active_contracts: list[dict] = []

    def refresh_position_snapshot(refresh_reason: str = '') -> None:
        nonlocal market_cache
        nonlocal open_positions
        nonlocal subscribe_tokens
        nonlocal token_market_data
        nonlocal last_running_trade_records
        nonlocal first_open_strategy_payload_printed

        _print_position_refresh_status(
            'refresh_started',
            trade_date=trade_date,
            activation_mode=activation_mode,
            reason=refresh_reason,
        )

        trade_query = _build_trade_query(
            trade_date,
            activation_mode=activation_mode,
            statuses=[RUNNING_STATUS],
        )
        trade_query['active_on_server'] = True
        if _is_algo_backtest_status(subscription_status):
            _print_open_strategy_fetch_flow(
                'getting_open_strategy',
                trade_date=trade_date,
                activation_mode=activation_mode,
            )
            _print_trade_query_debug(
                trade_query,
                trade_date=trade_date,
                activation_mode=activation_mode,
            )

        records = _load_running_trade_records(db, trade_date, activation_mode=activation_mode)
        last_running_trade_records = [dict(record) for record in records if isinstance(record, dict)]
        if _is_algo_backtest_status(subscription_status):
            _print_open_strategy_fetch_flow(
                'open_strategy_taken',
                trade_date=trade_date,
                activation_mode=activation_mode,
                count=len(records),
            )
            _print_running_trade_record_groups(
                records,
                trade_date=trade_date,
                activation_mode=activation_mode,
            )
            if not first_open_strategy_payload_printed and records:
                _print_first_open_strategy_payload(
                    records,
                    trade_date=trade_date,
                    activation_mode=activation_mode,
                )
                first_open_strategy_payload_printed = True
        if _is_algo_backtest_status(subscription_status) and not market_cache:
            market_cache = _preload_trade_date_market_cache(db, trade_date, records)
        strategy_spot_tokens = _build_strategy_spot_subscribe_tokens(
            records,
            db,
            trade_date,
            market_cache=market_cache,
        )
        open_positions = _build_open_position_updates(
            records,
            db,
            trade_date,
            market_cache=market_cache,
        )
        subscribe_tokens = _build_subscribe_tokens(
            open_positions,
            strategy_spot_tokens=strategy_spot_tokens,
        )
        token_market_data = _build_subscribed_token_market_data(
            open_positions,
            strategy_spot_tokens=strategy_spot_tokens,
            db=db,
            trade_date=trade_date,
            market_cache=market_cache,
        )
        subscribed_token_values = [
            str(item.get('token') or '').strip()
            for item in subscribe_tokens
            if str(item.get('token') or '').strip()
        ]
        print(f'[SUBSCRIBED TOKENS] {json.dumps(subscribed_token_values)}')
        _print_position_refresh_status(
            'refresh_completed',
            trade_date=trade_date,
            activation_mode=activation_mode,
            reason=refresh_reason,
            count=len(open_positions),
        )

    try:
        while True:
            try:
                raw_message = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                try:
                    parsed_message = json.loads(raw_message)
                except json.JSONDecodeError:
                    parsed_message = {}
                requested_date = str((parsed_message or {}).get('trade_date') or '').strip()
                requested_mode = str((parsed_message or {}).get('activation_mode') or '').strip()
                requested_status = str((parsed_message or {}).get('status') or '').strip()
                requested_action = str((parsed_message or {}).get('action') or '').strip().lower()
                requested_reason = str((parsed_message or {}).get('reason') or '').strip()
                requested_listen_timestamp = str((parsed_message or {}).get('listen_timestamp') or '').strip()
                requested_autoload = (parsed_message or {}).get('autoload')
                _print_position_refresh_status(
                    'message_received',
                    trade_date=requested_date or trade_date,
                    activation_mode=requested_mode or activation_mode,
                    reason=requested_reason or requested_action or 'subscription',
                )
                if requested_date:
                    trade_date = requested_date
                    first_open_strategy_payload_printed = False
                if requested_mode:
                    activation_mode = requested_mode
                    first_open_strategy_payload_printed = False
                if requested_status:
                    subscription_status = requested_status
                if isinstance(requested_autoload, bool):
                    autorun_enabled = requested_autoload
                elif str(requested_autoload).strip().lower() in {'true', 'false'}:
                    autorun_enabled = str(requested_autoload).strip().lower() == 'true'
                if requested_listen_timestamp:
                    parsed_listen_at = _parse_listen_timestamp(requested_listen_timestamp)
                    if parsed_listen_at is not None:
                        simulated_listen_at = parsed_listen_at
                elif simulated_listen_at is None:
                    listening_time_state = _get_listening_time_state(
                        requested_date or trade_date,
                        activation_mode=requested_mode or activation_mode,
                    )
                    fallback_listen_timestamp = str((listening_time_state or {}).get('listen_timestamp') or '').strip()
                    parsed_listen_at = _parse_listen_timestamp(fallback_listen_timestamp)
                    if parsed_listen_at is not None:
                        simulated_listen_at = parsed_listen_at
                    elif requested_date or trade_date:
                        simulated_listen_at = _parse_listen_timestamp(f'{requested_date or trade_date}T09:15:00')
                if requested_date or requested_mode or requested_status:
                    _clear_market_cache_snapshot(market_cache)
                    market_cache = None
                    open_positions = []
                    subscribe_tokens = []
                    token_market_data = []
                    last_running_trade_records = []
                if requested_action == 'get-position':
                    refresh_position_snapshot(requested_reason or 'explicit_trigger')
                    await websocket.send_text(_build_message(
                        'update',
                        'Open position snapshot refreshed',
                        {
                            'trade_date': trade_date,
                            'activation_mode': activation_mode,
                            'status': subscription_status,
                            'trigger_reason': requested_reason or 'explicit_trigger',
                            'refresh_status': 'completed',
                            'open_positions': open_positions,
                            'subscribe_tokens': subscribe_tokens,
                            'token_market_data': token_market_data,
                            'subscribed_tokens_count': len(subscribe_tokens),
                            'count': len(open_positions),
                        },
                    ))
                    continue
                await websocket.send_text(_build_message(
                    'subscription_ack',
                    'Update websocket subscription confirmed',
                    {
                        'trade_date': trade_date,
                        'activation_mode': activation_mode,
                        'status': subscription_status,
                        'position_refresh_enabled': True,
                    },
                ))
                continue
            except asyncio.TimeoutError:
                if not autorun_enabled:
                    continue
                if simulated_listen_at is None and trade_date:
                    listening_time_state = _get_listening_time_state(
                        trade_date,
                        activation_mode=activation_mode,
                    )
                    fallback_listen_timestamp = str((listening_time_state or {}).get('listen_timestamp') or '').strip()
                    simulated_listen_at = (
                        _parse_listen_timestamp(fallback_listen_timestamp)
                        or _parse_listen_timestamp(f'{trade_date}T09:15:00')
                    )
                market_close_at = _parse_listen_timestamp(f'{trade_date}T15:30:00')
                if simulated_listen_at is not None and market_close_at is not None and simulated_listen_at <= market_close_at:
                    listen_timestamp = _format_listen_timestamp(simulated_listen_at)
                    listen_time = simulated_listen_at.strftime('%H:%M:%S')
                    current_listen_hhmm = listen_time[:5]
                    print(f"[UPDATE SOCKET] listen_time={listen_timestamp}")

                    # ── Reload records every tick to pick up newly activated strategies ──
                    last_running_trade_records = _load_running_trade_records(
                        db, trade_date, activation_mode=activation_mode
                    )

                    # ── Entry execution on matching entry_time ─────────────────
                    if _is_algo_backtest_status(subscription_status) and last_running_trade_records:
                        entries_executed = _execute_backtest_entries(
                            db,
                            last_running_trade_records,
                            current_listen_hhmm,
                            listen_timestamp,
                            market_cache=market_cache,
                        )
                        if entries_executed:
                            entered_count = sum(1 for e in entries_executed if e.get('entered'))
                            failed_count = len(entries_executed) - entered_count
                            for entry in entries_executed:
                                if entry.get('entered'):
                                    print(
                                        f'[ENTRY SUCCESS] '
                                        f"trade_date={trade_date} "
                                        f"listen_time={current_listen_hhmm} "
                                        f"strategy_name={entry.get('strategy_name') or '-'} "
                                        f"group_name={entry.get('group_name') or '-'} "
                                        f"leg_id={entry.get('leg_id') or '-'} "
                                        f"status=entry_taken"
                                    )
                                else:
                                    print(
                                        f'[ENTRY FAILED] '
                                        f"trade_date={trade_date} "
                                        f"listen_time={current_listen_hhmm} "
                                        f"strategy_name={entry.get('strategy_name') or '-'} "
                                        f"leg_id={entry.get('leg_id') or '-'} "
                                        f"reason={entry.get('reason') or '-'}"
                                    )
                            print(
                                f'[ENTRY SUMMARY] '
                                f"listen_time={current_listen_hhmm} "
                                f"entered={entered_count} "
                                f"failed={failed_count}"
                            )

                    # ── SL / TP / Overall checks for all open legs ─────────────
                    if _is_algo_backtest_status(subscription_status):
                        try:
                            bt_tick_result = _backtest_minute_tick(
                                db,
                                trade_date,
                                listen_timestamp,
                                activation_mode=activation_mode,
                                market_cache=market_cache,
                            )
                            if bt_tick_result.get('actions_taken'):
                                print(
                                    f'[BT TICK] listen_time={current_listen_hhmm} '
                                    f"actions={bt_tick_result['actions_taken']}"
                                )
                        except Exception as _bte:
                            log.warning('_backtest_minute_tick error: %s', _bte)

                    # ── Always rebuild active_contracts from current records ────
                    # Includes already-entered legs from any previous tick
                    active_contracts = _build_active_contracts_from_records(
                        last_running_trade_records, db=db,
                        trade_date=trade_date, market_cache=market_cache,
                    )
                    # Also include momentum-pending legs so their LTP is sent to the frontend
                    # while they wait for the momentum trigger price to be hit.
                    _append_momentum_pending_to_contracts(active_contracts, db, last_running_trade_records)

                    # ── LTP tick for active positions ──────────────────────────
                    subscribed_token_strs = [
                        c.get('token') or '' for c in active_contracts if c.get('token')
                    ]
                    current_token_set = set(subscribed_token_strs)
                    newly_subscribed = current_token_set - prev_subscribed_token_set
                    newly_unsubscribed = prev_subscribed_token_set - current_token_set
                    for tok in newly_subscribed:
                        leg_meta = next((c for c in active_contracts if c.get('token') == tok), {})
                        print(
                            f'[TOKEN SUBSCRIBE] listen_time={current_listen_hhmm} '
                            f"token={tok} "
                            f"leg_id={leg_meta.get('leg_id') or '-'} "
                            f"strike={leg_meta.get('strike') or '-'} "
                            f"option={leg_meta.get('option') or '-'} "
                            f"expiry={leg_meta.get('expiry_date') or '-'}"
                        )
                    for tok in newly_unsubscribed:
                        print(f'[TOKEN UNSUBSCRIBE] listen_time={current_listen_hhmm} token={tok}')
                    prev_subscribed_token_set = current_token_set
                    print(
                        f'[SUBSCRIBED TOKENS] '
                        f"listen_time={current_listen_hhmm} "
                        f"count={len(subscribed_token_strs)} "
                        f"tokens={subscribed_token_strs}"
                    )
                    if listen_timestamp:
                        ltp_list: list[dict] = []
                        if active_contracts:
                            chain_col = db._db[OPTION_CHAIN_COLLECTION]
                            index_spot_col = db._db['option_chain_index_spot']

                            option_contracts = [c for c in active_contracts if c.get('option') != 'SPOT']
                            spot_contracts   = [c for c in active_contracts if c.get('option') == 'SPOT']

                            option_ticks = _fetch_active_leg_ticks(
                                chain_col, option_contracts, listen_timestamp, market_cache=market_cache,
                            )
                            option_ltp = [_format_chain_tick_as_ltp(t) for t in option_ticks]
                            spot_ltp = _fetch_spot_ticks(
                                index_spot_col, spot_contracts, listen_timestamp, market_cache=market_cache,
                            )
                            ltp_list = option_ltp + spot_ltp
                            print(
                                f'[LTP] listen_time={current_listen_hhmm} '
                                f'option={len(option_ltp)} spot={len(spot_ltp)} total={len(ltp_list)}'
                            )
                        # Always emit ltp_update every tick so frontend timestamp stays
                        # in sync even before any legs enter (ltp=[] keeps countdown ticking)
                        await websocket.send_text(_build_message(
                            'ltp_update',
                            'LTP update for active positions',
                            {
                                'trade_date': trade_date,
                                'listen_time': current_listen_hhmm,
                                'listen_timestamp': listen_timestamp,
                                'ltp': ltp_list,
                            },
                        ))

                    simulated_listen_at = simulated_listen_at + timedelta(minutes=1)
                else:
                    print('[UPDATE SOCKET] listen_time=unavailable')
                if _is_algo_backtest_status(subscription_status):
                    _print_running_trade_record_groups(
                        last_running_trade_records,
                        trade_date=trade_date,
                        activation_mode=activation_mode,
                        listen_time=current_listen_hhmm,
                    )
                    # Sync already-entered legs (imported with entry_trade) to history
                    _sync_entered_legs_to_history(db, last_running_trade_records)
                _print_open_position_groups(open_positions, trade_date=trade_date)
            continue

    except WebSocketDisconnect:
        return
    finally:
        _unregister_channel_websocket('update', websocket)
        _clear_market_cache_snapshot(market_cache)
        db.close()
