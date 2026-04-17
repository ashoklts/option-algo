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
from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect
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

# ─── trading_core integration ─────────────────────────────────────────────────
# trading_core.py is the canonical implementation of all trading logic.
# It is the single source of truth shared by every execution mode:
#   algo-backtest, strategy-backtest, portfolio-backtest, fast-forward, live-trade.
#
# Functions imported here replace their local duplicates in this file.
# Migration order (inch by inch):
#   ✓ Phase 1: compute_strategy_mtm  (replaces _compute_strategy_mtm_snapshot)
#   → Phase 2: square_off_trade      (replaces _square_off_trade_like_manual)
#   → Phase 3: process_tick          (replaces inner loop of _backtest_minute_tick)
#   → Phase 4: process_pending_entries (replaces _execute_backtest_entries)
from features.trading_core import (          # type: ignore
    TickContext,            # §2b  per-tick context object
    TickResult,             # §2b  return type of process_tick()
    compute_strategy_mtm,   # §8   MTM/PnL — replaces _compute_strategy_mtm_snapshot
    process_broker_tick,    # §18  live/fast-forward broker-WS tick processor
    resolve_broker_ltp,     # §18  per-leg LTP from broker_ltp_map
)
# Backward-compat alias so ALL existing callers continue to work unchanged.
_compute_strategy_mtm_snapshot = compute_strategy_mtm
# ─────────────────────────────────────────────────────────────────────────────

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
# user_id-based rooms: key = "{user_id}:{channel}" → list of WebSockets
CONNECTED_USER_CHANNEL_WEBSOCKETS: dict[str, list[WebSocket]] = {}
# reverse map: id(websocket) → (user_id, channel) for fast cleanup
WEBSOCKET_USER_MAP: dict[int, tuple[str, str]] = {}

# ── Change-detection signatures ───────────────────────────────────────────────
# Keyed by user_id.  Only emit execute_order / update when data actually changed.
# algo_trades + algo_trade_positions_history + algo_leg_feature_status changes
# are all reflected in normalized_records (via _populate_legs_from_history →
# _attach_leg_feature_statuses), so a single signature over those records covers
# all three collections the user cares about.
LAST_EXECUTE_ORDER_SIG: dict[str, str] = {}   # user_id → md5 of record structure
LAST_OPEN_POSITION_SIG: dict[str, str] = {}   # user_id → md5 of open positions
PENDING_EXECUTE_ORDER_DIRTY: dict[str, dict[str, Any]] = {}

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
    """Send to sockets NOT registered in any user room (non-user / legacy connections)."""
    channel_key = str(channel or '').strip()
    if not channel_key or not message:
        return 0
    sockets = list(CONNECTED_CHANNEL_WEBSOCKETS.get(channel_key) or [])
    delivered = 0
    for socket in sockets:
        if id(socket) in WEBSOCKET_USER_MAP:
            # This socket belongs to a user room — skip it.
            # It will only receive messages via _broadcast_user_channel_message.
            continue
        try:
            await socket.send_text(message)
            delivered += 1
        except Exception:
            _unregister_channel_websocket(channel_key, socket)
    return delivered


# ─── User room helpers ────────────────────────────────────────────────────────

def _register_user_websocket(channel: str, user_id: str, websocket: WebSocket) -> None:
    uid = str(user_id or '').strip()
    ch  = str(channel or '').strip()
    if not uid or not ch:
        return
    existing_entry = WEBSOCKET_USER_MAP.get(id(websocket))
    if existing_entry and existing_entry != (uid, ch):
        _unregister_user_websocket(websocket)
    room_key = f'{uid}:{ch}'
    bucket = CONNECTED_USER_CHANNEL_WEBSOCKETS.setdefault(room_key, [])
    if websocket not in bucket:
        bucket.append(websocket)
    WEBSOCKET_USER_MAP[id(websocket)] = (uid, ch)


def _unregister_user_websocket(websocket: WebSocket) -> None:
    entry = WEBSOCKET_USER_MAP.pop(id(websocket), None)
    if not entry:
        return
    uid, ch = entry
    room_key = f'{uid}:{ch}'
    bucket = CONNECTED_USER_CHANNEL_WEBSOCKETS.get(room_key) or []
    if websocket in bucket:
        bucket.remove(websocket)


async def _broadcast_user_channel_message(user_id: str, channel: str, message: str) -> int:
    """Send message only to WebSocket connections registered for this user_id + channel."""
    uid = str(user_id or '').strip()
    ch  = str(channel or '').strip()
    if not uid or not ch or not message:
        return 0
    room_key = f'{uid}:{ch}'
    sockets = list(CONNECTED_USER_CHANNEL_WEBSOCKETS.get(room_key) or [])
    delivered = 0
    for socket in sockets:
        try:
            await socket.send_text(message)
            delivered += 1
        except Exception:
            _unregister_user_websocket(socket)
    return delivered


async def _broadcast_all_user_rooms(channel: str, message: str) -> int:
    """Send message to ALL connected user rooms for this channel."""
    ch = str(channel or '').strip()
    if not ch or not message:
        return 0
    suffix = f':{ch}'
    delivered = 0
    for room_key, sockets in list(CONNECTED_USER_CHANNEL_WEBSOCKETS.items()):
        if not room_key.endswith(suffix):
            continue
        for socket in list(sockets):
            try:
                await socket.send_text(message)
                delivered += 1
            except Exception:
                _unregister_user_websocket(socket)
    return delivered


async def broadcast_to_channel(channel: str, message: str) -> int:
    """
    Master broadcast: sends to non-user-room sockets AND all user rooms.

    Use this instead of _broadcast_channel_message when you want every
    connected client (with or without a user_id room) to receive the message.
    """
    if not message:
        return 0
    delivered  = await _broadcast_channel_message(channel, message)
    delivered += await _broadcast_all_user_rooms(channel, message)
    return delivered


# ─── Change-detection helpers ─────────────────────────────────────────────────

def _record_signature(records: list[dict]) -> str:
    """
    MD5 of structural fields from algo_trades + algo_trade_positions_history
    + algo_leg_feature_status (all reflected in normalized_records).

    Deliberately excludes LTP / market prices so a pure price tick with no
    structural change does NOT trigger an execute_order emit.
    """
    import hashlib, json as _json
    sig: list[dict] = []
    for r in sorted(records, key=lambda x: str(x.get('_id') or '')):
        legs = r.get('legs') or []
        sig.append({
            'id':     str(r.get('_id') or ''),
            'status': str(r.get('status') or ''),
            'active': bool(r.get('active_on_server')),
            'legs': sorted(
                [
                    {
                        'id':     str(l.get('id') or l.get('leg_id') or ''),
                        'status': str(l.get('status') or ''),
                        'entry':  l.get('entry_price'),
                        'exit':   l.get('exit_price'),
                        'feat':   l.get('feature_status'),   # algo_leg_feature_status
                    }
                    for l in legs
                ],
                key=lambda x: x['id'],
            ),
        })
    return hashlib.md5(_json.dumps(sig, sort_keys=True, default=str).encode()).hexdigest()


def _position_signature(open_positions: list[dict]) -> str:
    """MD5 of open-position structural fields (excludes LTP)."""
    import hashlib, json as _json
    sig = sorted(
        [
            {
                'trade_id': str(p.get('trade_id') or ''),
                'leg_id':   str(p.get('leg_id') or ''),
                'position': str(p.get('position') or ''),
                'entry':    p.get('entry_price'),
            }
            for p in open_positions
        ],
        key=lambda x: x['trade_id'] + x['leg_id'],
    )
    return hashlib.md5(_json.dumps(sig, sort_keys=True, default=str).encode()).hexdigest()


def clear_user_emit_signatures(user_id: str = '') -> None:
    """
    Clear cached signatures for a user (or all users if user_id is empty).
    Call when a new trade date is selected so the next tick always emits fresh data.
    """
    if user_id:
        LAST_EXECUTE_ORDER_SIG.pop(user_id, None)
        LAST_OPEN_POSITION_SIG.pop(user_id, None)
    else:
        LAST_EXECUTE_ORDER_SIG.clear()
        LAST_OPEN_POSITION_SIG.clear()


def _extract_trade_date_from_doc(trade: dict | None) -> str:
    if not isinstance(trade, dict):
        return ''
    creation_ts = str(trade.get('creation_ts') or '').strip()
    if len(creation_ts) >= 10:
        return creation_ts[:10]
    return ''


def mark_execute_order_dirty(
    *,
    user_id: str,
    trade_date: str,
    activation_mode: str = 'algo-backtest',
    group_id: str = '',
    trade_id: str = '',
) -> None:
    uid = str(user_id or '').strip()
    normalized_trade_date = str(trade_date or '').strip()
    normalized_mode = str(activation_mode or 'algo-backtest').strip() or 'algo-backtest'
    normalized_group_id = str(group_id or '').strip()
    normalized_trade_id = str(trade_id or '').strip()
    if not uid or not normalized_trade_date:
        return
    dirty_key = f'{uid}|{normalized_trade_date}|{normalized_mode}'
    dirty_entry = PENDING_EXECUTE_ORDER_DIRTY.setdefault(
        dirty_key,
        {
            'user_id': uid,
            'trade_date': normalized_trade_date,
            'activation_mode': normalized_mode,
            'group_ids': set(),
            'trade_ids': set(),
        },
    )
    if normalized_group_id:
        dirty_entry['group_ids'].add(normalized_group_id)
    if normalized_trade_id:
        dirty_entry['trade_ids'].add(normalized_trade_id)


def mark_execute_order_dirty_from_trade(trade: dict | None) -> None:
    if not isinstance(trade, dict):
        return
    mark_execute_order_dirty(
        user_id=str(trade.get('user_id') or '').strip(),
        trade_date=_extract_trade_date_from_doc(trade),
        activation_mode=str(trade.get('activation_mode') or 'algo-backtest').strip() or 'algo-backtest',
        group_id=str(((trade.get('portfolio') or {}).get('group_id')) or '').strip(),
        trade_id=str(trade.get('_id') or '').strip(),
    )


def mark_execute_order_dirty_from_trade_id(
    db: MongoData,
    trade_id: str,
) -> None:
    normalized_trade_id = str(trade_id or '').strip()
    if not normalized_trade_id:
        return
    try:
        trade = db._db['algo_trades'].find_one(
            {'_id': normalized_trade_id},
            {'_id': 1, 'user_id': 1, 'creation_ts': 1, 'activation_mode': 1, 'portfolio.group_id': 1},
        ) or {}
    except Exception as exc:
        log.warning('mark_execute_order_dirty_from_trade_id lookup error trade=%s: %s', normalized_trade_id, exc)
        return
    if trade:
        mark_execute_order_dirty_from_trade(trade)


def _consume_pending_execute_order_dirty(
    *,
    user_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    allowed_user_ids = {str(uid or '').strip() for uid in (user_ids or set()) if str(uid or '').strip()}
    dirty_items: list[dict[str, Any]] = []
    keys_to_remove: list[str] = []
    for dirty_key, dirty_entry in PENDING_EXECUTE_ORDER_DIRTY.items():
        uid = str((dirty_entry or {}).get('user_id') or '').strip()
        if allowed_user_ids and uid not in allowed_user_ids:
            continue
        dirty_items.append({
            'user_id': uid,
            'trade_date': str((dirty_entry or {}).get('trade_date') or '').strip(),
            'activation_mode': str((dirty_entry or {}).get('activation_mode') or 'algo-backtest').strip() or 'algo-backtest',
            'group_ids': sorted(str(item).strip() for item in ((dirty_entry or {}).get('group_ids') or set()) if str(item).strip()),
            'trade_ids': sorted(str(item).strip() for item in ((dirty_entry or {}).get('trade_ids') or set()) if str(item).strip()),
        })
        keys_to_remove.append(dirty_key)
    for dirty_key in keys_to_remove:
        PENDING_EXECUTE_ORDER_DIRTY.pop(dirty_key, None)
    return dirty_items


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


async def emit_execute_order_for_user(
    db: MongoData,
    *,
    user_id: str,
    trade_date: str,
    activation_mode: str = 'algo-backtest',
    group_id: str = '',
    trade_ids: list[str] | tuple[str, ...] | None = None,
    trigger: str = 'db-change',
    message: str = 'Strategy execution updated',
    force: bool = False,
) -> int:
    """
    Emit execute_order only to the specific user's room after a DB mutation.

    Signature comparison is done over the user's full strategy state
    (algo_trades + algo_trade_positions_history + algo_leg_feature_status)
    so repeated calls with no effective DB change are suppressed.
    """
    uid = str(user_id or '').strip()
    normalized_trade_date = str(trade_date or '').strip()
    normalized_group_id = str(group_id or '').strip()
    normalized_trade_ids = [
        str(item or '').strip()
        for item in (trade_ids or [])
        if str(item or '').strip()
    ]
    if not uid or not normalized_trade_date:
        return 0

    full_records = _load_execute_order_group_records(
        db,
        normalized_trade_date,
        activation_mode,
        '',
        uid,
    )
    full_enriched_records = _populate_legs_from_history(db, full_records)
    full_signature = _record_signature(full_enriched_records)
    if not force and LAST_EXECUTE_ORDER_SIG.get(uid) == full_signature:
        return 0
    LAST_EXECUTE_ORDER_SIG[uid] = full_signature

    payload_records = full_enriched_records
    resolved_group_id = normalized_group_id
    if normalized_group_id:
        payload_records = [
            record
            for record in full_enriched_records
            if str(((record.get('portfolio') or {}).get('group_id')) or '').strip() == normalized_group_id
        ]
    elif normalized_trade_ids:
        affected_group_ids = {
            str(((record.get('portfolio') or {}).get('group_id')) or '').strip()
            for record in full_enriched_records
            if str(record.get('_id') or '').strip() in normalized_trade_ids
            and str(((record.get('portfolio') or {}).get('group_id')) or '').strip()
        }
        if affected_group_ids:
            payload_records = [
                record
                for record in full_enriched_records
                if str(((record.get('portfolio') or {}).get('group_id')) or '').strip() in affected_group_ids
            ]
            if len(affected_group_ids) == 1:
                resolved_group_id = next(iter(affected_group_ids))
        else:
            payload_records = [
                record
                for record in full_enriched_records
                if str(record.get('_id') or '').strip() in normalized_trade_ids
            ]

    payload = _build_execute_order_socket_payload(
        payload_records,
        trigger=trigger,
        group_id=resolved_group_id,
    )
    if normalized_trade_ids:
        payload['trade_ids'] = normalized_trade_ids
    return await _broadcast_user_channel_message(
        uid,
        'execute-orders',
        _build_message('execute_order', message, payload),
    )


async def flush_pending_execute_order_emits(
    db: MongoData,
    *,
    user_ids: set[str] | None = None,
    force: bool = False,
) -> int:
    """
    Flush batched execute_order emits for dirty users.

    This is the preferred professional pattern:
      DB writes mark the user dirty
      batch/tick boundary flushes one consolidated emit per user
    """
    delivered = 0
    dirty_items = _consume_pending_execute_order_dirty(user_ids=user_ids)
    for dirty_entry in dirty_items:
        group_ids = dirty_entry.get('group_ids') or []
        group_id = group_ids[0] if len(group_ids) == 1 else ''
        delivered += await emit_execute_order_for_user(
            db,
            user_id=str(dirty_entry.get('user_id') or '').strip(),
            trade_date=str(dirty_entry.get('trade_date') or '').strip(),
            activation_mode=str(dirty_entry.get('activation_mode') or 'algo-backtest').strip() or 'algo-backtest',
            group_id=group_id,
            trade_ids=dirty_entry.get('trade_ids') or [],
            trigger='db-change',
            message='Strategy execution updated',
            force=force,
        )
    return delivered


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
        if leg_id:
            # Use arrayFilters to match by leg id — safe when trade.legs has mixed
            # string refs and dict objects (positional index would be wrong).
            db._db['algo_trades'].update_one(
                {'_id': trade_id},
                {'$set': {
                    'legs.$[elem].status': CLOSED_LEG_STATUS,
                    'legs.$[elem].exit_reason': exit_reason,
                    'legs.$[elem].exit_trade': exit_trade_payload,
                    'legs.$[elem].last_saw_price': exit_price,
                }},
                array_filters=[{'elem.id': leg_id}],
            )
        else:
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
        log.error('close_leg_in_db error trade=%s leg=%s: %s', trade_id, leg_id or leg_index, exc)
    if leg_id:
        _update_position_history_exit(db, trade_id, leg_id, exit_price, exit_reason, now_ts)
    mark_execute_order_dirty_from_trade_id(db, trade_id)


def update_leg_sl_in_db(db: MongoData, trade_id: str, leg_index: int,
                         new_sl: float, last_price: float, leg_id: str = '') -> None:
    """Update running SL fields only when the stoploss level actually changes."""
    try:
        if leg_id:
            db._db['algo_trades'].update_one(
                {'_id': trade_id},
                {'$set': {
                    'legs.$[elem].current_sl_price': new_sl,
                    'legs.$[elem].display_sl_value': new_sl,
                    'legs.$[elem].last_saw_price': last_price,
                }},
                array_filters=[{'elem.id': leg_id}],
            )
        else:
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
    mark_execute_order_dirty_from_trade_id(db, trade_id)


def push_new_leg_in_db(db: MongoData, trade_id: str, new_leg: dict) -> None:
    """Append a new leg entry to algo_trades.legs."""
    try:
        db._db['algo_trades'].update_one(
            {'_id': trade_id},
            {'$push': {'legs': new_leg}},
        )
    except Exception as exc:
        log.error('push_new_leg_in_db error: %s', exc)
    mark_execute_order_dirty_from_trade_id(db, trade_id)


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
    mark_execute_order_dirty_from_trade_id(db, normalized_trade_id)


def _square_off_trade_like_manual(
    db: MongoData,
    trade_rec: dict,
    *,
    exit_timestamp: str,
) -> bool:
    t_id = str((trade_rec or {}).get('_id') or '').strip()
    if not t_id:
        return False

    chain_col = db._db[OPTION_CHAIN_COLLECTION]
    algo_trades_col = db._db['algo_trades']
    history_col = db._db['algo_trade_positions_history']
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
        return True

    print(
        f'[SQUARE OFF] trade={t_id} not marked SquaredOff because one or more legs were not closed '
        f'pending_exit_legs={len(pending_exit_legs)} '
        f'remaining_history_open_legs={len(remaining_history_open_legs)}'
    )
    return False


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
    mark_execute_order_dirty_from_trade_id(db, normalized_trade_id)


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
    mark_execute_order_dirty_from_trade(trade)


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
    mark_execute_order_dirty_from_trade_id(db, normalized_trade_id)


# _compute_strategy_mtm_snapshot is now imported from trading_core.py (§8).
# Alias defined at top of file:
#   _compute_strategy_mtm_snapshot = compute_strategy_mtm
#
# trading_core.compute_strategy_mtm has the same signature and algorithm.
# It additionally includes `ltp` and `entry_price` in the legs snapshot
# (a superset — existing callers that ignore extra fields are unaffected).


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
        'triggered_by': str(leg.get('triggered_by') or ''),
        'lazy_leg_ref': str(leg.get('lazy_leg_ref') or ''),
        'parent_leg_id': str(leg.get('parent_leg_id') or ''),
        'reentry_type': str(leg.get('reentry_type') or ''),
        'reentry_count_remaining': _safe_int(leg.get('reentry_count_remaining')),
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
        feature_leg_id = inserted_id
        # Parent legs and their direct reentries are tracked in runtime flows by the
        # business leg id (for example `callLeg1` / `callLeg1_re_...`). Seed those
        # feature rows with the same id so cycle 2+ feature state stays attached to
        # the parent leg lineage instead of only the history document id.
        if not bool(leg.get('is_lazy')):
            feature_leg_id = str(history_doc.get('leg_id') or leg.get('id') or inserted_id)
        record_leg_features_at_entry(
            db._db, trade, leg, resolved_leg_cfg,
            timestamp=history_doc.get('entry_timestamp') or inserted_id,
            feature_leg_id=feature_leg_id,
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

    mark_execute_order_dirty_from_trade(trade)
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
    mark_execute_order_dirty_from_trade_id(db, trade_id)


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

    # Also include leg_ids already queued as momentum_pending in algo_leg_feature_status
    # so that socket restarts don't double-queue parent legs with LegMomentum.
    # IMPORTANT: also add lazy_leg_ref (the original config leg id) — OverallReentry
    # queues legs with triggered_by='overall_<orig_id>', so the stored leg_id becomes
    # '<orig_id>-overall_<orig_id>' which doesn't match the config key '<orig_id>'.
    # Adding lazy_leg_ref prevents _queue_original_legs_if_needed from creating a
    # duplicate momentum_pending for the same underlying leg after reentry.
    try:
        momentum_queued_ids: set[str] = set()
        for _doc in db._db['algo_leg_feature_status'].find(
            {'trade_id': trade_id, 'feature': 'momentum_pending', 'status': 'active'},
            {'leg_id': 1, 'lazy_leg_ref': 1},
        ):
            momentum_queued_ids.add(str(_doc.get('leg_id') or ''))
            _lazy_ref = str(_doc.get('lazy_leg_ref') or '').strip()
            if _lazy_ref:
                momentum_queued_ids.add(_lazy_ref)
        existing_ids |= momentum_queued_ids
    except Exception as exc:
        log.warning('_queue_original_legs_if_needed momentum_pending check error trade=%s: %s', trade_id, exc)

    created_any = False

    for idx, (leg_id, leg_config) in enumerate(leg_configs.items()):
        if leg_id in existing_ids:
            continue
        leg_type = f'leg_{idx + 1}'

        # Parent leg with LegMomentum → same flow as IdleLeg + LegMomentum:
        # queue to algo_leg_feature_status as momentum_pending.
        # _process_momentum_pending_feature_legs enters it when momentum triggers
        # and pushes the history ID into legs[].
        if _has_momentum_config(leg_config):
            queued = _queue_lazy_momentum_to_feature_status(
                db, trade_id, leg_id, leg_config,
                triggered_by='',
                leg_type=leg_type,
                now_ts=now_ts,
            )
            if queued:
                print(
                    f'[LEG MOMENTUM QUEUED] trade={trade_id} leg_id={leg_id} leg_type={leg_type} '
                    f'momentum={leg_config.get("LegMomentum")}'
                )
                created_any = True
            continue

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
      2. triggered_by field  (parent-leg direct reentries like leg1_re_...)
      3. lazy_leg_ref field  (momentum lazy legs created after the fix was applied)
      4. Parent reentry base-id prefix match  (leg1_re_20260416...)
      5. Longest dash-prefix match  (momentum lazy legs from existing/pre-fix history docs)
    """
    # 1. Exact match
    cfg = all_leg_configs.get(leg_id)
    if cfg:
        return cfg

    # 2. Direct parent reentries keep the original parent id in triggered_by.
    #    Example: leg_id='putLeg2_re_20260416...', triggered_by='putLeg2'
    triggered_by = str(leg.get('triggered_by') or '').strip()
    if triggered_by:
        cfg = all_leg_configs.get(triggered_by)
        if cfg:
            return cfg

    # 3. Stored lazy_leg_ref  (set by _process_momentum_pending_feature_legs)
    lazy_ref = str(leg.get('lazy_leg_ref') or '').strip()
    if lazy_ref:
        cfg = all_leg_configs.get(lazy_ref)
        if cfg:
            return cfg

    # 4. Parent immediate/at-cost/like-original reentries use "<orig>_re_<ts>" ids.
    #    Recover the original parent leg config from that base id.
    if '_re_' in leg_id:
        reentry_base = leg_id.split('_re_', 1)[0].strip()
        if reentry_base:
            cfg = all_leg_configs.get(reentry_base)
            if cfg:
                return cfg

    # 5. Longest-prefix match — handles existing history docs without lazy_leg_ref.
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
        mark_execute_order_dirty_from_trade_id(db, trade_id)
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
        new_leg['skip_momentum_check'] = True  # Immediate reentry must enter ASAP, skip LegMomentum gate
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
        _rc_count = _safe_int((reentry_value or {}).get('ReentryCount') if isinstance(reentry_value, dict) else reentry_value)
        new_leg['reentry_count_remaining'] = max(0, _rc_count - 1)
        # AtCost must skip LegMomentum gate — it uses its own cost price check instead.
        new_leg['skip_momentum_check'] = True
        # Copy parent leg's strike/expiry/token/symbol (same strike for re-entry) and cost price.
        _at_parent = next(
            (l for l in (trade.get('legs') or [])
             if isinstance(l, dict) and str(l.get('id') or '') == triggered_by),
            None,
        )
        if _at_parent:
            _at_cost_price = _safe_float((_at_parent.get('entry_trade') or {}).get('price'))
            new_leg['strike']      = _at_parent.get('strike')
            new_leg['expiry_date'] = _at_parent.get('expiry_date')
            new_leg['token']       = _at_parent.get('token')
            new_leg['symbol']      = _at_parent.get('symbol')
            new_leg['spot_at_queue'] = _at_parent.get('last_saw_price') or _at_parent.get('spot_at_queue')
            # momentum_base_price = cost price (original entry price to wait for)
            if _at_cost_price > 0:
                new_leg['momentum_base_price'] = _at_cost_price
                # momentum_target_price = same as cost price (AtCost enters exactly at cost)
                new_leg['momentum_target_price'] = _at_cost_price
                new_leg['momentum_reference_set_at'] = now_ts
                new_leg['momentum_armed_notified_at'] = now_ts
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
    # skip_momentum_check=True bypasses LegMomentum for this leg.
    # Set on all overall-reentry legs (both Immediate and LikeOriginal) so they
    # always enter directly — momentum is only for the initial parent entry.
    if leg.get('skip_momentum_check'):
        _momentum_type  = 'None'
        _momentum_value = 0.0
    else:
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
            # ── AtCost gate: wait for option price to return to original entry cost ──
            # strike/expiry/token/symbol are already set (copied from parent at queue time).
            # momentum_base_price = cost price (parent's entry price).
            if str(leg.get('reentry_type') or '') == 'AtCost' and leg.get('strike') and leg.get('expiry_date'):
                _at_expiry = str(leg.get('expiry_date') or '')
                if ' ' in _at_expiry:
                    _at_expiry = _at_expiry[:10]
                _at_strike  = leg['strike']
                _at_chain   = _normalize_chain_market_fields(
                    get_chain_at_time(chain_col, underlying, _at_expiry, _at_strike, option_type,
                                      snapshot_timestamp or now_ts, market_cache=market_cache)
                    if snapshot_timestamp
                    else get_latest_chain_doc(chain_col, underlying, _at_expiry, _at_strike, option_type, trade_date)
                )
                _at_cur_price = _resolve_chain_market_price(_at_chain)
                _at_cost      = _safe_float(leg.get('momentum_base_price'))
                _at_is_sell   = 'sell' in str(leg.get('position') or '').lower()
                # SELL: SL hit = price ROSE above cost; wait for price to FALL back to cost
                # BUY:  SL hit = price FELL below cost; wait for price to RISE back to cost
                _at_reached = (_at_cur_price <= _at_cost) if _at_is_sell else (_at_cur_price >= _at_cost)
                if _at_cost <= 0 or not _at_reached:
                    print(
                        f'[ATCOST WAIT] leg={leg_id_log} cost={_at_cost} '
                        f'current={_at_cur_price} sell={_at_is_sell}'
                    )
                    return False, 'atcost_wait'
                print(f'[ATCOST OK] leg={leg_id_log} cost={_at_cost} current={_at_cur_price} — entering at cost')
                expiry            = _at_expiry
                strike            = _at_strike
                resolved_chain_doc = _at_chain
                token  = str(_at_chain.get('token') or token or make_token(underlying, expiry, strike, option_type))
                symbol = str(_at_chain.get('symbol') or symbol or token)
                entry_price = _at_cost  # limit-order style: enter exactly at original entry cost
            else:
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

        # ── AtCost reentry entered: update feature status to triggered ─────────
        if str(leg.get('reentry_type') or '') == 'AtCost':
            _triggered_by = str(leg.get('triggered_by') or '')
            if _triggered_by:
                try:
                    from features.notification_manager import upsert_recost_feature_status
                    _parent_leg = next(
                        (l for l in (trade.get('legs') or [])
                         if isinstance(l, dict) and str(l.get('id') or '') == _triggered_by),
                        None,
                    )
                    if _parent_leg:
                        upsert_recost_feature_status(
                            db._db, trade, _parent_leg, now_ts,
                            cost_price=entry_price,
                            exit_price=_safe_float((_parent_leg.get('entry_trade') or {}).get('price')),
                            status='triggered',
                            recost_leg_id=leg_id_str,
                        )
                except Exception as _rce:
                    log.warning('recost feature status triggered error leg=%s: %s', leg_id_str, _rce)

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
        'broker': str(item.get('broker') or ''),
        'user_id': str(item.get('user_id') or ''),
        'ticker': str(item.get('ticker') or ((item.get('config') or {}).get('Ticker') or '')),
        'legs': item.get('legs') if isinstance(item.get('legs'), list) else [],
        'creation_ts': str(item.get('creation_ts') or ''),
        'entry_time': str(item.get('entry_time') or ''),
        'exit_time': str(item.get('exit_time') or ''),
        'overall_sl_reentry_done': _safe_int(item.get('overall_sl_reentry_done')),
        'overall_tgt_reentry_done': _safe_int(item.get('overall_tgt_reentry_done')),
        'current_overall_sl_threshold': _safe_float(item.get('current_overall_sl_threshold')),
        'peak_mtm': _safe_float(item.get('peak_mtm')),
        'last_overall_event_at': str(item.get('last_overall_event_at') or ''),
        'last_overall_event_reason': str(item.get('last_overall_event_reason') or ''),
        'config': item.get('config') if isinstance(item.get('config'), dict) else {},
        'portfolio': item.get('portfolio') if isinstance(item.get('portfolio'), dict) else {},
    }


def _extract_broker_configuration_label(document: dict, fallback_broker_id: str = '') -> str:
    if not isinstance(document, dict):
        return str(fallback_broker_id or '').strip()
    for key in (
        'broker_name',
        'display_name',
        'name',
        'title',
        'broker',
        'broker_type',
        'provider',
        'vendor',
    ):
        value = str(document.get(key) or '').strip()
        if value:
            return value
    return str(fallback_broker_id or '').strip()


def _attach_broker_configuration_details(db: MongoData, records: list[dict]) -> list[dict]:
    if not records:
        return records

    broker_object_ids: list[ObjectId] = []
    for record in records:
        broker_id = str((record or {}).get('broker') or '').strip()
        if not broker_id:
            continue
        try:
            broker_object_ids.append(ObjectId(broker_id))
        except Exception:
            continue

    if not broker_object_ids:
        return records

    broker_docs_by_id: dict[str, dict] = {}
    try:
        cursor = db._db['broker_configuration'].find(
            {'_id': {'$in': broker_object_ids}},
            {
                '_id': 1,
                'broker_name': 1,
                'display_name': 1,
                'name': 1,
                'title': 1,
                'broker': 1,
                'broker_icon': 1,
                'broker_type': 1,
                'provider': 1,
                'vendor': 1,
            },
        )
        for item in cursor:
            item_id = str((item or {}).get('_id') or '').strip()
            if item_id:
                normalized_item = dict(item)
                normalized_item['_id'] = item_id
                broker_docs_by_id[item_id] = normalized_item
    except Exception:
        return records

    if not broker_docs_by_id:
        return records

    enriched_records: list[dict] = []
    for record in records:
        new_record = dict(record)
        broker_id = str(new_record.get('broker') or '').strip()
        broker_doc = broker_docs_by_id.get(broker_id)
        if broker_doc:
            new_record['broker_details'] = dict(broker_doc)
            new_record['broker_label'] = _extract_broker_configuration_label(broker_doc, broker_id)
        enriched_records.append(new_record)
    return enriched_records


def _build_broker_settings_snapshot(db: MongoData, records: list[dict]) -> list[dict]:
    """
    Build broker-settings snapshot for all brokers found in records.
    Queries algo_borker_stoploss_settings (any status) for each unique
    (user_id, broker, activation_mode) group found in the trade records.
    Returns a list of dicts ready for frontend display.
    """
    seen: dict[str, tuple] = {}
    for rec in (records or []):
        uid  = str(rec.get('user_id') or '').strip()
        bkr  = str(rec.get('broker') or '').strip()
        mode = str(rec.get('activation_mode') or '').strip()
        if not uid or not bkr or not mode:
            continue
        key = f'{uid}|{bkr}|{mode}'
        if key not in seen:
            broker_details = rec.get('broker_details') or {}
            label = (
                rec.get('broker_label')
                or broker_details.get('broker_name')
                or broker_details.get('display_name')
                or broker_details.get('name')
                or bkr
            )
            seen[key] = (uid, bkr, mode, str(label).strip())

    result: list[dict] = []
    for _key, (uid, bkr, mode, label) in seen.items():
        try:
            doc = db._db['algo_borker_stoploss_settings'].find_one({
                'user_id': uid, 'broker': bkr, 'activation_mode': mode,
            })
        except Exception as exc:
            log.warning('_build_broker_settings_snapshot error broker=%s: %s', bkr, exc)
            continue
        if not doc:
            continue

        sl_val    = doc.get('StopLoss')
        tgt_val   = doc.get('Target')
        lat_cfg   = doc.get('LockAndTrail') or {}
        trail_cfg = doc.get('OverallTrailSL') or {}

        lat_out = None
        if lat_cfg and lat_cfg.get('InstrumentMove') and lat_cfg.get('StopLossMove'):
            lat_out = {
                'instrument_move': lat_cfg.get('InstrumentMove'),
                'stop_loss_move':  lat_cfg.get('StopLossMove'),
            }

        trail_out = None
        if trail_cfg and trail_cfg.get('InstrumentMove') and trail_cfg.get('StopLossMove'):
            trail_out = {
                'instrument_move': trail_cfg.get('InstrumentMove'),
                'stop_loss_move':  trail_cfg.get('StopLossMove'),
            }

        result.append({
            'broker_id':      bkr,
            'broker_label':   label,
            'user_id':        uid,
            'mode':           mode,
            'settings_active': bool(doc.get('status') == 1),
            'stop_loss':      sl_val,
            'target':         tgt_val,
            'lock_and_trail': lat_out,
            'trail_sl':       trail_out,
            'state': {
                'lock_activated':     bool(doc.get('lock_activated') or False),
                'current_lock_floor': float(doc.get('current_lock_floor') or 0),
                'lock_peak_mtm':      float(doc.get('lock_peak_mtm') or 0),
                'effective_sl':       doc.get('effective_sl'),
                'sl_peak_mtm':        float(doc.get('sl_peak_mtm') or 0),
            },
        })

    return result


async def emit_broker_settings_for_user(user_id: str, activation_mode: str = '') -> int:
    """
    Query ALL broker settings for user_id from DB and emit broker-settings
    via execute-orders WebSocket channel.

    Call this after any DB action that touches broker data for a user so the
    frontend always receives the latest full snapshot.

    Returns the number of WebSocket clients the message was delivered to.
    """
    uid = str(user_id or '').strip()
    if not uid:
        return 0

    from features.mongo_data import MongoData as _MongoData
    from bson import ObjectId

    db = _MongoData()
    brokers_out: list[dict] = []
    try:
        query: dict = {'user_id': uid}
        if activation_mode:
            query['activation_mode'] = str(activation_mode).strip()

        docs = list(db._db['algo_borker_stoploss_settings'].find(query))

        # Batch-load broker labels from broker_configuration
        broker_ids = []
        for doc in docs:
            bkr = str(doc.get('broker') or '').strip()
            if bkr:
                try:
                    broker_ids.append(ObjectId(bkr))
                except Exception:
                    pass

        label_map: dict[str, str] = {}
        if broker_ids:
            for bconf in db._db['broker_configuration'].find(
                {'_id': {'$in': broker_ids}},
                {'broker_name': 1, 'display_name': 1, 'name': 1, 'title': 1, 'broker': 1},
            ):
                bid = str(bconf.get('_id') or '')
                label_map[bid] = _extract_broker_configuration_label(bconf, bid)

        for doc in docs:
            bkr   = str(doc.get('broker') or '').strip()
            mode  = str(doc.get('activation_mode') or '').strip()
            label = label_map.get(bkr) or bkr

            lat_cfg   = doc.get('LockAndTrail') or {}
            trail_cfg = doc.get('OverallTrailSL') or {}

            lat_out = None
            if lat_cfg and lat_cfg.get('InstrumentMove') and lat_cfg.get('StopLossMove'):
                lat_out = {
                    'instrument_move': lat_cfg['InstrumentMove'],
                    'stop_loss_move':  lat_cfg['StopLossMove'],
                }

            trail_out = None
            if trail_cfg and trail_cfg.get('InstrumentMove') and trail_cfg.get('StopLossMove'):
                trail_out = {
                    'instrument_move': trail_cfg['InstrumentMove'],
                    'stop_loss_move':  trail_cfg['StopLossMove'],
                }

            brokers_out.append({
                'broker_id':       bkr,
                'broker_label':    label,
                'user_id':         uid,
                'mode':            mode,
                'settings_active': bool(doc.get('status') == 1),
                'stop_loss':       doc.get('StopLoss'),
                'target':          doc.get('Target'),
                'lock_and_trail':  lat_out,
                'trail_sl':        trail_out,
                'state': {
                    'lock_activated':     bool(doc.get('lock_activated') or False),
                    'current_lock_floor': float(doc.get('current_lock_floor') or 0),
                    'lock_peak_mtm':      float(doc.get('lock_peak_mtm') or 0),
                    'effective_sl':       doc.get('effective_sl'),
                    'sl_peak_mtm':        float(doc.get('sl_peak_mtm') or 0),
                },
            })
    except Exception as exc:
        log.warning('emit_broker_settings_for_user error user=%s: %s', uid, exc)
        return 0
    finally:
        try:
            db.close()
        except Exception:
            pass

    msg = _build_message(
        'broker-settings',
        'Broker settings updated',
        {'brokers': brokers_out},
    )
    delivered = await _broadcast_user_channel_message(uid, 'execute-orders', msg)
    log.info('[BROKER SETTINGS EMIT] user=%s brokers=%d delivered=%d', uid, len(brokers_out), delivered)
    return delivered


def _load_running_trade_records(
    db: MongoData,
    trade_date: str,
    activation_mode: str = 'algo-backtest',
    user_id: str = '',
) -> list[dict]:
    query = _build_trade_query(
        trade_date,
        activation_mode=activation_mode,
        statuses=[RUNNING_STATUS],
    )
    query['active_on_server'] = True
    normalized_user_id = str(user_id or '').strip()
    if normalized_user_id:
        query['user_id'] = normalized_user_id
    cursor = db._db['algo_trades'].find(query).sort('creation_ts', 1)
    serialized_records = [_serialize_trade_record(item) for item in cursor]
    return _attach_broker_configuration_details(db, serialized_records)


def _load_execute_order_group_records(
    db: MongoData,
    trade_date: str,
    activation_mode: str,
    group_id: str = '',
    user_id: str = '',
) -> list[dict]:
    normalized_user_id = str(user_id or '').strip()
    running_records = _load_running_trade_records(
        db,
        trade_date,
        activation_mode=activation_mode,
        user_id=normalized_user_id,
    )
    normalized_group_id = str(group_id or '').strip()
    if not normalized_group_id:
        return running_records

    grouped_running_records = _collect_records_for_group(running_records, normalized_group_id)
    seen_trade_ids = {str(item.get('_id') or '') for item in grouped_running_records}

    group_query = _build_trade_query(trade_date, activation_mode=activation_mode)
    group_query.pop('trade_status', None)
    group_query.pop('status', None)
    group_query.pop('active_on_server', None)
    group_query['portfolio.group_id'] = normalized_group_id
    if normalized_user_id:
        group_query['user_id'] = normalized_user_id

    extra_records: list[dict] = []
    for item in db._db['algo_trades'].find(group_query).sort('creation_ts', 1):
        serialized = _serialize_trade_record(item)
        trade_id = str(serialized.get('_id') or '')
        if trade_id in seen_trade_ids:
            continue
        extra_records.append(serialized)
        seen_trade_ids.add(trade_id)

    squared_off_query = _build_trade_query(trade_date, activation_mode=activation_mode)
    squared_off_query['portfolio.group_id'] = normalized_group_id
    squared_off_query['active_on_server'] = False
    squared_off_query['status'] = 'StrategyStatus.SquaredOff'
    squared_off_query['trade_status'] = {'$in': [1, 2]}
    if normalized_user_id:
        squared_off_query['user_id'] = normalized_user_id

    for item in db._db['algo_trades'].find(squared_off_query).sort('creation_ts', 1):
        serialized = _serialize_trade_record(item)
        trade_id = str(serialized.get('_id') or '')
        if trade_id in seen_trade_ids:
            continue
        extra_records.append(serialized)
        seen_trade_ids.add(trade_id)

    return _attach_broker_configuration_details(db, grouped_running_records + extra_records)


def _trade_emit_signature(record: dict) -> str:
    legs = record.get('legs') if isinstance(record.get('legs'), list) else []
    parts = [
        str(record.get('_id') or ''),
        str(record.get('status') or ''),
        str(record.get('active_on_server') or ''),
        str(record.get('overall_sl_reentry_done') or ''),
        str(record.get('overall_tgt_reentry_done') or ''),
        str(record.get('current_overall_sl_threshold') or ''),
        str(record.get('peak_mtm') or ''),
        str(record.get('last_overall_event_at') or ''),
        str(record.get('last_overall_event_reason') or ''),
    ]
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
        has_overall_event = bool(str(record.get('last_overall_event_reason') or '').strip())
        if not has_overall_event and not _has_opened_order(record):
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
        if feature_key == 'reCost':
            strike = str(row.get('strike') or '').strip() or '-'
            option = str(row.get('option_type') or row.get('option') or '').strip().upper() or '-'
            position = str(row.get('position') or '').split('.')[-1].strip() or 'Position'
            cost_price = _format_feature_status_price(row.get('trigger_price'))
            exit_price = _format_feature_status_price(row.get('exit_price'))
            re_status = str(row.get('status') or '').strip().lower()
            if re_status == 'triggered':
                triggered_at = _format_feature_status_timestamp(row.get('triggered_at'))
                return (
                    f'RE-COST triggered for {strike} {option} {position} at {triggered_at or "-"}. '
                    f'Price returned to {cost_price}. Re-entry taken.'
                )
            if re_status == 'disabled':
                disabled_at = _format_feature_status_timestamp(row.get('disabled_at'))
                return (
                    f'RE-COST disabled for {strike} {option} {position} at {disabled_at or "-"}. '
                    f'Waiting cost {cost_price} not reached.'
                )
            # pending
            is_sell = 'sell' in position.lower()
            direction = 'falls back to' if is_sell else 'rises back to'
            return (
                f'RE-COST pending: {position} {strike} {option} exited @ {exit_price}. '
                f'Waiting for price to {direction} {cost_price} (original entry cost) to re-enter.'
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
            'triggered_by': row_copy.get('triggered_by'),
            'lazy_leg_ref': row_copy.get('lazy_leg_ref'),
            'parent_leg_id': row_copy.get('parent_leg_id'),
            'reentry_type': row_copy.get('reentry_type'),
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
            leg_id_candidates: list[str] = []
            for raw_leg_id in (leg.get('_id'), leg.get('leg_id'), leg.get('id')):
                normalized_leg_id = str(raw_leg_id or '').strip()
                if normalized_leg_id and normalized_leg_id not in leg_id_candidates:
                    leg_id_candidates.append(normalized_leg_id)

            for candidate in leg_id_candidates:
                existing_leg_ids.add(candidate)

            feature_rows: list[dict] = []
            seen_feature_row_ids: set[str] = set()
            for candidate in leg_id_candidates:
                for row in feature_rows_by_key.get((trade_id, candidate), []):
                    row_id = str(row.get('_id') or '').strip()
                    if row_id and row_id in seen_feature_row_ids:
                        continue
                    if row_id:
                        seen_feature_row_ids.add(row_id)
                    feature_rows.append(row)

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
    # broker SL: collect per-trade MTM for group-level check after the loop
    _trade_mtm_map: dict[str, float] = {}   # trade_id → current_trade_mtm
    # track trade IDs hit by overall SL/Target or broker SL/Target → emit to execute-orders
    hit_trade_ids: list[str] = []
    # LTP snapshot at moment of hit: trade_id → [{leg_id, ltp, entry_price, pnl, ...}]
    hit_ltp_snapshots: dict[str, list] = {}

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
                        if 'AtCost' in re_type:
                            try:
                                from features.notification_manager import upsert_recost_feature_status
                                _cost_price = _safe_float((leg.get('entry_trade') or {}).get('price'))
                                _reentry_id = f'{leg_id}_re_{now_ts.replace(":", "").replace("T", "")}'
                                upsert_recost_feature_status(
                                    db._db, trade, leg, now_ts,
                                    cost_price=_cost_price,
                                    exit_price=current_price,
                                    status='pending',
                                    recost_leg_id=_reentry_id,
                                )
                            except Exception as _rce:
                                log.warning('recost feature status armed error leg=%s: %s', leg_id, _rce)
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
                        if 'AtCost' in re_type:
                            try:
                                from features.notification_manager import upsert_recost_feature_status
                                _cost_price = _safe_float((leg.get('entry_trade') or {}).get('price'))
                                _reentry_id = f'{leg_id}_re_{now_ts.replace(":", "").replace("T", "")}'
                                upsert_recost_feature_status(
                                    db._db, trade, leg, now_ts,
                                    cost_price=_cost_price,
                                    exit_price=current_price,
                                    status='pending',
                                    recost_leg_id=_reentry_id,
                                )
                            except Exception as _rce:
                                log.warning('recost feature status armed error leg=%s: %s', leg_id, _rce)
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
            _trade_mtm_map[trade_id] = current_trade_mtm   # collect for broker SL check
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
            _last_overall_event_at = str(trade.get('last_overall_event_at') or '').strip().replace(' ', 'T')[:19]
            _last_overall_event_reason = str(trade.get('last_overall_event_reason') or '').strip()
            _skip_same_tick_overall = bool(
                open_leg_ids
                and _last_overall_event_at
                and _last_overall_event_at == str(now_ts or '').strip()[:19]
                and _last_overall_event_reason in {'overall_sl', 'overall_target'}
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
            print(
                '[OVERALL SL DEBUG]',
                {
                    'mode': 'backtest',
                    'trade_id': trade_id,
                    'strategy_name': str(trade.get('name') or ''),
                    'timestamp': now_ts,
                    'current_mtm': round(current_trade_mtm, 2),
                    'base_overall_sl': round(_safe_float(osl_val), 2),
                    'reentry_done': _ore_done,
                    'cycle_number': _ore_done + 1,
                    'cycle_overall_sl': round(_safe_float(_effective_sl), 2),
                    'dynamic_sl_threshold': round(_safe_float(_dyn_sl), 2),
                    'checked_stoploss': round(_safe_float(_dyn_sl or _effective_sl), 2),
                    'would_hit_cycle_sl': bool(_safe_float(_effective_sl) > 0 and current_trade_mtm <= -_safe_float(_effective_sl)),
                    'would_hit_dynamic_sl': bool(_dyn_sl > 0 and current_trade_mtm <= -_dyn_sl),
                    'skip_same_tick_overall': _skip_same_tick_overall,
                    'last_overall_event_at': _last_overall_event_at,
                    'last_overall_event_reason': _last_overall_event_reason,
                },
            )

            if _skip_same_tick_overall:
                actions_taken.append(
                    f'{trade_id}: skipped same-tick overall recheck after {_last_overall_event_reason}'
                )
                continue

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
                db._db['algo_trades'].update_one(
                    {'_id': trade_id},
                    {'$set': {
                        'last_overall_event_at': now_ts,
                        'last_overall_event_reason': 'overall_sl',
                    }},
                )
                mark_execute_order_dirty_from_trade(trade)
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
                try:
                    from features.notification_manager import disable_all_trade_notifications
                    disable_all_trade_notifications(db._db, trade_id, reason='overall_sl', timestamp=now_ts)
                except Exception as _e:
                    log.warning('bt disable_all_trade_notifications overall_sl error: %s', _e)
                _bt_close_remaining('overall_sl')
                actions_taken.append(f'{trade_id}: overall SL hit mtm={current_trade_mtm}')
                hit_trade_ids.append(trade_id)
                hit_ltp_snapshots[trade_id] = list((strategy_map.get(trade_id) or {}).get('open_positions') or [])
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
                    _next_cycle_sl = _resolve_overall_cycle_value(osl_val, _ore_done + 1)
                    db._db['algo_trades'].update_one(
                        {'_id': trade_id},
                        {'$set': {
                            'overall_sl_reentry_done': _ore_done + 1,
                            'peak_mtm': 0.0,
                            'current_overall_sl_threshold': _next_cycle_sl,
                            'last_overall_event_at': now_ts,
                            'last_overall_event_reason': 'overall_sl',
                        }},
                    )
                    mark_execute_order_dirty_from_trade(trade)
                    _reentry_trade = db._db['algo_trades'].find_one({'_id': trade_id}) or trade
                    print(
                        '[OVERALL REENTRY PREPARE]',
                        {
                            'mode': 'backtest',
                            'trade_id': trade_id,
                            'reason': 'overall_sl',
                            'timestamp': now_ts,
                            'base_overall_sl': round(_safe_float(osl_val), 2),
                            'updated_reentry_done': _ore_done + 1,
                            'updated_cycle_number': _ore_done + 2,
                            'updated_overall_sl': round(_safe_float(_next_cycle_sl), 2),
                            'current_overall_sl_threshold': round(_safe_float((_reentry_trade or {}).get('current_overall_sl_threshold')), 2),
                            'entry_check_time': now_time,
                            'reentry_type': _ore_type,
                        },
                    )
                    _new_ids = _requeue_original_legs_bt(db, _reentry_trade, trade_id, strategy_cfg, now_ts, reentry_type=_ore_type)
                    if _new_ids:
                        _reentry_trade = db._db['algo_trades'].find_one({'_id': trade_id}) or _reentry_trade
                        _reentry_entries = _execute_backtest_entries(
                            db,
                            [_serialize_trade_record(_reentry_trade)],
                            now_time,
                            now_ts,
                            market_cache=market_cache,
                        )
                        print(
                            '[OVERALL REENTRY ENTRY CHECK]',
                            {
                                'mode': 'backtest',
                                'trade_id': trade_id,
                                'reason': 'overall_sl',
                                'timestamp': now_ts,
                                'entry_check_time': now_time,
                                'queued_leg_ids': _new_ids,
                                'immediate_entries': len(_reentry_entries),
                                'entry_condition': 'same_tick_backtest_entry_time_match',
                                'updated_overall_sl': round(_safe_float(_next_cycle_sl), 2),
                                'current_overall_sl_threshold': round(_safe_float(((_reentry_trade or {}).get('current_overall_sl_threshold'))), 2),
                                'check_message': 'reentry uses updated overall SL before immediate entry attempt',
                            },
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
                                'immediate_entries': len(_reentry_entries),
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
                    else:
                        _square_off_trade_like_manual(
                            db,
                            db._db['algo_trades'].find_one({'_id': trade_id}) or trade,
                            exit_timestamp=now_ts,
                        )
                else:
                    _square_off_trade_like_manual(
                        db,
                        db._db['algo_trades'].find_one({'_id': trade_id}) or trade,
                        exit_timestamp=now_ts,
                    )

            elif (
                open_leg_ids
                and otgt_type != 'None'
                and _safe_float(_effective_tgt) > 0
                and current_trade_mtm >= _safe_float(_effective_tgt)
            ):
                db._db['algo_trades'].update_one(
                    {'_id': trade_id},
                    {'$set': {
                        'last_overall_event_at': now_ts,
                        'last_overall_event_reason': 'overall_target',
                    }},
                )
                mark_execute_order_dirty_from_trade(trade)
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
                try:
                    from features.notification_manager import disable_all_trade_notifications
                    disable_all_trade_notifications(db._db, trade_id, reason='overall_target', timestamp=now_ts)
                except Exception as _e:
                    log.warning('bt disable_all_trade_notifications overall_target error: %s', _e)
                _bt_close_remaining('overall_target')
                actions_taken.append(f'{trade_id}: overall Target hit mtm={current_trade_mtm}')
                hit_trade_ids.append(trade_id)
                hit_ltp_snapshots[trade_id] = list((strategy_map.get(trade_id) or {}).get('open_positions') or [])
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
                    _reset_sl_threshold = _resolve_overall_cycle_value(osl_val, _ore_done)
                    db._db['algo_trades'].update_one(
                        {'_id': trade_id},
                        {'$set': {
                            'overall_tgt_reentry_done': _ort_done + 1,
                            'peak_mtm': 0.0,
                            'current_overall_sl_threshold': _reset_sl_threshold,
                            'last_overall_event_at': now_ts,
                            'last_overall_event_reason': 'overall_target',
                        }},
                    )
                    mark_execute_order_dirty_from_trade(trade)
                    _reentry_trade = db._db['algo_trades'].find_one({'_id': trade_id}) or trade
                    print(
                        '[OVERALL REENTRY PREPARE]',
                        {
                            'mode': 'backtest',
                            'trade_id': trade_id,
                            'reason': 'overall_target',
                            'timestamp': now_ts,
                            'base_overall_sl': round(_safe_float(osl_val), 2),
                            'updated_reentry_done': _ort_done + 1,
                            'updated_cycle_number': _ort_done + 2,
                            'updated_overall_sl': round(_safe_float(_reset_sl_threshold), 2),
                            'current_overall_sl_threshold': round(_safe_float((_reentry_trade or {}).get('current_overall_sl_threshold')), 2),
                            'entry_check_time': now_time,
                            'reentry_type': _ort_type,
                        },
                    )
                    _new_ids = _requeue_original_legs_bt(db, _reentry_trade, trade_id, strategy_cfg, now_ts, reentry_type=_ort_type)
                    if _new_ids:
                        _reentry_trade = db._db['algo_trades'].find_one({'_id': trade_id}) or _reentry_trade
                        _reentry_entries = _execute_backtest_entries(
                            db,
                            [_serialize_trade_record(_reentry_trade)],
                            now_time,
                            now_ts,
                            market_cache=market_cache,
                        )
                        print(
                            '[OVERALL REENTRY ENTRY CHECK]',
                            {
                                'mode': 'backtest',
                                'trade_id': trade_id,
                                'reason': 'overall_target',
                                'timestamp': now_ts,
                                'entry_check_time': now_time,
                                'queued_leg_ids': _new_ids,
                                'immediate_entries': len(_reentry_entries),
                                'entry_condition': 'same_tick_backtest_entry_time_match',
                                'updated_overall_sl': round(_safe_float(_reset_sl_threshold), 2),
                                'current_overall_sl_threshold': round(_safe_float(((_reentry_trade or {}).get('current_overall_sl_threshold'))), 2),
                                'check_message': 'reentry uses updated overall SL before immediate entry attempt',
                            },
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
                                'immediate_entries': len(_reentry_entries),
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
                    else:
                        _square_off_trade_like_manual(
                            db,
                            db._db['algo_trades'].find_one({'_id': trade_id}) or trade,
                            exit_timestamp=now_ts,
                        )
                else:
                    _square_off_trade_like_manual(
                        db,
                        db._db['algo_trades'].find_one({'_id': trade_id}) or trade,
                        exit_timestamp=now_ts,
                    )
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

    # ── Broker-level SL / Target / LockAndTrail check ────────────────────────
    # Group running trades by user_id + broker + activation_mode.
    # Sum their MTMs (open live + closed realized) and run all broker features.
    _broker_groups: dict[str, list[dict]] = {}
    for _t in running_trades:
        _uid  = str(_t.get('user_id') or '').strip()
        _bkr  = str(_t.get('broker') or '').strip()
        _mode = str(_t.get('activation_mode') or activation_mode).strip()
        if not _uid or not _bkr or not _mode:
            continue
        _gkey = f'{_uid}|{_bkr}|{_mode}'
        _broker_groups.setdefault(_gkey, []).append(_t)

    for _gkey, _group in _broker_groups.items():
        _uid, _bkr, _mode = _gkey.split('|', 2)

        # ── Resolve broker name from broker_configuration ─────────────────
        _broker_name = _bkr  # fallback to raw ID
        try:
            _bconf = db._db['broker_configuration'].find_one(
                {'_id': ObjectId(_bkr)},
                {'broker_name': 1, 'display_name': 1, 'name': 1, 'title': 1, 'broker': 1},
            )
            if _bconf:
                _broker_name = _extract_broker_configuration_label(_bconf, _bkr)
        except Exception:
            pass  # fallback to raw ID already set

        # ── Broker group border — easy cross-verification in logs ─────────
        print(
            f'\n{"━" * 65}\n'
            f'  [BROKER GROUP]  broker={_broker_name}  |  mode={_mode}  |  user={_uid}\n'
            f'{"━" * 65}'
        )

        try:
            _bsl_doc = db._db['algo_borker_stoploss_settings'].find_one({
                'user_id':         _uid,
                'broker':          _bkr,
                'activation_mode': _mode,
                'status':          1,
            })
        except Exception as _e:
            log.warning('[BROKER SL CHECK] settings fetch error: %s', _e)
            continue

        if not _bsl_doc:
            continue

        _bsl_val  = _bsl_doc.get('StopLoss')
        _btgt_val = _bsl_doc.get('Target')
        _lat_cfg  = _bsl_doc.get('LockAndTrail') or {}    # LockAndTrail config
        _trail_cfg = _bsl_doc.get('OverallTrailSL')       # None = trail disabled

        # Skip if no feature configured
        if not _bsl_val and not _btgt_val and not _lat_cfg and not _trail_cfg:
            continue

        # ── Total broker MTM = closed (realized) + open (live) ────────────
        _group_mtm   = 0.0
        _open_trades = []
        _closed_mtm  = 0.0
        try:
            _all_broker_trades = list(db._db['algo_trades'].find({
                'user_id':    _uid,
                'broker':     _bkr,
                'creation_ts': {'$regex': f'^{re.escape(trade_date)}'},
            }))
        except Exception as _qe:
            log.warning('[BROKER SL CHECK] all-trades query error: %s', _qe)
            _all_broker_trades = list(_group)

        for _t in _all_broker_trades:
            _tid = str(_t.get('_id') or '').strip()
            if not _tid:
                continue
            if _tid in _trade_mtm_map:
                _group_mtm += _trade_mtm_map[_tid]
                if any(str(_rt.get('_id') or '') == _tid for _rt in _group):
                    _open_trades.append(_t)
            else:
                try:
                    _c_mtm, _ = _compute_strategy_mtm_snapshot(db, _tid, now_ts)
                    _closed_mtm += _c_mtm
                    _group_mtm  += _c_mtm
                except Exception as _ce:
                    log.warning('[BROKER SL CHECK] closed MTM error trade=%s: %s', _tid, _ce)

        if not _open_trades and _closed_mtm == 0.0:
            continue

        _open_mtm = _group_mtm - _closed_mtm

        # ── OverallTrailSL standalone (Case A) ────────────────────────────
        # When LockAndTrail is null but OverallTrailSL is present:
        # For every `InstrumentMove` increase in profit → reduce StopLoss by `StopLossMove`
        # e.g. StopLoss=3700, InstrumentMove=100, StopLossMove=50
        #      profit 100 → effective_sl=3650, profit 200 → 3600, etc.
        _effective_sl_val = _bsl_val  # default: use original StopLoss unchanged
        _settings_id      = _bsl_doc.get('_id')

        if _trail_cfg and not _lat_cfg and _bsl_val:
            _sl_trail_every = _safe_float((_trail_cfg or {}).get('InstrumentMove') or 0)
            _sl_trail_by    = _safe_float((_trail_cfg or {}).get('StopLossMove') or 0)
            if _sl_trail_every > 0 and _sl_trail_by > 0:
                # Reset sl trail state when config changes or new run date
                _sl_sig = f"{trade_date}|{_bsl_val}|{_sl_trail_every}|{_sl_trail_by}"
                _stored_sl_sig = _bsl_doc.get('sl_settings_sig') or ''
                if _stored_sl_sig != _sl_sig:
                    _sl_peak_mtm      = 0.0
                    _effective_sl_val = _bsl_val
                    try:
                        db._db['algo_borker_stoploss_settings'].update_one(
                            {'_id': _settings_id},
                            {'$set': {
                                'sl_settings_sig': _sl_sig,
                                'sl_peak_mtm':     0.0,
                                'effective_sl':    _bsl_val,
                            }},
                        )
                    except Exception as _sue:
                        log.warning('[SL TRAIL STATE RESET] save error: %s', _sue)
                    print(f'[SL TRAIL STATE RESET - {_bkr}]', {
                        'timestamp': now_ts, 'broker': _bkr, 'mode': _mode,
                        'reason': 'settings changed or new run date',
                        'old_sig': _stored_sl_sig, 'new_sig': _sl_sig,
                    })
                else:
                    _sl_peak_mtm = _safe_float(_bsl_doc.get('sl_peak_mtm') or 0)
                _sl_state_upd: dict = {}
                if _group_mtm > _sl_peak_mtm:
                    _sl_peak_mtm = _group_mtm
                    _sl_state_upd['sl_peak_mtm'] = _group_mtm
                _steps    = int(_sl_peak_mtm / _sl_trail_every) if _sl_peak_mtm >= _sl_trail_every else 0
                _new_eff  = round(float(_bsl_val) - _steps * _sl_trail_by, 2)
                _prev_eff = _safe_float(_bsl_doc.get('effective_sl') or _bsl_val)
                # Only tighten the SL (move it lower), never loosen it
                _effective_sl_val = min(_new_eff, _prev_eff)
                if _effective_sl_val < _prev_eff:
                    _sl_state_upd['effective_sl'] = _effective_sl_val
                    print(f'[BROKER SL TRAIL UPDATE - {_bkr}]', {
                        'timestamp':   now_ts,
                        'broker':      _bkr,
                        'mode':        _mode,
                        'broker_mtm':  round(_group_mtm, 2),
                        'original_sl': _bsl_val,
                        'old_eff_sl':  _prev_eff,
                        'new_eff_sl':  _effective_sl_val,
                        'sl_peak_mtm': _sl_peak_mtm,
                        'trail_steps': _steps,
                        'trail_every': _sl_trail_every,
                        'trail_by':    _sl_trail_by,
                    })
                if _sl_state_upd and _settings_id is not None:
                    try:
                        db._db['algo_borker_stoploss_settings'].update_one(
                            {'_id': _settings_id}, {'$set': _sl_state_upd}
                        )
                    except Exception as _sue:
                        log.warning('[SL TRAIL STATE] save error: %s', _sue)
                print(f'[BROKER SL TRAIL CHECK - {_bkr}]', {
                    'timestamp':     now_ts,
                    'broker':        _bkr,
                    'mode':          _mode,
                    'broker_mtm':    round(_group_mtm, 2),
                    'original_sl':   _bsl_val,
                    'effective_sl':  _effective_sl_val,
                    'sl_peak_mtm':   _sl_peak_mtm,
                    'trail_steps':   _steps,
                    'trail_every':   _sl_trail_every,
                    'trail_by':      _sl_trail_by,
                })

        _sl_rem     = round(_group_mtm + float(_effective_sl_val or 0), 2)  if _effective_sl_val  else None
        _tgt_rem    = round(float(_btgt_val or 0) - _group_mtm, 2) if _btgt_val else None
        _sl_status  = 'HIT' if (_effective_sl_val and _group_mtm <= -float(_effective_sl_val)) else 'active'
        _tgt_status = 'HIT' if (_btgt_val and _group_mtm >= float(_btgt_val)) else 'active'
        print(f'[BROKER SL/TGT CHECK - {_bkr}]', {
            'timestamp':     now_ts,
            'user_id':       _uid,
            'broker':        _bkr,
            'mode':          _mode,
            'open_mtm':      round(_open_mtm, 2),
            'closed_mtm':    round(_closed_mtm, 2),
            'broker_mtm':    round(_group_mtm, 2),
            'broker_sl':     _effective_sl_val,
            'original_sl':   _bsl_val if _effective_sl_val != _bsl_val else None,
            'sl_remaining':  _sl_rem,
            'sl_status':     _sl_status,
            'broker_target': _btgt_val,
            'tgt_remaining': _tgt_rem,
            'tgt_status':    _tgt_status,
            'open_trades':   len(_open_trades),
            'total_trades':  len(_all_broker_trades),
        })

        # ── Shared square-off helper ─────────────────────────────────────
        def _broker_square_off_all(reason: str, lock_floor: float = 0.0) -> None:
            """Close all open trades + bulk-close pending + disable settings."""
            _trade_ids = [str(_t.get('_id') or '') for _t in _open_trades]
            print(f'[BROKER {reason} - {_bkr}]', {
                'timestamp':           now_ts,
                'user_id':             _uid,
                'broker':              _bkr,
                'mode':                _mode,
                'broker_mtm':          round(_group_mtm, 2),
                'lock_floor':          lock_floor if lock_floor else None,
                'squaring_off_trades': _trade_ids,
            })
            # Step 1: close open-leg trades at current LTP
            for _t in _open_trades:
                _fresh = db._db['algo_trades'].find_one({'_id': _t['_id']}) or _t
                _square_off_trade_like_manual(db, _fresh, exit_timestamp=now_ts)
                print(f'  [BROKER {reason} - {_bkr}] trade={str(_t.get("_id") or "")[:16]} legs closed')
            # Step 2: bulk-close all pending/import trades under this broker
            try:
                _bulk_result = db._db['algo_trades'].update_many(
                    {'broker': _bkr, 'user_id': _uid, 'activation_mode': _mode, 'active_on_server': True},
                    {'$set': {'active_on_server': False, 'status': 'StrategyStatus.SquaredOff', 'trade_status': 2}},
                )
                print(f'  [BROKER {reason} - {_bkr}] group close matched={_bulk_result.matched_count} modified={_bulk_result.modified_count}')
            except Exception as _be:
                log.warning('[BROKER %s] bulk close error: %s', reason, _be)
            # Step 3: disable settings (status → 0)
            try:
                _dis = db._db['algo_borker_stoploss_settings'].update_one(
                    {'user_id': _uid, 'broker': _bkr, 'activation_mode': _mode, 'status': 1},
                    {'$set': {'status': 0}},
                )
                print(f'  [BROKER {reason} - {_bkr}] settings disabled matched={_dis.matched_count}')
            except Exception as _de:
                log.warning('[BROKER %s] disable settings error: %s', reason, _de)
            for _t in _all_broker_trades:
                mark_execute_order_dirty_from_trade(_t)
            try:
                from features.notification_manager import upsert_broker_feature_status
                _feature_name = 'broker_sl' if 'SL' in str(reason or '').upper() else 'broker_target'
                _trigger_value = _safe_float(_effective_sl_val if _feature_name == 'broker_sl' else _btgt_val)
                upsert_broker_feature_status(
                    db._db,
                    trade=(_group[0] if _group else {}),
                    user_id=_uid,
                    broker=_bkr,
                    activation_mode=_mode,
                    feature=_feature_name,
                    trigger_value=_trigger_value,
                    current_mtm=_group_mtm,
                    timestamp=now_ts,
                )
            except Exception as _bfe:
                log.warning('[BROKER %s] feature status upsert error: %s', reason, _bfe)
            actions_taken.append(f'broker_{reason.lower().replace(" ","_")} broker={_bkr} mtm={round(_group_mtm,2)}')
            for _t in _group:
                _tid = str(_t.get('_id') or '').strip()
                if _tid and _tid not in hit_trade_ids:
                    hit_trade_ids.append(_tid)
                    hit_ltp_snapshots[_tid] = list((strategy_map.get(_tid) or {}).get('open_positions') or [])

        # ── 1. StopLoss hit ───────────────────────────────────────────────
        # Uses _effective_sl_val which may be trailed lower than original _bsl_val
        if _effective_sl_val and _group_mtm <= -float(_effective_sl_val):
            _broker_square_off_all('SL HIT')
            continue

        # ── 2. Target hit ─────────────────────────────────────────────────
        if _btgt_val and _group_mtm >= float(_btgt_val):
            _broker_square_off_all('TARGET HIT')
            continue

        # ── 3. LockAndTrail check ─────────────────────────────────────────
        if not _lat_cfg:
            continue

        _lat_instr_move = _safe_float(_lat_cfg.get('InstrumentMove') or 0)  # 600
        _lat_sl_move    = _safe_float(_lat_cfg.get('StopLossMove') or 0)    # 400
        if not _lat_instr_move or not _lat_sl_move:
            continue

        # OverallTrailSL — None means plain lock only, no trailing
        _trail_every = _safe_float((_trail_cfg or {}).get('InstrumentMove') or 0)  # 100
        _trail_by    = _safe_float((_trail_cfg or {}).get('StopLossMove') or 0)    # 20
        _has_trail   = bool(_trail_cfg and _trail_every > 0 and _trail_by > 0)

        _settings_id = _bsl_doc.get('_id')
        _state_upd: dict = {}

        # ── Settings signature: reset state when user changes config mid-run ──
        # Covers both (a) new backtest date and (b) live settings edit.
        # Fingerprint = trade_date + all config values that affect lock logic.
        _lock_sig = (
            f"{trade_date}|"
            f"{_lat_instr_move}|{_lat_sl_move}|"
            f"{_trail_every}|{_trail_by}|"
            f"{_bsl_val}|{_btgt_val}"
        )
        _stored_lock_sig    = _bsl_doc.get('lock_settings_sig') or ''
        _was_lock_activated = bool(_bsl_doc.get('lock_activated') or False)
        if _stored_lock_sig != _lock_sig:
            if _was_lock_activated:
                # Lock already activated — NEVER reset back to False.
                # Only update the signature; preserve activation state and floor.
                _lock_activated     = True
                _current_lock_floor = _safe_float(_bsl_doc.get('current_lock_floor') or 0)
                _lock_peak_mtm      = _safe_float(_bsl_doc.get('lock_peak_mtm') or 0)
                _state_upd['lock_settings_sig'] = _lock_sig
                print(f'[LOCK SIG UPDATED - ACTIVATION PRESERVED - {_bkr}]', {
                    'timestamp':   now_ts,
                    'broker':      _bkr,
                    'mode':        _mode,
                    'reason':      'settings changed but lock already activated — not resetting',
                    'lock_floor':  _current_lock_floor,
                    'old_sig':     _stored_lock_sig,
                    'new_sig':     _lock_sig,
                })
            else:
                # Lock not yet activated — safe to reset for the new config
                _lock_activated     = False
                _current_lock_floor = 0.0
                _lock_peak_mtm      = 0.0
                _state_upd.update({
                    'lock_settings_sig':  _lock_sig,
                    'lock_activated':     False,
                    'current_lock_floor': 0.0,
                    'lock_peak_mtm':      0.0,
                })
                print(f'[LOCK STATE RESET - {_bkr}]', {
                    'timestamp': now_ts,
                    'broker':    _bkr,
                    'mode':      _mode,
                    'reason':    'settings changed or new run date',
                    'old_sig':   _stored_lock_sig,
                    'new_sig':   _lock_sig,
                })
        else:
            _lock_activated     = _was_lock_activated
            _current_lock_floor = _safe_float(_bsl_doc.get('current_lock_floor') or 0)
            _lock_peak_mtm      = _safe_float(_bsl_doc.get('lock_peak_mtm') or 0)

            # Safety guard: lock_floor must never decrease once set.
            # If DB stored a stale/missing floor, reconstruct from peak.
            if _lock_activated and _lock_peak_mtm > _lat_instr_move:
                if _has_trail and _trail_every > 0:
                    _pa = _lock_peak_mtm - _lat_instr_move
                    _steps_so_far = int(_pa / _trail_every) if _pa >= _trail_every else 0
                    _implied_floor = round(_lat_sl_move + _steps_so_far * _trail_by, 2)
                else:
                    _implied_floor = _lat_sl_move
                if _implied_floor > _current_lock_floor:
                    _current_lock_floor = _implied_floor
                    _state_upd['current_lock_floor'] = _implied_floor

        # 3a. Activate lock when MTM first crosses InstrumentMove
        if not _lock_activated:
            if _group_mtm >= _lat_instr_move:
                _lock_activated     = True
                _current_lock_floor = _lat_sl_move
                _lock_peak_mtm      = _group_mtm
                _state_upd.update({
                    'lock_activated':      True,
                    'current_lock_floor':  _lat_sl_move,
                    'lock_peak_mtm':       _group_mtm,
                    'lock_activated_at':   now_ts,
                    'lock_activation_mtm': _group_mtm,
                })
                print(f'[LOCK ACTIVATED - {_bkr}]', {
                    'timestamp':         now_ts,
                    'broker':            _bkr,
                    'mode':              _mode,
                    'broker_mtm':        _group_mtm,
                    'instrument_move':   _lat_instr_move,
                    'lock_floor_set_to': _lat_sl_move,
                    'trail_enabled':     _has_trail,
                })
            else:
                # Lock not yet activated — print full settings snapshot for cross-verification
                print(f'[BROKER LOCK CHECK - {_bkr}]', {
                    'timestamp':          now_ts,
                    'broker':             _bkr,
                    'mode':               _mode,
                    'broker_mtm':         round(_group_mtm, 2),
                    'lock_activated':     False,
                    # ── LockAndTrail config (from DB) ──
                    'activate_at':        _lat_instr_move,
                    'lock_profit':        _lat_sl_move,
                    'remaining_to_lock':  round(_lat_instr_move - _group_mtm, 2),
                    # ── OverallTrailSL config (from DB) ──
                    'trail_enabled':      _has_trail,
                    'trail_every':        _trail_every if _has_trail else None,
                    'trail_by':           _trail_by    if _has_trail else None,
                    # ── StopLoss / Target (from DB) ──
                    'broker_sl':          _bsl_val,
                    'broker_target':      _btgt_val,
                    # ── Signature: changes when settings edited mid-run ──
                    'settings_sig':       _lock_sig,
                })
                continue

        # 3b. Update peak MTM
        if _group_mtm > _lock_peak_mtm:
            _lock_peak_mtm = _group_mtm
            _state_upd['lock_peak_mtm'] = _group_mtm

        # 3c. Trail the lock floor (OverallTrailSL)
        if _has_trail:
            # new_floor = StopLossMove + floor((peak - InstrumentMove) / trail_every) × trail_by
            _profit_above = _lock_peak_mtm - _lat_instr_move
            _steps        = int(_profit_above / _trail_every) if _profit_above >= _trail_every else 0
            _new_floor    = round(_lat_sl_move + _steps * _trail_by, 2)
            if _new_floor > _current_lock_floor:
                print(f'[LOCK TRAIL UPDATE - {_bkr}]', {
                    'timestamp':      now_ts,
                    'broker':         _bkr,
                    'mode':           _mode,
                    'broker_mtm':     _group_mtm,
                    'lock_peak_mtm':  _lock_peak_mtm,
                    'old_lock_floor': _current_lock_floor,
                    'new_lock_floor': _new_floor,
                    'trail_steps':    _steps,
                    'trail_every':    _trail_every,
                    'trail_by':       _trail_by,
                })
                _current_lock_floor = _new_floor
                _state_upd['current_lock_floor'] = _new_floor

        # Persist state changes to DB
        # lock_peak_mtm and current_lock_floor use $max so they are
        # monotonically non-decreasing — stale DB reads can never roll them back.
        if _state_upd and _settings_id is not None:
            try:
                _max_fields = {}
                _set_fields  = {}
                for _k, _v in _state_upd.items():
                    if _k in ('lock_peak_mtm', 'current_lock_floor'):
                        _max_fields[_k] = _v
                    else:
                        _set_fields[_k] = _v
                _mongo_op: dict = {}
                if _max_fields:
                    _mongo_op['$max'] = _max_fields
                if _set_fields:
                    _mongo_op['$set'] = _set_fields
                if _mongo_op:
                    db._db['algo_borker_stoploss_settings'].update_one(
                        {'_id': _settings_id},
                        _mongo_op,
                    )
            except Exception as _sue:
                log.warning('[LOCK STATE] save error: %s', _sue)

        # Print lock status every tick
        print(f'[BROKER LOCK CHECK - {_bkr}]', {
            'timestamp':          now_ts,
            'broker':             _bkr,
            'mode':               _mode,
            'broker_mtm':         round(_group_mtm, 2),
            'lock_activated':     _lock_activated,
            # ── Lock state ──
            'lock_floor':         _current_lock_floor,
            'lock_peak_mtm':      _lock_peak_mtm,
            'remaining_to_exit':  round(_group_mtm - _current_lock_floor, 2),
            # ── LockAndTrail config (from DB) ──
            'activate_at':        _lat_instr_move,
            'lock_profit':        _lat_sl_move,
            # ── OverallTrailSL config (from DB) ──
            'trail_enabled':      _has_trail,
            'trail_every':        _trail_every if _has_trail else None,
            'trail_by':           _trail_by    if _has_trail else None,
            # ── StopLoss / Target (from DB) ──
            'broker_sl':          _bsl_val,
            'broker_target':      _btgt_val,
            # ── Signature: changes when settings edited mid-run ──
            'settings_sig':       _lock_sig,
        })

        # 3d. Exit when MTM drops below lock floor
        if _group_mtm < _current_lock_floor:
            _broker_square_off_all('LOCK AND TRAIL HIT', lock_floor=_current_lock_floor)

    # ── Strategy-level SL / Target / TrailSL / LockAndTrail check ───────────
    # Group running trades by executed_strategy_id.
    # Sum their MTMs (open live + closed realized) and apply the strategy's own
    # OverallSL / OverallTgt / OverallTrailSL / LockAndTrail fields.
    # State (peak MTM, lock floor etc.) is persisted in algo_strategy_sl_state.
    #
    # Config formats supported:
    #   OverallSL        : {"Type": "..MTM..",  "Value": 1300}
    #   OverallTgt       : {"Type": "..MTM..",  "Value": 600}
    #   OverallTrailSL   : {"Type": "..MTM..",  "Value": {"InstrumentMove": 100, "StopLossMove": 50}}
    #   LockAndTrail     : {"Type": "TrailingOption.LockAndTrail",
    #                       "Value": {"ProfitReaches": 1000, "LockProfit": 600,
    #                                 "IncreaseInProfitBy": 100, "TrailProfitBy": 60}}
    # ────────────────────────────────────────────────────────────────────────
    _strat_groups: dict[str, list[dict]] = {}
    for _t in running_trades:
        _esid = str(_t.get('executed_strategy_id') or _t.get('strategy_id') or '').strip()
        if not _esid:
            continue
        _strat_groups.setdefault(_esid, []).append(_t)

    for _esid, _sgroup in _strat_groups.items():
        # ── Get strategy config from first trade in group ─────────────────
        _strat_trade = _sgroup[0]
        _strat_cfg   = _strat_trade.get('strategy') or {}
        _s_uid       = str(_strat_trade.get('user_id') or '').strip()
        _s_bkr       = str(_strat_trade.get('broker') or '').strip()
        _s_mode      = str(_strat_trade.get('activation_mode') or activation_mode).strip()

        # ── Parse OverallSL ───────────────────────────────────────────────
        _osl_cfg  = _strat_cfg.get('OverallSL') or {}
        _osl_type = str(_osl_cfg.get('Type') or 'None')
        _ssl_val  = float(_osl_cfg.get('Value') or 0) if 'None' not in _osl_type else 0.0
        _ssl_val  = _ssl_val if _ssl_val > 0 else None

        # ── Parse OverallTgt ──────────────────────────────────────────────
        _otgt_cfg  = _strat_cfg.get('OverallTgt') or {}
        _otgt_type = str(_otgt_cfg.get('Type') or 'None')
        _stgt_val  = float(_otgt_cfg.get('Value') or 0) if 'None' not in _otgt_type else 0.0
        _stgt_val  = _stgt_val if _stgt_val > 0 else None

        # ── Parse OverallTrailSL {InstrumentMove, StopLossMove} ───────────
        _otrail_cfg  = _strat_cfg.get('OverallTrailSL') or {}
        _otrail_type = str(_otrail_cfg.get('Type') or 'None')
        _otrail_val  = _otrail_cfg.get('Value') or {}
        _s_trail_cfg = None
        if 'None' not in _otrail_type and isinstance(_otrail_val, dict):
            _s_trl_every = _safe_float(_otrail_val.get('InstrumentMove') or 0)
            _s_trl_by    = _safe_float(_otrail_val.get('StopLossMove') or 0)
            if _s_trl_every > 0 and _s_trl_by > 0:
                _s_trail_cfg = {'InstrumentMove': _s_trl_every, 'StopLossMove': _s_trl_by}

        # ── Parse LockAndTrail {ProfitReaches, LockProfit, IncreaseInProfitBy, TrailProfitBy} ──
        _olat_cfg  = _strat_cfg.get('LockAndTrail') or {}
        _olat_type = str(_olat_cfg.get('Type') or 'None')
        _olat_val  = _olat_cfg.get('Value') or {}
        _s_lat_cfg = None
        if 'None' not in _olat_type and isinstance(_olat_val, dict):
            _slat_pr = _safe_float(_olat_val.get('ProfitReaches') or 0)
            if _slat_pr > 0:
                _s_lat_cfg = _olat_val  # keep original dict; parsed below

        # Skip if no strategy-level feature is configured
        if not _ssl_val and not _stgt_val and not _s_trail_cfg and not _s_lat_cfg:
            continue

        # ── Compute total strategy MTM (all trades today for this strategy) ──
        _sg_mtm      = 0.0
        _sg_open_trd: list[dict] = []
        _sg_closed   = 0.0
        try:
            _all_strat_trades = list(db._db['algo_trades'].find({
                'executed_strategy_id': _esid,
                'creation_ts': {'$regex': f'^{re.escape(trade_date)}'},
            }))
        except Exception as _sqe:
            log.warning('[STRAT SL CHECK] trades query error: %s', _sqe)
            _all_strat_trades = list(_sgroup)

        for _st in _all_strat_trades:
            _stid = str(_st.get('_id') or '').strip()
            if not _stid:
                continue
            if _stid in _trade_mtm_map:
                _sg_mtm += _trade_mtm_map[_stid]
                if any(str(_rt.get('_id') or '') == _stid for _rt in _sgroup):
                    _sg_open_trd.append(_st)
            else:
                try:
                    _sc_mtm, _ = _compute_strategy_mtm_snapshot(db, _stid, now_ts)
                    _sg_closed += _sc_mtm
                    _sg_mtm    += _sc_mtm
                except Exception as _sce:
                    log.warning('[STRAT SL CHECK] closed MTM error trade=%s: %s', _stid, _sce)

        _same_tick_overall_trade = next(
            (
                _st for _st in _all_strat_trades
                if str((_st or {}).get('last_overall_event_at') or '').strip().replace(' ', 'T')[:19] == str(now_ts or '').strip()[:19]
                and str((_st or {}).get('last_overall_event_reason') or '').strip() in {'overall_sl', 'overall_target'}
            ),
            None,
        )
        if _same_tick_overall_trade:
            print(f'[STRAT CHECK SKIPPED - {_s_bkr}]', {
                'timestamp': now_ts,
                'strat': _esid,
                'mode': _s_mode,
                'reason': 'same_tick_overall_event',
                'trade_id': str((_same_tick_overall_trade or {}).get('_id') or ''),
                'last_overall_event_at': str((_same_tick_overall_trade or {}).get('last_overall_event_at') or ''),
                'last_overall_event_reason': str((_same_tick_overall_trade or {}).get('last_overall_event_reason') or ''),
            })
            continue

        if not _sg_open_trd and _sg_closed == 0.0:
            continue

        _sg_open_mtm = _sg_mtm - _sg_closed
        _runtime_strat_sl = next(
            (
                _safe_float((_st.get('current_overall_sl_threshold') or 0))
                for _st in (_sg_open_trd or _all_strat_trades)
                if _safe_float((_st.get('current_overall_sl_threshold') or 0)) > 0
            ),
            0.0,
        )

        # ── Load persistent state from algo_strategy_sl_state ────────────
        try:
            _sstate_doc = db._db['algo_strategy_sl_state'].find_one({
                'executed_strategy_id': _esid,
                'trade_date':           trade_date,
            }) or {}
        except Exception as _sle:
            log.warning('[STRAT SL] state load error: %s', _sle)
            _sstate_doc = {}

        _sstate_upd: dict = {}

        def _save_sstate(
            _esid=_esid, _trade_date=trade_date,
            _s_uid=_s_uid, _s_bkr=_s_bkr, _s_mode=_s_mode,
        ) -> None:
            """Upsert state changes into algo_strategy_sl_state."""
            if not _sstate_upd:
                return
            try:
                _smax: dict = {}
                _sset: dict = {}
                for _k, _v in _sstate_upd.items():
                    if _k in ('strat_lock_peak_mtm', 'strat_current_lock_floor', 'strat_sl_peak_mtm'):
                        _smax[_k] = _v
                    else:
                        _sset[_k] = _v
                _sset.update({
                    'executed_strategy_id': _esid,
                    'trade_date':           _trade_date,
                    'user_id':              _s_uid,
                    'broker':               _s_bkr,
                    'activation_mode':      _s_mode,
                })
                _sop: dict = {'$set': _sset}
                if _smax:
                    _sop['$max'] = _smax
                db._db['algo_strategy_sl_state'].update_one(
                    {'executed_strategy_id': _esid, 'trade_date': _trade_date},
                    _sop,
                    upsert=True,
                )
            except Exception as _se:
                log.warning('[STRAT SL] state save error: %s', _se)

        # ── Shared square-off helper ──────────────────────────────────────
        def _strat_square_off_all(
            reason: str, lock_floor: float = 0.0,
            _esid=_esid, _s_uid=_s_uid, _s_bkr=_s_bkr, _s_mode=_s_mode,
            _sg_open_trd=_sg_open_trd, _sg_mtm=_sg_mtm, _sgroup=_sgroup,
        ) -> None:
            """Close all open trades for this strategy."""
            _strade_ids = [str(_st2.get('_id') or '') for _st2 in _sg_open_trd]
            print(f'[STRAT {reason} - {_s_bkr}]', {
                'timestamp':            now_ts,
                'executed_strategy_id': _esid,
                'user_id':              _s_uid,
                'broker':               _s_bkr,
                'mode':                 _s_mode,
                'strat_mtm':            round(_sg_mtm, 2),
                'lock_floor':           lock_floor if lock_floor else None,
                'squaring_off_trades':  _strade_ids,
            })
            for _st2 in _sg_open_trd:
                _sfresh = db._db['algo_trades'].find_one({'_id': _st2['_id']}) or _st2
                _square_off_trade_like_manual(db, _sfresh, exit_timestamp=now_ts)
                print(f'  [STRAT {reason} - {_s_bkr}] trade={str(_st2.get("_id") or "")[:16]} legs closed')
            try:
                _sbulk = db._db['algo_trades'].update_many(
                    {'executed_strategy_id': _esid, 'activation_mode': _s_mode, 'active_on_server': True},
                    {'$set': {'active_on_server': False, 'status': 'StrategyStatus.SquaredOff', 'trade_status': 2}},
                )
                print(f'  [STRAT {reason} - {_s_bkr}] group close matched={_sbulk.matched_count} modified={_sbulk.modified_count}')
            except Exception as _sbe:
                log.warning('[STRAT %s] bulk close error: %s', reason, _sbe)
            for _st2 in _sgroup:
                mark_execute_order_dirty_from_trade(_st2)
            actions_taken.append(f'strat_{reason.lower().replace(" ","_")} strat={_esid} mtm={round(_sg_mtm,2)}')
            for _st2 in _sgroup:
                _stid2 = str(_st2.get('_id') or '').strip()
                if _stid2 and _stid2 not in hit_trade_ids:
                    hit_trade_ids.append(_stid2)
                    hit_ltp_snapshots[_stid2] = list((strategy_map.get(_stid2) or {}).get('open_positions') or [])

        # ── Case A: OverallTrailSL standalone (no LockAndTrail) ──────────
        _s_effective_sl = _runtime_strat_sl or _ssl_val
        if _s_trail_cfg and not _s_lat_cfg and _ssl_val:
            _st_every = _safe_float(_s_trail_cfg.get('InstrumentMove') or 0)
            _st_by    = _safe_float(_s_trail_cfg.get('StopLossMove') or 0)
            if _st_every > 0 and _st_by > 0:
                _st_sig        = f"{trade_date}|{_ssl_val}|{_st_every}|{_st_by}"
                _stored_st_sig = _sstate_doc.get('strat_sl_settings_sig') or ''
                if _stored_st_sig != _st_sig:
                    _st_peak_mtm    = 0.0
                    _s_effective_sl = _ssl_val
                    _sstate_upd.update({
                        'strat_sl_settings_sig': _st_sig,
                        'strat_sl_peak_mtm':     0.0,
                        'strat_effective_sl':    _ssl_val,
                    })
                    print(f'[STRAT SL TRAIL RESET - {_s_bkr}]', {
                        'timestamp': now_ts, 'strat': _esid,
                        'reason': 'settings changed or new run date',
                        'old_sig': _stored_st_sig, 'new_sig': _st_sig,
                    })
                else:
                    _st_peak_mtm = _safe_float(_sstate_doc.get('strat_sl_peak_mtm') or 0)
                _stsl_upd: dict = {}
                if _sg_mtm > _st_peak_mtm:
                    _st_peak_mtm = _sg_mtm
                    _stsl_upd['strat_sl_peak_mtm'] = _sg_mtm
                _st_steps   = int(_st_peak_mtm / _st_every) if _st_peak_mtm >= _st_every else 0
                _new_eff_s  = round(float(_ssl_val) - _st_steps * _st_by, 2)
                _prev_eff_s = _safe_float(_sstate_doc.get('strat_effective_sl') or _ssl_val)
                _s_effective_sl = min(_new_eff_s, _prev_eff_s)
                if _s_effective_sl < _prev_eff_s:
                    _stsl_upd['strat_effective_sl'] = _s_effective_sl
                    print(f'[STRAT SL TRAIL UPDATE - {_s_bkr}]', {
                        'timestamp':    now_ts, 'strat': _esid,
                        'strat_mtm':    round(_sg_mtm, 2),
                        'original_sl':  _ssl_val,
                        'old_eff_sl':   _prev_eff_s,
                        'new_eff_sl':   _s_effective_sl,
                        'sl_peak_mtm':  _st_peak_mtm,
                        'trail_steps':  _st_steps,
                        'trail_every':  _st_every,
                        'trail_by':     _st_by,
                    })
                _sstate_upd.update(_stsl_upd)
                print(f'[STRAT SL TRAIL CHECK - {_s_bkr}]', {
                    'timestamp':    now_ts, 'strat': _esid,
                    'strat_mtm':    round(_sg_mtm, 2),
                    'original_sl':  _ssl_val,
                    'effective_sl': _s_effective_sl,
                    'sl_peak_mtm':  _st_peak_mtm,
                    'trail_steps':  _st_steps,
                    'trail_every':  _st_every,
                    'trail_by':     _st_by,
                })

        _ssl_rem     = round(_sg_mtm + float(_s_effective_sl or 0), 2) if _s_effective_sl else None
        _stgt_rem    = round(float(_stgt_val or 0) - _sg_mtm, 2) if _stgt_val else None
        _ssl_status  = 'HIT' if (_s_effective_sl and _sg_mtm <= -float(_s_effective_sl)) else 'active'
        _stgt_status = 'HIT' if (_stgt_val and _sg_mtm >= float(_stgt_val)) else 'active'
        print(f'[STRAT SL/TGT CHECK - {_s_bkr}]', {
            'timestamp':     now_ts,
            'strat':         _esid,
            'user_id':       _s_uid,
            'mode':          _s_mode,
            'open_mtm':      round(_sg_open_mtm, 2),
            'closed_mtm':    round(_sg_closed, 2),
            'strat_mtm':     round(_sg_mtm, 2),
            'strat_sl':      _s_effective_sl,
            'runtime_current_overall_sl_threshold': _runtime_strat_sl or None,
            'original_sl':   _ssl_val if _s_effective_sl != _ssl_val else None,
            'sl_remaining':  _ssl_rem,
            'sl_status':     _ssl_status,
            'strat_target':  _stgt_val,
            'tgt_remaining': _stgt_rem,
            'tgt_status':    _stgt_status,
            'open_trades':   len(_sg_open_trd),
            'total_trades':  len(_all_strat_trades),
        })

        # ── 1. StopLoss hit ───────────────────────────────────────────────
        if _s_effective_sl and _sg_mtm <= -float(_s_effective_sl):
            _save_sstate()
            _strat_square_off_all('SL HIT')
            continue

        # ── 2. Target hit ─────────────────────────────────────────────────
        if _stgt_val and _sg_mtm >= float(_stgt_val):
            _save_sstate()
            _strat_square_off_all('TARGET HIT')
            continue

        # ── 3. LockAndTrail (Case B) ──────────────────────────────────────
        if not _s_lat_cfg:
            _save_sstate()
            continue

        _slat_instr_move  = _safe_float(_s_lat_cfg.get('ProfitReaches') or 0)
        _slat_sl_move     = _safe_float(_s_lat_cfg.get('LockProfit') or 0)
        _slat_trail_every = _safe_float(_s_lat_cfg.get('IncreaseInProfitBy') or 0)
        _slat_trail_by    = _safe_float(_s_lat_cfg.get('TrailProfitBy') or 0)
        _s_has_trail      = _slat_trail_every > 0 and _slat_trail_by > 0

        if _slat_instr_move <= 0:
            _save_sstate()
            continue

        # ── Build lock signature ──────────────────────────────────────────
        _s_lock_sig = (
            f"{trade_date}|{_slat_instr_move}|{_slat_sl_move}|"
            f"{_slat_trail_every}|{_slat_trail_by}|"
            f"{_ssl_val}|{_stgt_val}"
        )
        _s_stored_lock_sig    = _sstate_doc.get('strat_lock_settings_sig') or ''
        _s_was_lock_activated = bool(_sstate_doc.get('strat_lock_activated') or False)

        if _s_stored_lock_sig != _s_lock_sig:
            if _s_was_lock_activated:
                # Lock already activated — NEVER reset. Only update signature.
                _s_lock_activated     = True
                _s_current_lock_floor = _safe_float(_sstate_doc.get('strat_current_lock_floor') or 0)
                _s_lock_peak_mtm      = _safe_float(_sstate_doc.get('strat_lock_peak_mtm') or 0)
                _sstate_upd['strat_lock_settings_sig'] = _s_lock_sig
                print(f'[STRAT LOCK SIG UPDATED - ACTIVATION PRESERVED - {_s_bkr}]', {
                    'timestamp':  now_ts, 'strat': _esid,
                    'reason':     'settings changed but lock already activated — not resetting',
                    'lock_floor': _s_current_lock_floor,
                    'old_sig':    _s_stored_lock_sig, 'new_sig': _s_lock_sig,
                })
            else:
                # Lock not yet activated — safe to reset for new config
                _s_lock_activated     = False
                _s_current_lock_floor = 0.0
                _s_lock_peak_mtm      = 0.0
                _sstate_upd.update({
                    'strat_lock_settings_sig':  _s_lock_sig,
                    'strat_lock_activated':     False,
                    'strat_current_lock_floor': 0.0,
                    'strat_lock_peak_mtm':      0.0,
                })
                print(f'[STRAT LOCK STATE RESET - {_s_bkr}]', {
                    'timestamp': now_ts, 'strat': _esid,
                    'reason':    'settings changed or new run date',
                    'old_sig':   _s_stored_lock_sig, 'new_sig': _s_lock_sig,
                })
        else:
            _s_lock_activated     = _s_was_lock_activated
            _s_current_lock_floor = _safe_float(_sstate_doc.get('strat_current_lock_floor') or 0)
            _s_lock_peak_mtm      = _safe_float(_sstate_doc.get('strat_lock_peak_mtm') or 0)

            # Safety guard: floor must never decrease once set
            if _s_lock_activated and _s_lock_peak_mtm > _slat_instr_move:
                if _s_has_trail and _slat_trail_every > 0:
                    _spa       = _s_lock_peak_mtm - _slat_instr_move
                    _ssteps    = int(_spa / _slat_trail_every) if _spa >= _slat_trail_every else 0
                    _simplied  = round(_slat_sl_move + _ssteps * _slat_trail_by, 2)
                else:
                    _simplied = _slat_sl_move
                if _simplied > _s_current_lock_floor:
                    _s_current_lock_floor = _simplied
                    _sstate_upd['strat_current_lock_floor'] = _simplied

        # ── 3a. Activate lock when MTM first crosses ProfitReaches ───────
        if not _s_lock_activated:
            if _sg_mtm >= _slat_instr_move:
                _s_lock_activated     = True
                _s_current_lock_floor = _slat_sl_move
                _s_lock_peak_mtm      = _sg_mtm
                _sstate_upd.update({
                    'strat_lock_activated':      True,
                    'strat_current_lock_floor':  _slat_sl_move,
                    'strat_lock_peak_mtm':       _sg_mtm,
                    'strat_lock_activated_at':   now_ts,
                    'strat_lock_activation_mtm': _sg_mtm,
                })
                print(f'[STRAT LOCK ACTIVATED - {_s_bkr}]', {
                    'timestamp':         now_ts, 'strat': _esid,
                    'mode':              _s_mode,
                    'strat_mtm':         _sg_mtm,
                    'profit_reaches':    _slat_instr_move,
                    'lock_floor_set_to': _slat_sl_move,
                    'trail_enabled':     _s_has_trail,
                })
            else:
                print(f'[STRAT LOCK CHECK - {_s_bkr}]', {
                    'timestamp':          now_ts, 'strat': _esid,
                    'mode':               _s_mode,
                    'strat_mtm':          round(_sg_mtm, 2),
                    'lock_activated':     False,
                    'activate_at':        _slat_instr_move,
                    'lock_profit':        _slat_sl_move,
                    'remaining_to_lock':  round(_slat_instr_move - _sg_mtm, 2),
                    'trail_enabled':      _s_has_trail,
                    'trail_every':        _slat_trail_every if _s_has_trail else None,
                    'trail_by':           _slat_trail_by    if _s_has_trail else None,
                })
        else:
            # ── 3b. Update peak MTM ───────────────────────────────────────
            if _sg_mtm > _s_lock_peak_mtm:
                _s_lock_peak_mtm = _sg_mtm
                _sstate_upd['strat_lock_peak_mtm'] = _sg_mtm

            # ── 3c. Trail the floor upward ────────────────────────────────
            if _s_has_trail:
                _s_profit_above = _s_lock_peak_mtm - _slat_instr_move
                _s_steps        = int(_s_profit_above / _slat_trail_every) if _s_profit_above >= _slat_trail_every else 0
                _s_new_floor    = round(_slat_sl_move + _s_steps * _slat_trail_by, 2)
                if _s_new_floor > _s_current_lock_floor:
                    print(f'[STRAT LOCK TRAIL UPDATE - {_s_bkr}]', {
                        'timestamp':      now_ts, 'strat': _esid,
                        'mode':           _s_mode,
                        'strat_mtm':      _sg_mtm,
                        'lock_peak_mtm':  _s_lock_peak_mtm,
                        'old_lock_floor': _s_current_lock_floor,
                        'new_lock_floor': _s_new_floor,
                        'trail_steps':    _s_steps,
                        'trail_every':    _slat_trail_every,
                        'trail_by':       _slat_trail_by,
                    })
                    _s_current_lock_floor = _s_new_floor
                    _sstate_upd['strat_current_lock_floor'] = _s_new_floor

            print(f'[STRAT LOCK CHECK - {_s_bkr}]', {
                'timestamp':          now_ts, 'strat': _esid,
                'mode':               _s_mode,
                'strat_mtm':          round(_sg_mtm, 2),
                'lock_activated':     _s_lock_activated,
                'lock_floor':         _s_current_lock_floor,
                'lock_peak_mtm':      _s_lock_peak_mtm,
                'remaining_to_exit':  round(_sg_mtm - _s_current_lock_floor, 2),
                'activate_at':        _slat_instr_move,
                'lock_profit':        _slat_sl_move,
                'trail_enabled':      _s_has_trail,
                'trail_every':        _slat_trail_every if _s_has_trail else None,
                'trail_by':           _slat_trail_by    if _s_has_trail else None,
                'strat_sl':           _ssl_val,
                'strat_target':       _stgt_val,
                'settings_sig':       _s_lock_sig,
            })

            # ── 3d. Exit when MTM drops below lock floor ──────────────────
            if _sg_mtm < _s_current_lock_floor:
                _save_sstate()
                _strat_square_off_all('LOCK AND TRAIL HIT', lock_floor=_s_current_lock_floor)
                continue

        _save_sstate()

    return {
        'actions_taken':    actions_taken,
        'checked_at':       now_ts,
        'total_open':       len(open_positions),
        'hit_trade_ids':    hit_trade_ids,      # strategies hit by overall SL/TGT or broker SL/TGT
        'hit_ltp_snapshots': hit_ltp_snapshots, # {trade_id: [{leg_id, ltp, entry_price, pnl}]} at hit time
    }


def _requeue_original_legs_bt(db, trade: dict, trade_id: str, strategy_cfg: dict, now_ts: str, reentry_type: str = '') -> list[str]:
    """Re-queue original legs for overall reentry in backtest mode.

    reentry_type: ore_type / ort_type from parse_overall_reentry_sl/tgt.

    LikeOriginal behaviour (same method as NextLeg + momentum):
      - If original leg has LegMomentum → queue to algo_leg_feature_status as
        momentum_pending (handled by _process_momentum_pending_feature_legs).
      - If no LegMomentum → push as regular pending leg (enters on next tick).

    Immediate (and other types):
      - Push with skip_momentum_check=True → enters directly, no momentum wait.
    """
    original_cfgs = strategy_cfg.get('ListOfLegConfigs') or []
    if not original_cfgs:
        original_cfgs = list((_resolve_trade_leg_configs(trade) or {}).values())
    new_ids: list[str] = []
    existing_ids = {str(l.get('id') or '') for l in (trade.get('legs') or []) if isinstance(l, dict)}
    ts_sfx = now_ts.replace(':', '').replace('T', '').replace('-', '')[:14]
    is_like_original = 'LikeOriginal' in reentry_type
    for lc in original_cfgs:
        if not isinstance(lc, dict):
            continue
        orig_id = str(lc.get('id') or '')
        if not orig_id:
            continue
        new_id  = f'{orig_id}-ore-{ts_sfx}'
        if new_id in existing_ids:
            continue
        leg_type = f'{orig_id}-overall_reentry'

        new_leg = _build_pending_leg(
            new_id,
            lc,
            trade,
            now_ts,
            f'overall_{orig_id}',
            leg_type=leg_type,
        )
        new_leg.update({
            'is_lazy': True,
            'is_reentered_leg': True,
            'lazy_leg_ref': orig_id,
            'parent_leg_id': orig_id,
            # Overall reentry always skips momentum — the leg must enter immediately
            # regardless of the original leg's LegMomentum config.
            'skip_momentum_check': True,
        })
        if is_like_original:
            new_leg['reentry_type'] = 'LikeOriginal'
        try:
            db._db['algo_trades'].update_one({'_id': trade_id}, {'$push': {'legs': new_leg}})
            new_ids.append(new_id)
            mark_execute_order_dirty_from_trade(trade)
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
                        if 'AtCost' in re_type:
                            try:
                                from features.notification_manager import upsert_recost_feature_status
                                _cost_price = _safe_float((leg.get('entry_trade') or {}).get('price'))
                                _reentry_id = f'{leg_id}_re_{now_ts.replace(":", "").replace("T", "")}'
                                upsert_recost_feature_status(
                                    db._db, trade, leg, now_ts,
                                    cost_price=_cost_price,
                                    exit_price=current_price,
                                    status='pending',
                                    recost_leg_id=_reentry_id,
                                )
                            except Exception as _rce:
                                log.warning('recost feature status armed error leg=%s: %s', leg_id, _rce)
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
                        if 'AtCost' in re_type:
                            try:
                                from features.notification_manager import upsert_recost_feature_status
                                _cost_price = _safe_float((leg.get('entry_trade') or {}).get('price'))
                                _reentry_id = f'{leg_id}_re_{now_ts.replace(":", "").replace("T", "")}'
                                upsert_recost_feature_status(
                                    db._db, trade, leg, now_ts,
                                    cost_price=_cost_price,
                                    exit_price=current_price,
                                    status='pending',
                                    recost_leg_id=_reentry_id,
                                )
                            except Exception as _rce:
                                log.warning('recost feature status armed error leg=%s: %s', leg_id, _rce)
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
                        # Overall reentry must enter immediately — skip LegMomentum gate.
                        'skip_momentum_check': True,
                    })
                    try:
                        db._db['algo_trades'].update_one(
                            {'_id': trade_id}, {'$push': {'legs': _new_leg}}
                        )
                        _new_ids.append(_new_id)
                        mark_execute_order_dirty_from_trade(trade)
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
            _last_overall_event_at = str(trade.get('last_overall_event_at') or '').strip().replace(' ', 'T')[:19]
            _last_overall_event_reason = str(trade.get('last_overall_event_reason') or '').strip()

            # Also check dynamic SL threshold from overall trail SL
            _dyn_sl_threshold = _safe_float(
                trade.get('current_overall_sl_threshold'),
                _effective_sl or osl_val,
            )
            _skip_same_tick_overall = bool(
                open_leg_ids
                and _last_overall_event_at
                and _last_overall_event_at == str(now_ts or '').strip()[:19]
                and _last_overall_event_reason in {'overall_sl', 'overall_target'}
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
            print(
                '[OVERALL SL DEBUG]',
                {
                    'mode': 'live',
                    'trade_id': trade_id,
                    'strategy_name': str(trade.get('name') or ''),
                    'timestamp': now_ts,
                    'current_mtm': round(current_trade_mtm, 2),
                    'base_overall_sl': round(_safe_float(osl_val), 2),
                    'reentry_done': _ore_done,
                    'cycle_number': _ore_done + 1,
                    'cycle_overall_sl': round(_safe_float(_effective_sl), 2),
                    'dynamic_sl_threshold': round(_safe_float(_dyn_sl_threshold), 2),
                    'checked_stoploss': round(_safe_float(_dyn_sl_threshold or _effective_sl), 2),
                    'would_hit_cycle_sl': bool(_safe_float(_effective_sl) > 0 and current_trade_mtm <= -_safe_float(_effective_sl)),
                    'would_hit_dynamic_sl': bool(_dyn_sl_threshold > 0 and current_trade_mtm <= -_dyn_sl_threshold),
                    'skip_same_tick_overall': _skip_same_tick_overall,
                    'last_overall_event_at': _last_overall_event_at,
                    'last_overall_event_reason': _last_overall_event_reason,
                },
            )
            if _skip_same_tick_overall:
                actions_taken.append(
                    f'{trade_id}: skipped same-tick overall recheck after {_last_overall_event_reason}'
                )
                continue
            if not _osl_triggered and _dyn_sl_threshold > 0 and current_trade_mtm <= -_dyn_sl_threshold:
                _osl_triggered = True

            # Guard: if no legs are open, these events have already fired — skip.
            if _osl_triggered and not open_leg_ids:
                _osl_triggered = False

            if _osl_triggered:
                db._db['algo_trades'].update_one(
                    {'_id': trade_id},
                    {'$set': {
                        'last_overall_event_at': now_ts,
                        'last_overall_event_reason': 'overall_sl',
                    }},
                )
                mark_execute_order_dirty_from_trade(trade)
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
                try:
                    from features.notification_manager import disable_all_trade_notifications
                    disable_all_trade_notifications(db._db, trade_id, reason='overall_sl', timestamp=now_ts)
                except Exception as _e:
                    log.warning('disable_all_trade_notifications overall_sl error: %s', _e)
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
                        mark_execute_order_dirty_from_trade(trade)
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
                    else:
                        _square_off_trade_like_manual(
                            db,
                            db._db['algo_trades'].find_one({'_id': trade_id}) or trade,
                            exit_timestamp=now_ts,
                        )
                else:
                    _square_off_trade_like_manual(
                        db,
                        db._db['algo_trades'].find_one({'_id': trade_id}) or trade,
                        exit_timestamp=now_ts,
                    )

            elif (
                open_leg_ids
                and otgt_type != 'None'
                and _safe_float(_effective_tgt) > 0
                and current_trade_mtm >= _safe_float(_effective_tgt)
            ):
                db._db['algo_trades'].update_one(
                    {'_id': trade_id},
                    {'$set': {
                        'last_overall_event_at': now_ts,
                        'last_overall_event_reason': 'overall_target',
                    }},
                )
                mark_execute_order_dirty_from_trade(trade)
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
                try:
                    from features.notification_manager import disable_all_trade_notifications
                    disable_all_trade_notifications(db._db, trade_id, reason='overall_target', timestamp=now_ts)
                except Exception as _e:
                    log.warning('disable_all_trade_notifications overall_target error: %s', _e)
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
                        mark_execute_order_dirty_from_trade(trade)
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
                    else:
                        _square_off_trade_like_manual(
                            db,
                            db._db['algo_trades'].find_one({'_id': trade_id}) or trade,
                            exit_timestamp=now_ts,
                        )
                else:
                    _square_off_trade_like_manual(
                        db,
                        db._db['algo_trades'].find_one({'_id': trade_id}) or trade,
                        exit_timestamp=now_ts,
                    )

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


# ─── Core: broker WebSocket tick processor (live-trade & fast-forward) ────────
#
# Architecture:
#   Backtest:             _backtest_minute_tick  → LTP from option_chain_historical_data
#   Live / Fast-forward:  _broker_live_tick      → LTP from broker WebSocket (on_ticks)
#
# The broker (Zerodha KiteTicker, Angel SmartWebSocket, etc.) emits a list of
# ticks on every price change.  The caller normalises them to:
#   broker_ltp_map = {token_str: last_price}
# where token_str is the composite key used in algo_trades.legs[].token,
# e.g. 'NIFTY_2025-11-04_24500_CE'  OR  the numeric/exchange token stored in the leg.
#
# For each open leg:
#   current_price = broker_ltp_map.get(leg_token)   ← live broker price
#   fallback:     = get_chain_at_time(...)           ← DB lookup if token not in map
#
# subscribe_tokens (returned) should be sent to the broker WS to subscribe
# so it starts emitting ticks for those contracts.
# ─────────────────────────────────────────────────────────────────────────────

def _build_broker_ltp_map(broker_ticks: list[dict]) -> dict[str, float]:
    """
    Normalise a list of raw broker tick dicts to { token_str: last_price }.

    Handles multiple broker tick formats:
      Zerodha KiteTicker:  {'instrument_token': 12345, 'last_price': 156.5, ...}
      Angel SmartWS:       {'tk': '12345', 'lp': '156.50', ...}
      Our own tick format: {'token': 'NIFTY_2025-11-04_24500_CE', 'ltp': 156.5}

    Returns dict with ALL available key variants so resolve_broker_ltp
    can find the right entry regardless of which format is stored in the leg.
    """
    result: dict[str, float] = {}
    for tick in (broker_ticks or []):
        if not isinstance(tick, dict):
            continue
        # Our format / already normalised
        tok = str(tick.get('token') or '').strip()
        ltp = _safe_float(tick.get('ltp') or tick.get('last_price') or tick.get('lp') or 0)
        if tok and ltp:
            result[tok] = ltp
        # Zerodha numeric instrument_token (stored as int or str)
        instr = str(tick.get('instrument_token') or '').strip()
        if instr and ltp:
            result[instr] = ltp
        # Angel SmartWS 'tk' key
        angel_tk = str(tick.get('tk') or '').strip()
        if angel_tk and ltp:
            result[angel_tk] = ltp
    return result


def _broker_live_tick(
    db: MongoData,
    trade_date: str,
    now_ts: str,
    broker_ltp_map: dict[str, float],
    activation_mode: str = 'live',
    running_trades: list[dict] | None = None,
) -> dict:
    """
    Called on every broker WebSocket tick in live-trade and fast-forward modes.

    Parameters
    ----------
    db:              MongoData instance.
    trade_date:      'YYYY-MM-DD' — today's date for live, past date for fast-forward.
    now_ts:          ISO timestamp of this tick ('YYYY-MM-DDTHH:MM:SS').
    broker_ltp_map:  { token_str: ltp } normalised from broker WS on_ticks.
                     Use _build_broker_ltp_map(raw_ticks) to prepare this.
    activation_mode: 'live' | 'fast-forward'.
    running_trades:  pre-loaded list of algo_trades docs; if None, loads from DB.

    Returns
    -------
    dict with keys:
      actions_taken      – list of audit strings
      checked_at         – now_ts echoed back
      hit_trade_ids      – list of trade_ids whose overall/broker SL-TGT fired
      hit_ltp_snapshots  – {trade_id: [{leg_id, ltp, entry_price, pnl}]}
      open_positions     – [{trade_id, leg_id, ltp, pnl}] for broadcast
      subscribe_tokens   – tokens currently active → subscribe to broker WS
    """
    from features.trading_core import (  # type: ignore
        TickContext,
        process_broker_tick,
    )

    if running_trades is None:
        _trade_query = _build_trade_query(
            trade_date,
            activation_mode=activation_mode,
            statuses=[RUNNING_STATUS],
        )
        running_trades = list(db._db['algo_trades'].find(_trade_query))

    ctx = TickContext(
        db=db,
        trade_date=trade_date,
        now_ts=now_ts,
        activation_mode=activation_mode,
    )

    result = process_broker_tick(ctx, running_trades, broker_ltp_map)

    # Build subscribe_tokens from all open legs (for broker WS subscription management)
    subscribe_tokens: list[str] = []
    for trade in running_trades:
        underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
        for leg in (trade.get('legs') or []):
            if not isinstance(leg, dict):
                continue
            if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
                continue
            if not isinstance(leg.get('entry_trade'), dict):
                continue  # pending
            entry_trade = leg.get('entry_trade') or {}
            tok = str(
                leg.get('token')
                or leg.get('instrument_token')
                or entry_trade.get('token')
                or entry_trade.get('instrument_token')
                or ''
            ).strip()
            if not tok:
                tok = make_token(
                    underlying,
                    str(leg.get('expiry_date') or ''),
                    leg.get('strike'),
                    str(leg.get('option') or ''),
                )
            if tok and tok not in subscribe_tokens:
                subscribe_tokens.append(tok)

    print('[BROKER TICK]', {
        'mode':            activation_mode,
        'timestamp':       now_ts,
        'ticks_received':  len(broker_ltp_map),
        'running_trades':  len(running_trades),
        'subscribe_count': len(subscribe_tokens),
        'actions':         result.actions_taken,
        'hit_trades':      result.hit_trade_ids,
    })

    return {
        'actions_taken':    result.actions_taken,
        'checked_at':       result.checked_at,
        'hit_trade_ids':    result.hit_trade_ids,
        'hit_ltp_snapshots': result.hit_ltp_snapshots,
        'open_positions':   result.open_positions,
        'subscribe_tokens': subscribe_tokens,
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

    # ── Emit hit strategies + LTP snapshot to execute-orders socket ─────────
    _bt_hit_ids       = bt_tick_result.get('hit_trade_ids') or []
    _bt_ltp_snapshots = bt_tick_result.get('hit_ltp_snapshots') or {}
    if _bt_hit_ids:
        try:
            _hit_query = {'_id': {'$in': _bt_hit_ids}}
            _hit_raw   = list(db._db['algo_trades'].find(_hit_query))
            _hit_ser   = [_serialize_trade_record(r) for r in _hit_raw]
            _hit_enr   = _populate_legs_from_history(db, _hit_ser)

            # Group hit records by user_id for per-user routing
            _hit_by_user: dict[str, list[dict]] = {}
            for _r in _hit_enr:
                _u = str(_r.get('user_id') or '').strip()
                _hit_by_user.setdefault(_u, []).append(_r)

            async def _emit_hit_per_user() -> None:
                for _uid, _uid_records in _hit_by_user.items():
                    _payload = _build_execute_order_socket_payload(_uid_records, trigger='sl-target-hit')
                    _payload['hit_ltp_snapshots'] = _bt_ltp_snapshots
                    _msg = _build_message('execute_order', 'Strategy SL/Target hit', _payload)
                    if _uid:
                        await _broadcast_user_channel_message(_uid, 'execute-orders', _msg)
                    else:
                        await _broadcast_channel_message('execute-orders', _msg)

            try:
                _loop = asyncio.get_event_loop()
                _loop.create_task(_emit_hit_per_user())
            except RuntimeError:
                asyncio.run(_emit_hit_per_user())
            print(f'[SL/TGT HIT EMIT] sent {len(_hit_enr)} strategy(s) to execute-orders hit_ids={_bt_hit_ids}')
            for _tid, _ltp_snap in _bt_ltp_snapshots.items():
                print(f'  [LTP SNAPSHOT] trade={_tid} legs={[{"leg_id": l.get("leg_id"), "ltp": l.get("ltp"), "entry_price": l.get("entry_price"), "pnl": l.get("pnl")} for l in _ltp_snap]}')
        except Exception as _he:
            log.warning('[SL/TGT HIT EMIT] error: %s', _he)

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
    normalized_records = _attach_broker_configuration_details(db, normalized_records)
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


def _get_connected_user_ids(channel: str) -> list[str]:
    """Return list of user_ids that have at least one active socket for this channel."""
    ch = str(channel or '').strip()
    if not ch:
        return []
    suffix = f':{ch}'
    result: list[str] = []
    for room_key, sockets in CONNECTED_USER_CHANNEL_WEBSOCKETS.items():
        if room_key.endswith(suffix) and sockets:
            uid = room_key[: -len(suffix)]
            if uid:
                result.append(uid)
    return result


def _build_user_simulation_messages(
    result: dict,
    normalized_records: list[dict],
    user_id: str,
) -> dict[str, str]:
    """
    Build all socket messages for one user by filtering normalized_records to
    only that user's trades.  LTP and entry_snapshots are shared market data
    so they are sent to every user unfiltered.
    """
    uid = str(user_id or '').strip()

    # Records for this user only
    uid_records = [r for r in normalized_records if str(r.get('user_id') or '').strip() == uid]

    # trade_id set for this user (used to filter entries_executed + open_positions)
    uid_trade_ids: set[str] = {str(r.get('_id') or '').strip() for r in uid_records if r.get('_id')}

    entries_executed  = result.get('entries_executed') or []
    open_positions    = result.get('open_positions') or []
    entry_snapshots   = result.get('entry_snapshots') or []
    selected_date     = str(result.get('trade_date') or '')
    listen_time       = str(result.get('listen_time') or '')
    listen_timestamp  = str(result.get('listen_timestamp') or '')

    uid_entries        = [e for e in entries_executed if str(e.get('trade_id') or '').strip() in uid_trade_ids]
    uid_open_positions = [p for p in open_positions    if str(p.get('trade_id') or '').strip() in uid_trade_ids]

    execute_order_payload = _build_execute_order_socket_payload(uid_records, trigger='manual-simulator')
    execute_order_payload['entries_executed'] = uid_entries
    execute_order_payload['entries_count']    = len(uid_entries)

    return {
        'execute-orders': _build_message(
            'execute_order',
            'Strategy execution refreshed',
            execute_order_payload,
        ),
        'update': _build_message(
            'update',
            'Open position snapshot refreshed',
            {
                'trade_date':       selected_date,
                'activation_mode':  'algo-backtest',
                'status':           'algo-backtest',
                'trigger_reason':   'manual_simulator_run',
                'refresh_status':   'completed',
                'open_positions':   uid_open_positions,
                'count':            len(uid_open_positions),
            },
        ),
        'executions': _build_message(
            'countdown_update',
            'Execution listening time updated',
            {
                'selected_date':    selected_date,
                'listen_time':      listen_time,
                'listen_timestamp': listen_timestamp,
                'records':          uid_records,
                'count':            len(uid_records),
                'entry_snapshots':  entry_snapshots,   # market data — sent to all users
                'entries_executed': uid_entries,
                'mode':             'backtest',
            },
        ),
        'ltp_update': _build_message(
            'ltp_update',
            'LTP update for active positions',
            {
                'trade_date':       selected_date,
                'listen_time':      listen_time,
                'listen_timestamp': listen_timestamp,
                'ltp':              result.get('ltp') or [],  # market data — sent to all users
            },
        ),
    }


async def broadcast_backtest_simulation_step(
    db: MongoData,
    result: dict,
    prebuilt_messages: dict[str, str] | None = None,
) -> dict[str, int]:
    """
    Broadcast simulation step to connected WebSocket clients.

    Change-detection rules (per user_id):
      • execute_order  → only when algo_trades / algo_trade_positions_history /
                         algo_leg_feature_status data changed (signature diff)
      • update         → only when open_positions changed (signature diff)
      • ltp_update     → every tick (market prices)
      • countdown_update → every tick (time progresses)

    Non-user-room (legacy) sockets always receive the full payload unchanged.
    """
    # Populate legs once — covers algo_trades + algo_trade_positions_history
    # + algo_leg_feature_status (called inside _populate_legs_from_history)
    normalized_records = _populate_legs_from_history(db, result.get('records') or [])
    normalized_records = _attach_broker_configuration_details(db, normalized_records)

    # ── Non-user (legacy) sockets → full payload every tick ─────────────────
    full_msgs = prebuilt_messages or build_backtest_simulation_socket_messages(db, result)
    # execute_order is user-sensitive strategy state, so never fan it out through
    # legacy non-user sockets. It must flow only via user rooms below.
    d_eo  = 0
    d_upd = await _broadcast_channel_message('update',         full_msgs.get('update') or '')
    d_ltp = await _broadcast_channel_message('update',         full_msgs.get('ltp_update') or '')
    d_exc = await _broadcast_channel_message('executions',     full_msgs.get('executions') or '')

    # ── User rooms → per-user, change-gated payload ─────────────────────────
    eo_uids  = set(_get_connected_user_ids('execute-orders'))
    upd_uids = set(_get_connected_user_ids('update'))
    exc_uids = set(_get_connected_user_ids('executions'))
    d_eo += await flush_pending_execute_order_emits(db, user_ids=eo_uids, force=False)
    all_uids = upd_uids | exc_uids

    for uid in all_uids:
        user_msgs = _build_user_simulation_messages(result, normalized_records, uid)

        # ── update (open_positions): only if positions changed
        if uid in upd_uids:
            uid_open_positions = [
                p for p in (result.get('open_positions') or [])
                if str(p.get('trade_id') or '').strip() in
                {str(r.get('_id') or '') for r in normalized_records if str(r.get('user_id') or '').strip() == uid}
            ]
            pos_sig = _position_signature(uid_open_positions)
            if LAST_OPEN_POSITION_SIG.get(uid) != pos_sig:
                LAST_OPEN_POSITION_SIG[uid] = pos_sig
                d_upd += await _broadcast_user_channel_message(uid, 'update', user_msgs['update'])
                log.debug('[EMIT update] user=%s sig_changed=True', uid)

            # ltp_update always (market prices change every tick)
            d_ltp += await _broadcast_user_channel_message(uid, 'update', user_msgs['ltp_update'])

        # ── countdown_update always (time progresses every tick)
        if uid in exc_uids:
            d_exc += await _broadcast_user_channel_message(uid, 'executions', user_msgs['executions'])

    return {
        'execute-orders': d_eo,
        'update':         d_upd + d_ltp,
        'executions':     d_exc,
    }


# ─── WebSocket handler ────────────────────────────────────────────────────────

@socket_router.websocket('/ws/executions')
async def executions_socket(
    websocket: WebSocket,
    user_id: str = Query(default=''),
):
    await websocket.accept()
    _register_channel_websocket('executions', websocket)
    uid = str(user_id or '').strip()
    if uid:
        _register_user_websocket('executions', uid, websocket)
    await websocket.send_text(_build_message(
        'connection_established',
        'Algo execution websocket connected',
        {'channel': 'executions', 'user_id': uid},
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
        _unregister_user_websocket(websocket)
        _clear_listening_strategy_snapshot(selected_date)
        _clear_market_cache_snapshot(market_cache)
        db.close()


@socket_router.websocket('/ws/execute-orders')
async def execute_orders_socket(
    websocket: WebSocket,
    user_id: str = Query(default=''),
):
    await websocket.accept()
    _register_channel_websocket('execute-orders', websocket)
    uid = str(user_id or '').strip()
    if uid:
        _register_user_websocket('execute-orders', uid, websocket)
    await websocket.send_text(_build_message(
        'connection_established',
        'Execute orders websocket connected',
        {'channel': 'execute-orders', 'user_id': uid},
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
                requested_user_id = str((parsed_message or {}).get('user_id') or '').strip()
                requested_autoload = (parsed_message or {}).get('autoload')
                if requested_user_id and requested_user_id != uid:
                    uid = requested_user_id
                    _register_user_websocket('execute-orders', uid, websocket)
                if requested_date:
                    trade_date = requested_date
                    seen_signatures = {}
                    clear_user_emit_signatures(uid)
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
                    if uid:
                        cancel_query['user_id'] = uid
                    if cancel_strategy_id:
                        cancel_query['_id'] = cancel_strategy_id
                    elif cancel_group_id:
                        cancel_query['portfolio.group_id'] = cancel_group_id
                    algo_trades_col = db._db['algo_trades']
                    cancelled_records: list[dict] = []
                    affected_group_id = cancel_group_id
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
                        if uid:
                            group_reload_query['user_id'] = uid
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
                    if uid:
                        await emit_execute_order_for_user(
                            db,
                            user_id=uid,
                            trade_date=trade_date,
                            activation_mode=activation_mode,
                            group_id=cancel_group_id or affected_group_id,
                            trade_ids=[cancel_strategy_id] if cancel_strategy_id else [],
                            trigger='cancel-deployment',
                            message='Deployment cancelled',
                            force=True,
                        )
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
                    if uid:
                        sq_query['user_id'] = uid
                    if sq_strategy_id:
                        sq_query['$or'] = [{'_id': sq_strategy_id}, {'strategy_id': sq_strategy_id}]
                    elif sq_group_id:
                        sq_query['portfolio.group_id'] = sq_group_id

                    sq_records: list[dict] = []
                    affected_sq_group_id = sq_group_id
                    try:
                        trade_records = list(algo_trades_col.find(sq_query))
                        for trade_rec in trade_records:
                            _square_off_trade_like_manual(
                                db,
                                trade_rec,
                                exit_timestamp=exit_timestamp,
                            )

                        # Reload affected group and emit
                        affected_sq_group_id = sq_group_id or str(
                            (((trade_records[0].get('portfolio') or {}).get('group_id')) or '') if trade_records else ''
                        )
                        sq_reload_query = _build_trade_query(sq_trade_date, activation_mode=activation_mode)
                        sq_reload_query.pop('trade_status', None)
                        sq_reload_query.pop('status', None)
                        if uid:
                            sq_reload_query['user_id'] = uid
                        if sq_strategy_id:
                            sq_reload_query['$or'] = [{'_id': sq_strategy_id}, {'strategy_id': sq_strategy_id}]
                        elif affected_sq_group_id:
                            sq_reload_query['portfolio.group_id'] = affected_sq_group_id
                        raw_sq_records = list(algo_trades_col.find(sq_reload_query))
                        sq_records = [_serialize_trade_record(r) for r in raw_sq_records]
                    except Exception as exc:
                        log.error('squared-off error: %s', exc)
                        print(f'[SQUARE OFF] error: {exc}')

                    if uid:
                        await emit_execute_order_for_user(
                            db,
                            user_id=uid,
                            trade_date=sq_trade_date or trade_date,
                            activation_mode=activation_mode,
                            group_id=sq_group_id or affected_sq_group_id,
                            trade_ids=[sq_strategy_id] if sq_strategy_id else [],
                            trigger='squared-off',
                            message='Strategy squared off',
                            force=True,
                        )
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
                    current_records = _load_execute_order_group_records(
                        db,
                        trade_date,
                        activation_mode,
                        subscribed_group_id,
                        uid,
                    )
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

            # execute_order emits are driven by explicit DB-mutation call sites.
            # Keep the socket alive here, but do not poll and push structural state.
            continue

    except WebSocketDisconnect:
        return
    finally:
        _unregister_channel_websocket('execute-orders', websocket)
        _unregister_user_websocket(websocket)
        db.close()


@socket_router.websocket('/ws/update')
async def update_socket(
    websocket: WebSocket,
    user_id: str = Query(default=''),
):
    await websocket.accept()
    _register_channel_websocket('update', websocket)
    uid = str(user_id or '').strip()
    if uid:
        _register_user_websocket('update', uid, websocket)
    await websocket.send_text(_build_message(
        'connection_established',
        'Update websocket connected',
        {'channel': 'update', 'user_id': uid},
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

    # ── Kite broker WebSocket — live LTP (common account, per-user filter) ──
    kite_ltp_queue: asyncio.Queue = asyncio.Queue(maxsize=500)
    kite_event_loop = asyncio.get_event_loop()
    kite_registered = False          # True once listener is attached
    # str(instrument_token) → only this user's open position tokens
    kite_user_token_set: set[str] = set()

    def _kite_on_tick(ltp_map: dict) -> None:
        """Tick callback — runs in KiteTicker thread. Filter to this user's tokens."""
        if not kite_user_token_set:
            return
        user_ltp = {tok: ltp for tok, ltp in ltp_map.items() if tok in kite_user_token_set}
        if not user_ltp:
            return
        try:
            kite_event_loop.call_soon_threadsafe(kite_ltp_queue.put_nowait, user_ltp)
        except Exception:
            pass

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
        # ── Update Kite shared WS subscription (live mode, common account) ───────
        if kite_registered and uid:
            from features.kite_broker_ws import (
                extract_instrument_tokens,
                refresh_user_tokens as _kite_refresh,
            )
            num_toks = extract_instrument_tokens(subscribe_tokens)
            kite_user_token_set.clear()
            kite_user_token_set.update(str(t) for t in num_toks)
            _kite_refresh(uid, num_toks)
            print(f'[KITE TOKENS] user={uid} subscribed={num_toks}')
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
                # ── Auto-register with Kite when live mode activated ───────────────
                if requested_mode == 'live' and not kite_registered and uid:
                    from features.kite_broker_ws import (
                        add_tick_listener as _kite_atl,
                        extract_instrument_tokens,
                        is_configured as _kite_ok,
                        refresh_user_tokens as _kite_refresh,
                    )
                    if _kite_ok():
                        _kite_atl(_kite_on_tick)
                        kite_registered = True
                        if subscribe_tokens:
                            num_toks = extract_instrument_tokens(subscribe_tokens)
                            kite_user_token_set.clear()
                            kite_user_token_set.update(str(t) for t in num_toks)
                            _kite_refresh(uid, num_toks)
                        print(f'[KITE LTP] auto-started user={uid} tokens={len(kite_user_token_set)}')
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
                            # emit hit strategies + LTP snapshot to execute-orders socket
                            _ws_hit_ids       = bt_tick_result.get('hit_trade_ids') or []
                            _ws_ltp_snapshots = bt_tick_result.get('hit_ltp_snapshots') or {}
                            if _ws_hit_ids:
                                try:
                                    _ws_hit_raw = list(db._db['algo_trades'].find({'_id': {'$in': _ws_hit_ids}}))
                                    _ws_hit_ser = [_serialize_trade_record(r) for r in _ws_hit_raw]
                                    _ws_hit_enr = _populate_legs_from_history(db, _ws_hit_ser)
                                    # Group by user_id and send per-user
                                    _ws_by_user: dict[str, list[dict]] = {}
                                    for _wr in _ws_hit_enr:
                                        _wu = str(_wr.get('user_id') or '').strip()
                                        _ws_by_user.setdefault(_wu, []).append(_wr)
                                    for _wu, _wu_records in _ws_by_user.items():
                                        _ws_payload = _build_execute_order_socket_payload(_wu_records, trigger='sl-target-hit')
                                        _ws_payload['hit_ltp_snapshots'] = _ws_ltp_snapshots
                                        _ws_hit_msg = _build_message('execute_order', 'Strategy SL/Target hit', _ws_payload)
                                        if _wu:
                                            await _broadcast_user_channel_message(_wu, 'execute-orders', _ws_hit_msg)
                                        else:
                                            await _broadcast_channel_message('execute-orders', _ws_hit_msg)
                                    print(f'[SL/TGT HIT EMIT] sent {len(_ws_hit_enr)} strategy(s) to execute-orders hit_ids={_ws_hit_ids}')
                                    for _tid, _ltp_snap in _ws_ltp_snapshots.items():
                                        print(f'  [LTP SNAPSHOT] trade={_tid} legs={[{"leg_id": l.get("leg_id"), "ltp": l.get("ltp"), "entry_price": l.get("entry_price"), "pnl": l.get("pnl")} for l in _ltp_snap]}')
                                except Exception as _whe:
                                    log.warning('[SL/TGT HIT EMIT] ws error: %s', _whe)
                        except Exception as _bte:
                            log.warning('_backtest_minute_tick error: %s', _bte)

                    if _is_algo_backtest_status(subscription_status):
                        try:
                            await flush_pending_execute_order_emits(
                                db,
                                user_ids=set(_get_connected_user_ids('execute-orders')),
                                force=False,
                            )
                        except Exception as _fee:
                            log.warning('flush_pending_execute_order_emits error: %s', _fee)

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

            # ── Kite live LTP: drain queue → emit ltp_update for this user ──
            if kite_registered and not kite_ltp_queue.empty():
                merged_ltp: dict[str, float] = {}
                while not kite_ltp_queue.empty():
                    try:
                        merged_ltp.update(kite_ltp_queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                if merged_ltp:
                    kite_ltp_list = [
                        {'token': tok, 'ltp': ltp}
                        for tok, ltp in merged_ltp.items()
                    ]
                    await websocket.send_text(_build_message(
                        'ltp_update',
                        'Live Kite LTP update',
                        {
                            'trade_date': trade_date,
                            'listen_time': _now_ist_str(),
                            'listen_timestamp': _now_iso(),
                            'ltp': kite_ltp_list,
                        },
                    ))
            continue

    except WebSocketDisconnect:
        return
    finally:
        _unregister_channel_websocket('update', websocket)
        _unregister_user_websocket(websocket)
        _clear_market_cache_snapshot(market_cache)
        if kite_registered:
            from features.kite_broker_ws import (
                remove_tick_listener as _kite_rtl,
                unregister_user as _kite_unreg,
            )
            _kite_rtl(_kite_on_tick)
            _kite_unreg(uid)
        db.close()


# ─── Shared Kite broker WebSocket — live LTP streaming ────────────────────────
#
# Endpoint: /algo/ws/live-ltp
#
# One KiteTicker connection is shared across ALL connected users.
# Tokens from every user's open positions are merged into a single subscription
# set.  When Kite emits a tick, LTP data is fanned-out to every connected
# frontend WebSocket.
#
# Frontend handshake (send as first JSON message after connecting):
#   {
#     "action":       "subscribe",
#     "api_key":      "<zerodha api_key>",
#     "access_token": "<zerodha access_token>",
#     "user_id":      "<your user_id>",
#     "tokens":       [12345678, 87654321, ...]   // numeric instrument tokens
#   }
#
# To update subscriptions (e.g. a new leg opened):
#   {"action": "update_tokens", "tokens": [...]}
#
# Server → client messages:
#   {"type": "ltp_subscribed",  "data": {"token_count": N}}
#   {"type": "ltp_update",      "data": {"ltp": [{"token": "12345", "ltp": 156.5}, ...]}}
#   {"type": "ltp_error",       "data": {"message": "..."}}
# ─────────────────────────────────────────────────────────────────────────────

@socket_router.websocket('/ws/live-ltp')
async def live_ltp_socket(
    websocket: WebSocket,
    user_id: str = Query(default=''),
):
    """
    Shared Kite broker WebSocket endpoint.

    Multiple frontend clients connect here.  A single KiteTicker connection
    (per api_key) is maintained server-side.  All clients' tokens are merged
    into one shared Kite subscription.

    Each client only receives LTP updates for *its own* open position tokens —
    not for other users' tokens.  The Kite connection itself is shared
    (one subscribe call for the union of all tokens), but the data emitted
    to each frontend WebSocket is filtered per-user.
    """
    import asyncio as _asyncio

    from features.kite_broker_ws import (
        extract_instrument_tokens,
        get_ltp_map,
        refresh_user_tokens,
        register_user_tokens,
        unregister_user,
    )

    await websocket.accept()

    uid = str(user_id or '').strip()
    api_key_used = ''
    queue: asyncio.Queue = asyncio.Queue(maxsize=500)
    loop = asyncio.get_event_loop()

    # Tokens that THIS user cares about (str representation, e.g. "12345678")
    # LTP update is filtered to only these tokens before sending to the client.
    user_token_set: set[str] = set()

    # Tick listener — called from KiteTicker's thread.
    # Puts ONLY this user's token LTPs into this socket's asyncio queue.
    def _on_tick(api_key: str, ltp_map: dict) -> None:
        if not user_token_set:
            return
        # Filter: keep only tokens this user has open positions in
        user_ltp = {tok: ltp for tok, ltp in ltp_map.items() if tok in user_token_set}
        if not user_ltp:
            return
        try:
            loop.call_soon_threadsafe(queue.put_nowait, user_ltp)
        except Exception:
            pass

    try:
        await websocket.send_text(json.dumps({
            'type': 'connected',
            'data': {'message': 'live-ltp socket connected', 'user_id': uid},
        }))

        while True:
            # Non-blocking: check for incoming client messages (subscribe/update)
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=0.05)
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    msg = {}

                action = str((msg or {}).get('action') or '').strip().lower()

                if action in ('subscribe', 'update_tokens'):
                    new_api_key     = str((msg or {}).get('api_key') or '').strip()
                    new_access_tok  = str((msg or {}).get('access_token') or '').strip()
                    raw_tokens      = (msg or {}).get('tokens') or []
                    if msg.get('user_id'):
                        uid = str(msg['user_id']).strip()

                    int_tokens: list[int] = []
                    for t in raw_tokens:
                        try:
                            int_tokens.append(int(t))
                        except (TypeError, ValueError):
                            pass

                    # Update this socket's filter set (str keys matching ltp_map)
                    user_token_set.clear()
                    user_token_set.update(str(t) for t in int_tokens)

                    if action == 'subscribe' and new_api_key and new_access_tok:
                        # First subscription — register listener on this api_key
                        if api_key_used and api_key_used != new_api_key:
                            # api_key changed — deregister from old key
                            from features.kite_broker_ws import remove_tick_listener as _rtl
                            _rtl(api_key_used, _on_tick)
                            unregister_user(uid, api_key_used)

                        api_key_used = new_api_key
                        # Shared subscription: adds this user's tokens to the global set
                        register_user_tokens(uid, api_key_used, new_access_tok, int_tokens)

                        from features.kite_broker_ws import add_tick_listener as _atl
                        _atl(api_key_used, _on_tick)

                        await websocket.send_text(json.dumps({
                            'type': 'ltp_subscribed',
                            'data': {
                                'user_id': uid,
                                'token_count': len(int_tokens),
                                'tokens': int_tokens,
                            },
                        }))
                        log.info(
                            '[live-ltp] user=%s subscribed %d token(s) on api_key=%s',
                            uid, len(int_tokens), api_key_used,
                        )

                    elif action == 'update_tokens' and api_key_used:
                        # Token set changed (new leg entered / leg closed)
                        access_tok = str((msg or {}).get('access_token') or new_access_tok or '').strip()
                        if access_tok:
                            refresh_user_tokens(uid, api_key_used, access_tok, int_tokens)
                            await websocket.send_text(json.dumps({
                                'type': 'ltp_subscribed',
                                'data': {
                                    'user_id': uid,
                                    'token_count': len(int_tokens),
                                    'tokens': int_tokens,
                                },
                            }))
                            log.info(
                                '[live-ltp] user=%s updated to %d token(s)',
                                uid, len(int_tokens),
                            )

            except asyncio.TimeoutError:
                pass  # no client message — continue to drain tick queue

            # Drain tick queue — send pending LTP updates to this client
            # (already pre-filtered to this user's tokens in _on_tick)
            ltp_batch: list[dict] = []
            while not queue.empty():
                try:
                    ltp_batch.append(queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            if ltp_batch:
                # Merge all pending updates; latest value per token wins
                merged: dict[str, float] = {}
                for snapshot in ltp_batch:
                    merged.update(snapshot)

                ltp_list = [
                    {'token': tok, 'ltp': price}
                    for tok, price in merged.items()
                ]
                await websocket.send_text(json.dumps({
                    'type': 'ltp_update',
                    'data': {
                        'ltp': ltp_list,
                        'count': len(ltp_list),
                    },
                }))

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.warning('[live-ltp] socket error user=%s: %s', uid, exc)
        try:
            await websocket.send_text(json.dumps({
                'type': 'ltp_error',
                'data': {'message': str(exc)},
            }))
        except Exception:
            pass
    finally:
        if api_key_used:
            from features.kite_broker_ws import remove_tick_listener as _rtl
            _rtl(api_key_used, _on_tick)
            unregister_user(uid, api_key_used)
            log.info('[live-ltp] user=%s disconnected — tokens unregistered', uid)
