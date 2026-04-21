"""
live_event.py
─────────────
Live-mode market-data adapter.

Job: expose only market-data fetch helpers for live mode with the same public
contract as algo_backtest_event.py / fast_forward_event.py.

Important:
  - This file is data-only.
  - No execution logic, SL/TP logic, re-entry logic, or DB write logic belongs here.
  - execution_socket.py remains the shared execution/action layer.

Today, the live adapter reuses the common helper implementations so the mode
boundary is stable even while live entry prices come from Kite socket flow in
live_monitor_socket.py.
"""

from __future__ import annotations

from typing import Any

from features.mongo_data import MongoData
from features.algo_backtest_event import (
    INDEX_SPOT_COLLECTION,
    OPEN_LEG_STATUS,
    OPTION_CHAIN_COLLECTION,
    get_chain_doc_at_time,
    get_chain_doc_by_token,
    get_latest_chain_doc,
    get_open_legs_ltp_array,
    get_option_ltp,
    get_spot_doc_at_time,
    get_spot_price,
)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _subscribe_live_option_token(token: str, symbol: str = '') -> None:
    normalized_token = str(token or '').strip()
    if not normalized_token:
        return
    try:
        from features.kite_ticker import ticker_manager

        if not ticker_manager._ticker or ticker_manager.status != 'running':
            return
        if normalized_token in getattr(ticker_manager, 'subscribed_tokens', set()):
            if symbol:
                ticker_manager.register_option_token(normalized_token, symbol)
            return
        subscribe_token = int(normalized_token)
        ticker_manager._ticker.subscribe([subscribe_token])
        ticker_manager._ticker.set_mode(ticker_manager._ticker.MODE_LTP, [subscribe_token])
        ticker_manager.register_option_token(normalized_token, symbol)
        print(f'[LIVE OPTION SUBSCRIBE] token={normalized_token} symbol={symbol or "-"}')
    except Exception:
        return


def sync_live_open_position_subscriptions(trade_date: str = '') -> int:
    db = MongoData()
    try:
        query: dict[str, Any] = {
            'activation_mode': 'live',
            'active_on_server': True,
            'trade_status': 1,
            'status': 'StrategyStatus.Live_Running',
        }
        normalized_trade_date = str(trade_date or '').strip()
        if normalized_trade_date:
            query['creation_ts'] = {'$regex': f'^{normalized_trade_date}'}

        trades = list(db._db['algo_trades'].find(query, {'_id': 1, 'name': 1}))
        trade_ids = [str(item.get('_id') or '').strip() for item in trades if str(item.get('_id') or '').strip()]
        if not trade_ids:
            return 0

        subscribed = 0
        for row in db._db['algo_trade_positions_history'].find(
            {
                'trade_id': {'$in': trade_ids},
                'status': 1,
                'exit_trade': None,
            },
            {
                'trade_id': 1,
                'token': 1,
                'symbol': 1,
                'leg_id': 1,
            },
        ):
            token = str(row.get('token') or '').strip()
            if not token:
                continue
            symbol = str(row.get('symbol') or '').strip()
            _subscribe_live_option_token(token, symbol)
            subscribed += 1
        print(
            f'[LIVE OPEN POSITION SUBSCRIBE] trade_date={normalized_trade_date or "-"} '
            f'trades={len(trade_ids)} subscribed_tokens={subscribed}'
        )
        return subscribed
    except Exception:
        return 0
    finally:
        try:
            db.close()
        except Exception:
            pass


def resolve_live_pending_entry_snapshot(
    db,
    trade: dict,
    leg_cfg: dict,
    *,
    now_ts: str,
) -> dict:
    underlying = str(
        (trade.get('strategy') or {}).get('Ticker')
        or (trade.get('config') or {}).get('Ticker')
        or trade.get('ticker')
        or ''
    ).strip().upper()
    if not underlying:
        return {}
    try:
        from features.kite_ticker import ticker_manager
        from features.backtest_engine import STRIKE_STEPS, _resolve_expiry, _resolve_strike
        from features.spot_atm_utils import resolve_atm_price
    except Exception:
        return {}

    spot_price = _safe_float(ticker_manager.get_spot(underlying))
    if spot_price <= 0:
        return {}

    contract_cfg = leg_cfg.get('ContractType') or {}
    option_raw = str(contract_cfg.get('Option') or leg_cfg.get('InstrumentKind') or '')
    option_type = option_raw.split('.')[-1] if '.' in option_raw else option_raw
    expiry_kind = str(contract_cfg.get('Expiry') or leg_cfg.get('ExpiryKind') or 'ExpiryType.Weekly')
    strike_param = str(contract_cfg.get('StrikeParameter') or leg_cfg.get('StrikeParameter') or 'StrikeType.ATM')
    step = STRIKE_STEPS.get(underlying, 50)
    atm_price = resolve_atm_price(underlying, spot_price) if spot_price > 0 else 0
    strike = _resolve_strike(spot_price, strike_param, option_type, step) if spot_price > 0 else None

    expiry = None
    token = ''
    symbol = ''
    ltp = 0.0
    expiries = sorted([
        str(e)
        for e in db._db['active_option_tokens'].distinct(
            'expiry',
            {'instrument': underlying, 'expiry': {'$gte': str(now_ts or '')[:10]}},
        )
        if e
    ])
    expiry = _resolve_expiry(str(now_ts or '')[:10], expiry_kind, expiries) if expiries else None
    if expiry and strike not in (None, ''):
        token_doc = db._db['active_option_tokens'].find_one({
            'instrument': underlying,
            'expiry': expiry,
            'strike': strike,
            'option_type': option_type.upper(),
        }) or {}
        token = str(token_doc.get('token') or token_doc.get('tokens') or '').strip()
        symbol = str(token_doc.get('symbol') or '').strip()
        if token:
            _subscribe_live_option_token(token, symbol)
        ltp = _safe_float(ticker_manager.get_ltp(token))

    print(
        '[LIVE ENTRY SNAPSHOT] '
        f'trade={str(trade.get("_id") or "")} '
        f'leg={str(leg_cfg.get("id") or "")} '
        f'underlying={underlying} '
        f'spot_price={spot_price} '
        f'atm_price={atm_price} '
        f'strike={strike if strike not in (None, "") else "NOT_FOUND"} '
        f'option={option_type or "-"} '
        f'expiry={expiry or "NOT_FOUND"} '
        f'token={token or "NOT_FOUND"} '
        f'symbol={symbol or "-"} '
        f'ltp={ltp}'
    )
    return {
        'spot_at_queue': spot_price,
        'live_spot_price': spot_price,
        'atm_price': atm_price,
        'strike': strike,
        'expiry_date': expiry,
        'token': token,
        'symbol': symbol,
        'ltp': ltp,
    }


def resolve_live_entry_execution_payload(
    db,
    trade: dict,
    leg: dict,
    *,
    now_ts: str,
) -> dict:
    contract_cfg = {
        'id': str(leg.get('id') or ''),
        'ContractType': {
            'Option': str(leg.get('option') or leg.get('InstrumentKind') or ''),
            'Expiry': str(leg.get('expiry_kind') or leg.get('ExpiryKind') or 'ExpiryType.Weekly'),
            'StrikeParameter': str(leg.get('strike_parameter') or leg.get('StrikeParameter') or 'StrikeType.ATM'),
        },
        'InstrumentKind': str(leg.get('option') or leg.get('InstrumentKind') or ''),
        'ExpiryKind': str(leg.get('expiry_kind') or leg.get('ExpiryKind') or 'ExpiryType.Weekly'),
        'StrikeParameter': str(leg.get('strike_parameter') or leg.get('StrikeParameter') or 'StrikeType.ATM'),
    }
    snapshot = resolve_live_pending_entry_snapshot(
        db,
        trade,
        contract_cfg,
        now_ts=now_ts,
    ) or {}
    entry_price = _safe_float(snapshot.get('ltp'))
    return {
        'spot_price': _safe_float(
            snapshot.get('spot_at_queue')
            or snapshot.get('live_spot_price')
            or leg.get('spot_at_queue')
        ),
        'strike': snapshot.get('strike') if snapshot.get('strike') not in (None, '') else leg.get('strike'),
        'expiry_date': str(snapshot.get('expiry_date') or leg.get('expiry_date') or '').strip(),
        'token': str(snapshot.get('token') or leg.get('token') or '').strip(),
        'symbol': str(snapshot.get('symbol') or leg.get('symbol') or '').strip(),
        'entry_price': entry_price,
        'current_option_price': entry_price,
        'entry_price_source': 'kite_live',
        'ltp': entry_price,
        'atm_price': snapshot.get('atm_price'),
    }


__all__ = [
    'OPTION_CHAIN_COLLECTION',
    'INDEX_SPOT_COLLECTION',
    'OPEN_LEG_STATUS',
    'get_latest_chain_doc',
    'get_chain_doc_at_time',
    'get_chain_doc_by_token',
    'get_spot_doc_at_time',
    'get_spot_price',
    'get_option_ltp',
    'get_open_legs_ltp_array',
    'resolve_live_pending_entry_snapshot',
    'resolve_live_entry_execution_payload',
    'sync_live_open_position_subscriptions',
]
