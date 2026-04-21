"""
fast_forward_event.py
─────────────────────
Fast-forward-mode market-data adapter.

Job:
  - expose price-fetch helpers for fast-forward mode
  - fetch underlying spot / quote data
  - resolve option token + symbol + entry LTP for a pending leg

Important:
  - This file is data-only.
  - No SL / TP / trail / re-entry / recost / DB write logic belongs here.
  - All execution/event handling must remain in execution_socket.py /
    trading_core.py.
"""

from __future__ import annotations

from typing import Any

from bson import ObjectId

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

QUOTE_INSTRUMENT_BY_UNDERLYING = {
    'NIFTY': 'NSE:NIFTY 50',
    'BANKNIFTY': 'NSE:NIFTY BANK',
    'FINNIFTY': 'NSE:NIFTY FIN SERVICE',
    'SENSEX': 'BSE:SENSEX',
    'MIDCPNIFTY': 'NSE:NIFTY MID SELECT',
}


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


def _normalize_expiry_key(value: Any) -> str:
    raw = str(value or '').strip()
    if not raw:
        return ''
    return raw[:10]


def _is_truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value or '').strip().lower()
    return normalized in {'1', 'true', 'yes', 'y', 'on'}


def should_use_fast_forward_quote(trade: dict) -> bool:
    if str(trade.get('activation_mode') or '').strip() != 'fast-forward':
        return False
    if 'get_quote' in trade:
        return _is_truthy_flag(trade.get('get_quote'))
    strategy_cfg = trade.get('strategy') or {}
    config_cfg = trade.get('config') or {}
    if 'get_quote' in strategy_cfg:
        return _is_truthy_flag(strategy_cfg.get('get_quote'))
    return _is_truthy_flag(config_cfg.get('get_quote'))


def _build_kite_quote_instrument(symbol: str, underlying: str) -> str:
    normalized_symbol = str(symbol or '').strip()
    if not normalized_symbol:
        return ''
    if ':' in normalized_symbol:
        return normalized_symbol
    normalized_underlying = str(underlying or '').strip().upper()
    exchange = 'BFO' if normalized_underlying in {'SENSEX', 'BANKEX'} else 'NFO'
    return f'{exchange}:{normalized_symbol}'


def _get_socket_entry_ltp(token: str) -> float:
    normalized_token = str(token or '').strip()
    if not normalized_token:
        return 0.0
    try:
        from features.kite_ticker import ticker_manager
        return _safe_float(ticker_manager.get_ltp(normalized_token))
    except Exception:
        return 0.0


def _subscribe_option_token(token: str, symbol: str = '') -> None:
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
        print(f'[FF OPTION SUBSCRIBE] token={normalized_token}')
    except Exception:
        return


def sync_fast_forward_open_position_subscriptions(trade_date: str = '') -> int:
    db = MongoData()
    try:
        query: dict[str, Any] = {
            'activation_mode': 'fast-forward',
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
            _subscribe_option_token(token, symbol)
            subscribed += 1
        print(
            f'[FF OPEN POSITION SUBSCRIBE] trade_date={normalized_trade_date or "-"} '
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


def _get_quote_access_token(db, trade: dict) -> str:
    broker_id = str(trade.get('broker') or '').strip()
    if broker_id:
        broker_doc = db._db['broker_configuration'].find_one(
            {'_id': ObjectId(broker_id)},
            {'access_token': 1},
        ) or {}
        access_token = str(broker_doc.get('access_token') or '').strip()
        if access_token:
            return access_token
    market_cfg = db._db['kite_market_config'].find_one({'enabled': True}, {'access_token': 1}) or {}
    return str(market_cfg.get('access_token') or '').strip()


def get_fast_forward_quote_spot_price(db, trade: dict, underlying: str) -> tuple[float, str]:
    normalized_underlying = str(underlying or '').strip().upper()
    instrument = QUOTE_INSTRUMENT_BY_UNDERLYING.get(normalized_underlying, '')
    if not instrument or not should_use_fast_forward_quote(trade):
        return 0.0, instrument
    try:
        from features.kite_broker import get_kite_instance

        access_token = _get_quote_access_token(db, trade)
        if not access_token:
            return 0.0, instrument
        kite = get_kite_instance(access_token)
        quote_map = kite.quote([instrument]) or {}
        quote_doc = quote_map.get(instrument) or {}
        for key in ('last_price', 'last_trade_price'):
            price = _safe_float(quote_doc.get(key))
            if price > 0:
                return price, instrument
        ohlc = quote_doc.get('ohlc') if isinstance(quote_doc.get('ohlc'), dict) else {}
        for key in ('close', 'open'):
            price = _safe_float(ohlc.get(key))
            if price > 0:
                return price, instrument
    except Exception:
        return 0.0, instrument
    return 0.0, instrument


def resolve_fast_forward_pending_entry_snapshot(
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

    spot_price, quote_instrument = get_fast_forward_quote_spot_price(db, trade, underlying)
    if spot_price <= 0:
        try:
            from features.kite_ticker import ticker_manager
            spot_price = _safe_float(ticker_manager.get_spot(underlying))
        except Exception:
            spot_price = 0.0
    if spot_price <= 0:
        return {}

    from features.backtest_engine import STRIKE_STEPS, _resolve_expiry, _resolve_strike
    from features.spot_atm_utils import resolve_atm_price

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
    active_tokens_col = db._db['active_option_tokens']
    date_key = str(now_ts or '')[:10]
    token_base_query = {
        '$or': [
            {'instrument': underlying},
            {'underlying': underlying},
        ],
    }
    expiry_candidates: set[str] = set()
    for row in active_tokens_col.find(
        token_base_query,
        {'expiry': 1, 'expiry_date': 1},
    ):
        expiry_value = _normalize_expiry_key(row.get('expiry') or row.get('expiry_date'))
        if expiry_value and expiry_value >= date_key:
            expiry_candidates.add(expiry_value)
    expiries = sorted(expiry_candidates)
    expiry = _resolve_expiry(date_key, expiry_kind, expiries) if expiries else None
    if expiry and strike not in (None, ''):
        option_key = option_type.upper()
        strike_value = _safe_int(strike)
        token_doc = active_tokens_col.find_one({
            **token_base_query,
            '$and': [
                {'$or': [{'expiry': expiry}, {'expiry_date': expiry}]},
                {'strike': strike_value},
                {'option_type': option_key},
            ],
        }) or {}
        if not token_doc:
            token_doc = active_tokens_col.find_one({
                **token_base_query,
                '$and': [
                    {'$or': [{'expiry': expiry}, {'expiry_date': expiry}]},
                    {'option_type': option_key},
                ],
            })
            if token_doc:
                print(
                    '[FF ENTRY SNAPSHOT FALLBACK] '
                    f'trade={str(trade.get("_id") or "")} '
                    f'leg={str(leg_cfg.get("id") or "")} '
                    f'requested_strike={strike_value} '
                    f'fallback_strike={_safe_int(token_doc.get("strike"))} '
                    f'expiry={expiry} option={option_key}'
                )
        token = str(token_doc.get('token') or token_doc.get('tokens') or '').strip()
        symbol = str(token_doc.get('symbol') or '').strip()
        strike = token_doc.get('strike') if token_doc.get('strike') not in (None, '') else strike
        if token:
            _subscribe_option_token(token, symbol)
        ltp = _get_socket_entry_ltp(token)
        ltp_source = 'socket_ltp'
        if ltp <= 0 and symbol:
            ltp, ltp_source = resolve_fast_forward_entry_price(db, trade, token, symbol, 0.0)
    else:
        ltp_source = 'token_not_found'

    spot_source = 'quote_api' if should_use_fast_forward_quote(trade) else 'socket'
    print(
        '[FF SPOT SNAPSHOT] '
        f'mode=fast-forward '
        f'ticker={underlying} '
        f'quote_enabled={should_use_fast_forward_quote(trade)} '
        f'source={spot_source} '
        f'instrument={quote_instrument or "SOCKET"} '
        f'spot_price={spot_price} '
        f'atm_price={atm_price}'
    )
    print(
        '[FF ENTRY SNAPSHOT] '
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
        f'ltp={ltp} '
        f'ltp_source={ltp_source}'
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
        'quote_instrument': quote_instrument,
    }


def _get_fast_forward_quote_price(db, trade: dict, symbol: str) -> float:
    instrument = _build_kite_quote_instrument(
        symbol,
        str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or ''),
    )
    if not instrument:
        return 0.0
    try:
        from features.kite_broker import get_kite_instance

        broker_id = str(trade.get('broker') or '').strip()
        access_token = ''
        if broker_id:
            broker_doc = db._db['broker_configuration'].find_one(
                {'_id': ObjectId(broker_id)},
                {'access_token': 1},
            )
            access_token = str((broker_doc or {}).get('access_token') or '').strip()
        if not access_token:
            market_cfg = db._db['kite_market_config'].find_one({'enabled': True}, {'access_token': 1})
            access_token = str((market_cfg or {}).get('access_token') or '').strip()
        if not access_token:
            return 0.0

        kite = get_kite_instance(access_token)
        quote_map = kite.quote([instrument]) or {}
        quote_doc = quote_map.get(instrument) or {}
        for key in ('last_price', 'last_trade_price'):
            price = _safe_float(quote_doc.get(key))
            if price > 0:
                return price
        ohlc = quote_doc.get('ohlc') if isinstance(quote_doc.get('ohlc'), dict) else {}
        for key in ('close', 'open'):
            price = _safe_float(ohlc.get(key))
            if price > 0:
                return price
    except Exception:
        return 0.0
    return 0.0


def resolve_fast_forward_entry_price(
    db,
    trade: dict,
    token: str,
    symbol: str,
    fallback_price: float,
) -> tuple[float, str]:
    if str(trade.get('activation_mode') or '').strip() != 'fast-forward':
        return fallback_price, 'default'

    socket_ltp = _get_socket_entry_ltp(token)
    if should_use_fast_forward_quote(trade):
        quote_price = _get_fast_forward_quote_price(db, trade, symbol)
        if quote_price > 0:
            return quote_price, 'quote'
        if socket_ltp > 0:
            return socket_ltp, 'socket_ltp_fallback'
        return fallback_price, 'chain_fallback'

    if socket_ltp > 0:
        return socket_ltp, 'socket_ltp'
    return fallback_price, 'chain_fallback'


def resolve_fast_forward_entry_execution_payload(
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
    snapshot = resolve_fast_forward_pending_entry_snapshot(
        db,
        trade,
        contract_cfg,
        now_ts=now_ts,
    ) or {}

    spot_price = _safe_float(
        snapshot.get('spot_at_queue')
        or snapshot.get('live_spot_price')
        or leg.get('spot_at_queue')
    )
    strike = snapshot.get('strike')
    if strike in (None, ''):
        strike = leg.get('strike')
    expiry = str(snapshot.get('expiry_date') or leg.get('expiry_date') or '').strip()
    if ' ' in expiry:
        expiry = expiry[:10]
    token = str(snapshot.get('token') or leg.get('token') or '').strip()
    symbol = str(snapshot.get('symbol') or leg.get('symbol') or '').strip()
    ltp_fallback = _safe_float(snapshot.get('ltp'))
    entry_price, price_source = resolve_fast_forward_entry_price(
        db,
        trade,
        token,
        symbol,
        ltp_fallback,
    )
    current_option_price = entry_price if entry_price > 0 else ltp_fallback
    return {
        'spot_price': spot_price,
        'strike': strike,
        'expiry_date': expiry,
        'token': token,
        'symbol': symbol,
        'entry_price': entry_price,
        'current_option_price': current_option_price,
        'entry_price_source': price_source,
        'ltp': ltp_fallback,
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
    'should_use_fast_forward_quote',
    'get_fast_forward_quote_spot_price',
    'resolve_fast_forward_pending_entry_snapshot',
    'resolve_fast_forward_entry_price',
    'resolve_fast_forward_entry_execution_payload',
    'sync_fast_forward_open_position_subscriptions',
]
