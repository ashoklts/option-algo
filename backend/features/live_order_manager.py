"""
live_order_manager.py
─────────────────────
Places, tracks and converts broker orders for live-mode strategy entries and exits.

Broker selection
────────────────
Each algo_trade has a `broker` field (ObjectId → broker_configuration).
get_broker_for_trade() reads the broker doc's `name` / `broker_icon` field:
  - "flattrade" in name/icon → FlatTradeAdapter  (FlatTrade REST API)
  - otherwise                → KiteConnect        (Zerodha Kite API)

LTP data always comes from Kite WebSocket (kite_ticker), regardless of which
broker is used for order placement.

Entry flow
──────────
1. Entry conditions met → place_live_entry_order()
   - Reads EntryOrder config from leg_cfg (defaults: LIMIT, LimitBuffer=3pts, ConvertAfter=40s)
   - Calculates limit_price = LTP ± LimitBuffer
   - Places order via selected broker (Kite or FlatTrade)
   - Returns {order_id, order_type, limit_price, order_status}

2. entry_trade_payload stored in DB with order_id + order_status='OPEN'

3. Background poller (poll_pending_order_fills) called from live_fast_monitor loop
   - Finds legs with order_status='OPEN'
   - Calls broker.orders() to check fill
   - On fill  → updates entry_trade.price = actual_fill_price, order_status='COMPLETE'
   - On cancel/reject → marks order_status='REJECTED', entry_trade.price=0
   - On timeout (ConvertAfter exceeded) → cancel + re-place as aggressive limit (bid/ask based)

Exit flow
─────────
close_leg_in_db already handles writing exit_trade.
This module provides place_live_exit_order() for SL/TP/exit_time → limit orders.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any

# ── broker_orders collection name ─────────────────────────────────────────────
_BROKER_ORDERS_COL = 'broker_orders'

log = logging.getLogger(__name__)

# ── Exchange / product constants ──────────────────────────────────────────────
_NFO  = 'NFO'
_BFO  = 'BFO'
_NSE  = 'NSE'
_MIS  = 'MIS'
_NRML = 'NRML'
_VARIETY_REGULAR = 'regular'

_ORDER_TYPE_MARKET = 'MARKET'
_ORDER_TYPE_LIMIT  = 'LIMIT'
_ORDER_TYPE_MPP    = 'MPP'      # Market Price Protection (internally → LIMIT with bid/ask base)
_ORDER_TYPE_SL     = 'SL'       # SL-L: trigger + limit
_ORDER_TYPE_SLM    = 'SL-M'     # SL-Market

_TXN_BUY  = 'BUY'
_TXN_SELL = 'SELL'

_ORDER_STATUS_COMPLETE = 'COMPLETE'
_ORDER_STATUS_OPEN     = 'OPEN'
_ORDER_STATUS_REJECTED = 'REJECTED'
_ORDER_STATUS_CANCELLED= 'CANCELLED'


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _is_sell(position_str: str) -> bool:
    return 'sell' in str(position_str or '').lower()


_MIN_OPTION_PRICE = 0.05   # NSE minimum option tick / floor price

def _round_price(price: float) -> float:
    """Round to nearest 0.05 (Kite NSE option tick size)."""
    return round(round(price / 0.05) * 0.05, 2)


def _clamp_limit_price(price: float, is_buy: bool) -> float:
    """
    NSE MPP price rules:
      1. Align to nearest 0.05 tick
      2. Sell price cannot be below ₹0.05 — default to ₹0.05
    """
    rounded = _round_price(price)
    if not is_buy and rounded < _MIN_OPTION_PRICE:
        return _MIN_OPTION_PRICE
    return rounded


def _mpp_protection_pct(ltp: float, is_option: bool = True) -> float:
    """
    MPP protection % by security type and LTP range (NSE official rules).

    OPT:  <10 → 5%  |  10-100 → 3%  |  100-500 → 2%  |  >500 → 1%
    EQ/FUT: <100 → 2%  |  100-500 → 1%  |  >500 → 0.5%
    """
    if is_option:
        if ltp < 10:   return 5.0
        if ltp < 100:  return 3.0
        if ltp < 500:  return 2.0
        return 1.0
    else:
        if ltp < 100:  return 2.0
        if ltp < 500:  return 1.0
        return 0.5


def _resolve_exchange(symbol: str = '', trade: dict | None = None, leg: dict | None = None, fallback: str = _NFO) -> str:
    """Resolve option exchange for order placement. SENSEX options trade on BFO."""
    candidates = [
        (leg or {}).get('exchange'),
        ((leg or {}).get('entry_trade') or {}).get('exchange'),
        (trade or {}).get('exchange'),
        (trade or {}).get('ticker'),
        ((trade or {}).get('strategy') or {}).get('Ticker'),
        ((trade or {}).get('config') or {}).get('Ticker'),
        symbol,
    ]
    text = ' '.join(str(item or '').upper() for item in candidates)
    if 'BFO' in text or 'BSE' in text or 'SENSEX' in text:
        return _BFO
    if 'NFO' in text or 'NSE' in text:
        return _NFO
    return fallback


def _get_bid_ask(kite, symbol: str, ltp: float, exchange: str = _NFO) -> tuple[float, float]:
    """Fetch best bid/ask via kite.quote(). Falls back to ltp on any error."""
    try:
        exch = str(exchange or _NFO).upper()
        sym_key = f'{exch}:{symbol}'
        q = kite.quote([sym_key])
        depth = (q.get(sym_key) or {}).get('depth') or {}
        buy_depth  = depth.get('buy')  or []
        sell_depth = depth.get('sell') or []
        bid = _safe_float((buy_depth[0]  if buy_depth  else {}).get('price'), ltp)
        ask = _safe_float((sell_depth[0] if sell_depth else {}).get('price'), ltp)
        if bid <= 0:
            bid = ltp
        if ask <= 0:
            ask = ltp
        return bid, ask
    except Exception as exc:
        log.debug('_get_bid_ask error exchange=%s symbol=%s: %s', exchange, symbol, exc)
        return ltp, ltp


# ── Kite instance ─────────────────────────────────────────────────────────────

def _get_leg_modification_config(trade: dict, leg_id: str) -> tuple[bool, int]:
    """Return (continuous_monitoring, modification_frequency_seconds) for a leg."""
    strategy_cfg  = trade.get('strategy') or {}
    leg_list      = list(strategy_cfg.get('ListOfLegConfigs') or [])
    exec_extra    = trade.get('execution_config_extra') or {}
    leg_exec_cfgs = exec_extra.get('ListOfLegExecutionConfig') or []
    for idx, base_leg in enumerate(leg_list):
        if not isinstance(base_leg, dict):
            continue
        if str(base_leg.get('id') or '') == leg_id and idx < len(leg_exec_cfgs):
            lec = leg_exec_cfgs[idx]
            if not isinstance(lec, dict):
                continue
            entry_order = lec.get('EntryOrder') or {}
            value       = entry_order.get('Value') or {}
            mod         = value.get('Modification') or {}
            continuous  = str(mod.get('ContinuousMonitoring') or 'False').lower() == 'true'
            freq        = int(mod.get('ModificationFrequency') or 0)
            return continuous, freq
    return False, 0


def _get_leg_entry_buffer(trade: dict, leg_id: str) -> tuple[float, str]:
    """Read LimitBuffer + buffer_type for a leg from execution_config_extra."""
    strategy_cfg  = trade.get('strategy') or {}
    leg_list      = list(strategy_cfg.get('ListOfLegConfigs') or [])
    exec_extra    = trade.get('execution_config_extra') or {}
    leg_exec_cfgs = exec_extra.get('ListOfLegExecutionConfig') or []
    for idx, base_leg in enumerate(leg_list):
        if not isinstance(base_leg, dict):
            continue
        if str(base_leg.get('id') or '') == leg_id and idx < len(leg_exec_cfgs):
            lec = leg_exec_cfgs[idx]
            if not isinstance(lec, dict):
                continue
            entry_order = lec.get('EntryOrder') or {}
            value       = entry_order.get('Value') or {}
            buf_cfg     = value.get('Buffer') or {}
            buf_val     = buf_cfg.get('Value') or {}
            buf_type_raw = str(buf_cfg.get('Type') or 'BufferType.Points').lower()
            buffer_type  = 'percentage' if 'percent' in buf_type_raw else 'points'
            limit_buffer = _safe_float(buf_val.get('LimitBuffer', 3))
            return limit_buffer, buffer_type
    return 3.0, 'points'   # default


# ── broker_orders helpers ─────────────────────────────────────────────────────

def _broker_type_label(broker) -> str:
    """Return 'flattrade' or 'kite' based on broker instance type."""
    try:
        from features.flattrade_broker import FlatTradeAdapter
        if isinstance(broker, FlatTradeAdapter):
            return 'flattrade'
    except Exception:
        pass
    return 'kite'


def _save_broker_order(
    db,
    trade: dict,
    broker,
    order_id: str,
    order_side: str,        # 'entry' | 'exit'
    symbol: str,
    exchange: str,
    txn_type: str,          # 'BUY' | 'SELL'
    qty: int,
    order_type: str,
    price: float,
    trigger_price: float,
    product: str,
    leg_id: str = '',
    exit_reason: str = '',
) -> None:
    """Insert a new row into broker_orders when an order is placed."""
    try:
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        trade_id   = str(trade.get('_id') or '').strip()
        broker_id  = str(trade.get('broker') or '').strip()
        broker_lbl = _broker_type_label(broker)
        db._db[_BROKER_ORDERS_COL].insert_one({
            'order_id':         order_id,
            'broker_doc_id':    broker_id,
            'broker_type':      broker_lbl,
            'trade_id':         trade_id,
            'leg_id':           leg_id,
            'order_side':       order_side,
            'symbol':           symbol,
            'exchange':         exchange,
            'transaction_type': txn_type,
            'quantity':         int(qty),
            'order_type':       order_type,
            'price':            float(price or 0),
            'trigger_price':    float(trigger_price or 0),
            'product':          product,
            'exit_reason':      exit_reason,
            'status':           'OPEN',
            'fill_price':       0.0,
            'fill_qty':         0,
            'rejection_reason': '',
            'placed_at':        now,
            'updated_at':       now,
            'filled_at':        '',
        })
    except Exception as exc:
        log.debug('[BROKER ORDERS] save failed order_id=%s: %s', order_id, exc)


def _update_broker_order_status(
    db,
    order_id: str,
    status: str,
    fill_price: float = 0.0,
    fill_qty: int = 0,
    rejection_reason: str = '',
) -> None:
    """Update status in broker_orders collection only."""
    try:
        now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        set_fields: dict = {
            'status':     status,
            'updated_at': now,
        }
        if status == 'COMPLETE':
            set_fields['fill_price'] = float(fill_price or 0)
            set_fields['fill_qty']   = int(fill_qty or 0)
            set_fields['filled_at']  = now
        elif status in ('REJECTED', 'CANCELLED'):
            set_fields['rejection_reason'] = str(rejection_reason or '')
        db._db[_BROKER_ORDERS_COL].update_one(
            {'order_id': order_id},
            {'$set': set_fields},
        )
    except Exception as exc:
        log.debug('[BROKER ORDERS] update failed order_id=%s: %s', order_id, exc)


def process_broker_order_update(
    db,
    order_id: str,
    status: str,
    fill_price: float = 0.0,
    fill_qty: int = 0,
    rejection_reason: str = '',
    source: str = 'poll',       # 'poll' | 'postback'
) -> bool:
    """
    Central handler for any broker order status change (fill / reject / cancel).

    Called from:
      - poll_pending_order_fills()  (every 5 sec, fallback)
      - flattrade_postback()        (real-time push)

    Updates:
      1. broker_orders collection
      2. algo_trade_positions_history  (entry_trade.price / order_status)
      3. algo_trades legs array
      4. Marks execution socket dirty

    Returns True if algo DB was updated, False if order not found / already processed.
    """
    now_ts = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    # 1. Update broker_orders collection
    _update_broker_order_status(db, order_id, status, fill_price, fill_qty, rejection_reason)

    # 2. Find the matching history leg document
    hist_col   = db._db['algo_trade_positions_history']
    trades_col = db._db['algo_trades']

    hist_doc = hist_col.find_one(
        {'entry_trade.order_id': order_id},
        {'_id': 1, 'trade_id': 1, 'leg_id': 1, 'entry_trade.order_status': 1},
    )
    if not hist_doc:
        return False

    # Skip if already processed to avoid double updates
    current_status = str(
        (hist_doc.get('entry_trade') or {}).get('order_status') or ''
    ).upper()
    if current_status == status:
        return False

    trade_id = str(hist_doc.get('trade_id') or '')
    leg_id   = str(hist_doc.get('leg_id')   or '')

    if status == _ORDER_STATUS_COMPLETE and fill_price > 0:
        hist_col.update_one(
            {'_id': hist_doc['_id']},
            {'$set': {
                'entry_trade.price':        fill_price,
                'entry_trade.order_status': _ORDER_STATUS_COMPLETE,
                'entry_trade.fill_qty':     int(fill_qty),
                'entry_trade.filled_at':    now_ts,
            }},
        )
        trades_col.update_one(
            {'_id': trade_id},
            {'$set': {
                'legs.$[elem].last_saw_price':          fill_price,
                'legs.$[elem].entry_trade.price':       fill_price,
                'legs.$[elem].entry_trade.order_status':_ORDER_STATUS_COMPLETE,
            }},
            array_filters=[{'elem.id': leg_id}],
        )
        try:
            from features.execution_socket import mark_execute_order_dirty_from_trade_id
            mark_execute_order_dirty_from_trade_id(db, trade_id)
        except Exception:
            pass
        print(
            f'[ORDER FILLED][{source}] trade={trade_id} leg={leg_id} '
            f'order_id={order_id} fill_price={fill_price} qty={fill_qty}'
        )
        return True

    elif status in (_ORDER_STATUS_REJECTED, _ORDER_STATUS_CANCELLED):
        hist_col.update_one(
            {'_id': hist_doc['_id']},
            {'$set': {
                'entry_trade.order_status':     status,
                'entry_trade.rejection_reason': rejection_reason,
            }},
        )
        print(
            f'[ORDER {status}][{source}] trade={trade_id} leg={leg_id} '
            f'order_id={order_id} reason={rejection_reason or "-"}'
        )
        return True

    return False


def get_broker_for_trade(db, trade: dict):
    """
    Return an authenticated broker instance for the trade's mapped broker.

    Reads broker_configuration by trade['broker'] ObjectId.
    Detects broker type from doc's `name` / `broker_icon` field:
      - "flattrade" in name/icon → FlatTradeAdapter
      - otherwise                → KiteConnect (Zerodha)

    Falls back to default KiteConnect via kite_market_config when no broker
    is mapped on the trade.
    """
    broker_id = str(trade.get('broker') or '').strip()

    if broker_id:
        try:
            from bson import ObjectId
            from features.flattrade_broker import _is_flattrade_doc, get_flattrade_instance
            broker_doc = db._db['broker_configuration'].find_one(
                {'_id': ObjectId(broker_id)},
                {'access_token': 1, 'user_id': 1, 'name': 1, 'broker_icon': 1},
            ) or {}
            access_token = str(broker_doc.get('access_token') or '').strip()
            if access_token and _is_flattrade_doc(broker_doc):
                user_id = str(broker_doc.get('user_id') or '').strip()
                ft = get_flattrade_instance(user_id, access_token)
                if ft:
                    log.debug('broker=flattrade trade=%s', str(trade.get('_id') or ''))
                    return ft
            elif access_token:
                from features.kite_broker import get_kite_instance
                return get_kite_instance(access_token)
        except Exception as exc:
            log.debug('broker lookup error broker=%s: %s', broker_id, exc)

    # Fallback — default Kite market config
    try:
        from features.kite_broker import get_kite_instance
        market_cfg = db._db['kite_market_config'].find_one({'enabled': True}, {'access_token': 1}) or {}
        access_token = str(market_cfg.get('access_token') or '').strip()
        if access_token:
            return get_kite_instance(access_token)
    except Exception as exc:
        log.debug('market config token lookup error: %s', exc)

    return None


# Keep old name as alias so other callers (if any) don't break
get_kite_for_trade = get_broker_for_trade


# ── Order config resolution ───────────────────────────────────────────────────

def _resolve_entry_order_config(leg_cfg: dict) -> dict:
    """
    Read EntryOrder config from leg_cfg.
    Returns dict with keys:
      order_type        – 'LIMIT' or 'MARKET'
      limit_buffer      – float (points or %)
      trigger_buffer    – float (for SL-L; 0 = plain LIMIT)
      buffer_type       – 'points' or 'percentage'
      convert_after     – int seconds (0 = never convert to market)
    """
    entry_order = (leg_cfg.get('EntryOrder') or {})
    if isinstance(entry_order.get('Config'), dict):
        entry_order = entry_order['Config']

    raw_type = str(entry_order.get('Type') or 'OrderType.MPP').lower()
    is_mpp   = 'mpp' in raw_type
    is_limit = 'limit' in raw_type and not is_mpp

    value = entry_order.get('Value') or {}
    if not isinstance(value, dict):
        value = {}
    buffer_cfg = value.get('Buffer') or {}
    if not isinstance(buffer_cfg, dict):
        buffer_cfg = {}
    buf_val = buffer_cfg.get('Value') or {}
    if not isinstance(buf_val, dict):
        buf_val = {}
    buf_type_raw = str(buffer_cfg.get('Type') or 'BufferType.Points').lower()
    buffer_type = 'percentage' if 'percent' in buf_type_raw else 'points'

    limit_buffer   = _safe_float(buf_val.get('LimitBuffer', 3))
    trigger_buffer = _safe_float(buf_val.get('TriggerBuffer', 0))

    mod = value.get('Modification') or {}
    convert_after = int(mod.get('MarketOrderAfter') or 40)

    if is_mpp:
        order_type = _ORDER_TYPE_MPP
    elif is_limit:
        order_type = _ORDER_TYPE_LIMIT
    else:
        order_type = _ORDER_TYPE_MARKET

    return {
        'order_type':     order_type,
        'limit_buffer':   limit_buffer,
        'trigger_buffer': trigger_buffer,
        'buffer_type':    buffer_type,
        'convert_after':  convert_after,
    }


def _resolve_exit_order_config(leg_cfg: dict) -> dict:
    exit_order = (leg_cfg.get('ExitOrder') or {})
    if isinstance(exit_order.get('Config'), dict):
        exit_order = exit_order['Config']

    raw_type = str(exit_order.get('Type') or 'OrderType.MPP').lower()
    is_mpp   = 'mpp' in raw_type
    is_limit = 'limit' in raw_type and not is_mpp

    value = exit_order.get('Value') or {}
    if not isinstance(value, dict):
        value = {}
    buffer_cfg = value.get('Buffer') or {}
    if not isinstance(buffer_cfg, dict):
        buffer_cfg = {}
    buf_val = buffer_cfg.get('Value') or {}
    if not isinstance(buf_val, dict):
        buf_val = {}
    buf_type_raw = str(buffer_cfg.get('Type') or 'BufferType.Points').lower()
    buffer_type = 'percentage' if 'percent' in buf_type_raw else 'points'

    limit_buffer   = _safe_float(buf_val.get('LimitBuffer', 3))
    trigger_buffer = _safe_float(buf_val.get('TriggerBuffer', 0))

    mod = value.get('Modification') or {}
    convert_after = int(mod.get('MarketOrderAfter') or 40)

    if is_mpp:
        order_type = _ORDER_TYPE_MPP
    elif is_limit:
        order_type = _ORDER_TYPE_LIMIT
    else:
        order_type = _ORDER_TYPE_MARKET

    return {
        'order_type':     order_type,
        'limit_buffer':   limit_buffer,
        'trigger_buffer': trigger_buffer,
        'buffer_type':    buffer_type,
        'convert_after':  convert_after,
    }


def _apply_buffer(price: float, buffer: float, buffer_type: str, is_buy: bool) -> float:
    """Add buffer for BUY (chase price up), subtract for SELL (chase price down)."""
    if buffer_type == 'percentage':
        delta = price * buffer / 100.0
    else:
        delta = buffer
    return _clamp_limit_price(price + delta if is_buy else price - delta, is_buy)


# ── Order placement ───────────────────────────────────────────────────────────

def place_live_entry_order(
    db,
    trade: dict,
    leg: dict,
    leg_cfg: dict,
    symbol: str,
    qty: int,
    ltp: float,
) -> dict:
    """
    Place an entry order for a live strategy leg.

    Returns dict:
      order_id     – Kite order ID string (empty on failure)
      order_type   – 'LIMIT' / 'MARKET' / 'SL'
      limit_price  – price submitted to broker
      trigger_price– trigger price (for SL-L; 0 otherwise)
      order_status – 'OPEN' / 'MARKET_PLACED' / 'FAILED'
      error        – error message if failed
    """
    from features.app_logger import log_db_write, log_db_error
    trade_id = str(trade.get('_id') or '')
    leg_id   = str(leg.get('id') or '')

    if str(trade.get('activation_mode') or '').strip() != 'live':
        log.warning('[LIVE ORDER] skipped — not live mode trade=%s', trade_id)
        return {'order_id': '', 'order_type': _ORDER_TYPE_MARKET, 'limit_price': ltp,
                'trigger_price': 0.0, 'order_status': 'FAILED', 'error': 'not_live_mode'}

    if not symbol:
        log.warning('[LIVE ORDER] no symbol for leg=%s trade=%s', leg_id, trade_id)
        return {'order_id': '', 'order_type': _ORDER_TYPE_MARKET, 'limit_price': ltp,
                'trigger_price': 0.0, 'order_status': 'FAILED', 'error': 'no_symbol'}

    qty = 65

    kite = get_broker_for_trade(db, trade)
    if not kite:
        log.warning('[LIVE ORDER] no broker instance for trade=%s', trade_id)
        return {'order_id': '', 'order_type': _ORDER_TYPE_MARKET, 'limit_price': ltp,
                'trigger_price': 0.0, 'order_status': 'FAILED', 'error': 'no_kite_session'}

    cfg = _resolve_entry_order_config(leg_cfg)
    position_str = str(leg.get('position') or 'PositionType.Sell')
    is_sell = _is_sell(position_str)
    # Entry: BUY means we're buying the option; SELL means we're selling it
    txn_type = _TXN_SELL if is_sell else _TXN_BUY
    is_buy_order = not is_sell

    # Product type from config
    product_raw = str((leg_cfg.get('ProductType') or leg_cfg.get('Product') or _NRML)).upper()
    product = _MIS if 'MIS' in product_raw else _NRML
    exchange = _resolve_exchange(symbol, trade, leg)

    order_type    = cfg['order_type']
    limit_buffer  = cfg['limit_buffer']
    trigger_buffer= cfg['trigger_buffer']
    buffer_type   = cfg['buffer_type']

    limit_price   = 0.0
    trigger_price = 0.0
    kite_order_type = order_type

    if order_type == _ORDER_TYPE_MPP:
        # Algotest MPP formula:
        #   BUY  → BID + pct%  (crosses ask → guaranteed fill, less overpay)
        #   SELL → ASK - pct%  (crosses bid → guaranteed fill, less slippage)
        # Then: tick align to 0.05, min sell price ₹0.05
        bid, ask = _get_bid_ask(kite, symbol, ltp, exchange)
        pct = _mpp_protection_pct(ltp, is_option=True)
        if is_buy_order:
            base_price = bid if bid > 0 else ltp
            raw_price  = base_price * (1 + pct / 100)
        else:
            base_price = ask if ask > 0 else ltp
            raw_price  = base_price * (1 - pct / 100)
        limit_price = _clamp_limit_price(raw_price, is_buy_order)
        kite_order_type = _ORDER_TYPE_LIMIT
        print(
            f'[MPP ENTRY] symbol={symbol} ltp={ltp} bid={bid} ask={ask} '
            f'pct={pct}% limit_price={limit_price} is_buy={is_buy_order}'
        )
    elif order_type == _ORDER_TYPE_LIMIT:
        if trigger_buffer > 0:
            # SL-L order: trigger first, then limit fills
            trigger_price = _apply_buffer(ltp, trigger_buffer, buffer_type, is_buy_order)
            limit_price   = _apply_buffer(trigger_price, limit_buffer, buffer_type, is_buy_order)
            kite_order_type = _ORDER_TYPE_SL
        else:
            limit_price   = _apply_buffer(ltp, limit_buffer, buffer_type, is_buy_order)
            kite_order_type = _ORDER_TYPE_LIMIT

    try:
        order_params: dict[str, Any] = {
            'tradingsymbol':   symbol,
            'exchange':        exchange,
            'transaction_type':txn_type,
            'quantity':        int(qty),
            'order_type':      kite_order_type,
            'product':         product,
            'variety':         _VARIETY_REGULAR,
        }
        if kite_order_type in (_ORDER_TYPE_LIMIT, _ORDER_TYPE_SL):
            order_params['price'] = limit_price
        if kite_order_type == _ORDER_TYPE_SL:
            order_params['trigger_price'] = trigger_price

        order_id = kite.place_order(**order_params)
        order_id = str(order_id or '').strip()

        print(
            f'[LIVE ORDER PLACED] trade={trade_id} leg={leg_id} '
            f'exchange={exchange} symbol={symbol} txn={txn_type} qty={qty} '
            f'order_type={kite_order_type} limit_price={limit_price} '
            f'trigger_price={trigger_price} order_id={order_id}'
        )
        _save_broker_order(
            db, trade, kite, order_id, 'entry',
            symbol, exchange, txn_type, qty,
            kite_order_type, limit_price, trigger_price, product,
            leg_id=leg_id,
        )
        log_db_write('broker_orders', 'place_entry_order', order_id, {
            'trade_id': trade_id, 'leg_id': leg_id, 'exchange': exchange, 'symbol': symbol,
            'order_type': kite_order_type, 'limit_price': limit_price,
        })
        return {
            'order_id':      order_id,
            'order_type':    kite_order_type,
            'exchange':      exchange,
            'limit_price':   limit_price,
            'trigger_price': trigger_price,
            'order_status':  _ORDER_STATUS_OPEN,
            'convert_after': cfg['convert_after'],
            'error':         '',
        }
    except Exception as exc:
        log.error('[LIVE ORDER FAILED] trade=%s leg=%s symbol=%s: %s', trade_id, leg_id, symbol, exc)
        log_db_error('broker_orders', 'place_entry_order', exc, f'{trade_id}/{leg_id}')
        return {
            'order_id':      '',
            'order_type':    kite_order_type,
            'limit_price':   limit_price,
            'trigger_price': trigger_price,
            'order_status':  'FAILED',
            'error':         str(exc),
        }


def place_live_exit_order(
    db,
    trade: dict,
    leg: dict,
    leg_cfg: dict,
    symbol: str,
    qty: int,
    exit_price: float,
    exit_reason: str,
) -> dict:
    """
    Place an exit order for a live strategy leg.
    exit_reason: 'stoploss' | 'target' | 'exit_time' | 'squared_off' | 'overall_sl' etc.
    """
    from features.app_logger import log_db_write, log_db_error
    trade_id = str(trade.get('_id') or '')
    leg_id   = str(leg.get('id') or '')

    if str(trade.get('activation_mode') or '').strip() != 'live':
        log.warning('[LIVE EXIT ORDER] skipped — not live mode trade=%s', trade_id)
        return {'order_id': '', 'order_type': _ORDER_TYPE_MARKET, 'order_status': 'FAILED', 'error': 'not_live_mode'}

    if not symbol:
        return {'order_id': '', 'order_type': _ORDER_TYPE_MARKET, 'order_status': 'FAILED', 'error': 'no_symbol'}

    qty = 65

    kite = get_broker_for_trade(db, trade)
    if not kite:
        return {'order_id': '', 'order_type': _ORDER_TYPE_MARKET, 'order_status': 'FAILED', 'error': 'no_broker_session'}

    cfg = _resolve_exit_order_config(leg_cfg)
    position_str = str(leg.get('position') or 'PositionType.Sell')
    is_sell = _is_sell(position_str)
    # Exit: reverse of position — SELL exits BUY; BUY exits SELL
    txn_type = _TXN_BUY if is_sell else _TXN_SELL
    is_buy_order = not is_sell  # direction of the exit order itself

    product_raw = str((leg_cfg.get('ProductType') or leg_cfg.get('Product') or _NRML)).upper()
    product = _MIS if 'MIS' in product_raw else _NRML
    exchange = _resolve_exchange(symbol, trade, leg)

    order_type     = cfg['order_type']
    limit_buffer   = cfg['limit_buffer']
    trigger_buffer = cfg['trigger_buffer']
    buffer_type    = cfg['buffer_type']

    limit_price   = 0.0
    trigger_price = 0.0
    kite_order_type = order_type

    if order_type == _ORDER_TYPE_MPP:
        # Algotest MPP formula:
        #   BUY to close  → BID + pct%
        #   SELL to close → ASK - pct%
        bid, ask = _get_bid_ask(kite, symbol, exit_price, exchange)
        is_exit_buy = is_sell   # sell position → BUY to close; buy position → SELL to close
        pct = _mpp_protection_pct(exit_price, is_option=True)
        if is_exit_buy:
            base_price = bid if bid > 0 else exit_price
            raw_price  = base_price * (1 + pct / 100)
        else:
            base_price = ask if ask > 0 else exit_price
            raw_price  = base_price * (1 - pct / 100)
        limit_price = _clamp_limit_price(raw_price, is_exit_buy)
        kite_order_type = _ORDER_TYPE_LIMIT
        print(
            f'[MPP EXIT] symbol={symbol} exit_price={exit_price} bid={bid} ask={ask} '
            f'pct={pct}% limit_price={limit_price} reason={exit_reason}'
        )
    elif exit_reason == 'stoploss':
        # SL-L: already at SL price; trigger = exit_price, limit = exit_price - buffer
        if trigger_buffer > 0:
            trigger_price = _round_price(exit_price)
            limit_price   = _apply_buffer(exit_price, limit_buffer, buffer_type, not is_buy_order)
            kite_order_type = _ORDER_TYPE_SL
        else:
            limit_price = _apply_buffer(exit_price, limit_buffer, buffer_type, not is_buy_order)
            kite_order_type = _ORDER_TYPE_LIMIT
    elif exit_reason == 'target':
        limit_price = _round_price(exit_price)
        kite_order_type = _ORDER_TYPE_LIMIT
    else:
        # exit_time / squared_off / overall_sl etc.
        if order_type == _ORDER_TYPE_LIMIT:
            limit_price = _apply_buffer(exit_price, limit_buffer, buffer_type, not is_buy_order)
            kite_order_type = _ORDER_TYPE_LIMIT
        else:
            kite_order_type = _ORDER_TYPE_MARKET

    try:
        order_params: dict[str, Any] = {
            'tradingsymbol':    symbol,
            'exchange':         exchange,
            'transaction_type': txn_type,
            'quantity':         int(qty),
            'order_type':       kite_order_type,
            'product':          product,
            'variety':          _VARIETY_REGULAR,
        }
        if kite_order_type in (_ORDER_TYPE_LIMIT, _ORDER_TYPE_SL):
            order_params['price'] = limit_price
        if kite_order_type == _ORDER_TYPE_SL:
            order_params['trigger_price'] = trigger_price

        order_id = kite.place_order(**order_params)
        order_id = str(order_id or '').strip()

        print(
            f'[LIVE EXIT ORDER] trade={trade_id} leg={leg_id} '
            f'exchange={exchange} symbol={symbol} txn={txn_type} qty={qty} '
            f'reason={exit_reason} order_type={kite_order_type} '
            f'limit_price={limit_price} order_id={order_id}'
        )
        _save_broker_order(
            db, trade, kite, order_id, 'exit',
            symbol, exchange, txn_type, qty,
            kite_order_type, limit_price, trigger_price, product,
            leg_id=leg_id, exit_reason=exit_reason,
        )
        log_db_write('broker_orders', 'place_exit_order', order_id, {
            'trade_id': trade_id, 'leg_id': leg_id, 'exchange': exchange, 'symbol': symbol,
            'exit_reason': exit_reason, 'order_type': kite_order_type,
        })
        return {
            'order_id':      order_id,
            'order_type':    kite_order_type,
            'exchange':      exchange,
            'limit_price':   limit_price,
            'trigger_price': trigger_price,
            'order_status':  _ORDER_STATUS_OPEN,
            'error':         '',
        }
    except Exception as exc:
        log.error('[LIVE EXIT ORDER FAILED] trade=%s leg=%s: %s', trade_id, leg_id, exc)
        log_db_error('broker_orders', 'place_exit_order', exc, f'{trade_id}/{leg_id}')
        return {
            'order_id':      '',
            'order_type':    kite_order_type,
            'limit_price':   limit_price,
            'trigger_price': trigger_price,
            'order_status':  'FAILED',
            'error':         str(exc),
        }


# ── Order fill poller ─────────────────────────────────────────────────────────

_poll_lock = threading.Lock()


def poll_pending_order_fills(db) -> int:
    """
    Check all live legs with order_status='OPEN' and update fill price.
    Called from live_fast_monitor loop every ~5 seconds.
    Returns number of legs updated.
    """
    if not _poll_lock.acquire(blocking=False):
        return 0
    updated = 0
    try:
        now_ts = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        # Find open-status legs with pending broker orders
        hist_col   = db._db['algo_trade_positions_history']
        trades_col = db._db['algo_trades']

        pending_legs = list(hist_col.find(
            {
                'entry_trade.order_status': _ORDER_STATUS_OPEN,
                'exit_trade': None,
            },
            {
                'trade_id': 1, 'leg_id': 1,
                'entry_trade.order_id': 1,
                'entry_trade.order_placed_at': 1,
                'entry_trade.last_modified_at': 1,
                'entry_trade.convert_after': 1,
            }
        ))

        if not pending_legs:
            return 0

        # Group by trade to share Kite instance
        trade_ids = list({str(doc.get('trade_id') or '') for doc in pending_legs if doc.get('trade_id')})
        trades    = {
            str(t.get('_id') or ''): t
            for t in trades_col.find({'_id': {'$in': trade_ids}, 'activation_mode': 'live'})
        }

        # Fetch kite orders once per unique broker
        broker_orders_cache: dict[str, list[dict]] = {}

        for hist_doc in pending_legs:
            trade_id = str(hist_doc.get('trade_id') or '')
            leg_id   = str(hist_doc.get('leg_id') or '')
            trade    = trades.get(trade_id)
            if not trade:
                continue

            entry_trade = hist_doc.get('entry_trade') or {}
            order_id    = str(entry_trade.get('order_id') or '').strip()
            if not order_id:
                continue

            broker_id = str(trade.get('broker') or '').strip()
            cache_key = broker_id or '_default'

            if cache_key not in broker_orders_cache:
                kite = get_broker_for_trade(db, trade)
                if kite:
                    try:
                        broker_orders_cache[cache_key] = kite.orders() or []
                    except Exception as exc:
                        log.debug('kite.orders() error: %s', exc)
                        broker_orders_cache[cache_key] = []
                else:
                    broker_orders_cache[cache_key] = []

            orders_list = broker_orders_cache.get(cache_key) or []
            kite_order  = next((o for o in orders_list if str(o.get('order_id') or '') == order_id), None)

            if not kite_order:
                continue

            status     = str(kite_order.get('status') or '').upper()
            fill_price = _safe_float(kite_order.get('average_price') or kite_order.get('price'))
            fill_qty   = int(kite_order.get('filled_quantity') or 0)

            if status == _ORDER_STATUS_COMPLETE and fill_price > 0:
                if process_broker_order_update(
                    db, order_id, _ORDER_STATUS_COMPLETE,
                    fill_price=fill_price, fill_qty=fill_qty, source='poll',
                ):
                    updated += 1

            elif status in (_ORDER_STATUS_REJECTED, _ORDER_STATUS_CANCELLED):
                status_msg = str(
                    kite_order.get('status_message') or kite_order.get('status_message_raw') or ''
                ).lower()
                process_broker_order_update(
                    db, order_id, status,
                    rejection_reason=status_msg, source='poll',
                )
                # MarginAutoSquareOff: margin rejection → exit all open legs
                is_margin_error = any(
                    kw in status_msg for kw in ('margin', 'insufficient', 'rms')
                )
                if is_margin_error:
                    exec_base = trade.get('execution_config_base') or {}
                    if bool(exec_base.get('MarginAutoSquareOff', False)):
                        kite_sq = get_broker_for_trade(db, trade)
                        if kite_sq:
                            print(
                                f'[MARGIN AUTO SQUAREOFF] trade={trade_id} '
                                f'leg={leg_id} triggering full exit'
                            )
                            _margin_squareoff_trade(db, trade, kite_sq, now_ts)

            else:
                # Still pending — check convert_after and ContinuousMonitoring
                placed_at     = str(entry_trade.get('order_placed_at') or '').strip()
                convert_after = int(entry_trade.get('convert_after') or 40)
                if not placed_at:
                    continue
                try:
                    placed_dt = datetime.fromisoformat(placed_at)
                    total_elapsed = (datetime.now() - placed_dt).total_seconds()

                    if convert_after > 0 and total_elapsed >= convert_after:
                        # Final retry — convert_after timeout exceeded
                        kite = get_broker_for_trade(db, trade)
                        if kite:
                            _convert_to_aggressive_limit(
                                db, kite, trade, hist_doc, order_id, kite_order, now_ts
                            )
                    else:
                        # ContinuousMonitoring — re-place at ModificationFrequency intervals
                        continuous, mod_freq = _get_leg_modification_config(trade, leg_id)
                        if continuous and mod_freq > 0:
                            last_mod = str(
                                entry_trade.get('last_modified_at') or placed_at
                            ).strip()
                            last_mod_dt      = datetime.fromisoformat(last_mod)
                            elapsed_since_mod = (datetime.now() - last_mod_dt).total_seconds()
                            if elapsed_since_mod >= mod_freq:
                                kite = get_broker_for_trade(db, trade)
                                if kite:
                                    _convert_to_aggressive_limit(
                                        db, kite, trade, hist_doc, order_id, kite_order, now_ts
                                    )
                except Exception as exc:
                    log.debug('order modification check error: %s', exc)

    except Exception as exc:
        log.error('[ORDER POLL ERROR] %s', exc, exc_info=True)
    finally:
        _poll_lock.release()
    return updated


def _get_leg_product(trade: dict, leg_id: str) -> str:
    """Read NRML/MIS for a leg from execution_config_extra by matching leg index."""
    strategy_cfg  = trade.get('strategy') or {}
    leg_list      = list(strategy_cfg.get('ListOfLegConfigs') or [])
    exec_extra    = trade.get('execution_config_extra') or {}
    leg_exec_cfgs = exec_extra.get('ListOfLegExecutionConfig') or []
    for idx, base_leg in enumerate(leg_list):
        if not isinstance(base_leg, dict):
            continue
        if str(base_leg.get('id') or '') == leg_id and idx < len(leg_exec_cfgs):
            lec = leg_exec_cfgs[idx]
            if isinstance(lec, dict):
                product_raw = str(lec.get('ProductType') or _NRML).upper()
                return _MIS if 'MIS' in product_raw else _NRML
    return _NRML


def _margin_squareoff_trade(db, trade: dict, kite, _now_ts: str) -> None:
    """Place MARKET exit orders for all open legs when a margin rejection triggers full squareoff."""
    trade_id = str(trade.get('_id') or '')
    if str(trade.get('activation_mode') or '').strip() != 'live':
        log.warning('[MARGIN SQUAREOFF] skipped — not live mode trade=%s', trade_id)
        return
    hist_col = db._db['algo_trade_positions_history']
    open_legs = list(hist_col.find(
        {'trade_id': trade_id, 'status': 1, 'exit_trade': None},
        {'leg_id': 1, 'symbol': 1, 'quantity': 1, 'position': 1},
    ))
    for hist in open_legs:
        symbol   = str(hist.get('symbol') or '').strip()
        qty      = int(hist.get('quantity') or 0)
        leg_id   = str(hist.get('leg_id') or '').strip()
        exchange = _resolve_exchange(symbol, trade, {'id': leg_id})
        is_sell  = 'sell' in str(hist.get('position') or '').lower()
        txn_type = _TXN_BUY if is_sell else _TXN_SELL
        product  = _get_leg_product(trade, leg_id)
        if not symbol or qty <= 0:
            continue
        try:
            order_id = kite.place_order(
                tradingsymbol=symbol,
                exchange=exchange,
                transaction_type=txn_type,
                quantity=qty,
                order_type=_ORDER_TYPE_MARKET,
                product=product,
                variety=_VARIETY_REGULAR,
            )
            print(
                f'[MARGIN SQUAREOFF] trade={trade_id} leg={leg_id} '
                f'exchange={exchange} symbol={symbol} txn={txn_type} qty={qty} '
                f'product={product} order_id={order_id}'
            )
        except Exception as exc:
            log.error('[MARGIN SQUAREOFF FAILED] trade=%s symbol=%s: %s', trade_id, symbol, exc)


def _convert_to_aggressive_limit(
    db, kite, trade, hist_doc, order_id: str, kite_order: dict, now_ts: str
) -> None:
    """
    Cancel a stale pending limit order and re-place it as a fresh aggressive
    limit order using real-time bid/ask (3× normal protection buffer).
    Used instead of converting to MARKET (which many brokers block for options).
    """
    trade_id = str(trade.get('_id') or '')
    leg_id   = str(hist_doc.get('leg_id') or '')
    symbol   = str(kite_order.get('tradingsymbol') or '').strip()
    exchange = str(kite_order.get('exchange') or '').strip().upper()
    txn_type = str(kite_order.get('transaction_type') or '').upper()
    product  = str(kite_order.get('product') or _NRML).upper()
    qty      = int(kite_order.get('quantity') or 0)

    if not symbol or qty <= 0 or txn_type not in (_TXN_BUY, _TXN_SELL):
        log.warning('[AGGRESSIVE LIMIT] missing order info trade=%s leg=%s', trade_id, leg_id)
        return
    if not exchange:
        exchange = _resolve_exchange(symbol, trade, {'id': leg_id})

    is_buy_order = txn_type == _TXN_BUY

    # Step 1 — Cancel the stale pending order
    try:
        kite.cancel_order(variety=_VARIETY_REGULAR, order_id=order_id)
        print(f'[AGGRESSIVE LIMIT] cancelled stale order trade={trade_id} leg={leg_id} order_id={order_id}')
    except Exception as exc:
        log.warning('[AGGRESSIVE LIMIT] cancel failed order_id=%s: %s — placing fresh order anyway', order_id, exc)

    # Step 2 — Fresh bid/ask via kite.quote()
    ltp = _safe_float(kite_order.get('last_price') or kite_order.get('average_price'))
    bid, ask = _get_bid_ask(kite, symbol, ltp, exchange)
    base_price = ask if is_buy_order else bid
    if base_price <= 0:
        base_price = ltp

    # Use LimitBuffer from config (same buffer user configured for this leg)
    limit_buffer, buffer_type = _get_leg_entry_buffer(trade, leg_id)
    limit_price = _apply_buffer(base_price, limit_buffer, buffer_type, is_buy_order)

    # Step 3 — Place fresh aggressive limit order
    try:
        new_order_id = str(kite.place_order(
            tradingsymbol=symbol,
            exchange=exchange,
            transaction_type=txn_type,
            quantity=qty,
            order_type=_ORDER_TYPE_LIMIT,
            price=limit_price,
            product=product,
            variety=_VARIETY_REGULAR,
        ) or '').strip()

        # Update DB so poller tracks the new order_id + reset modification timer
        db._db['algo_trade_positions_history'].update_one(
            {'_id': hist_doc['_id']},
            {'$set': {
                'entry_trade.order_id':          new_order_id,
                'entry_trade.order_status':       _ORDER_STATUS_OPEN,
                'entry_trade.order_placed_at':    now_ts,
                'entry_trade.last_modified_at':   now_ts,
                'entry_trade.aggressive_retry':   True,
            }},
        )
        print(
            f'[AGGRESSIVE LIMIT PLACED] trade={trade_id} leg={leg_id} '
            f'exchange={exchange} symbol={symbol} txn={txn_type} bid={bid} ask={ask} '
            f'limit_price={limit_price} new_order_id={new_order_id}'
        )
    except Exception as exc:
        log.error('[AGGRESSIVE LIMIT FAILED] trade=%s leg=%s symbol=%s: %s', trade_id, leg_id, symbol, exc)
