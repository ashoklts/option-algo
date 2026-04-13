"""
strike_selector.py
──────────────────
Reusable strike-selection logic for backtest, live-trade, and forward-test.

Public API
──────────
    resolve_expiry(chain_col, underlying, option_type, trade_date,
                   snapshot_timestamp=None) -> tuple[str, str | None]

    resolve_strike(chain_col, underlying, option_type, entry_kind,
                   strike_param_raw, position, spot_price, expiry,
                   trade_date, snapshot_timestamp=None,
                   market_cache=None, leg_id='') -> StrikeResult

StrikeResult fields
───────────────────
    strike       : float        resolved strike price
    entry_price  : float        option close price at that strike
    chain_doc    : dict | None  raw chain document (contains token, symbol, greeks)
    error        : str | None   None = success; non-None = failure reason (leg skipped)

Supported entry_kind values
───────────────────────────
    EntryType.EntryByPremiumCloseToStraddle  ->  strike_param = {'Multiplier': 0.6, ...}
    EntryType.EntryByDeltaRange              ->  strike_param = {'LowerRange': 40, 'UpperRange': 70}
    EntryType.EntryByPremium (Geq/Lte)      ->  strike_param = float (plain premium target)
    anything else / ATM / offset            ->  strike_param = int offset from ATM (0 = ATM)
"""

from __future__ import annotations

import ast
import logging
from dataclasses import dataclass, field
from typing import Any

from pymongo import DESCENDING

from features.spot_atm_utils import get_cached_chain_doc, get_cached_spot_doc

log = logging.getLogger(__name__)


# ── helpers ──────────────────────────────────────────────────────────────────

def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _is_sell(position_str: str) -> bool:
    return 'sell' in str(position_str or '').lower()


def _parse_sp_dict(raw) -> dict:
    """Parse strike_parameter that may be a dict or a string repr of a dict."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and '{' in raw:
        try:
            return ast.literal_eval(raw)
        except Exception:
            pass
    return {}


# ── result type ──────────────────────────────────────────────────────────────

@dataclass
class StrikeResult:
    strike: float = 0.0
    entry_price: float = 0.0
    chain_doc: dict | None = None   # resolved chain doc → use for token / symbol
    error: str | None = None        # None = success


# ── expiry resolver ───────────────────────────────────────────────────────────

def resolve_expiry(
    chain_col,
    underlying: str,
    option_type: str,
    trade_date: str,
    snapshot_timestamp: str | None = None,
) -> tuple[str, str | None]:
    """
    Find the nearest expiry >= trade_date that has data in the option chain.
    Returns (expiry, error_reason).  error_reason is None on success.
    """
    try:
        base_q = {
            'underlying': underlying,
            'type': option_type,
            'expiry': {'$gte': trade_date},
        }
        if snapshot_timestamp:
            doc = chain_col.find_one(
                {**base_q, 'timestamp': snapshot_timestamp},
                sort=[('expiry', 1)],
            )
            if not doc:
                doc = chain_col.find_one(
                    {**base_q, 'timestamp': {'$lte': snapshot_timestamp}},
                    sort=[('expiry', 1), ('timestamp', DESCENDING)],
                )
        else:
            doc = chain_col.find_one(
                {**base_q, 'timestamp': {'$regex': f'^{trade_date}'}},
                sort=[('expiry', 1), ('timestamp', DESCENDING)],
            )
        if not doc:
            return '', 'no_expiry_data'
        return str(doc.get('expiry') or ''), None
    except Exception as exc:
        return '', f'expiry_query_error:{exc}'


# ── chain fetch helpers ───────────────────────────────────────────────────────

def _chain_at_time(
    chain_col,
    underlying: str,
    expiry: str,
    strike: float,
    option_type: str,
    snapshot_ts: str,
    market_cache: dict | None = None,
) -> dict:
    cached = get_cached_chain_doc(market_cache, underlying, expiry, strike, option_type, snapshot_ts)
    if cached:
        return cached
    base = {'underlying': underlying, 'expiry': expiry,
            'strike': float(strike), 'type': option_type}
    doc = chain_col.find_one({**base, 'timestamp': snapshot_ts})
    if not doc:
        doc = chain_col.find_one(
            {**base, 'timestamp': {'$lte': snapshot_ts}},
            sort=[('timestamp', DESCENDING)],
        )
    return doc or {}


def _chain_latest(
    chain_col,
    underlying: str,
    expiry: str,
    strike: float,
    option_type: str,
    trade_date: str,
    market_cache: dict | None = None,
) -> dict:
    cached = get_cached_chain_doc(market_cache, underlying, expiry, strike, option_type)
    if cached:
        return cached
    doc = chain_col.find_one(
        {'underlying': underlying, 'expiry': expiry,
         'strike': float(strike), 'type': option_type,
         'timestamp': {'$regex': f'^{trade_date}'}},
        sort=[('timestamp', DESCENDING)],
    )
    return doc or {}


# ── individual selectors ──────────────────────────────────────────────────────

def _select_premium_close_to_straddle(
    chain_col,
    underlying: str,
    option_type: str,
    expiry: str,
    strike_param_raw: Any,
    spot_price: float,
    trade_date: str,
    snapshot_timestamp: str | None,
    market_cache: dict | None,
    leg_id: str,
) -> StrikeResult:
    """
    EntryByPremiumCloseToStraddle
    strike_param = {'Multiplier': 0.6, 'StrikeKind': 'StrikeType.ATM'}

    1. Find ATM strike from spot_price
    2. straddle = CE_ATM_close + PE_ATM_close
    3. target = straddle * Multiplier
    4. Find strike whose close is CLOSEST to target
    """
    from features.backtest_engine import _resolve_strike

    sp = _parse_sp_dict(strike_param_raw)
    multiplier = _safe_float(sp.get('Multiplier') or 0.5)

    step = 50 if underlying.upper() == 'NIFTY' else 100
    atm_strike = _resolve_strike(spot_price, '0', 'CE', step)

    if snapshot_timestamp:
        ce_doc = _chain_at_time(chain_col, underlying, expiry, atm_strike, 'CE', snapshot_timestamp, market_cache)
        pe_doc = _chain_at_time(chain_col, underlying, expiry, atm_strike, 'PE', snapshot_timestamp, market_cache)
    else:
        ce_doc = _chain_latest(chain_col, underlying, expiry, atm_strike, 'CE', trade_date, market_cache)
        pe_doc = _chain_latest(chain_col, underlying, expiry, atm_strike, 'PE', trade_date, market_cache)

    ce_price = _safe_float((ce_doc or {}).get('close'))
    pe_price = _safe_float((pe_doc or {}).get('close'))
    straddle = ce_price + pe_price
    target = straddle * multiplier

    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=PremiumCloseToStraddle '
        f'atm={atm_strike} ce={ce_price} pe={pe_price} straddle={straddle} '
        f'multiplier={multiplier} target={target}'
    )

    if target <= 0:
        print(f'[STRIKE CALC FAILED] leg={leg_id} method=PremiumCloseToStraddle reason=straddle_or_multiplier_zero')
        return StrikeResult(error='straddle_premium_zero')

    def _closest(ts_filter: dict) -> dict | None:
        base = {'underlying': underlying, 'expiry': expiry, 'type': option_type, **ts_filter}
        below = chain_col.find_one({**base, 'close': {'$lte': target}}, sort=[('close', DESCENDING)])
        above = chain_col.find_one({**base, 'close': {'$gte': target}}, sort=[('close', 1)])
        if not below and not above:
            return None
        if not below:
            return above
        if not above:
            return below
        return below if abs(_safe_float(below.get('close')) - target) <= abs(_safe_float(above.get('close')) - target) else above

    if snapshot_timestamp:
        doc = _closest({'timestamp': snapshot_timestamp}) or _closest({'timestamp': {'$lte': snapshot_timestamp}})
    else:
        doc = _closest({'timestamp': {'$regex': f'^{trade_date}'}})

    if not doc:
        print(
            f'[STRIKE CALC FAILED] leg={leg_id} type={option_type} method=PremiumCloseToStraddle '
            f'target={target} expiry={expiry} reason=no_strike_found'
        )
        return StrikeResult(error='no_strike_for_straddle_premium')

    strike = _safe_float(doc.get('strike'))
    entry_price = _safe_float(doc.get('close'))
    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=PremiumCloseToStraddle '
        f'resolved_strike={strike} entry_price={entry_price} diff={round(abs(entry_price - target), 2)}'
    )
    return StrikeResult(strike=strike, entry_price=entry_price, chain_doc=doc)


def _select_delta_range(
    chain_col,
    underlying: str,
    option_type: str,
    expiry: str,
    strike_param_raw: Any,
    position: str,
    trade_date: str,
    snapshot_timestamp: str | None,
    leg_id: str,
) -> StrikeResult:
    """
    EntryByDeltaRange
    strike_param = {'LowerRange': 40, 'UpperRange': 70}

    LowerRange / UpperRange are percentage integers (40 → delta 0.40).
    CE deltas are positive; PE deltas are negative in the chain.

    Buy  → lowest delta in range  (most OTM)
    Sell → highest delta in range (least OTM / closest to ATM)

    If no strikes fall in range → leg entry is skipped.
    """
    sp = _parse_sp_dict(strike_param_raw)
    lower_pct = _safe_float(sp.get('LowerRange') or 0)
    upper_pct = _safe_float(sp.get('UpperRange') or 0)
    lower_delta = lower_pct / 100.0
    upper_delta = upper_pct / 100.0

    is_pe   = option_type.upper() == 'PE'
    is_sell = _is_sell(position)

    if is_pe:
        # PE deltas are negative: -0.70 to -0.40 for range 40–70
        delta_q = {'$gte': -upper_delta, '$lte': -lower_delta}
        # Sell → most negative (highest abs) → sort ASC
        # Buy  → least negative (lowest abs) → sort DESC
        delta_sort = 1 if is_sell else -1
    else:
        delta_q = {'$gte': lower_delta, '$lte': upper_delta}
        # Sell → highest delta → sort DESC
        # Buy  → lowest delta → sort ASC
        delta_sort = -1 if is_sell else 1

    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=DeltaRange '
        f'range={lower_pct}%–{upper_pct}% delta_q={delta_q} '
        f'position={"Sell" if is_sell else "Buy"} sort={"DESC" if delta_sort == -1 else "ASC"}'
    )

    base_q = {'underlying': underlying, 'expiry': expiry, 'type': option_type, 'delta': delta_q}
    if snapshot_timestamp:
        doc = chain_col.find_one({**base_q, 'timestamp': snapshot_timestamp}, sort=[('delta', delta_sort)])
        if not doc:
            doc = chain_col.find_one(
                {**base_q, 'timestamp': {'$lte': snapshot_timestamp}},
                sort=[('timestamp', DESCENDING), ('delta', delta_sort)],
            )
    else:
        doc = chain_col.find_one(
            {**base_q, 'timestamp': {'$regex': f'^{trade_date}'}},
            sort=[('delta', delta_sort)],
        )

    if not doc:
        print(
            f'[STRIKE CALC FAILED] leg={leg_id} type={option_type} method=DeltaRange '
            f'range={lower_pct}%–{upper_pct}% expiry={expiry} reason=no_strike_in_delta_range — leg skipped'
        )
        return StrikeResult(error='no_strike_in_delta_range')

    strike = _safe_float(doc.get('strike'))
    entry_price = _safe_float(doc.get('close'))
    selected_delta = _safe_float(doc.get('delta'))
    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=DeltaRange '
        f'resolved_strike={strike} delta={selected_delta} entry_price={entry_price}'
    )
    return StrikeResult(strike=strike, entry_price=entry_price, chain_doc=doc)


def _select_premium(
    chain_col,
    underlying: str,
    option_type: str,
    expiry: str,
    strike_param: float,
    entry_kind: str,
    trade_date: str,
    snapshot_timestamp: str | None,
    leg_id: str,
) -> StrikeResult:
    """
    EntryByPremium (Geq / Lte)
    strike_param = float  — target premium value
    Geq → close >= target  (buy above this premium)
    Lte → close <= target  (sell below this premium)
    """
    op = '$gte' if 'Geq' in entry_kind else '$lte'
    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=Premium '
        f'op={op} target={strike_param} expiry={expiry}'
    )
    base_q = {'underlying': underlying, 'expiry': expiry, 'type': option_type, 'close': {op: strike_param}}
    if snapshot_timestamp:
        doc = chain_col.find_one({**base_q, 'timestamp': snapshot_timestamp}, sort=[('strike', 1)])
        if not doc:
            doc = chain_col.find_one(
                {**base_q, 'timestamp': {'$lte': snapshot_timestamp}},
                sort=[('timestamp', DESCENDING)],
            )
    else:
        doc = chain_col.find_one({**base_q, 'timestamp': {'$regex': f'^{trade_date}'}}, sort=[('timestamp', DESCENDING)])

    if not doc:
        print(
            f'[STRIKE CALC FAILED] leg={leg_id} type={option_type} method=Premium '
            f'op={op} target={strike_param} expiry={expiry} reason=no_strike_found'
        )
        return StrikeResult(error='no_strike_for_premium')

    strike = _safe_float(doc.get('strike'))
    entry_price = _safe_float(doc.get('close'))
    print(f'[STRIKE CALC] leg={leg_id} type={option_type} method=Premium resolved_strike={strike} entry_price={entry_price}')
    return StrikeResult(strike=strike, entry_price=entry_price, chain_doc=doc)


def _select_atm_offset(
    chain_col,
    underlying: str,
    option_type: str,
    expiry: str,
    strike_param: float,
    spot_price: float,
    trade_date: str,
    snapshot_timestamp: str | None,
    market_cache: dict | None,
    leg_id: str,
) -> StrikeResult:
    """
    ATM or fixed offset from ATM.
    strike_param = int offset (0 = ATM, 1 = 1 step OTM, -1 = 1 step ITM, etc.)
    """
    from features.backtest_engine import _resolve_strike

    step = 50 if underlying.upper() == 'NIFTY' else 100
    strike = _resolve_strike(spot_price, str(int(strike_param)), option_type, step)
    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} method=ATM/Offset '
        f'spot={spot_price} param={strike_param} step={step} resolved_strike={strike}'
    )

    if snapshot_timestamp:
        doc = _chain_at_time(chain_col, underlying, expiry, strike, option_type, snapshot_timestamp, market_cache)
    else:
        doc = _chain_latest(chain_col, underlying, expiry, strike, option_type, trade_date, market_cache)

    if not doc:
        print(
            f'[STRIKE CALC FAILED] leg={leg_id} type={option_type} method=ATM/Offset '
            f'strike={strike} expiry={expiry} reason=no_chain_doc'
        )
        return StrikeResult(error='no_chain_doc')

    entry_price = _safe_float(doc.get('close'))
    print(f'[STRIKE CALC] leg={leg_id} type={option_type} method=ATM/Offset resolved_strike={strike} close={entry_price}')
    return StrikeResult(strike=strike, entry_price=entry_price, chain_doc=doc)


# ── main public function ──────────────────────────────────────────────────────

def resolve_strike(
    chain_col,
    underlying: str,
    option_type: str,
    entry_kind: str,
    strike_param_raw: Any,
    position: str,
    spot_price: float,
    expiry: str,
    trade_date: str,
    snapshot_timestamp: str | None = None,
    market_cache: dict | None = None,
    leg_id: str = '',
) -> StrikeResult:
    """
    Resolve the strike and entry price for a pending leg.

    Parameters
    ----------
    chain_col          : MongoDB collection  (option_chain_historical_data)
    underlying         : 'NIFTY' | 'BANKNIFTY' | ...
    option_type        : 'CE' | 'PE'
    entry_kind         : EntryType string from strategy config
    strike_param_raw   : raw StrikeParameter (dict, string-repr of dict, or float/int)
    position           : 'PositionType.Sell' | 'PositionType.Buy'
    spot_price         : current underlying spot price
    expiry             : resolved expiry date string (use resolve_expiry first)
    trade_date         : 'YYYY-MM-DD'
    snapshot_timestamp : ISO timestamp for backtest / forward-test lookup
    market_cache       : optional preloaded cache dict
    leg_id             : for logging only

    Returns
    -------
    StrikeResult  — check .error first; None means success.
    """
    strike_param_float = _safe_float(strike_param_raw)

    print(
        f'[STRIKE CALC] leg={leg_id} type={option_type} entry_kind={entry_kind or "ATM"} '
        f'strike_parameter={strike_param_raw} underlying={underlying} '
        f'snapshot={snapshot_timestamp or trade_date}'
    )

    if 'PremiumCloseToStraddle' in entry_kind:
        result = _select_premium_close_to_straddle(
            chain_col, underlying, option_type, expiry,
            strike_param_raw, spot_price,
            trade_date, snapshot_timestamp, market_cache, leg_id,
        )

    elif 'DeltaRange' in entry_kind:
        result = _select_delta_range(
            chain_col, underlying, option_type, expiry,
            strike_param_raw, position,
            trade_date, snapshot_timestamp, leg_id,
        )

    elif 'Premium' in entry_kind:
        result = _select_premium(
            chain_col, underlying, option_type, expiry,
            strike_param_float, entry_kind,
            trade_date, snapshot_timestamp, leg_id,
        )

    else:
        result = _select_atm_offset(
            chain_col, underlying, option_type, expiry,
            strike_param_float, spot_price,
            trade_date, snapshot_timestamp, market_cache, leg_id,
        )

    if result.error:
        return result

    if result.entry_price <= 0:
        print(
            f'[STRIKE CALC FAILED] leg={leg_id} type={option_type} '
            f'strike={result.strike} entry_price={result.entry_price} reason=entry_price_zero'
        )
        return StrikeResult(error='entry_price_zero')

    return result
