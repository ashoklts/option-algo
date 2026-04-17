"""
trading_core.py
══════════════════════════════════════════════════════════════════════════════
Heart of the trading system — reusable core engine.

This module contains ALL trading logic, independent of transport (WebSocket /
HTTP / CLI).  Every execution mode imports from here:

    • algo_backtest      – execution_socket._backtest_minute_tick
    • strategy_backtest  – single-strategy historical replay
    • portfolio_backtest – multi-strategy historical replay
    • fast_forward       – accelerated simulation (skip non-event ticks)
    • live_trade         – execution_socket._live_minute_tick

Architecture
────────────
                         trading_core.py
  ┌──────────────────────────────────────────────────────────────────┐
  │  §1  Imports & Types                                             │
  │  §2  Constants & Status Codes                                    │
  │  §3  Utility Helpers          (safe_float, is_sell, timestamps)  │
  │  §4  Market Data Helpers      (chain lookup, spot lookup)        │
  │  §5  Strike & Expiry Selection (WEEKLY/MONTH/ATM/OTM/ITM)       │
  │  §6  Leg Entry & Position History                                │
  │  §7  Feature Status Management (algo_leg_feature_status)         │
  │  §8  MTM / PnL Computation                                       │
  │  §9  Leg-Level SL / Target / Trail SL                            │
  │  §10 Overall SL / Target / Trail SL / LockAndTrail               │
  │  §11 Broker-Level SL / Target  (algo_borker_stoploss_settings)   │
  │  §12 Simple Momentum Engine                                      │
  │  §13 Re-entry Engine          (SL reentry, Target reentry)       │
  │  §14 Lazy Leg Engine          (pending legs, lazy entry)         │
  │  §15 Square-Off Helpers                                          │
  │  §16 Tick Processor           (per-minute core loop)             │
  │  §17 Entry Processor          (pending leg resolution)           │
  └──────────────────────────────────────────────────────────────────┘

Usage
─────
    from features.trading_core import (
        TickContext,
        process_tick,          # §16 — main per-minute loop
        process_pending_entries,  # §17 — entry resolution
        compute_strategy_mtm,  # §8  — PnL snapshot
        square_off_trade,      # §15 — manual/event square-off
    )

    ctx = TickContext(db=db, trade_date='2025-11-03',
                      now_ts='2025-11-03T09:18:00',
                      activation_mode='algo-backtest')
    result = process_tick(ctx, running_trades)

All functions accept a MongoData `db` instance so the caller controls the
database connection — no module-level singletons.
══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

log = logging.getLogger(__name__)


def _trace_stdout(message: str) -> None:
    """Print trading-core entry traces immediately to the Python terminal."""
    print(message, flush=True)

# ──────────────────────────────────────────────────────────────────────────────
# §1  IMPORTS & TYPES
# ──────────────────────────────────────────────────────────────────────────────

# MongoData wrapper  (thin pymongo helper — db._db is the raw pymongo Database)
from features.mongo_data import MongoData  # type: ignore

# Market data cache helpers (shared with execution_socket.py)
from features.spot_atm_utils import (     # type: ignore
    get_cached_chain_doc,
    get_cached_spot_doc,
    preload_market_data_cache,
    clear_market_data_cache,
    build_entry_spot_snapshots,
)

# Position-manager: pure math, no DB side-effects
from features.position_manager import (   # type: ignore
    # ── leg-level SL / TP ────────────────────────────────────────────────────
    calc_sl_price,           # compute SL price from leg_cfg + entry_price
    calc_tp_price,           # compute TP price from leg_cfg + entry_price
    is_sl_hit,               # bool: current_price hit SL threshold
    is_tp_hit,               # bool: current_price hit TP threshold
    check_leg_exit,          # unified: returns LegCheckResult(event, price)
    # ── trailing SL ──────────────────────────────────────────────────────────
    get_trail_config,        # parse LegTrailSL config from leg_cfg
    update_trail_sl,         # recalculate SL when price moves favourably
    # ── reentry ──────────────────────────────────────────────────────────────
    get_reentry_sl_config,   # reentry config after SL hit
    get_reentry_tp_config,   # reentry config after TP hit
    build_reentry_action,    # constructs ReentryAction object
    # ── overall SL / target ──────────────────────────────────────────────────
    parse_overall_sl,        # extract overall SL (type + value) from strategy_cfg
    parse_overall_tgt,       # extract overall target from strategy_cfg
    check_overall_sl,        # bool: trade MTM hit overall SL
    check_overall_tgt,       # bool: trade MTM hit overall target
    # ── overall reentry ──────────────────────────────────────────────────────
    parse_overall_reentry_sl,   # reentry config after overall SL hit
    parse_overall_reentry_tgt,  # reentry config after overall target hit
    # ── overall trail SL ─────────────────────────────────────────────────────
    parse_overall_trail_sl,  # extract OverallTrailSL from strategy_cfg
    update_overall_trail_sl, # recalculate overall SL threshold
    # ── lock and trail ───────────────────────────────────────────────────────
    parse_lock_and_trail,    # extract LockAndTrail from strategy_cfg
    check_lock_and_trail,    # bool + floor: LockAndTrail exit check
    # ── types ────────────────────────────────────────────────────────────────
    LockAndTrailConfig,
    ReentryAction,
    LegCheckResult,
)

# ──────────────────────────────────────────────────────────────────────────────
# §2  CONSTANTS & STATUS CODES
# ──────────────────────────────────────────────────────────────────────────────

#: Leg status stored in algo_trades.legs[].status
OPEN_LEG_STATUS   = 1   # leg is open / active
CLOSED_LEG_STATUS = 0   # leg has been exited

#: Trade status stored in algo_trades.trade_status
TRADE_STATUS_RUNNING    = 1
TRADE_STATUS_SQUARED_OFF = 2

#: algo_trades.status strings
RUNNING_STATUS        = 'StrategyStatus.Live_Running'
SQUARED_OFF_STATUS    = 'StrategyStatus.SquaredOff'
BACKTEST_IMPORT_STATUS = 'StrategyStatus.Import'

#: MongoDB collection names
COL_ALGO_TRADES     = 'algo_trades'
COL_POSITIONS_HIST  = 'algo_trade_positions_history'
COL_LEG_FEATURES    = 'algo_leg_feature_status'
COL_NOTIFICATIONS   = 'algo_trade_notification'
COL_OPTION_CHAIN    = 'option_chain_historical_data'
COL_SPOT            = 'option_chain_index_spot'
COL_BROKER_SL       = 'algo_borker_stoploss_settings'

#: Synthetic leg_id used for trade-level (overall) feature records
OVERALL_LEG_ID = '__overall__'


# ──────────────────────────────────────────────────────────────────────────────
# §2b  TICK CONTEXT  — carries all per-tick state (replaces scattered globals)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class TickContext:
    """
    Immutable-ish context object passed into every core function.

    Centralises: db, trade_date, now_ts, activation_mode, market_cache.
    This replaces the scattered parameter lists in execution_socket.py
    and makes every function testable without a running server.

    Example
    -------
        ctx = TickContext(
            db=db,
            trade_date='2025-11-03',
            now_ts='2025-11-03T09:18:00',
            activation_mode='algo-backtest',
        )
        result = process_tick(ctx, running_trades)
    """
    db:              MongoData
    trade_date:      str
    now_ts:          str                       # ISO timestamp of current candle
    activation_mode: str = 'algo-backtest'
    market_cache:    dict | None = None        # pre-loaded chain/spot cache
    live_ltp_map:    dict | None = None        # live kite LTP map {str(token): float} for live entry pricing

    # ── mutable state collected during the tick (filled by process_tick) ─────
    trade_mtm_map:     dict = field(default_factory=dict)  # trade_id → float MTM
    hit_trade_ids:     list = field(default_factory=list)  # IDs hit this tick
    hit_ltp_snapshots: dict = field(default_factory=dict)  # trade_id → leg LTP list
    actions_taken:     list = field(default_factory=list)  # audit strings


@dataclass
class TickResult:
    """
    Returned by process_tick().
    Frontend / websocket layer reads this to decide what to broadcast.
    """
    actions_taken:     list[str]         # human-readable audit log
    hit_trade_ids:     list[str]         # strategies whose overall/broker SL-TGT fired
    hit_ltp_snapshots: dict[str, list]   # {trade_id: [{leg_id, ltp, entry_price, pnl}]}
    open_positions:    list[dict]        # snapshot of all open positions this tick
    checked_at:        str               # now_ts echoed back


# ──────────────────────────────────────────────────────────────────────────────
# §3  UTILITY HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def safe_float(value: Any, default: float = 0.0) -> float:
    """
    Safely convert any value to float.
    Returns `default` on None, empty string, or conversion failure.

    Used everywhere a config value might be None or a string number.
    """
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    """
    Safely convert any value to int (via float to handle '75.0').
    Returns `default` on failure.
    """
    if value is None:
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def is_sell(position_str: str) -> bool:
    """
    Returns True if the position string indicates a SELL (short) position.
    Handles: 'PositionType.Sell', 'sell', 'SHORT', etc.
    """
    return 'sell' in str(position_str or '').lower()


def parse_timestamp(value: Any) -> datetime | None:
    """
    Parse ISO timestamp strings into datetime objects.
    Accepts: 'YYYY-MM-DDTHH:MM:SS', 'YYYY-MM-DD HH:MM:SS', 'YYYY-MM-DDTHH:MM'.

    Returns None if parsing fails — callers must handle None gracefully.
    """
    raw = str(value or '').strip()
    if not raw:
        return None
    normalized = raw.replace(' ', 'T')
    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M'):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def format_timestamp(dt: datetime) -> str:
    """Format datetime to canonical 'YYYY-MM-DDTHH:MM:SS' string."""
    return dt.strftime('%Y-%m-%dT%H:%M:%S')


def now_iso() -> str:
    """Current UTC time in ISO format ('YYYY-MM-DDTHH:MM:SSZ')."""
    return datetime.now(timezone.utc).replace(tzinfo=None).strftime('%Y-%m-%dT%H:%M:%SZ')


def build_trade_query(
    trade_date: str,
    *,
    activation_mode: str | None = None,
    statuses: list[str] | None = None,
    include_squared_off: bool = False,
) -> dict:
    """
    Build a MongoDB query for algo_trades.

    Parameters
    ----------
    trade_date:           'YYYY-MM-DD'  (filters creation_ts with regex)
    activation_mode:      'algo-backtest' | 'live' | 'forward-test'
    statuses:             list of status strings; omit for default (Live_Running)
    include_squared_off:  if True, omits trade_status filter so both 1 and 2 are included

    Returns a pymongo-compatible filter dict.
    """
    query: dict[str, Any] = {}
    if not include_squared_off:
        query['trade_status'] = TRADE_STATUS_RUNNING
    normalized_date = str(trade_date or '').strip()
    if normalized_date:
        query['creation_ts'] = {'$regex': f'^{re.escape(normalized_date)}'}
    if activation_mode:
        query['activation_mode'] = str(activation_mode).strip()
    normalized_statuses = [str(s).strip() for s in (statuses or []) if str(s or '').strip()]
    if len(normalized_statuses) == 1:
        query['status'] = normalized_statuses[0]
    elif normalized_statuses:
        query['status'] = {'$in': normalized_statuses}
    return query


def normalize_expiry(raw_expiry: str) -> str:
    """
    Normalize expiry_date to 'YYYY-MM-DD' format.
    History collection may store '2025-11-06 15:30:00'; strip to date-only.
    """
    raw = str(raw_expiry or '').strip()
    return raw[:10] if ' ' in raw else raw


# ──────────────────────────────────────────────────────────────────────────────
# §4  MARKET DATA HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def make_option_token(underlying: str, expiry: str, strike: Any, option_type: str) -> str:
    """
    Build a composite token string for an option contract.

    Example:
        make_option_token('NIFTY', '2025-11-04', 24500, 'CE')
        → 'NIFTY_2025-11-04_24500_CE'

    Used as cache key and subscription identifier across all execution modes.
    """
    strike_str = str(int(float(strike))) if strike is not None else '0'
    return f'{underlying}_{expiry}_{strike_str}_{option_type}'


def get_chain_at_time(
    db: MongoData,
    underlying: str,
    expiry: str,
    strike: Any,
    option_type: str,
    now_ts: str,
    market_cache: dict | None = None,
) -> dict | None:
    """
    Fetch option chain document at a specific historical timestamp.

    Tries market_cache first (O(1)) then falls back to MongoDB.
    Returns None if no matching chain document is found.

    Used by:
      - Strike resolution at entry time
      - SL / TP price calculation
      - Backtest tick processing
    """
    try:
        return get_cached_chain_doc(
            db._db,
            underlying=underlying,
            expiry=expiry,
            strike=strike,
            option_type=option_type,
            timestamp=now_ts,
            cache=market_cache,
        )
    except Exception as exc:
        log.warning('get_chain_at_time error underlying=%s strike=%s: %s', underlying, strike, exc)
        return None


def get_chain_by_token_at_time(
    db: MongoData,
    token: str,
    now_ts: str,
    market_cache: dict | None = None,
) -> dict | None:
    """
    Fetch option chain document using a composite token string.

    Token format: 'UNDERLYING_EXPIRY_STRIKE_OPTIONTYPE'
    Example: 'NIFTY_2025-11-04_24500_CE'

    Used during square-off when we already have the token from position history.
    """
    parts = token.split('_')
    if len(parts) < 4:
        return None
    underlying   = parts[0]
    option_type  = parts[-1]
    strike       = parts[-2]
    expiry       = '_'.join(parts[1:-2])
    return get_chain_at_time(db, underlying, expiry, strike, option_type, now_ts, market_cache)


def get_spot_at_time(
    db: MongoData,
    underlying: str,
    now_ts: str,
    market_cache: dict | None = None,
) -> float:
    """
    Fetch index spot price at a specific timestamp.

    Returns 0.0 if not found.

    Used by:
      - Strike selection (ATM calculation needs current spot)
      - Upper/Lower adjustment level checks
    """
    try:
        doc = get_cached_spot_doc(
            db._db,
            underlying=underlying,
            timestamp=now_ts,
            cache=market_cache,
        )
        if not doc:
            return 0.0
        return safe_float(
            doc.get('close') or doc.get('ltp') or doc.get('last_price') or doc.get('price')
        )
    except Exception as exc:
        log.warning('get_spot_at_time error underlying=%s: %s', underlying, exc)
        return 0.0


def resolve_chain_price(chain_doc: dict | None) -> float:
    """
    Extract the best available price from a chain document.

    Priority: close → ltp → current_price → price → last_price
    Returns 0.0 if chain_doc is None or all fields are missing/zero.

    This is the single authoritative price-extraction function — all
    leg-level SL/TP/entry computations should use this.
    """
    if not chain_doc:
        return 0.0
    for field_name in ('close', 'ltp', 'current_price', 'price', 'last_price'):
        val = safe_float(chain_doc.get(field_name))
        if val > 0:
            return val
    return 0.0


def normalize_chain_fields(chain_doc: dict) -> dict:
    """
    Ensure all standard price fields on a chain document are populated.

    Sets ltp, close, current_price, price, last_price to the best available
    value so downstream code can use any field without extra None checks.
    """
    best = resolve_chain_price(chain_doc)
    for f in ('ltp', 'close', 'current_price', 'price', 'last_price'):
        if not chain_doc.get(f):
            chain_doc[f] = best
    return chain_doc


def preload_market_cache(
    db: MongoData,
    trade_date: str,
    trade_records: list[dict],
) -> dict:
    """
    Pre-load all option chain + spot data for a trade_date into memory.

    Call once per trade_date before the tick loop to make all chain/spot
    lookups O(1) in-memory instead of per-tick MongoDB queries.

    Returns a market_cache dict that should be passed into TickContext.
    """
    try:
        underlyings = {
            str((t.get('config') or {}).get('Ticker') or t.get('ticker') or '')
            for t in trade_records
        } - {''}
        cache: dict = {}
        for underlying in underlyings:
            preload_market_data_cache(
                db._db,
                underlying=underlying,
                trade_date=trade_date,
                cache=cache,
            )
        return cache
    except Exception as exc:
        log.warning('preload_market_cache error date=%s: %s', trade_date, exc)
        return {}


# ──────────────────────────────────────────────────────────────────────────────
# §5  STRIKE & EXPIRY SELECTION
# ──────────────────────────────────────────────────────────────────────────────
#
# Expiry types supported:
#   WEEKLY      → current week's expiry (nearest expiry from now_ts)
#   NEXT_WEEK   → next week's expiry
#   MONTH       → current month's expiry (last Thursday / configured day)
#   NEXT_MONTH  → next month's expiry
#
# Strike types supported:
#   ATM         → At-the-money (nearest strike to spot)
#   OTM         → Out-of-the-money  (+N strikes for CE, -N strikes for PE)
#   ITM         → In-the-money      (-N strikes for CE, +N strikes for PE)
#
# Both resolve through features.strike_selector which uses the option_chain
# collection to find the actual listed strike nearest to the required value.
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_live_expiries(underlying: str, from_date: str) -> list[str]:
    """
    Fetch sorted expiry dates for *underlying* >= from_date from the
    Kite instruments daily cache.  Live / fast-forward only — never touches DB.
    """
    from features.spot_atm_utils import get_kite_expiries  # type: ignore
    return get_kite_expiries(underlying, from_date)


def resolve_leg_expiry(
    db: MongoData,
    leg_cfg: dict,
    underlying: str,
    now_ts: str,
    market_cache: dict | None = None,
) -> str | None:
    """
    Resolve the expiry date string for a pending leg.

    Reads ExpiryKind from leg_cfg and returns the matching expiry as 'YYYY-MM-DD'.

    ExpiryKind mapping:
      ExpiryType.Weekly      → nearest upcoming weekly expiry (current week)
      ExpiryType.NextWeekly  → next weekly expiry after the current one
      ExpiryType.Monthly     → last expiry in current calendar month
      ExpiryType.NextMonthly → last expiry in next calendar month

    For live / fast-forward: expiries from Kite instruments daily cache (never DB).
    For backtest: expiries taken from pre-loaded market_cache (DB).

    Returns None if resolution fails — caller should skip entry for this tick.
    """
    try:
        from features.backtest_engine import _resolve_expiry  # type: ignore

        expiry_kind = str(
            leg_cfg.get('ExpiryKind')
            or leg_cfg.get('expiry_kind')
            or leg_cfg.get('ExpiryType')
            or 'ExpiryType.Weekly'
        ).strip()

        trade_date = now_ts[:10]
        underlying_upper = str(underlying or '').strip().upper()

        if market_cache:
            # Backtest path — use pre-loaded cache
            expiries = sorted(
                (market_cache.get('expiries_by_underlying') or {}).get(underlying_upper, [])
            )
        else:
            # Live / fast-forward path — fetch from Kite instruments cache only
            expiries = _fetch_live_expiries(underlying_upper, trade_date)

        if not expiries:
            log.warning(
                'resolve_leg_expiry: no expiries found underlying=%s date=%s kind=%s',
                underlying_upper, trade_date, expiry_kind,
            )
            return None

        return _resolve_expiry(trade_date, expiry_kind, expiries)
    except Exception as exc:
        log.warning('resolve_leg_expiry error underlying=%s: %s', underlying, exc)
        return None


def resolve_leg_strike(
    db: MongoData,
    leg_cfg: dict,
    underlying: str,
    expiry: str,
    option_type: str,
    spot_price: float,
    now_ts: str,
    market_cache: dict | None = None,
) -> int | None:
    """
    Resolve the strike price for a pending leg.

    For live / fast-forward (market_cache=None): queries DB for contract
    metadata; entry price is overlaid with Kite LTP by resolve_pending_leg_entry
    via ctx.live_ltp_map — no separate handling needed here.

    Returns an integer strike price, or None on failure.
    """
    try:
        from features.strike_selector import resolve_strike  # type: ignore

        entry_kind   = str(
            leg_cfg.get('EntryType') or leg_cfg.get('entry_type')
            or 'EntryType.EntryByStrikeType'
        ).strip()
        strike_param = (
            leg_cfg.get('StrikeParameter')
            or leg_cfg.get('strike_parameter')
            or leg_cfg.get('StrikeType')
            or 'StrikeType.ATM'
        )
        position     = str(
            leg_cfg.get('PositionType') or leg_cfg.get('position_type') or 'PositionType.Sell'
        ).strip()
        trade_date   = now_ts[:10]

        if market_cache:
            # ── Backtest: use pre-loaded DB cache via strike_selector ─────────
            chain_col = db._db['option_chain_historical_data']
            result = resolve_strike(
                chain_col,
                underlying=underlying,
                option_type=option_type,
                entry_kind=entry_kind,
                strike_param_raw=strike_param,
                position=position,
                spot_price=spot_price,
                expiry=expiry,
                trade_date=trade_date,
                snapshot_timestamp=now_ts,
                market_cache=market_cache,
                leg_id=str(leg_cfg.get('id') or ''),
            )
            if result.error:
                log.warning(
                    'resolve_leg_strike error underlying=%s expiry=%s: %s',
                    underlying, expiry, result.error,
                )
                return None
            return int(result.strike) if result.strike else None

        else:
            # ── Live / fast-forward: resolve strike from Kite instruments ─────
            # Never touch option_chain_historical_data — use Kite data only.
            from features.spot_atm_utils import (  # type: ignore
                get_strike_step, get_kite_chain_doc, _load_kite_instruments,
            )
            from features.backtest_engine import _resolve_strike as _be_resolve_strike  # type: ignore

            step = get_strike_step(underlying)

            if 'Premium' in entry_kind:
                # Premium-based: scan all Kite instruments for this expiry,
                # get LTP via REST quote cache (works before WS subscription).
                from features.spot_atm_utils import (  # type: ignore
                    fetch_kite_quotes_for_expiry as _fetch_quotes,
                    safe_float as _sf,
                )
                from features.kite_broker_ws import get_ltp_map  # type: ignore

                target = _sf(strike_param) if not isinstance(strike_param, dict) else (
                    (_sf(strike_param.get('LowerRange', 0)) + _sf(strike_param.get('UpperRange', 0))) / 2
                )
                # Fetch all LTPs for the expiry in one REST call
                quotes    = _fetch_quotes(underlying, expiry)
                ws_ltp    = get_ltp_map()
                inst_cache = _load_kite_instruments()
                und_upper  = underlying.upper()
                best_strike: int | None = None
                best_diff = float('inf')
                for (name, exp, stk, otype), inst in inst_cache.items():
                    if name != und_upper or exp != expiry or otype != option_type.upper():
                        continue
                    tok = str(inst['token'])
                    # REST quote first, WS LTP map as fallback
                    ltp = float(quotes.get(tok, 0.0)) or float(ws_ltp.get(tok, 0.0))
                    if ltp <= 0:
                        continue
                    diff = abs(ltp - target)
                    if diff < best_diff:
                        best_diff   = diff
                        best_strike = int(stk)
                return best_strike

            else:
                # ATM / offset-based: compute directly from spot — no Kite lookup needed
                strike_int = _be_resolve_strike(spot_price, str(strike_param), option_type, step)
                return int(strike_int)
    except Exception as exc:
        log.warning('resolve_leg_strike error underlying=%s expiry=%s: %s', underlying, expiry, exc)
        return None


# ──────────────────────────────────────────────────────────────────────────────
# §6  LEG ENTRY & POSITION HISTORY
# ──────────────────────────────────────────────────────────────────────────────

def build_exit_trade_payload(
    exit_price: float,
    exit_reason: str,
    now_ts: str,
) -> dict:
    """
    Build the exit_trade dict stored inside algo_trades.legs[].exit_trade
    and algo_trade_positions_history.exit_trade.

    exit_reason choices:
      'stoploss'   – leg-level SL hit
      'target'     – leg-level TP hit
      'overall_sl' – trade-level overall SL hit
      'overall_target' – trade-level overall target hit
      'exit_time'  – time-based forced exit
      'squared_off'– manual or broker-level square-off
    """
    return {
        'trigger_timestamp':  now_ts,
        'trigger_price':      exit_price,
        'price':              exit_price,
        'traded_timestamp':   now_ts,
        'exchange_timestamp': now_ts,
        'exit_reason':        exit_reason,
    }


def close_leg_in_db(
    db: MongoData,
    trade_id: str,
    leg_index: int,
    exit_price: float,
    exit_reason: str,
    now_ts: str,
    leg_id: str = '',
) -> None:
    """
    Mark a leg as CLOSED in algo_trades and update its position history record.

    Steps:
      1. Sets legs[leg_index].status = CLOSED_LEG_STATUS (0)
      2. Sets legs[leg_index].exit_trade with price + reason + timestamps
      3. Calls update_position_history_exit() to mirror the exit in history

    Called by every exit path: SL hit, TP hit, exit_time, square-off.
    """
    exit_payload = build_exit_trade_payload(exit_price, exit_reason, now_ts)
    try:
        db._db[COL_ALGO_TRADES].update_one(
            {'_id': trade_id},
            {'$set': {
                f'legs.{leg_index}.status':      CLOSED_LEG_STATUS,
                f'legs.{leg_index}.exit_reason': exit_reason,
                f'legs.{leg_index}.exit_trade':  exit_payload,
                f'legs.{leg_index}.last_saw_price': exit_price,
            }},
        )
    except Exception as exc:
        log.error('close_leg_in_db error trade=%s leg=%s: %s', trade_id, leg_index, exc)
    update_position_history_exit(db, trade_id, leg_index, exit_price, exit_reason, now_ts, leg_id=leg_id)


def update_position_history_exit(
    db: MongoData,
    trade_id: str,
    leg_index: int,
    exit_price: float,
    exit_reason: str,
    now_ts: str,
    leg_id: str = '',
) -> None:
    """
    Write exit_trade data to algo_trade_positions_history.

    Finds the history record by leg_id (preferred) or trade_id + array index,
    sets exit_trade, updates last_saw_price and display fields.

    Called exclusively from close_leg_in_db — not called directly.
    """
    exit_payload = build_exit_trade_payload(exit_price, exit_reason, now_ts)
    try:
        if leg_id:
            db._db[COL_POSITIONS_HIST].update_one(
                {'trade_id': trade_id, 'leg_id': leg_id},
                {'$set': {
                    'exit_trade':     exit_payload,
                    'last_saw_price': exit_price,
                    'status':         CLOSED_LEG_STATUS,
                }},
            )
        else:
            db._db[COL_POSITIONS_HIST].update_one(
                {'trade_id': trade_id},
                {'$set': {
                    'exit_trade':     exit_payload,
                    'last_saw_price': exit_price,
                    'status':         CLOSED_LEG_STATUS,
                }},
                sort=[('_id', 1)],
            )
    except Exception as exc:
        log.error('update_position_history_exit error trade=%s leg=%s: %s', trade_id, leg_id, exc)


def resolve_trade_leg_configs(trade: dict) -> dict[str, dict]:
    """
    Merge ListOfLegConfigs + IdleLegConfigs into a single dict keyed by leg_id.

    This is the canonical way to get the config for any leg by its id,
    used by every SL/TP/Trail/Reentry check to read feature settings.

    Returns {} if trade has no leg configs.
    """
    merged: dict[str, dict] = {}
    for cfg_list in (
        trade.get('ListOfLegConfigs') or [],
        (trade.get('strategy') or {}).get('ListOfLegConfigs') or [],
        trade.get('IdleLegConfigs') or [],
        (trade.get('strategy') or {}).get('IdleLegConfigs') or [],
    ):
        for cfg in cfg_list:
            if not isinstance(cfg, dict):
                continue
            leg_id = str(cfg.get('id') or '').strip()
            if leg_id:
                merged[leg_id] = cfg
    return merged


def resolve_leg_cfg(leg_id: str, leg: dict, all_leg_configs: dict[str, dict]) -> dict:
    """
    Get the leg config dict for a specific leg.

    Tries all_leg_configs first (from resolve_trade_leg_configs),
    then falls back to the leg document itself.

    Returns {} if not found — callers must treat missing config as
    "feature disabled".
    """
    return all_leg_configs.get(leg_id) or dict(leg)


def resolve_leg_option_type(leg: dict, leg_cfg: dict | None = None) -> str:
    """
    Resolve CE/PE for a leg from runtime leg data or strategy config.

    Priority:
      1. leg['option']
      2. leg_cfg['OptionType']
      3. leg_cfg['option']
      4. leg_cfg['InstrumentKind']  ("LegType.PE" -> "PE")
      5. default "CE"
    """
    leg_cfg = leg_cfg or {}

    option_raw = str(leg.get('option') or '').strip().upper()
    if option_raw in {'CE', 'PE'}:
        return option_raw

    option_raw = str(leg_cfg.get('OptionType') or leg_cfg.get('option') or '').strip().upper()
    if option_raw in {'CE', 'PE'}:
        return option_raw

    instrument_kind = str(leg_cfg.get('InstrumentKind') or '').strip()
    if instrument_kind:
        option_raw = instrument_kind.split('.')[-1].strip().upper()
        if option_raw in {'CE', 'PE'}:
            return option_raw

    return 'CE'


def build_pending_leg(
    leg_id: str,
    leg_cfg: dict,
    trade: dict,
    now_ts: str,
    triggered_by: str = '',
    *,
    leg_type: str = '',
) -> dict:
    """
    Construct a new pending (lazy) leg dict to push into algo_trades.legs.

    The leg has no entry_trade yet — it will be filled by
    resolve_pending_leg_entry() (§17) when entry conditions are met.

    Fields set:
      id, status=OPEN, entry_trade=None, is_lazy=True,
      triggered_by, leg_type, ExpiryKind, StrikeParameter, etc.
    """
    return {
        'id':              leg_id,
        'status':          OPEN_LEG_STATUS,
        'entry_trade':     None,
        'exit_trade':      None,
        'is_lazy':         True,
        'triggered_by':    triggered_by,
        'leg_type':        leg_type or leg_id,
        'option':          resolve_leg_option_type({}, leg_cfg),
        'position':        str(leg_cfg.get('Position') or leg_cfg.get('position') or 'PositionType.Sell'),
        'quantity':        safe_int(leg_cfg.get('Quantity') or leg_cfg.get('quantity') or 1),
        'lot_size':        safe_int(leg_cfg.get('LotSize') or leg_cfg.get('lot_size') or 1),
        'expiry_kind':     str(leg_cfg.get('ExpiryKind') or leg_cfg.get('ExpiryType') or 'WEEKLY'),
        'strike_parameter': str(leg_cfg.get('StrikeParameter') or leg_cfg.get('StrikeType') or 'ATM'),
        'strike_value':    safe_float(leg_cfg.get('StrikeValue') or leg_cfg.get('strike_value') or 0),
        'created_at':      now_ts,
        'parent_trade_id': str(trade.get('_id') or ''),
    }


def push_new_leg_in_db(db: MongoData, trade_id: str, leg: dict) -> bool:
    """
    Append a new pending leg dict to algo_trades.legs array.

    Returns True on success, False on error.
    Called by _handle_reentry (§13) and queue_original_legs (§14).
    """
    try:
        db._db[COL_ALGO_TRADES].update_one(
            {'_id': trade_id},
            {'$push': {'legs': leg}},
        )
        return True
    except Exception as exc:
        log.error('push_new_leg_in_db error trade=%s: %s', trade_id, exc)
        return False


# ──────────────────────────────────────────────────────────────────────────────
# §7  FEATURE STATUS MANAGEMENT  (algo_leg_feature_status)
# ──────────────────────────────────────────────────────────────────────────────
#
# Feature status records track the state of every feature (SL, Target,
# TrailSL, Momentum, Overall SL/Target) per leg per trade.
#
# Each record:
#   trade_id, leg_id, feature, enabled, status, trigger_value,
#   triggered_at, current_mtm, disabled_reason
#
# Special leg_id '__overall__' is used for trade-level (overall) features.
# ─────────────────────────────────────────────────────────────────────────────

def set_overall_feature_state(
    db: MongoData,
    trade_id: str,
    feature: str,
    *,
    enabled: bool,
    status: str,
    now_ts: str,
    current_mtm: float = 0.0,
    disabled_reason: str = '',
) -> None:
    """
    Update a single overall feature record (overall_sl or overall_target).

    feature:         'overall_sl' | 'overall_target'
    enabled:         True = still watching; False = fired or disabled
    status:          'pending' | 'triggered' | 'disabled'
    disabled_reason: 'overall_sl_hit' | 'overall_target_hit' | 'cycle_completed'

    Called by sync_overall_feature_status() and when SL/Target fires.
    """
    try:
        db._db[COL_LEG_FEATURES].update_one(
            {
                'trade_id': trade_id,
                'leg_id':   OVERALL_LEG_ID,
                'feature':  feature,
            },
            {'$set': {
                'enabled':         enabled,
                'status':          status,
                'timestamp':       now_ts,
                'current_mtm':     round(current_mtm, 2),
                'disabled_reason': disabled_reason,
            }},
            upsert=True,
        )
    except Exception as exc:
        log.error('set_overall_feature_state error trade=%s feature=%s: %s', trade_id, feature, exc)


def sync_overall_feature_status(
    db: MongoData,
    trade: dict,
    now_ts: str,
    *,
    current_mtm: float,
    overall_sl_done: int,
    overall_tgt_done: int,
) -> None:
    """
    Upsert overall_sl and overall_target feature records for a trade.

    Creates 'pending' records on first call, updates current_mtm and
    reentry cycle counts on subsequent calls.

    Called every tick for trades with overall SL/Target configured.
    Ensures the feature audit trail is always up-to-date.
    """
    strategy_cfg = trade.get('strategy') or trade.get('config') or {}
    _osl_type, _osl_val   = parse_overall_sl(strategy_cfg)
    _otgt_type, _otgt_val = parse_overall_tgt(strategy_cfg)

    if _osl_type != 'None' and _osl_val:
        set_overall_feature_state(
            db, str(trade.get('_id') or ''), 'overall_sl',
            enabled=True, status='pending', now_ts=now_ts,
            current_mtm=current_mtm,
        )
    if _otgt_type != 'None' and _otgt_val:
        set_overall_feature_state(
            db, str(trade.get('_id') or ''), 'overall_target',
            enabled=True, status='pending', now_ts=now_ts,
            current_mtm=current_mtm,
        )


def disable_feature_records_for_cycle(
    db: MongoData,
    trade_id: str,
    *,
    reason: str,
    now_ts: str,
) -> None:
    """
    Disable ALL feature records for a trade at the end of a reentry cycle.

    Called when overall SL or overall Target fires and the trade starts
    a new reentry cycle.  Ensures stale feature records don't block the
    next cycle's checks.
    """
    try:
        db._db[COL_LEG_FEATURES].update_many(
            {'trade_id': trade_id, 'enabled': True},
            {'$set': {
                'enabled':         False,
                'disabled_reason': reason,
                'disabled_at':     now_ts,
            }},
        )
    except Exception as exc:
        log.error('disable_feature_records_for_cycle error trade=%s: %s', trade_id, exc)


# ──────────────────────────────────────────────────────────────────────────────
# §8  MTM / PnL COMPUTATION
# ──────────────────────────────────────────────────────────────────────────────

def compute_strategy_mtm(
    db: MongoData,
    trade_id: str,
    now_ts: str,
    open_positions: list[dict] | None = None,
) -> tuple[float, list[dict]]:
    """
    Compute total MTM (mark-to-market) PnL for a trade at now_ts.

    Algorithm
    ---------
    1. Load all legs from algo_trade_positions_history for trade_id.
    2. For each entered leg:
       - If exit_trade exists and exit_dt <= now_ts → use exit_price (realized)
       - Else → use current_price from open_positions or last_saw_price (unrealized)
    3. PnL formula:
       SELL: (entry_price - current_price) × quantity × lot_size
       BUY:  (current_price - entry_price) × quantity × lot_size
    4. Sum all leg PnLs → total_mtm

    Returns
    -------
    (total_mtm, legs_pnl_snapshot)
    where legs_pnl_snapshot = [{leg_id, pnl, quantity, ltp, entry_price}]

    Used by:
      - Backtest tick (§16) for overall SL/Target check
      - Broker-level SL check (§11)
      - LTP-snapshot capture before square-off
    """
    normalized_id = str(trade_id or '').strip()
    if not normalized_id:
        return 0.0, []

    # Index open_positions by leg_id for O(1) LTP lookup
    ltp_by_leg: dict[str, dict] = {}
    for pos in (open_positions or []):
        leg_id = str((pos or {}).get('leg_id') or '').strip()
        if leg_id:
            ltp_by_leg[leg_id] = dict(pos)

    history_docs = list(db._db[COL_POSITIONS_HIST].find(
        {'trade_id': normalized_id},
        {
            '_id': 1, 'leg_id': 1, 'id': 1,
            'position': 1, 'quantity': 1, 'lot_size': 1,
            'entry_trade': 1, 'exit_trade': 1, 'last_saw_price': 1,
        },
    ))

    now_dt      = parse_timestamp(now_ts)
    total_mtm   = 0.0
    legs_snapshot: list[dict] = []

    for doc in history_docs:
        entry_trade = doc.get('entry_trade') if isinstance(doc.get('entry_trade'), dict) else {}
        if not entry_trade:
            continue   # pending leg — not yet entered, no PnL

        leg_id       = str(doc.get('leg_id') or doc.get('id') or doc.get('_id') or '').strip()
        entry_price  = safe_float(entry_trade.get('price') or entry_trade.get('trigger_price'))
        lot_size     = safe_int(doc.get('lot_size'), 1)
        lots         = safe_int(doc.get('quantity') or entry_trade.get('quantity'))
        qty          = max(0, lots) * max(1, lot_size)

        if entry_price <= 0 or qty <= 0 or not leg_id:
            continue

        sell         = is_sell(str(doc.get('position') or ''))
        exit_trade   = doc.get('exit_trade') if isinstance(doc.get('exit_trade'), dict) else None
        exit_ts      = str((exit_trade or {}).get('traded_timestamp') or (exit_trade or {}).get('trigger_timestamp') or '').strip()
        exit_dt      = parse_timestamp(exit_ts)

        if exit_trade and (not now_dt or not exit_dt or exit_dt <= now_dt):
            # Leg already exited at or before now_ts → realized PnL
            current_price = safe_float(exit_trade.get('price') or exit_trade.get('trigger_price'))
        else:
            # Leg still open → unrealized PnL from live LTP or last_saw_price
            open_pos      = ltp_by_leg.get(leg_id) or {}
            current_price = safe_float(
                open_pos.get('current_price')
                or open_pos.get('ltp')
                or doc.get('last_saw_price')
            )

        pnl = ((entry_price - current_price) if sell else (current_price - entry_price)) * qty
        total_mtm += pnl
        legs_snapshot.append({
            'leg_id':      leg_id,
            'pnl':         round(pnl, 2),
            'quantity':    qty,
            'ltp':         current_price,
            'entry_price': entry_price,
        })

    return round(total_mtm, 2), legs_snapshot


# ──────────────────────────────────────────────────────────────────────────────
# §9  LEG-LEVEL SL / TARGET / TRAIL SL
# ──────────────────────────────────────────────────────────────────────────────
#
# Per-leg feature checks run inside the legs loop of process_tick().
# Each function is pure (no DB side-effects) — callers decide what to do
# with the result and write to DB via close_leg_in_db().
# ─────────────────────────────────────────────────────────────────────────────

def check_leg_sl(
    leg_cfg: dict,
    entry_price: float,
    current_price: float,
    stored_sl: float | None,
    is_sell_position: bool,
) -> tuple[bool, float]:
    """
    Check if the current price has hit the leg's stop-loss.

    Returns (sl_hit: bool, sl_price: float).

    Delegates to position_manager.is_sl_hit() and calc_sl_price().
    The stored_sl (from DB) takes priority over a freshly computed one —
    this preserves manual SL updates and trail-SL moves.
    """
    sl_price = stored_sl if stored_sl else calc_sl_price(leg_cfg, entry_price, is_sell_position)
    if not sl_price:
        return False, 0.0
    hit = is_sl_hit(sl_price, current_price, is_sell_position)
    return hit, sl_price


def check_leg_target(
    leg_cfg: dict,
    entry_price: float,
    current_price: float,
    stored_tp: float | None,
    is_sell_position: bool,
) -> tuple[bool, float]:
    """
    Check if the current price has hit the leg's take-profit target.

    Returns (tp_hit: bool, tp_price: float).
    """
    tp_price = stored_tp if stored_tp else calc_tp_price(leg_cfg, entry_price, is_sell_position)
    if not tp_price:
        return False, 0.0
    hit = is_tp_hit(tp_price, current_price, is_sell_position)
    return hit, tp_price


def compute_next_trail_sl(
    leg_cfg: dict,
    entry_price: float,
    current_price: float,
    stored_sl: float,
    is_sell_position: bool,
) -> float:
    """
    Compute the new trail-SL price if the current price moved favourably.

    Returns new_sl_price — if it equals stored_sl, no update is needed.
    Returns stored_sl unchanged if trail is not configured or conditions not met.

    Delegates to position_manager.update_trail_sl().
    """
    trail_cfg = get_trail_config(leg_cfg)
    if not trail_cfg:
        return stored_sl
    return update_trail_sl(trail_cfg, entry_price, current_price, stored_sl, is_sell_position) or stored_sl


def update_leg_sl_in_db(
    db: MongoData,
    trade_id: str,
    leg_index: int,
    new_sl_price: float,
    current_price: float,
    leg_id: str = '',
) -> None:
    """
    Persist a new SL price after a trail-SL move or manual SL update.

    Updates:
      - algo_trades.legs[leg_index].current_sl_price
      - algo_trades.legs[leg_index].last_saw_price
      - algo_trade_positions_history.current_sl_price  (by leg_id)

    Called only when new_sl_price != stored_sl (to avoid redundant writes).
    """
    try:
        db._db[COL_ALGO_TRADES].update_one(
            {'_id': trade_id},
            {'$set': {
                f'legs.{leg_index}.current_sl_price':  new_sl_price,
                f'legs.{leg_index}.last_saw_price':    current_price,
            }},
        )
        if leg_id:
            db._db[COL_POSITIONS_HIST].update_one(
                {'trade_id': trade_id, 'leg_id': leg_id},
                {'$set': {'current_sl_price': new_sl_price, 'last_saw_price': current_price}},
            )
    except Exception as exc:
        log.error('update_leg_sl_in_db error trade=%s leg=%s: %s', trade_id, leg_id, exc)


# ──────────────────────────────────────────────────────────────────────────────
# §10  OVERALL SL / TARGET / TRAIL SL / LOCK-AND-TRAIL  (per trade)
# ──────────────────────────────────────────────────────────────────────────────
#
# These checks operate on the total trade MTM, not individual legs.
#
# Priority order (same as execution_socket.py):
#   1. Overall SL        → close all legs, optional reentry
#   2. Lock-and-Trail    → close all legs, no reentry
#   3. Overall Trail SL  → adjust threshold (no close unless SL breached)
#   4. Overall Target    → close all legs, optional reentry
# ─────────────────────────────────────────────────────────────────────────────

def resolve_overall_cycle_value(base_value: float, completed_reentries: int) -> float:
    """
    Compute the effective overall SL/Target for the current reentry cycle.

    Formula: base_value × (completed_reentries + 1)

    Example: base=1000, cycle 0 → 1000, cycle 1 → 2000, cycle 2 → 3000
    This prevents cascading losses across reentry cycles.
    """
    normalized = safe_float(base_value)
    if normalized <= 0:
        return 0.0
    cycle_index = max(0, safe_int(completed_reentries)) + 1
    return round(normalized * cycle_index, 2)


def check_overall_sl_hit(
    strategy_cfg: dict,
    current_mtm: float,
    dynamic_sl_threshold: float | None,
    sl_reentry_done: int,
) -> bool:
    """
    Returns True if the trade's total MTM has hit the overall stop-loss.

    Checks both the cycle-adjusted SL and the dynamic (trail-adjusted) threshold.

    Parameters
    ----------
    strategy_cfg:         trade.strategy or trade.config
    current_mtm:          total trade PnL this tick (from compute_strategy_mtm)
    dynamic_sl_threshold: current trail-SL threshold (from algo_trades.current_overall_sl_threshold)
    sl_reentry_done:      number of SL reentry cycles already completed
    """
    osl_type, osl_val = parse_overall_sl(strategy_cfg)
    if osl_type == 'None' or not osl_val:
        return False
    effective_sl  = resolve_overall_cycle_value(osl_val, sl_reentry_done)
    dyn_sl        = safe_float(dynamic_sl_threshold, effective_sl or osl_val)
    return (effective_sl > 0 and current_mtm <= -effective_sl) or (dyn_sl > 0 and current_mtm <= -dyn_sl)


def check_overall_target_hit(
    strategy_cfg: dict,
    current_mtm: float,
    tgt_reentry_done: int,
) -> bool:
    """
    Returns True if the trade's total MTM has hit the overall target.

    Parameters
    ----------
    tgt_reentry_done: number of target reentry cycles already completed
    """
    otgt_type, otgt_val = parse_overall_tgt(strategy_cfg)
    if otgt_type == 'None' or not otgt_val:
        return False
    effective_tgt = resolve_overall_cycle_value(otgt_val, tgt_reentry_done)
    return effective_tgt > 0 and current_mtm >= effective_tgt


def compute_next_overall_trail_sl(
    strategy_cfg: dict,
    current_overall_sl: float,
    base_sl: float,
    peak_mtm: float,
) -> float:
    """
    Compute the new overall trail-SL threshold after a peak MTM update.

    Uses OverallTrailSL config from strategy_cfg.
    Returns new_threshold — if equal to current_overall_sl, no update needed.

    Called in the "elif open_leg_ids" branch after lock-and-trail is checked.
    """
    trail_type, for_every, trail_by = parse_overall_trail_sl(strategy_cfg)
    if trail_type == 'None' or for_every <= 0:
        return current_overall_sl
    new_threshold = update_overall_trail_sl(for_every, trail_by, base_sl, peak_mtm)
    return new_threshold if new_threshold < current_overall_sl else current_overall_sl


# ──────────────────────────────────────────────────────────────────────────────
# §11  BROKER-LEVEL SL / TARGET  (algo_borker_stoploss_settings)
# ──────────────────────────────────────────────────────────────────────────────
#
# Broker-level SL/Target aggregates MTM across ALL trades belonging to the
# same broker account (identified by user_id + broker + activation_mode).
#
# Total broker MTM = Σ open_trade_mtm + Σ closed_trade_realized_pnl
# (closed PnL is fetched via compute_strategy_mtm with exit_trade prices)
#
# When fired:
#   1. All open legs closed via square_off_trade()
#   2. All pending trades under the broker set to SquaredOff
#   3. algo_borker_stoploss_settings.status → 0  (disable further checks)
# ─────────────────────────────────────────────────────────────────────────────

def get_broker_sl_settings(
    db: MongoData,
    user_id: str,
    broker: str,
    activation_mode: str,
) -> dict | None:
    """
    Fetch broker-level SL settings from algo_borker_stoploss_settings.

    Query: {user_id, broker, activation_mode, status: 1}
    Returns the settings document or None if not found / disabled.

    Fields used:
      StopLoss  – total broker MTM floor  (e.g. -5000)
      Target    – total broker MTM ceiling (e.g. +3000)
      status    – 1=active, 0=disabled (set to 0 after hit)
    """
    if not user_id or not broker or not activation_mode:
        return None
    try:
        return db._db[COL_BROKER_SL].find_one({
            'user_id':         user_id,
            'broker':          broker,
            'activation_mode': activation_mode,
            'status':          1,
        })
    except Exception as exc:
        log.warning('get_broker_sl_settings error broker=%s: %s', broker, exc)
        return None


def disable_broker_sl_settings(
    db: MongoData,
    user_id: str,
    broker: str,
    activation_mode: str,
) -> None:
    """
    Set algo_borker_stoploss_settings.status = 0 after broker SL/Target fires.

    This prevents re-triggering on subsequent ticks.
    To re-enable: manually set status back to 1 in the UI/database.
    """
    try:
        db._db[COL_BROKER_SL].update_one(
            {
                'user_id':         user_id,
                'broker':          broker,
                'activation_mode': activation_mode,
                'status':          1,
            },
            {'$set': {'status': 0}},
        )
    except Exception as exc:
        log.warning('disable_broker_sl_settings error broker=%s: %s', broker, exc)


def compute_broker_group_mtm(
    db: MongoData,
    user_id: str,
    broker: str,
    trade_date: str,
    now_ts: str,
    running_trade_mtm_map: dict[str, float],
) -> tuple[float, float, float]:
    """
    Compute total MTM for all trades under a broker account on trade_date.

    Total = running_mtm (live) + closed_mtm (realized from history)

    Running trades use the pre-computed _trade_mtm_map from the current tick.
    Closed/squared-off trades call compute_strategy_mtm() to get final PnL.

    Returns (total_mtm, open_mtm, closed_mtm)

    This ensures a new position opened at 09:40 (open_mtm = -590) is combined
    with an earlier position that hit target at 09:33 (closed_mtm = +1700)
    → total = +1110, which is checked against broker Target of +1000.
    """
    open_mtm   = 0.0
    closed_mtm = 0.0
    try:
        all_trades = list(db._db[COL_ALGO_TRADES].find({
            'user_id':     user_id,
            'broker':      broker,
            'creation_ts': {'$regex': f'^{re.escape(trade_date)}'},
        }))
    except Exception as exc:
        log.warning('compute_broker_group_mtm query error: %s', exc)
        return 0.0, 0.0, 0.0

    for t in all_trades:
        tid = str(t.get('_id') or '').strip()
        if not tid:
            continue
        if tid in running_trade_mtm_map:
            open_mtm += running_trade_mtm_map[tid]
        else:
            try:
                c_mtm, _ = compute_strategy_mtm(db, tid, now_ts)
                closed_mtm += c_mtm
            except Exception as exc:
                log.warning('compute_broker_group_mtm closed PnL error trade=%s: %s', tid, exc)

    return round(open_mtm + closed_mtm, 2), round(open_mtm, 2), round(closed_mtm, 2)


def check_broker_sl_target(
    ctx: TickContext,
    running_trades: list[dict],
    strategy_map: dict[str, dict],
    open_trades_list: list[dict],
) -> tuple[list[str], dict[str, list]]:
    """
    Broker-level SL / Target / LockAndTrail check — run once per tick.

    Algorithm per broker group (user_id + broker + activation_mode):
      1. Fetch algo_borker_stoploss_settings (status=1).
      2. Compute total broker MTM = open (live) + closed (realized).
      3. Print [BROKER SL/TGT CHECK] every tick.
      4. StopLoss check  → MTM <= -StopLoss       → fire SL HIT
      5. Target check    → MTM >= Target           → fire TARGET HIT
      6. LockAndTrail    (if LockAndTrail field present in settings):
           a. Activate lock when MTM >= InstrumentMove → floor = StopLossMove
           b. OverallTrailSL (if not null):
                For every InstrumentMove increase above activation point,
                trail the floor up by StopLossMove.
           c. Print [BROKER LOCK CHECK] every tick after activation.
           d. Exit when MTM < current_lock_floor   → fire LOCK AND TRAIL HIT

    State for LockAndTrail is stored back in algo_borker_stoploss_settings:
      lock_activated    (bool)   – True after profit first crossed InstrumentMove
      current_lock_floor (float) – current exit floor (trails up, never down)
      lock_peak_mtm     (float)  – highest MTM seen since lock activated

    Returns (hit_trade_ids, hit_ltp_snapshots) for execute-orders emit.
    """
    hit_trade_ids:     list[str]       = []
    hit_ltp_snapshots: dict[str, list] = {}

    # ── Group running trades by (user_id, broker, activation_mode) ───────
    groups: dict[str, list[dict]] = {}
    for t in running_trades:
        _uid  = str(t.get('user_id') or '').strip()
        _bkr  = str(t.get('broker') or '').strip()
        _mode = str(t.get('activation_mode') or ctx.activation_mode).strip()
        if not _uid or not _bkr or not _mode:
            continue
        groups.setdefault(f'{_uid}|{_bkr}|{_mode}', []).append(t)

    for gkey, group in groups.items():
        _uid, _bkr, _mode = gkey.split('|', 2)

        # ── Broker group border — easy cross-verification in logs ─────────
        print(
            f'\n{"━" * 65}\n'
            f'  [BROKER GROUP]  broker={_bkr}  |  mode={_mode}  |  user={_uid}\n'
            f'{"━" * 65}'
        )

        settings = get_broker_sl_settings(ctx.db, _uid, _bkr, _mode)
        if not settings:
            continue

        sl_val   = settings.get('StopLoss')
        tgt_val  = settings.get('Target')
        lat_cfg  = settings.get('LockAndTrail') or {}   # LockAndTrail config
        trail_cfg = settings.get('OverallTrailSL')      # None = disabled

        # Skip if no feature is configured
        if not sl_val and not tgt_val and not lat_cfg and not trail_cfg:
            continue

        total_mtm, open_mtm, closed_mtm = compute_broker_group_mtm(
            ctx.db, _uid, _bkr, ctx.trade_date, ctx.now_ts, ctx.trade_mtm_map
        )

        # Identify currently open trades in this group
        open_trades = [
            t for t in group
            if str(t.get('_id') or '') in ctx.trade_mtm_map
        ]

        # ── OverallTrailSL standalone (Case A) ────────────────────────────
        # When LockAndTrail is null but OverallTrailSL is present:
        # For every `InstrumentMove` increase in profit → reduce StopLoss by `StopLossMove`
        # e.g. StopLoss=3700, InstrumentMove=100, StopLossMove=50
        #      profit 100 → effective_sl=3650, profit 200 → 3600, etc.
        effective_sl_val = sl_val  # default: use original StopLoss unchanged
        _settings_id = settings.get('_id')

        if trail_cfg and not lat_cfg and sl_val:
            _sl_trail_every = safe_float((trail_cfg or {}).get('InstrumentMove') or 0)
            _sl_trail_by    = safe_float((trail_cfg or {}).get('StopLossMove') or 0)
            if _sl_trail_every > 0 and _sl_trail_by > 0:
                # Reset sl trail state when config changes or new run date
                _sl_sig = f"{ctx.trade_date}|{sl_val}|{_sl_trail_every}|{_sl_trail_by}"
                _stored_sl_sig = settings.get('sl_settings_sig') or ''
                if _stored_sl_sig != _sl_sig:
                    _sl_peak_mtm     = 0.0
                    effective_sl_val = sl_val
                    try:
                        ctx.db._db[COL_BROKER_SL].update_one(
                            {'_id': _settings_id},
                            {'$set': {
                                'sl_settings_sig': _sl_sig,
                                'sl_peak_mtm':     0.0,
                                'effective_sl':    sl_val,
                            }},
                        )
                    except Exception as exc:
                        log.warning('[SL TRAIL STATE RESET] save error: %s', exc)
                    print('[SL TRAIL STATE RESET]', {
                        'timestamp': ctx.now_ts, 'broker': _bkr, 'mode': _mode,
                        'reason': 'settings changed or new run date',
                        'old_sig': _stored_sl_sig, 'new_sig': _sl_sig,
                    })
                else:
                    _sl_peak_mtm = safe_float(settings.get('sl_peak_mtm') or 0)
                _sl_state_upd: dict = {}
                if total_mtm > _sl_peak_mtm:
                    _sl_peak_mtm = total_mtm
                    _sl_state_upd['sl_peak_mtm'] = total_mtm
                _steps    = int(_sl_peak_mtm / _sl_trail_every) if _sl_peak_mtm >= _sl_trail_every else 0
                _new_eff  = round(safe_float(sl_val) - _steps * _sl_trail_by, 2)
                _prev_eff = safe_float(settings.get('effective_sl') or sl_val)
                # Only tighten the SL (move it lower), never loosen it
                effective_sl_val = min(_new_eff, _prev_eff)
                if effective_sl_val < _prev_eff:
                    _sl_state_upd['effective_sl'] = effective_sl_val
                    print('[BROKER SL TRAIL UPDATE]', {
                        'timestamp':   ctx.now_ts,
                        'broker':      _bkr,
                        'mode':        _mode,
                        'broker_mtm':  total_mtm,
                        'original_sl': sl_val,
                        'old_eff_sl':  _prev_eff,
                        'new_eff_sl':  effective_sl_val,
                        'sl_peak_mtm': _sl_peak_mtm,
                        'trail_steps': _steps,
                        'trail_every': _sl_trail_every,
                        'trail_by':    _sl_trail_by,
                    })
                if _sl_state_upd and _settings_id is not None:
                    try:
                        ctx.db._db[COL_BROKER_SL].update_one(
                            {'_id': _settings_id}, {'$set': _sl_state_upd}
                        )
                    except Exception as exc:
                        log.warning('[SL TRAIL STATE] save error: %s', exc)
                print('[BROKER SL TRAIL CHECK]', {
                    'timestamp':     ctx.now_ts,
                    'broker':        _bkr,
                    'mode':          _mode,
                    'broker_mtm':    total_mtm,
                    'original_sl':   sl_val,
                    'effective_sl':  effective_sl_val,
                    'sl_peak_mtm':   _sl_peak_mtm,
                    'trail_steps':   _steps,
                    'trail_every':   _sl_trail_every,
                    'trail_by':      _sl_trail_by,
                })

        sl_rem  = round(total_mtm + safe_float(effective_sl_val), 2)  if effective_sl_val  else None
        tgt_rem = round(safe_float(tgt_val) - total_mtm, 2) if tgt_val else None
        sl_status  = 'HIT' if (effective_sl_val and total_mtm <= -safe_float(effective_sl_val)) else 'active'
        tgt_status = 'HIT' if (tgt_val and total_mtm >= safe_float(tgt_val)) else 'active'

        # ── Print SL/TGT check every tick ────────────────────────────────
        print('[BROKER SL/TGT CHECK]', {
            'timestamp':     ctx.now_ts,
            'user_id':       _uid,
            'broker':        _bkr,
            'mode':          _mode,
            'open_mtm':      open_mtm,
            'closed_mtm':    closed_mtm,
            'broker_mtm':    total_mtm,
            'broker_sl':     effective_sl_val,
            'original_sl':   sl_val if effective_sl_val != sl_val else None,
            'sl_remaining':  sl_rem,
            'sl_status':     sl_status,
            'broker_target': tgt_val,
            'tgt_remaining': tgt_rem,
            'tgt_status':    tgt_status,
            'open_trades':   len(open_trades),
            'total_trades':  len(group),
        })

        strategy_groups: dict[str, list[dict]] = {}
        for _t in group:
            _esid = str(_t.get('executed_strategy_id') or _t.get('strategy_id') or '').strip()
            if _esid:
                strategy_groups.setdefault(_esid, []).append(_t)

        for _esid, _sgroup in strategy_groups.items():
            _sample = _sgroup[0] if _sgroup else {}
            _sg_open_trd = [
                _t for _t in _sgroup
                if str(_t.get('_id') or '').strip() in ctx.trade_mtm_map
            ]
            _sg_open_mtm = round(sum(
                safe_float(ctx.trade_mtm_map.get(str(_t.get('_id') or '').strip(), 0.0))
                for _t in _sg_open_trd
            ), 2)
            _sg_closed = 0.0
            for _t in _sgroup:
                _tid = str(_t.get('_id') or '').strip()
                if not _tid or _tid in ctx.trade_mtm_map:
                    continue
                try:
                    _sg_closed += safe_float(compute_strategy_mtm(ctx.db, _tid, ctx.now_ts)[0])
                except Exception as exc:
                    log.warning('strategy-group MTM closed PnL error trade=%s: %s', _tid, exc)
            _sg_closed = round(_sg_closed, 2)
            _sg_mtm = round(_sg_open_mtm + _sg_closed, 2)
            _runtime_strat_sl = safe_float(_sample.get('current_overall_sl_threshold') or 0) or None
            _sample_cfg = _sample.get('strategy') or _sample.get('config') or {}
            _ssl_val = safe_float(parse_overall_sl(_sample_cfg)[1])
            _stgt_val = safe_float(parse_overall_tgt(_sample_cfg)[1]) or None
            _effective_sl = _runtime_strat_sl or _ssl_val or None
            _ssl_rem = round(_sg_mtm + float(_effective_sl or 0), 2) if _effective_sl else None
            _stgt_rem = round(float(_stgt_val or 0) - _sg_mtm, 2) if _stgt_val else None
            _ssl_status = 'HIT' if (_effective_sl and _sg_mtm <= -float(_effective_sl)) else 'active'
            _stgt_status = 'HIT' if (_stgt_val and _sg_mtm >= float(_stgt_val)) else 'active'

            print(f'[STRAT SL/TGT CHECK - {_bkr}]', {
                'timestamp': ctx.now_ts,
                'strat': _esid,
                'user_id': _uid,
                'mode': _mode,
                'open_mtm': _sg_open_mtm,
                'closed_mtm': _sg_closed,
                'strat_mtm': _sg_mtm,
                'strat_sl': _effective_sl,
                'runtime_current_overall_sl_threshold': _runtime_strat_sl,
                'original_sl': _ssl_val if (_effective_sl and _ssl_val and _effective_sl != _ssl_val) else None,
                'sl_remaining': _ssl_rem,
                'sl_status': _ssl_status,
                'strat_target': _stgt_val,
                'tgt_remaining': _stgt_rem,
                'tgt_status': _stgt_status,
                'open_trades': len(_sg_open_trd),
                'total_trades': len(_sgroup),
            })

        # ── Shared fire helper ────────────────────────────────────────────
        def _fire(reason: str, lock_floor: float = 0.0) -> None:
            """Close all open trades + disable settings + collect hit IDs."""
            trade_ids = [str(t.get('_id') or '') for t in open_trades]
            print(f'[BROKER {reason}]', {
                'timestamp':           ctx.now_ts,
                'broker':              _bkr,
                'user_id':             _uid,
                'mode':                _mode,
                'broker_mtm':          total_mtm,
                'lock_floor':          lock_floor if lock_floor else None,
                'squaring_off_trades': trade_ids,
            })
            try:
                from features.notification_manager import upsert_broker_feature_status  # type: ignore
                _feature_name = 'broker_sl' if 'SL' in str(reason or '').upper() else 'broker_target'
                _trigger_value = safe_float(effective_sl_val) if _feature_name == 'broker_sl' else safe_float(tgt_val)
                upsert_broker_feature_status(
                    ctx.db._db,
                    trade=(group[0] if group else {}),
                    user_id=_uid,
                    broker=_bkr,
                    activation_mode=_mode,
                    feature=_feature_name,
                    trigger_value=_trigger_value,
                    current_mtm=total_mtm,
                    timestamp=ctx.now_ts,
                )
            except Exception as exc:
                log.warning('[BROKER %s] feature status upsert error: %s', reason, exc)
            # Step 1 — close open-leg trades at current LTP
            for t in open_trades:
                fresh = ctx.db._db[COL_ALGO_TRADES].find_one({'_id': t['_id']}) or t
                square_off_trade(ctx.db, fresh, ctx.now_ts, market_cache=ctx.market_cache)
                print(f'  [BROKER {reason}] trade={str(t.get("_id") or "")[:16]} closed')
            # Step 2 — bulk-close all pending/import trades under this broker
            try:
                res = ctx.db._db[COL_ALGO_TRADES].update_many(
                    {'broker': _bkr, 'user_id': _uid, 'activation_mode': _mode, 'active_on_server': True},
                    {'$set': {'active_on_server': False, 'status': SQUARED_OFF_STATUS, 'trade_status': TRADE_STATUS_SQUARED_OFF}},
                )
                print(f'  [BROKER {reason}] group close matched={res.matched_count} modified={res.modified_count}')
            except Exception as exc:
                log.warning('[BROKER %s] bulk close error: %s', reason, exc)
            # Step 3 — disable settings (status → 0) so next tick skips
            disable_broker_sl_settings(ctx.db, _uid, _bkr, _mode)
            print(f'  [BROKER {reason}] settings disabled user={_uid[:8]}.. broker={_bkr[:8]}..')
            ctx.actions_taken.append(
                f'broker_{reason.lower().replace(" ", "_")} broker={_bkr} mtm={total_mtm}'
            )
            # Collect hit IDs + LTP snapshots for execute-orders emit
            for t in group:
                tid = str(t.get('_id') or '').strip()
                if tid and tid not in hit_trade_ids:
                    hit_trade_ids.append(tid)
                    hit_ltp_snapshots[tid] = list(
                        (strategy_map.get(tid) or {}).get('open_positions') or []
                    )

        # ── 1. StopLoss check ─────────────────────────────────────────────
        # Uses effective_sl_val which may be trailed lower than original sl_val
        if effective_sl_val and total_mtm <= -safe_float(effective_sl_val):
            _fire('SL HIT')
            continue

        # ── 2. Target check ───────────────────────────────────────────────
        if tgt_val and total_mtm >= safe_float(tgt_val):
            _fire('TARGET HIT')
            continue

        # ── 3. LockAndTrail check ─────────────────────────────────────────
        # Only runs if LockAndTrail config is present in settings.
        if not lat_cfg:
            continue

        lat_instrument_move = safe_float(lat_cfg.get('InstrumentMove') or 0)  # e.g. 600
        lat_stop_loss_move  = safe_float(lat_cfg.get('StopLossMove') or 0)    # e.g. 400
        if not lat_instrument_move or not lat_stop_loss_move:
            continue

        # OverallTrailSL — None means disabled (only plain lock, no trail)
        trail_every = safe_float((trail_cfg or {}).get('InstrumentMove') or 0)  # e.g. 100
        trail_by    = safe_float((trail_cfg or {}).get('StopLossMove') or 0)    # e.g. 20
        has_trail   = bool(trail_cfg and trail_every > 0 and trail_by > 0)

        _settings_id  = settings.get('_id')
        _state_updates: dict = {}

        # ── Settings signature: reset state when user changes config mid-run ──
        # Covers both (a) new backtest date and (b) live settings edit.
        # Fingerprint = trade_date + all config values that affect lock logic.
        _lock_sig = (
            f"{ctx.trade_date}|"
            f"{lat_instrument_move}|{lat_stop_loss_move}|"
            f"{trail_every}|{trail_by}|"
            f"{sl_val}|{tgt_val}"
        )
        _stored_lock_sig    = settings.get('lock_settings_sig') or ''
        _was_lock_activated = bool(settings.get('lock_activated') or False)
        if _stored_lock_sig != _lock_sig:
            if _was_lock_activated:
                # Lock already activated — NEVER reset back to False.
                # Only update the signature; preserve activation state and floor.
                lock_activated     = True
                current_lock_floor = safe_float(settings.get('current_lock_floor') or 0)
                lock_peak_mtm      = safe_float(settings.get('lock_peak_mtm') or 0)
                _state_updates['lock_settings_sig'] = _lock_sig
                print('[LOCK SIG UPDATED - ACTIVATION PRESERVED]', {
                    'timestamp':   ctx.now_ts,
                    'broker':      _bkr,
                    'mode':        _mode,
                    'reason':      'settings changed but lock already activated — not resetting',
                    'lock_floor':  current_lock_floor,
                    'old_sig':     _stored_lock_sig,
                    'new_sig':     _lock_sig,
                })
            else:
                # Lock not yet activated — safe to reset for the new config
                lock_activated     = False
                current_lock_floor = 0.0
                lock_peak_mtm      = 0.0
                _state_updates.update({
                    'lock_settings_sig':  _lock_sig,
                    'lock_activated':     False,
                    'current_lock_floor': 0.0,
                    'lock_peak_mtm':      0.0,
                })
                print('[LOCK STATE RESET]', {
                    'timestamp':  ctx.now_ts,
                    'broker':     _bkr,
                    'mode':       _mode,
                    'reason':     'settings changed or new run date',
                    'old_sig':    _stored_lock_sig,
                    'new_sig':    _lock_sig,
                })
        else:
            lock_activated     = _was_lock_activated
            current_lock_floor = safe_float(settings.get('current_lock_floor') or 0)
            lock_peak_mtm      = safe_float(settings.get('lock_peak_mtm') or 0)

            # Safety guard: lock_floor must never decrease once set.
            # If DB stored a stale/missing floor, reconstruct from peak.
            if lock_activated and lock_peak_mtm > lat_instrument_move:
                if has_trail and trail_every > 0:
                    _pa = lock_peak_mtm - lat_instrument_move
                    _steps_so_far = int(_pa / trail_every) if _pa >= trail_every else 0
                    _implied_floor = round(lat_stop_loss_move + _steps_so_far * trail_by, 2)
                else:
                    _implied_floor = lat_stop_loss_move
                if _implied_floor > current_lock_floor:
                    current_lock_floor = _implied_floor
                    _state_updates['current_lock_floor'] = _implied_floor

        # ── 3a. Activate lock when MTM first crosses InstrumentMove ──────
        if not lock_activated:
            if total_mtm >= lat_instrument_move:
                lock_activated     = True
                current_lock_floor = lat_stop_loss_move
                lock_peak_mtm      = total_mtm
                _state_updates.update({
                    'lock_activated':      True,
                    'current_lock_floor':  lat_stop_loss_move,
                    'lock_peak_mtm':       total_mtm,
                    'lock_activated_at':   ctx.now_ts,
                    'lock_activation_mtm': total_mtm,
                })
                print('[LOCK ACTIVATED]', {
                    'timestamp':         ctx.now_ts,
                    'broker':            _bkr,
                    'mode':              _mode,
                    'broker_mtm':        total_mtm,
                    'instrument_move':   lat_instrument_move,
                    'lock_floor_set_to': lat_stop_loss_move,
                    'trail_enabled':     has_trail,
                })
            else:
                # Lock not yet activated — print full settings snapshot for cross-verification
                print('[BROKER LOCK CHECK]', {
                    'timestamp':         ctx.now_ts,
                    'broker':            _bkr,
                    'mode':              _mode,
                    'broker_mtm':        total_mtm,
                    'lock_activated':    False,
                    # ── LockAndTrail config (from DB) ──
                    'activate_at':       lat_instrument_move,
                    'lock_profit':       lat_stop_loss_move,
                    'remaining_to_lock': round(lat_instrument_move - total_mtm, 2),
                    # ── OverallTrailSL config (from DB) ──
                    'trail_enabled':     has_trail,
                    'trail_every':       trail_every if has_trail else None,
                    'trail_by':          trail_by    if has_trail else None,
                    # ── StopLoss / Target (from DB) ──
                    'broker_sl':         sl_val,
                    'broker_target':     tgt_val,
                    # ── Signature: changes when settings edited mid-run ──
                    'settings_sig':      _lock_sig,
                })
                continue

        # ── 3b. Update peak MTM ───────────────────────────────────────────
        if total_mtm > lock_peak_mtm:
            lock_peak_mtm = total_mtm
            _state_updates['lock_peak_mtm'] = total_mtm

        # ── 3c. Trail lock floor if OverallTrailSL is configured ─────────
        if has_trail:
            # For every `trail_every` increase above the activation point,
            # raise the floor by `trail_by`.
            # Formula: new_floor = stop_loss_move + floor((peak - instrument_move) / trail_every) × trail_by
            profit_above = lock_peak_mtm - lat_instrument_move
            trail_steps  = int(profit_above / trail_every) if profit_above >= trail_every else 0
            new_floor    = round(lat_stop_loss_move + trail_steps * trail_by, 2)
            if new_floor > current_lock_floor:
                print('[LOCK TRAIL UPDATE]', {
                    'timestamp':       ctx.now_ts,
                    'broker':          _bkr,
                    'mode':            _mode,
                    'broker_mtm':      total_mtm,
                    'lock_peak_mtm':   lock_peak_mtm,
                    'old_lock_floor':  current_lock_floor,
                    'new_lock_floor':  new_floor,
                    'trail_steps':     trail_steps,
                    'trail_every':     trail_every,
                    'trail_by':        trail_by,
                })
                current_lock_floor = new_floor
                _state_updates['current_lock_floor'] = new_floor

        # ── Persist state changes ─────────────────────────────────────────
        # lock_peak_mtm and current_lock_floor use $max so they are
        # monotonically non-decreasing — stale DB reads can never roll them back.
        if _state_updates and _settings_id is not None:
            try:
                _max_fields: dict = {}
                _set_fields: dict  = {}
                for _k, _v in _state_updates.items():
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
                    ctx.db._db[COL_BROKER_SL].update_one(
                        {'_id': _settings_id},
                        _mongo_op,
                    )
            except Exception as exc:
                log.warning('[LOCK STATE] save error: %s', exc)

        # ── Print lock status every tick ──────────────────────────────────
        print('[BROKER LOCK CHECK]', {
            'timestamp':         ctx.now_ts,
            'broker':            _bkr,
            'mode':              _mode,
            'broker_mtm':        total_mtm,
            'lock_activated':    lock_activated,
            # ── Lock state ──
            'lock_floor':        current_lock_floor,
            'lock_peak_mtm':     lock_peak_mtm,
            'remaining_to_exit': round(total_mtm - current_lock_floor, 2),
            # ── LockAndTrail config (from DB) ──
            'activate_at':       lat_instrument_move,
            'lock_profit':       lat_stop_loss_move,
            # ── OverallTrailSL config (from DB) ──
            'trail_enabled':     has_trail,
            'trail_every':       trail_every if has_trail else None,
            'trail_by':          trail_by    if has_trail else None,
            # ── StopLoss / Target (from DB) ──
            'broker_sl':         sl_val,
            'broker_target':     tgt_val,
            # ── Signature: changes when settings edited mid-run ──
            'settings_sig':      _lock_sig,
        })

        # ── 3d. Exit when MTM drops below lock floor ──────────────────────
        if total_mtm < current_lock_floor:
            _fire('LOCK AND TRAIL HIT', lock_floor=current_lock_floor)

    return hit_trade_ids, hit_ltp_snapshots


# ──────────────────────────────────────────────────────────────────────────────
# §12  SIMPLE MOMENTUM ENGINE
# ──────────────────────────────────────────────────────────────────────────────
#
# Simple Momentum: a leg only enters after its underlying price moves by
# a configured amount from a reference price.
#
# Types:
#   PercentageUp    – trigger when price rises N% from base
#   PercentageDown  – trigger when price falls N% from base
#   PointsUp        – trigger when price rises N points from base
#   PointsDown      – trigger when price falls N points from base
#
# Flow:
#   1. Leg is created as pending (is_lazy=True).
#   2. On first tick: set base_price = current spot/option price.
#   3. Every tick: check if current_price reached target_price.
#   4. On trigger: proceed with normal entry (resolve_pending_leg_entry).
# ─────────────────────────────────────────────────────────────────────────────

def has_momentum_config(leg_cfg: dict) -> bool:
    """
    Returns True if the leg has a LegMomentum config with Type != 'None'
    and a positive Value.

    Used to decide whether to arm momentum tracking for a pending leg.
    """
    momentum = (leg_cfg or {}).get('LegMomentum') or {}
    if not isinstance(momentum, dict):
        return False
    m_type  = str(momentum.get('Type') or 'None').strip()
    m_value = safe_float(momentum.get('Value') or 0)
    return m_type != 'None' and m_value > 0


def compute_momentum_target(
    momentum_type: str,
    base_price: float,
    momentum_value: float,
) -> float | None:
    """
    Compute the momentum trigger price from base_price.

    momentum_type:  'PercentageUp' | 'PercentageDown' | 'PointsUp' | 'PointsDown'
    momentum_value: percentage or points
    base_price:     price when momentum tracking was armed

    Returns the target price, or None if inputs are invalid.
    """
    if not base_price or not momentum_value:
        return None
    m = str(momentum_type or '').strip()
    if 'PercentageUp' in m:
        return round(base_price * (1 + momentum_value / 100), 2)
    if 'PercentageDown' in m:
        return round(base_price * (1 - momentum_value / 100), 2)
    if 'PointsUp' in m:
        return round(base_price + momentum_value, 2)
    if 'PointsDown' in m:
        return round(base_price - momentum_value, 2)
    return None


def is_momentum_triggered(
    momentum_type: str,
    current_price: float,
    target_price: float,
) -> bool:
    """
    Returns True if current_price has reached the momentum target.

    Direction is inferred from momentum_type:
      Up variants   → triggered when current_price >= target_price
      Down variants → triggered when current_price <= target_price
    """
    if not current_price or not target_price:
        return False
    m = str(momentum_type or '').strip()
    if 'Up' in m:
        return current_price >= target_price
    if 'Down' in m:
        return current_price <= target_price
    return False


# ──────────────────────────────────────────────────────────────────────────────
# §13  RE-ENTRY ENGINE
# ──────────────────────────────────────────────────────────────────────────────
#
# After a leg-level or overall SL/Target fires, the reentry engine may open
# new positions according to the strategy's reentry configuration.
#
# Reentry types:
#   NextLeg      – push a pre-configured "next leg" (lazy/pending)
#   Immediate    – re-enter same strike/expiry immediately
#   AtCost       – wait until underlying returns to entry cost before re-entering
#   LikeOriginal – re-enter N times with original leg parameters
#
# Overall reentry types (after overall SL/Target):
#   Same as above but applied to ALL trade legs simultaneously.
# ─────────────────────────────────────────────────────────────────────────────

def handle_leg_reentry(
    db: MongoData,
    trade: dict,
    leg: dict,
    leg_cfg: dict,
    exit_event: str,
    now_ts: str,
) -> str | None:
    """
    Process reentry config after a leg-level SL or Target hit.

    Parameters
    ----------
    exit_event:  'stoploss' | 'target'

    Returns a human-readable result string for the actions_taken log,
    or None if reentry is not configured / already exhausted.

    Called immediately after close_leg_in_db() when SL/TP fires.
    """
    reentry_cfg = (
        get_reentry_sl_config(leg_cfg) if exit_event == 'stoploss'
        else get_reentry_tp_config(leg_cfg)
    )
    if not reentry_cfg:
        return None

    trade_id = str(trade.get('_id') or '')
    leg_id   = str(leg.get('id') or '')
    action   = build_reentry_action(reentry_cfg, leg, trade)
    if not action:
        return None

    re_type  = str(action.reentry_type or '')
    re_kind  = (
        'lazy'         if 'NextLeg'   in re_type else
        'immediate'    if 'Immediate' in re_type else
        'at_cost'      if 'AtCost'    in re_type else
        'like_original'
    )

    if re_kind == 'lazy':
        # Push the pre-configured "next leg" (NextLegRef) as a pending leg
        next_leg_id  = str((reentry_cfg.get('Value') or {}).get('NextLegRef') or '')
        next_leg_cfg = (resolve_trade_leg_configs(trade) or {}).get(next_leg_id) or {}
        if next_leg_id and next_leg_cfg:
            new_leg = build_pending_leg(next_leg_id, next_leg_cfg, trade, now_ts,
                                        triggered_by=leg_id, leg_type=f'{leg_id}-lazyleg_1')
            push_new_leg_in_db(db, trade_id, new_leg)
    elif re_kind in ('immediate', 'at_cost', 'like_original'):
        # Re-queue original leg config as a fresh pending leg
        new_leg_id = f'{leg_id}-reentry-{now_ts.replace(":", "").replace("-", "").replace("T", "")[:14]}'
        new_leg = build_pending_leg(new_leg_id, leg_cfg, trade, now_ts,
                                    triggered_by=leg_id, leg_type=f'{leg_id}-reentry')
        push_new_leg_in_db(db, trade_id, new_leg)

    return f'reentry({re_kind}) queued after {exit_event} on leg={leg_id}'


def requeue_all_legs_for_overall_reentry(
    db: MongoData,
    trade: dict,
    trade_id: str,
    strategy_cfg: dict,
    now_ts: str,
    reentry_type: str = '',
) -> list[str]:
    """
    Re-queue ALL original leg configs as new pending legs for overall reentry.

    Called after overall SL or overall Target fires and the trade is configured
    for reentry (parse_overall_reentry_sl / parse_overall_reentry_tgt returns
    count > 0).

    reentry_type: ore_type / ort_type from parse_overall_reentry_sl/tgt
                  (e.g. 'LikeOriginal'). When 'LikeOriginal', new legs get
                  reentry_type='LikeOriginal' to match leg-level behaviour.

    Returns list of new leg_ids pushed.
    """
    original_cfgs = (
        strategy_cfg.get('ListOfLegConfigs')
        or list((resolve_trade_leg_configs(trade) or {}).values())
    )
    new_ids: list[str] = []
    ts_sfx = now_ts.replace(':', '').replace('T', '').replace('-', '')[:14]
    existing = {str(l.get('id') or '') for l in (trade.get('legs') or []) if isinstance(l, dict)}
    is_like_original = 'LikeOriginal' in reentry_type

    def _leg_has_momentum(leg_cfg: dict) -> bool:
        mom = leg_cfg.get('LegMomentum') or {}
        mom_type = str(mom.get('Type') or 'None')
        mom_val = safe_float(mom.get('Value'))
        return 'None' not in mom_type and mom_val > 0

    for cfg in original_cfgs:
        if not isinstance(cfg, dict):
            continue
        orig_id = str(cfg.get('id') or '')
        if not orig_id:
            continue
        new_id   = f'{orig_id}-ore-{ts_sfx}'
        leg_type = f'{orig_id}-overall_reentry'
        if new_id in existing:
            continue

        if is_like_original and _leg_has_momentum(cfg):
            # LikeOriginal + LegMomentum → same method as NextLeg+momentum:
            # queue to algo_leg_feature_status, momentum_pending flow enters it
            mom      = cfg.get('LegMomentum') or {}
            option_raw = str(cfg.get('InstrumentKind') or '')
            option_type = option_raw.split('.')[-1] if '.' in option_raw else option_raw
            mom_doc = {
                'trade_id':        trade_id,
                'leg_id':          new_id,
                'lazy_leg_ref':    orig_id,
                'parent_leg_id':   orig_id,
                'feature':         'momentum_pending',
                'enabled':         True,
                'status':          'active',
                'option':          option_type,
                'expiry_kind':     str(cfg.get('ExpiryKind') or 'ExpiryType.Weekly'),
                'strike_parameter': str(cfg.get('StrikeParameter') or 'StrikeType.ATM'),
                'entry_kind':      str(cfg.get('EntryType') or ''),
                'position':        str(cfg.get('PositionType') or ''),
                'lot_config_value': max(1, int((cfg.get('LotConfig') or {}).get('Value') or 1)),
                'momentum_type':   str(mom.get('Type') or 'None'),
                'momentum_value':  safe_float(mom.get('Value')),
                'triggered_by':    f'overall_{orig_id}',
                'leg_type':        leg_type,
                'queued_at':       now_ts,
                'is_reentered_leg': True,
                'reentry_type':    'LikeOriginal',
            }
            try:
                db._db['algo_leg_feature_status'].insert_one(mom_doc)
                new_ids.append(new_id)
            except Exception as _e:
                log.warning('overall reentry momentum_pending insert error leg=%s: %s', orig_id, _e)
            continue

        new_leg = build_pending_leg(new_id, cfg, trade, now_ts,
                                    triggered_by=f'overall_{orig_id}',
                                    leg_type=leg_type)
        new_leg['is_reentered_leg'] = True
        new_leg['lazy_leg_ref']     = orig_id
        new_leg['parent_leg_id']    = orig_id
        if is_like_original:
            # LikeOriginal, no LegMomentum → pending leg, enters on next tick
            new_leg['reentry_type'] = 'LikeOriginal'
        else:
            # Immediate (and others): bypass momentum gate, enter directly
            new_leg['skip_momentum_check'] = True
        push_new_leg_in_db(db, trade_id, new_leg)
        new_ids.append(new_id)

    return new_ids


# ──────────────────────────────────────────────────────────────────────────────
# §14  LAZY LEG ENGINE  (pending legs)
# ──────────────────────────────────────────────────────────────────────────────
#
# A "lazy leg" is a pending leg with entry_trade=None.
# It waits in algo_trades.legs[] until entry conditions are met on some tick.
#
# Entry conditions (checked in order):
#   1. entry_time  – not before the strategy's configured entry_time
#   2. Expiry      – resolve_leg_expiry must succeed
#   3. Strike      – resolve_leg_strike must succeed
#   4. Chain price – must be > 0 (contract must exist in option_chain)
#   5. Momentum    – if LegMomentum configured, trigger price must be reached
# ─────────────────────────────────────────────────────────────────────────────

def queue_original_legs_if_needed(
    db: MongoData,
    trade: dict,
    now_ts: str,
) -> bool:
    """
    On the first tick after a trade goes Live_Running, push all original
    leg configs as pending legs if algo_trades.legs is empty.

    This bootstraps the lazy leg system — after this call, the normal
    pending-leg entry loop (§17) picks them up.

    Returns True if legs were queued, False if already present or no configs.
    """
    existing_legs = [l for l in (trade.get('legs') or []) if isinstance(l, dict)]
    if existing_legs:
        return False

    strategy_cfg = trade.get('strategy') or trade.get('config') or {}
    leg_configs  = strategy_cfg.get('ListOfLegConfigs') or []
    if not leg_configs:
        return False

    trade_id = str(trade.get('_id') or '')
    queued   = 0
    for cfg in leg_configs:
        if not isinstance(cfg, dict):
            continue
        leg_id = str(cfg.get('id') or '').strip()
        if not leg_id:
            continue
        new_leg = build_pending_leg(leg_id, cfg, trade, now_ts)
        if push_new_leg_in_db(db, trade_id, new_leg):
            queued += 1

    if queued:
        log.info('queue_original_legs trade=%s queued=%d', trade_id, queued)
    return queued > 0


def get_pending_legs(trade: dict) -> list[tuple[int, dict]]:
    """
    Return list of (index, leg_dict) for all pending (unentried) legs.

    A leg is pending if entry_trade is None/missing AND status == OPEN.
    Returns index so callers can update legs[index] by position.
    """
    result = []
    for idx, leg in enumerate(trade.get('legs') or []):
        if not isinstance(leg, dict):
            continue
        if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
            continue
        if isinstance(leg.get('entry_trade'), dict):
            continue   # already entered
        result.append((idx, leg))
    return result


def _is_live_like_activation_mode(mode: str | None) -> bool:
    normalized = str(mode or '').strip().lower()
    return normalized in {'live', 'fast-forward', 'forward-test'}


def build_original_pending_legs_snapshot(
    db: MongoData,
    trade: dict,
    now_ts: str,
) -> list[tuple[int, dict]]:
    """
    Build in-memory pending legs from strategy.ListOfLegConfigs without writing
    them into algo_trades.legs.

    Used by live / fast-forward entry flow so retries do not persist pending
    legs in the trade document. Already-entered legs present in
    algo_trade_positions_history are skipped.
    """
    trade_id = str(trade.get('_id') or '').strip()
    if not trade_id:
        return []

    strategy_cfg = trade.get('strategy') or trade.get('config') or {}
    leg_configs = strategy_cfg.get('ListOfLegConfigs') or []
    if not leg_configs:
        return []

    existing_dict_leg_ids = {
        str(leg.get('id') or '').strip()
        for leg in (trade.get('legs') or [])
        if isinstance(leg, dict) and str(leg.get('id') or '').strip()
    }

    history_leg_ids: set[str] = set()
    try:
        for doc in db._db[COL_POSITIONS_HIST].find({'trade_id': trade_id}, {'leg_id': 1, 'id': 1}):
            history_leg_id = str(doc.get('leg_id') or doc.get('id') or '').strip()
            if history_leg_id:
                history_leg_ids.add(history_leg_id)
    except Exception as exc:
        log.warning('build_original_pending_legs_snapshot history lookup error trade=%s: %s', trade_id, exc)

    pending: list[tuple[int, dict]] = []
    for idx, cfg in enumerate(leg_configs):
        if not isinstance(cfg, dict):
            continue
        leg_id = str(cfg.get('id') or '').strip()
        if not leg_id:
            continue
        if leg_id in existing_dict_leg_ids or leg_id in history_leg_ids:
            continue
        pending.append((idx, build_pending_leg(leg_id, cfg, trade, now_ts)))

    return pending


def get_open_legs(trade: dict, history_docs: list[dict] | None = None) -> list[dict]:
    """
    Return all open (entered, not yet exited) legs for a trade.

    Merges legs from algo_trades.legs (dict entries) with any legs that
    have been moved to algo_trade_positions_history (string refs).

    If history_docs is None the caller must pass them; this function
    does NOT query the DB to keep it pure.
    """
    leg_ids_seen: set[str] = set()
    result: list[dict] = []

    for leg in (trade.get('legs') or []):
        if not isinstance(leg, dict):
            continue
        if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
            continue
        if not isinstance(leg.get('entry_trade'), dict):
            continue  # pending, not entered
        leg_id = str(leg.get('id') or '')
        if leg_id:
            leg_ids_seen.add(leg_id)
        result.append(leg)

    for hdoc in (history_docs or []):
        if int(hdoc.get('status') or 0) != OPEN_LEG_STATUS:
            continue
        h_leg_id = str(hdoc.get('leg_id') or hdoc.get('id') or '')
        if h_leg_id in leg_ids_seen:
            continue
        result.append(hdoc)

    return result


# ──────────────────────────────────────────────────────────────────────────────
# §15  SQUARE-OFF HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def mark_trade_squared_off(db: MongoData, trade_id: str) -> None:
    """
    Set algo_trades status to SquaredOff when all legs are confirmed closed.

    Sets: status=SquaredOff, trade_status=2, active_on_server=False.
    Does NOT close any legs — call close_leg_in_db() for each leg first.
    """
    try:
        db._db[COL_ALGO_TRADES].update_one(
            {'_id': trade_id},
            {'$set': {
                'active_on_server': False,
                'status':           SQUARED_OFF_STATUS,
                'trade_status':     TRADE_STATUS_SQUARED_OFF,
            }},
        )
    except Exception as exc:
        log.error('mark_trade_squared_off error trade=%s: %s', trade_id, exc)


def square_off_trade(
    db: MongoData,
    trade_rec: dict,
    exit_timestamp: str,
    *,
    market_cache: dict | None = None,
) -> bool:
    """
    Square off ALL open legs in a trade at current LTP.

    Algorithm:
      1. Collect open legs from algo_trades.legs[] (dict entries).
      2. Collect open legs from algo_trade_positions_history (string refs).
      3. For each open leg: fetch chain doc at exit_timestamp, get LTP.
      4. Call close_leg_in_db() at that LTP with reason='squared_off'.
      5. If all legs closed → call mark_trade_squared_off().

    Returns True if trade was marked SquaredOff, False otherwise.

    Used by:
      - Manual square-off from frontend (squared-off socket message)
      - Overall SL/Target hit → _bt_close_remaining()
      - Broker-level SL/Target hit → check_broker_sl_target()
    """
    t_id = str((trade_rec or {}).get('_id') or '').strip()
    if not t_id:
        return False

    algo_col   = db._db[COL_ALGO_TRADES]
    history_col = db._db[COL_POSITIONS_HIST]
    underlying = str(
        trade_rec.get('ticker')
        or (trade_rec.get('config') or {}).get('Ticker')
        or ''
    )
    all_open_closed = True

    # ── Close legs stored as dicts in algo_trades.legs[] ─────────────────
    for idx, leg in enumerate(trade_rec.get('legs') or []):
        if not isinstance(leg, dict):
            continue
        if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
            continue
        if not isinstance(leg.get('entry_trade'), dict):
            continue  # pending, not yet entered

        leg_id      = str(leg.get('id') or '')
        token       = str(leg.get('token') or '')
        expiry      = normalize_expiry(str(leg.get('expiry_date') or ''))
        strike      = leg.get('strike')
        option_type = str(leg.get('option') or '')

        chain_doc = None
        if token:
            chain_doc = get_chain_by_token_at_time(db, token, exit_timestamp, market_cache)
        if not chain_doc and underlying and expiry and strike and option_type:
            chain_doc = get_chain_at_time(db, underlying, expiry, strike, option_type, exit_timestamp, market_cache)

        exit_price = resolve_chain_price(chain_doc)
        if not exit_price:
            entry_price = safe_float((leg.get('entry_trade') or {}).get('price'))
            exit_price  = entry_price  # fallback to entry price if no chain data

        close_leg_in_db(db, t_id, idx, exit_price, 'squared_off', exit_timestamp, leg_id=leg_id)
        print(f'  [SQUARE OFF TOKEN LTP] trade={t_id} leg={leg_id} token={token} price={exit_price}')

    # ── Close legs already in position history (string refs) ─────────────
    history_open = list(history_col.find({'trade_id': t_id, 'status': OPEN_LEG_STATUS}))
    for hdoc in history_open:
        leg_id      = str(hdoc.get('leg_id') or hdoc.get('id') or '')
        token       = str(hdoc.get('token') or '')
        expiry      = normalize_expiry(str(hdoc.get('expiry_date') or ''))
        strike      = hdoc.get('strike')
        option_type = str(hdoc.get('option') or hdoc.get('option_type') or '')

        chain_doc = None
        if token:
            chain_doc = get_chain_by_token_at_time(db, token, exit_timestamp, market_cache)
        if not chain_doc and underlying and expiry and strike and option_type:
            chain_doc = get_chain_at_time(db, underlying, expiry, strike, option_type, exit_timestamp, market_cache)

        exit_price = resolve_chain_price(chain_doc)
        if not exit_price:
            entry_price = safe_float((hdoc.get('entry_trade') or {}).get('price'))
            exit_price  = entry_price

        # History legs don't have an array index — use leg_id match
        try:
            history_col.update_one(
                {'trade_id': t_id, 'leg_id': leg_id},
                {'$set': {
                    'exit_trade':     build_exit_trade_payload(exit_price, 'squared_off', exit_timestamp),
                    'last_saw_price': exit_price,
                    'status':         CLOSED_LEG_STATUS,
                }},
            )
        except Exception as exc:
            log.error('square_off_trade history update error leg=%s: %s', leg_id, exc)
            all_open_closed = False

        print(f'  [SQUARE OFF] closed leg trade={t_id} leg={leg_id} price={exit_price} ts={exit_timestamp}')

    # ── Verify all legs are now closed ────────────────────────────────────
    refreshed   = algo_col.find_one({'_id': t_id}) or {}
    pending_legs = [
        l for l in (refreshed.get('legs') or [])
        if isinstance(l, dict)
        and int(l.get('status') or 0) == OPEN_LEG_STATUS
        and not isinstance(l.get('exit_trade'), dict)
    ]
    remaining_open_history = list(history_col.find({'trade_id': t_id, 'status': OPEN_LEG_STATUS}))

    if all_open_closed and not pending_legs and not remaining_open_history:
        mark_trade_squared_off(db, t_id)
        print(f'[SQUARE OFF] trade={t_id} marked SquaredOff')
        return True

    print(
        f'[SQUARE OFF] trade={t_id} not marked SquaredOff '
        f'pending_exit_legs={len(pending_legs)} '
        f'remaining_history_open_legs={len(remaining_open_history)}'
    )
    return False


# ──────────────────────────────────────────────────────────────────────────────
# §16  TICK PROCESSOR  — main per-minute core loop
# ──────────────────────────────────────────────────────────────────────────────
#
# process_tick() is the heart of the engine.
# It runs once per candle minute for ALL running trades.
#
# Per-trade flow:
#   1. Load open legs (from algo_trades.legs + position history).
#   2. For each open leg:
#        a. Get LTP from chain at now_ts.
#        b. Check leg SL → close + reentry if hit.
#        c. Check leg Target → close + reentry if hit.
#        d. Check Trail SL → update SL if moved.
#   3. After legs loop:
#        a. Compute trade MTM (compute_strategy_mtm).
#        b. Sync overall feature status records.
#        c. Check overall SL → close all + optional reentry.
#        d. Check LockAndTrail → close all if floor breached.
#        e. Check overall Trail SL → update threshold.
#        f. Check overall Target → close all + optional reentry.
# 4. After trades loop:
#        a. Broker-level SL/Target check (check_broker_sl_target).
#
# This function is MODE-AGNOSTIC — it works identically for
# backtest, forward-test, and live execution.
# ─────────────────────────────────────────────────────────────────────────────

def process_tick(
    ctx: TickContext,
    running_trades: list[dict],
) -> TickResult:
    """
    Core per-minute tick processor.

    Runs all feature checks for every open trade and returns a TickResult
    that the caller uses to:
      - Broadcast position updates to the frontend.
      - Emit hit-strategy details to execute-orders socket.
      - Persist audit entries.

    Parameters
    ----------
    ctx:             TickContext with db, trade_date, now_ts, activation_mode.
    running_trades:  list of trade documents from algo_trades (pre-loaded by caller).

    Returns TickResult(actions_taken, hit_trade_ids, hit_ltp_snapshots,
                        open_positions, checked_at).

    Side effects (DB writes):
      - close_leg_in_db      when SL/TP/exit_time fires
      - update_leg_sl_in_db  when trail SL moves
      - push_new_leg_in_db   when reentry legs are queued
      - DB updates for overall SL/Target/LockAndTrail
      - disable_broker_sl_settings when broker SL/Target fires
    """
    open_positions: list[dict]       = []
    strategy_map:   dict[str, dict]  = {}

    now_dt = parse_timestamp(ctx.now_ts)

    for trade in running_trades:
        trade_id   = str(trade.get('_id') or '')
        underlying = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
        strategy_cfg     = trade.get('strategy') or trade.get('config') or {}
        all_leg_configs  = resolve_trade_leg_configs(trade)

        # Exit time check — skip all feature checks after configured exit time
        raw_exit_time  = str(trade.get('exit_time') or '')
        exit_time_hhmm = raw_exit_time[11:16] if len(raw_exit_time) >= 16 else raw_exit_time[:5]
        now_hhmm       = ctx.now_ts[11:16] if len(ctx.now_ts) >= 16 else ''
        past_exit      = bool(exit_time_hhmm and now_hhmm and now_hhmm >= exit_time_hhmm)

        # Load legs (dict entries + position history string refs)
        legs = [l for l in (trade.get('legs') or []) if isinstance(l, dict)]
        dict_leg_ids = {str(l.get('id') or '') for l in legs if l.get('id')}
        if any(isinstance(item, str) for item in (trade.get('legs') or [])):
            hist_col = ctx.db._db[COL_POSITIONS_HIST]
            for hdoc in hist_col.find({'trade_id': trade_id, 'status': OPEN_LEG_STATUS}):
                h_leg_id = str(hdoc.get('leg_id') or hdoc.get('id') or '')
                if not h_leg_id or h_leg_id in dict_leg_ids:
                    continue
                hdoc['expiry_date'] = normalize_expiry(str(hdoc.get('expiry_date') or ''))
                hdoc['id'] = h_leg_id
                legs.append(hdoc)

        # ── Per-leg loop ──────────────────────────────────────────────────
        for leg_index, leg in enumerate(legs):
            if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
                continue
            entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
            if not entry_trade:
                continue  # pending, not yet entered

            leg_id      = str(leg.get('id') or '')
            leg_cfg     = resolve_leg_cfg(leg_id, leg, all_leg_configs)
            entry_price = safe_float(entry_trade.get('price') or entry_trade.get('trigger_price'))
            lot_size    = safe_int(leg.get('lot_size'), 1)
            lots        = safe_int(leg.get('quantity') or entry_trade.get('quantity'))
            qty         = max(0, lots) * max(1, lot_size)
            expiry      = normalize_expiry(str(leg.get('expiry_date') or ''))
            strike      = leg.get('strike')
            option_type = str(leg.get('option') or '')
            sell_pos    = is_sell(str(leg.get('position') or ''))

            # ── Force-exit at exit_time ───────────────────────────────────
            if past_exit:
                chain_doc  = get_chain_at_time(ctx.db, underlying, expiry, strike, option_type, ctx.now_ts, ctx.market_cache)
                exit_price = resolve_chain_price(chain_doc) or entry_price
                close_leg_in_db(ctx.db, trade_id, leg_index, exit_price, 'exit_time', ctx.now_ts, leg_id=leg_id)
                ctx.actions_taken.append(f'{trade_id}/{leg_id}: exit_time @ {exit_price}')
                continue

            # ── Get current LTP ───────────────────────────────────────────
            chain_doc     = get_chain_at_time(ctx.db, underlying, expiry, strike, option_type, ctx.now_ts, ctx.market_cache)
            current_price = resolve_chain_price(chain_doc)
            if not current_price:
                continue  # no data for this candle

            pnl = ((entry_price - current_price) if sell_pos else (current_price - entry_price)) * qty

            # ── SL check ─────────────────────────────────────────────────
            stored_sl = safe_float(leg.get('current_sl_price') or leg.get('sl_price') or 0) or None
            sl_hit, sl_price = check_leg_sl(leg_cfg, entry_price, current_price, stored_sl, sell_pos)
            if sl_hit:
                close_leg_in_db(ctx.db, trade_id, leg_index, current_price, 'stoploss', ctx.now_ts, leg_id=leg_id)
                ctx.actions_taken.append(f'{trade_id}/{leg_id}: SL hit @ {current_price}')
                result = handle_leg_reentry(ctx.db, trade, leg, leg_cfg, 'stoploss', ctx.now_ts)
                if result:
                    ctx.actions_taken.append(result)
                continue

            # ── Target check ──────────────────────────────────────────────
            stored_tp = safe_float(leg.get('current_tp_price') or leg.get('tp_price') or 0) or None
            tp_hit, tp_price = check_leg_target(leg_cfg, entry_price, current_price, stored_tp, sell_pos)
            if tp_hit:
                close_leg_in_db(ctx.db, trade_id, leg_index, current_price, 'target', ctx.now_ts, leg_id=leg_id)
                ctx.actions_taken.append(f'{trade_id}/{leg_id}: TP hit @ {current_price}')
                result = handle_leg_reentry(ctx.db, trade, leg, leg_cfg, 'target', ctx.now_ts)
                if result:
                    ctx.actions_taken.append(result)
                continue

            # ── Trail SL update ───────────────────────────────────────────
            if stored_sl:
                new_sl = compute_next_trail_sl(leg_cfg, entry_price, current_price, stored_sl, sell_pos)
                if new_sl != stored_sl:
                    update_leg_sl_in_db(ctx.db, trade_id, leg_index, new_sl, current_price, leg_id=leg_id)
                    ctx.actions_taken.append(f'{trade_id}/{leg_id}: trail SL {stored_sl}→{new_sl}')

            # ── Accumulate open position snapshot ────────────────────────
            strat_entry = strategy_map.setdefault(trade_id, {
                'trade_id': trade_id, 'open_positions': [], 'total_pnl': 0.0,
            })
            strat_entry['open_positions'].append({
                'leg_id':      leg_id,
                'pnl':         round(pnl, 2),
                'ltp':         current_price,
                'current_price': current_price,
                'entry_price': entry_price,
                'quantity':    qty,
            })
            strat_entry['total_pnl'] = round(strat_entry['total_pnl'] + pnl, 2)
            open_positions.append({'trade_id': trade_id, 'leg_id': leg_id, 'ltp': current_price, 'pnl': round(pnl, 2)})

        # ── Overall SL / Target / Trail / LockAndTrail ────────────────────
        if not past_exit:
            strat_entry = strategy_map.get(trade_id)
            open_leg_ids = {p['leg_id'] for p in (strat_entry or {}).get('open_positions', [])}
            current_mtm, legs_snapshot = compute_strategy_mtm(
                ctx.db, trade_id, ctx.now_ts,
                (strat_entry or {}).get('open_positions', []),
            )
            ctx.trade_mtm_map[trade_id] = current_mtm

            peak_mtm  = max(safe_float(trade.get('peak_mtm'), current_mtm), current_mtm)
            ore_done  = safe_int(trade.get('overall_sl_reentry_done'))
            ort_done  = safe_int(trade.get('overall_tgt_reentry_done'))
            dyn_sl    = safe_float(trade.get('current_overall_sl_threshold'))
            eff_sl    = safe_float(resolve_overall_cycle_value(parse_overall_sl(strategy_cfg)[1], ore_done))
            eff_tgt   = safe_float(resolve_overall_cycle_value(parse_overall_tgt(strategy_cfg)[1], ort_done))
            last_overall_event_at = str(trade.get('last_overall_event_at') or '').strip().replace(' ', 'T')[:19]
            last_overall_event_reason = str(trade.get('last_overall_event_reason') or '').strip()
            skip_same_tick_overall = bool(
                open_leg_ids
                and last_overall_event_at
                and last_overall_event_at == str(ctx.now_ts or '').strip()[:19]
                and last_overall_event_reason in {'overall_sl', 'overall_target'}
            )

            sync_overall_feature_status(ctx.db, trade, ctx.now_ts,
                                        current_mtm=current_mtm,
                                        overall_sl_done=ore_done,
                                        overall_tgt_done=ort_done)

            print('[OVERALL CHECK]', {
                'mode':           ctx.activation_mode,
                'trade_id':       trade_id,
                'timestamp':      ctx.now_ts,
                'current_mtm':    round(current_mtm, 2),
                'overall_sl':     eff_sl,
                'overall_target': eff_tgt,
                'dynamic_sl':     dyn_sl,
            })
            print('[OVERALL SL DEBUG]', {
                'mode': ctx.activation_mode,
                'trade_id': trade_id,
                'timestamp': ctx.now_ts,
                'current_mtm': round(current_mtm, 2),
                'base_overall_sl': round(safe_float(parse_overall_sl(strategy_cfg)[1]), 2),
                'reentry_done': ore_done,
                'cycle_number': ore_done + 1,
                'cycle_overall_sl': round(eff_sl, 2),
                'dynamic_sl_threshold': round(safe_float(dyn_sl), 2),
                'checked_stoploss': round(safe_float(dyn_sl or eff_sl), 2),
                'would_hit_cycle_sl': bool(eff_sl > 0 and current_mtm <= -eff_sl),
                'would_hit_dynamic_sl': bool(dyn_sl > 0 and current_mtm <= -dyn_sl),
                'skip_same_tick_overall': skip_same_tick_overall,
                'last_overall_event_at': last_overall_event_at,
                'last_overall_event_reason': last_overall_event_reason,
            })

            if skip_same_tick_overall:
                ctx.actions_taken.append(
                    f'{trade_id}: skipped same-tick overall recheck after {last_overall_event_reason}'
                )
                continue

            # Helper: close all open legs and return True
            def _close_all(reason: str) -> None:
                for p in list((strat_entry or {}).get('open_positions', [])):
                    _lid = p.get('leg_id', '')
                    for i, l in enumerate(trade.get('legs') or []):
                        if isinstance(l, dict) and str(l.get('id') or '') == _lid:
                            close_leg_in_db(ctx.db, trade_id, i, p.get('ltp', 0), reason, ctx.now_ts, leg_id=_lid)
                # Also close history legs
                for hdoc in ctx.db._db[COL_POSITIONS_HIST].find({'trade_id': trade_id, 'status': OPEN_LEG_STATUS}):
                    h_lid = str(hdoc.get('leg_id') or '')
                    ltp   = safe_float(hdoc.get('last_saw_price'))
                    ctx.db._db[COL_POSITIONS_HIST].update_one(
                        {'trade_id': trade_id, 'leg_id': h_lid},
                        {'$set': {
                            'exit_trade':     build_exit_trade_payload(ltp, reason, ctx.now_ts),
                            'status':         CLOSED_LEG_STATUS,
                            'last_saw_price': ltp,
                        }},
                    )

            # ── 1. Overall SL ─────────────────────────────────────────────
            osl_hit = (eff_sl > 0 and current_mtm <= -eff_sl) or (dyn_sl > 0 and current_mtm <= -dyn_sl)
            if osl_hit and not open_leg_ids:
                osl_hit = False  # no open legs to close — guard against duplicate events

            if osl_hit:
                _close_all('overall_sl')
                ctx.actions_taken.append(f'{trade_id}: overall SL hit mtm={current_mtm}')
                ctx.hit_trade_ids.append(trade_id)
                ctx.hit_ltp_snapshots[trade_id] = list((strategy_map.get(trade_id) or {}).get('open_positions') or [])
                # Reentry
                ore_type, ore_count = parse_overall_reentry_sl(strategy_cfg)
                if ore_type != 'None' and ore_count > 0 and ore_done < ore_count:
                    next_cycle_sl = resolve_overall_cycle_value(
                        parse_overall_sl(strategy_cfg)[1],
                        ore_done + 1,
                    )
                    ctx.db._db[COL_ALGO_TRADES].update_one(
                        {'_id': trade_id},
                        {'$set': {
                            'overall_sl_reentry_done': ore_done + 1,
                            'peak_mtm': 0.0,
                            'current_overall_sl_threshold': next_cycle_sl,
                            'last_overall_event_at': ctx.now_ts,
                            'last_overall_event_reason': 'overall_sl',
                        }},
                    )
                    refreshed_trade = ctx.db._db[COL_ALGO_TRADES].find_one({'_id': trade_id}) or trade
                    print('[OVERALL REENTRY PREPARE]', {
                        'mode': ctx.activation_mode,
                        'trade_id': trade_id,
                        'reason': 'overall_sl',
                        'timestamp': ctx.now_ts,
                        'base_overall_sl': round(safe_float(parse_overall_sl(strategy_cfg)[1]), 2),
                        'updated_reentry_done': ore_done + 1,
                        'updated_cycle_number': ore_done + 2,
                        'updated_overall_sl': round(safe_float(next_cycle_sl), 2),
                        'current_overall_sl_threshold': round(safe_float((refreshed_trade or {}).get('current_overall_sl_threshold')), 2),
                        'reentry_type': ore_type,
                    })
                    new_ids = requeue_all_legs_for_overall_reentry(
                        ctx.db, refreshed_trade, trade_id, strategy_cfg, ctx.now_ts,
                        reentry_type=ore_type,
                    )
                    if new_ids:
                        refreshed_trade = ctx.db._db[COL_ALGO_TRADES].find_one({'_id': trade_id}) or refreshed_trade
                        immediate_entries = process_pending_entries(ctx, [refreshed_trade])
                        print('[OVERALL REENTRY ENTRY CHECK]', {
                            'mode': ctx.activation_mode,
                            'trade_id': trade_id,
                            'reason': 'overall_sl',
                            'timestamp': ctx.now_ts,
                            'queued_leg_ids': new_ids,
                            'immediate_entries': len(immediate_entries),
                            'entry_condition': 'same_tick_process_pending_entries',
                            'updated_overall_sl': round(safe_float(next_cycle_sl), 2),
                            'current_overall_sl_threshold': round(safe_float((refreshed_trade or {}).get('current_overall_sl_threshold')), 2),
                            'check_message': 'reentry uses updated overall SL before immediate entry attempt',
                        })
                        ctx.db._db[COL_ALGO_TRADES].update_one(
                            {'_id': trade_id},
                            {'$set': {'last_overall_event_at': ctx.now_ts, 'last_overall_event_reason': 'overall_sl'}},
                        )
                        ctx.actions_taken.append(
                            f'{trade_id}: overall SL reentry queued legs={new_ids} '
                            f'immediate_entries={len(immediate_entries)}'
                        )
                    else:
                        square_off_trade(ctx.db, trade, ctx.now_ts, market_cache=ctx.market_cache)
                else:
                    square_off_trade(ctx.db, trade, ctx.now_ts, market_cache=ctx.market_cache)
                continue

            # ── 2. Overall Target ─────────────────────────────────────────
            if open_leg_ids and eff_tgt > 0 and current_mtm >= eff_tgt:
                _close_all('overall_target')
                ctx.actions_taken.append(f'{trade_id}: overall Target hit mtm={current_mtm}')
                ctx.hit_trade_ids.append(trade_id)
                ctx.hit_ltp_snapshots[trade_id] = list((strategy_map.get(trade_id) or {}).get('open_positions') or [])
                ort_type, ort_count = parse_overall_reentry_tgt(strategy_cfg)
                if ort_type != 'None' and ort_count > 0 and ort_done < ort_count:
                    reset_sl_threshold = resolve_overall_cycle_value(
                        parse_overall_sl(strategy_cfg)[1],
                        ore_done,
                    )
                    ctx.db._db[COL_ALGO_TRADES].update_one(
                        {'_id': trade_id},
                        {'$set': {
                            'overall_tgt_reentry_done': ort_done + 1,
                            'peak_mtm': 0.0,
                            'current_overall_sl_threshold': reset_sl_threshold,
                            'last_overall_event_at': ctx.now_ts,
                            'last_overall_event_reason': 'overall_target',
                        }},
                    )
                    refreshed_trade = ctx.db._db[COL_ALGO_TRADES].find_one({'_id': trade_id}) or trade
                    print('[OVERALL REENTRY PREPARE]', {
                        'mode': ctx.activation_mode,
                        'trade_id': trade_id,
                        'reason': 'overall_target',
                        'timestamp': ctx.now_ts,
                        'base_overall_sl': round(safe_float(parse_overall_sl(strategy_cfg)[1]), 2),
                        'updated_reentry_done': ort_done + 1,
                        'updated_cycle_number': ort_done + 2,
                        'updated_overall_sl': round(safe_float(reset_sl_threshold), 2),
                        'current_overall_sl_threshold': round(safe_float((refreshed_trade or {}).get('current_overall_sl_threshold')), 2),
                        'reentry_type': ort_type,
                    })
                    new_ids = requeue_all_legs_for_overall_reentry(
                        ctx.db, refreshed_trade, trade_id, strategy_cfg, ctx.now_ts,
                        reentry_type=ort_type,
                    )
                    if new_ids:
                        refreshed_trade = ctx.db._db[COL_ALGO_TRADES].find_one({'_id': trade_id}) or refreshed_trade
                        immediate_entries = process_pending_entries(ctx, [refreshed_trade])
                        print('[OVERALL REENTRY ENTRY CHECK]', {
                            'mode': ctx.activation_mode,
                            'trade_id': trade_id,
                            'reason': 'overall_target',
                            'timestamp': ctx.now_ts,
                            'queued_leg_ids': new_ids,
                            'immediate_entries': len(immediate_entries),
                            'entry_condition': 'same_tick_process_pending_entries',
                            'updated_overall_sl': round(safe_float(reset_sl_threshold), 2),
                            'current_overall_sl_threshold': round(safe_float((refreshed_trade or {}).get('current_overall_sl_threshold')), 2),
                            'check_message': 'reentry uses updated overall SL before immediate entry attempt',
                        })
                        ctx.db._db[COL_ALGO_TRADES].update_one(
                            {'_id': trade_id},
                            {'$set': {'last_overall_event_at': ctx.now_ts, 'last_overall_event_reason': 'overall_target'}},
                        )
                        ctx.actions_taken.append(
                            f'{trade_id}: overall Target reentry queued legs={new_ids} '
                            f'immediate_entries={len(immediate_entries)}'
                        )
                    else:
                        square_off_trade(ctx.db, trade, ctx.now_ts, market_cache=ctx.market_cache)
                else:
                    square_off_trade(ctx.db, trade, ctx.now_ts, market_cache=ctx.market_cache)
                continue

            # ── 3. LockAndTrail ───────────────────────────────────────────
            if open_leg_ids:
                lock_cfg = parse_lock_and_trail(strategy_cfg)
                lock_exit, lock_floor = check_lock_and_trail(lock_cfg, current_mtm, peak_mtm)
                if lock_exit:
                    _close_all('lock_and_trail')
                    square_off_trade(ctx.db, trade, ctx.now_ts, market_cache=ctx.market_cache)
                    ctx.actions_taken.append(f'{trade_id}: LockAndTrail exit mtm={current_mtm} floor={lock_floor}')
                    continue

            # ── 4. Overall Trail SL threshold update ──────────────────────
            if open_leg_ids:
                new_thresh = compute_next_overall_trail_sl(strategy_cfg, dyn_sl or eff_sl, safe_float(parse_overall_sl(strategy_cfg)[1]), peak_mtm)
                if new_thresh != (dyn_sl or eff_sl):
                    ctx.db._db[COL_ALGO_TRADES].update_one(
                        {'_id': trade_id},
                        {'$set': {'peak_mtm': peak_mtm, 'current_overall_sl_threshold': new_thresh}},
                    )
                    ctx.actions_taken.append(f'{trade_id}: overall trail SL {dyn_sl}→{new_thresh}')
                elif peak_mtm > safe_float(trade.get('peak_mtm')):
                    ctx.db._db[COL_ALGO_TRADES].update_one({'_id': trade_id}, {'$set': {'peak_mtm': peak_mtm}})

        elif past_exit:
            mark_trade_squared_off(ctx.db, trade_id)

    # ── Broker-level SL/Target check ─────────────────────────────────────
    broker_hit_ids, broker_ltp_snaps = check_broker_sl_target(
        ctx, running_trades, strategy_map, []
    )
    for tid in broker_hit_ids:
        if tid not in ctx.hit_trade_ids:
            ctx.hit_trade_ids.append(tid)
    ctx.hit_ltp_snapshots.update(broker_ltp_snaps)

    return TickResult(
        actions_taken=ctx.actions_taken,
        hit_trade_ids=ctx.hit_trade_ids,
        hit_ltp_snapshots=ctx.hit_ltp_snapshots,
        open_positions=open_positions,
        checked_at=ctx.now_ts,
    )


# ──────────────────────────────────────────────────────────────────────────────
# §17  ENTRY PROCESSOR  — pending leg resolution
# ──────────────────────────────────────────────────────────────────────────────
#
# resolve_pending_leg_entry() attempts to fill a single pending leg on the
# current candle.  It is called for every pending leg every tick.
#
# Entry conditions checked in order:
#   1. entry_time  – not before strategy.entry_time (HH:MM)
#   2. Expiry      – resolve_leg_expiry() must succeed
#   3. Strike      – resolve_leg_strike() must succeed
#   4. Chain price – must be > 0
#   5. Momentum    – if configured, trigger must be reached first
#
# On success:
#   - entry_trade is set on the leg in algo_trades
#   - algo_trade_positions_history record is created
#   - SL/TP feature records are seeded (via notification_manager)
# ─────────────────────────────────────────────────────────────────────────────

def resolve_pending_leg_entry(
    ctx: TickContext,
    trade: dict,
    leg: dict,
    leg_index: int,
) -> dict | None:
    """
    Attempt to enter a single pending leg at the current candle.

    Returns the entry_trade dict if entry succeeded, None otherwise.

    The caller (process_pending_entries or _execute_backtest_entries) should:
      - On success: push entry data to DB + create position history record.
      - On None: skip this leg and try again next tick.

    This function is READ-ONLY with respect to the DB — it only computes
    whether entry should happen and what the entry price would be.
    The caller writes the actual DB updates.
    """
    trade_id    = str(trade.get('_id') or '')
    leg_id      = str(leg.get('id') or '')
    all_configs = resolve_trade_leg_configs(trade)
    leg_cfg     = resolve_leg_cfg(leg_id, leg, all_configs)
    underlying  = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
    option_type = resolve_leg_option_type(leg, leg_cfg)
    sell_pos    = is_sell(str(leg.get('position') or leg_cfg.get('Position') or ''))

    # 1. Entry time gate
    entry_time = str((trade.get('config') or {}).get('entry_time') or '').strip()
    if entry_time and ctx.now_ts[11:16] < entry_time:
        return None  # too early

    # 2. Resolve expiry
    expiry = resolve_leg_expiry(ctx.db, leg_cfg, underlying, ctx.now_ts, ctx.market_cache)
    if not expiry:
        _trace_stdout(
            f'[ENTRY DEBUG] leg={leg_id}  underlying={underlying}  '
            f'ts={ctx.now_ts[11:19]}  expiry=UNRESOLVED '
            f'(Kite instruments not loaded or no expiry found) — skip'
        )
        return None

    # 3. Resolve spot & strike
    spot = get_spot_at_time(ctx.db, underlying, ctx.now_ts, ctx.market_cache)
    if not spot:
        _trace_stdout(f'[ENTRY DEBUG] leg={leg_id}  underlying={underlying}  spot=UNAVAILABLE — skip')
        return None

    from features.spot_atm_utils import resolve_atm_price as _atm_price  # type: ignore
    atm_price = _atm_price(underlying, spot)

    strike = resolve_leg_strike(ctx.db, leg_cfg, underlying, expiry, option_type, spot, ctx.now_ts, ctx.market_cache)
    if not strike:
        _trace_stdout(
            f'[ENTRY DEBUG] leg={leg_id}  underlying={underlying}  '
            f'spot={spot}  atm={atm_price}  strike=UNRESOLVED — skip'
        )
        return None

    # 4. Fetch chain doc → entry price (live: Kite instruments cache + LTP)
    chain_doc = get_chain_at_time(ctx.db, underlying, expiry, strike, option_type, ctx.now_ts, ctx.market_cache)
    instrument_token = str(chain_doc.get('token') or '').strip() if chain_doc else ''
    symbol           = str(chain_doc.get('symbol') or '').strip() if chain_doc else ''

    # For live/fast-forward: try ctx.live_ltp_map first (most up-to-date tick).
    # get_kite_chain_doc may have returned close=0 if the tick hasn't arrived yet
    # via the cache-time snapshot — ctx.live_ltp_map is always the freshest map.
    entry_price = resolve_chain_price(chain_doc)
    if ctx.live_ltp_map and instrument_token:
        ltp_present = instrument_token in ctx.live_ltp_map
        live_ltp_value = safe_float(ctx.live_ltp_map.get(instrument_token, 0.0))
        _trace_stdout(
            f'[ENTRY LTP CHECK] leg={leg_id}  token={instrument_token}  '
            f'present_in_ltp_map={"yes" if ltp_present else "no"}  '
            f'ltp={live_ltp_value if live_ltp_value > 0 else "UNAVAILABLE"}'
        )
        kite_ltp = safe_float(ctx.live_ltp_map.get(instrument_token, 0.0))
        if kite_ltp > 0:
            entry_price = kite_ltp
    elif instrument_token:
        _trace_stdout(
            f'[ENTRY LTP CHECK] leg={leg_id}  token={instrument_token}  '
            f'present_in_ltp_map=no  ltp_map_snapshot=EMPTY'
        )

    _trace_stdout(
        f'[ENTRY DEBUG] ┌─ leg={leg_id}  {underlying} {option_type}  ts={ctx.now_ts[11:19]}\n'
        f'              │  spot={spot}  atm={atm_price}  expiry={expiry}\n'
        f'              │  strike={strike}  symbol={symbol or "-"}  token={instrument_token or "-"}\n'
        f'              └─ kite_ltp={entry_price if entry_price else "UNAVAILABLE — will retry"}'
    )

    if not entry_price:
        return None  # Kite LTP not yet received for this token — retry next tick

    # 5. Momentum gate (if configured)
    if has_momentum_config(leg_cfg):
        momentum_cfg    = leg_cfg.get('LegMomentum') or {}
        momentum_type   = str(momentum_cfg.get('Type') or '').strip()
        momentum_value  = safe_float(momentum_cfg.get('Value') or 0)
        stored_base     = safe_float(leg.get('momentum_base_price') or 0)
        stored_target   = safe_float(leg.get('momentum_target_price') or 0)

        if not stored_base:
            return {'__arm_momentum__': True, 'base_price': entry_price, 'momentum_type': momentum_type, 'momentum_value': momentum_value}

        target_price = stored_target or compute_momentum_target(momentum_type, stored_base, momentum_value)
        if not is_momentum_triggered(momentum_type, entry_price, target_price or 0):
            return None  # waiting for momentum trigger

    # ── Entry approved — build entry_trade ───────────────────────────────
    try:
        resolved_lot_size = safe_int(ctx.db.get_lot_size(ctx.trade_date, underlying) or 0)
    except Exception:
        resolved_lot_size = 0
    lot_size = safe_int(
        resolved_lot_size
        or leg_cfg.get('LotSize')
        or leg.get('lot_size')
        or 1
    )
    quantity = safe_int(leg_cfg.get('Quantity') or leg.get('quantity') or 1)

    _trace_stdout(
        f'[ENTRY APPROVED] leg={leg_id}  {underlying} {option_type}  '
        f'spot={spot}  atm={atm_price}  expiry={expiry}  '
        f'strike={strike}  token={instrument_token or "-"}  kite_ltp={entry_price}'
    )

    return {
        'trigger_timestamp':  ctx.now_ts,
        'trigger_price':      entry_price,
        'price':              entry_price,
        'traded_timestamp':   ctx.now_ts,
        'exchange_timestamp': ctx.now_ts,
        'spot_price':         spot,
        'expiry':             expiry,
        'strike':             strike,
        'option_type':        option_type,
        'quantity':           quantity,
        'lot_size':           lot_size,
        'token':              instrument_token or None,
        'symbol':             symbol or None,
        'instrument_token':   instrument_token or None,
    }


def _build_position_history_doc_for_entry(trade: dict, leg: dict) -> dict | None:
    entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
    entry_timestamp = str(
        entry_trade.get('traded_timestamp')
        or entry_trade.get('trigger_timestamp')
        or ''
    ).strip()
    if not entry_timestamp:
        return None

    strategy_cfg = trade.get('strategy') or trade.get('config') or {}
    return {
        'trade_id': str(trade.get('_id') or ''),
        'strategy_id': str(trade.get('strategy_id') or ''),
        'strategy_name': str(trade.get('name') or ''),
        'group_name': str(((trade.get('portfolio') or {}).get('group_name') or '')),
        'ticker': str(strategy_cfg.get('Ticker') or trade.get('ticker') or ''),
        'creation_ts': str(trade.get('creation_ts') or ''),
        'entry_timestamp': entry_timestamp,
        'history_type': 'position_entry',
        'created_at': now_iso(),
        'leg_id': str(leg.get('id') or ''),
        'id': str(leg.get('id') or ''),
        'status': int(leg.get('status') or OPEN_LEG_STATUS),
        'token': leg.get('token') or entry_trade.get('token') or entry_trade.get('instrument_token'),
        'symbol': leg.get('symbol') or entry_trade.get('symbol') or entry_trade.get('instrument_token'),
        'quantity': safe_int(leg.get('quantity')),
        'lot_size': safe_int(
            leg.get('lot_size')
            or entry_trade.get('lot_size')
            or leg.get('quantity')
        ),
        'position': leg.get('position'),
        'option': leg.get('option'),
        'expiry_date': str(leg.get('expiry_date') or '')[:10] or None,
        'strike': leg.get('strike'),
        'last_saw_price': safe_float(leg.get('last_saw_price')),
        'entry_trade': entry_trade,
        'exit_trade': leg.get('exit_trade') if leg.get('exit_trade') is not None else None,
        'is_reentered_leg': bool(leg.get('is_reentered_leg') or leg.get('triggered_by')),
        'transactions': leg.get('transactions') or {},
        'current_transaction_id': leg.get('current_transaction_id'),
        'initial_sl_value': leg.get('initial_sl_value', leg.get('current_sl_price')),
        'display_sl_value': leg.get('display_sl_value', leg.get('current_sl_price')),
        'display_target_value': leg.get('display_target_value'),
        'leg_type': str(leg.get('leg_type') or ''),
        'lot_config_value': safe_int(leg.get('lot_config_value') or 1),
        'momentum_base_price': safe_float(leg.get('momentum_base_price')) or None,
        'momentum_target_price': safe_float(leg.get('momentum_target_price')) or None,
        'momentum_reference_set_at': str(leg.get('momentum_reference_set_at') or ''),
        'momentum_triggered_at': str(leg.get('momentum_triggered_at') or ''),
    }


def _store_position_history_for_entry(
    db: MongoData,
    trade: dict,
    leg: dict,
    activation_mode: str = 'algo-backtest',
) -> tuple[bool, str | None]:
    history_doc = _build_position_history_doc_for_entry(trade, leg)
    if not history_doc:
        return False, None

    history_col = db._db[COL_POSITIONS_HIST]
    duplicate_query = {
        'trade_id': history_doc['trade_id'],
        'leg_id': history_doc['leg_id'],
        'entry_timestamp': history_doc['entry_timestamp'],
    }
    existing = history_col.find_one(duplicate_query, {'_id': 1})
    if existing:
        existing_id = str(existing.get('_id') or '').strip() or None
        return False, existing_id

    result = history_col.insert_one(history_doc)
    inserted_id = str(result.inserted_id)
    try:
        history_col.update_one(
            {'_id': result.inserted_id},
            {'$set': {'id': inserted_id}},
        )
    except Exception as exc:
        log.warning('position history id sync error trade=%s leg=%s: %s', history_doc['trade_id'], history_doc['leg_id'], exc)

    leg_id = str(leg.get('id') or '').strip()
    try:
        normalized_mode = str(activation_mode or '').strip().lower()
        if normalized_mode in {'live', 'fast-forward', 'forward-test'}:
            db._db[COL_ALGO_TRADES].update_one(
                {'_id': history_doc['trade_id']},
                {'$pull': {'legs': {'id': leg_id}}},
            )
            db._db[COL_ALGO_TRADES].update_one(
                {'_id': history_doc['trade_id']},
                {'$push': {'legs': inserted_id}},
            )
        else:
            db._db[COL_ALGO_TRADES].update_one(
                {'_id': history_doc['trade_id']},
                {
                    '$pull': {'legs': {'id': leg_id}},
                    '$push': {'legs': inserted_id},
                },
            )
    except Exception as exc:
        log.error('position history leg replace error trade=%s leg=%s: %s', history_doc['trade_id'], leg_id, exc)
        return False, None

    print(
        f'[POSITION HISTORY STORE] trade={history_doc["trade_id"]} '
        f'leg={history_doc["leg_id"]} history_id={inserted_id}'
    )
    return True, inserted_id


def process_pending_entries(
    ctx: TickContext,
    running_trades: list[dict],
) -> list[dict]:
    """
    Attempt to fill all pending legs across all running trades.

    Iterates every running trade, finds pending legs via get_pending_legs(),
    calls resolve_pending_leg_entry() for each.

    On success for each leg:
      - Writes entry_trade to algo_trades.legs[index]
      - Creates position history record in algo_trade_positions_history
      - Seeds feature status records via notification_manager

    Returns list of entry records (for broadcast / audit trail).

    Called before process_tick() in the backtest/live loop so that
    newly entered legs are immediately visible to the SL/Target checks.
    """
    entries_executed: list[dict] = []

    for trade in running_trades:
        trade_id   = str(trade.get('_id') or '')

        # Resolve all leg configs once per trade (ListOfLegConfigs + IdleLegConfigs)
        all_leg_configs = resolve_trade_leg_configs(trade)
        using_live_like_snapshot = False
        pending_legs = get_pending_legs(trade)

        # Live / fast-forward original entry should not persist pending legs.
        if not pending_legs and not any(isinstance(l, dict) for l in (trade.get('legs') or [])):
            if _is_live_like_activation_mode(ctx.activation_mode):
                pending_legs = build_original_pending_legs_snapshot(ctx.db, trade, ctx.now_ts)
                using_live_like_snapshot = bool(pending_legs)
            else:
                queue_original_legs_if_needed(ctx.db, trade, ctx.now_ts)
                trade = ctx.db._db[COL_ALGO_TRADES].find_one({'_id': trade_id}) or trade
                pending_legs = get_pending_legs(trade)

        for _leg_index, leg in pending_legs:
            leg_id  = str(leg.get('id') or '')
            leg_cfg = resolve_leg_cfg(leg_id, leg, all_leg_configs)

            result = resolve_pending_leg_entry(ctx, trade, leg, _leg_index)
            if result is None:
                continue

            # Handle momentum arming — store base price, defer actual entry
            if result.get('__arm_momentum__'):
                if using_live_like_snapshot:
                    _trace_stdout(
                        f'[ENTRY SKIP]  leg={leg_id}  reason=momentum_pending_requires_persisted_leg'
                    )
                    continue
                try:
                    # Use positional $ operator — index-safe even when other legs
                    # have already been pulled from the array.
                    ctx.db._db[COL_ALGO_TRADES].update_one(
                        {'_id': trade_id, 'legs.id': leg_id},
                        {'$set': {
                            'legs.$.momentum_base_price':       result['base_price'],
                            'legs.$.momentum_type':             result['momentum_type'],
                            'legs.$.momentum_value':            result['momentum_value'],
                            'legs.$.momentum_reference_set_at': ctx.now_ts,
                        }},
                    )
                    print(f'[MOMENTUM ARMED] leg={leg_id} base={result["base_price"]}')
                except Exception as exc:
                    log.error('momentum arm error leg=%s: %s', leg_id, exc)
                continue

            entry_trade = result

            if using_live_like_snapshot:
                refreshed_trade = ctx.db._db[COL_ALGO_TRADES].find_one({'_id': trade_id}) or trade
                refreshed_leg = {
                    **leg,
                    'entry_trade': entry_trade,
                    'expiry_date': entry_trade.get('expiry'),
                    'strike': entry_trade.get('strike'),
                    'last_saw_price': entry_trade.get('price'),
                    'token': entry_trade.get('token') or entry_trade.get('instrument_token'),
                    'symbol': entry_trade.get('symbol') or entry_trade.get('instrument_token'),
                    'lot_size': entry_trade.get('lot_size') or leg.get('lot_size'),
                }
            else:
                # ── 1. Write entry_trade to the correct leg using positional $ ─────
                # MUST NOT use numeric index here — previous legs processed in this
                # same loop have already been $pull-ed and $push-ed (as string IDs),
                # which shifts all remaining dict-leg indices.
                try:
                    ctx.db._db[COL_ALGO_TRADES].update_one(
                        {'_id': trade_id, 'legs.id': leg_id},
                        {'$set': {
                            'legs.$.entry_trade':    entry_trade,
                            'legs.$.expiry_date':    entry_trade.get('expiry'),
                            'legs.$.strike':         entry_trade.get('strike'),
                            'legs.$.last_saw_price': entry_trade.get('price'),
                            'legs.$.token':          entry_trade.get('token') or entry_trade.get('instrument_token'),
                            'legs.$.symbol':         entry_trade.get('symbol') or entry_trade.get('instrument_token'),
                            'legs.$.lot_size':       entry_trade.get('lot_size'),
                        }},
                    )
                except Exception as exc:
                    log.error('write entry_trade error trade=%s leg=%s: %s', trade_id, leg_id, exc)
                    continue

                # ── 2. Reload refreshed leg (now has entry_trade set) ─────────────
                try:
                    refreshed_trade = ctx.db._db[COL_ALGO_TRADES].find_one({'_id': trade_id}) or trade
                    refreshed_leg   = next(
                        (
                            item for item in (refreshed_trade.get('legs') or [])
                            if isinstance(item, dict) and str(item.get('id') or '') == leg_id
                        ),
                        None,
                    )
                except Exception as exc:
                    log.error('refresh leg error trade=%s leg=%s: %s', trade_id, leg_id, exc)
                    refreshed_trade = trade
                    refreshed_leg   = None

            # ── 3. Seed algo_leg_feature_status (SL / TP / Trail records) ─────
            # Pass leg_cfg (strategy config with LegStopLoss/LegTarget/LegTrailSL)
            # and the refreshed leg (which now has entry_trade populated).
            try:
                from features.notification_manager import record_entry_taken, record_leg_features_at_entry  # type: ignore
                notify_leg = refreshed_leg or {**leg, 'entry_trade': entry_trade}
                record_entry_taken(ctx.db._db, refreshed_trade, notify_leg, leg_cfg, ctx.now_ts)
                record_leg_features_at_entry(ctx.db._db, refreshed_trade, notify_leg, leg_cfg, ctx.now_ts)
            except Exception as exc:
                log.warning('record_entry_taken error leg=%s: %s', leg_id, exc)

            # ── 4. Insert into algo_trade_positions_history; replace leg dict
            #       with string ID in algo_trades.legs ─────────────────────────
            try:
                if refreshed_leg:
                    _store_position_history_for_entry(
                        ctx.db,
                        refreshed_trade,
                        refreshed_leg,
                        activation_mode=ctx.activation_mode,
                    )
            except Exception as exc:
                log.error('position history store error trade=%s leg=%s: %s', trade_id, leg_id, exc)

            entries_executed.append({
                'trade_id':    trade_id,
                'leg_id':      leg_id,
                'entry_price': entry_trade.get('price'),
                'strike':      entry_trade.get('strike'),
                'expiry':      entry_trade.get('expiry'),
                'option_type': entry_trade.get('option_type'),
                'instrument_token': entry_trade.get('instrument_token'),
                'timestamp':   ctx.now_ts,
            })
            print(
                f'[POSITION ENTRY] trade={trade_id}  leg={leg_id}  '
                f'strike={entry_trade.get("strike")}  price={entry_trade.get("price")}  '
                f'ts={ctx.now_ts}'
            )

    return entries_executed


# ──────────────────────────────────────────────────────────────────────────────
# §18  BROKER TICK PROCESSOR  — live trade & fast-forward via broker WebSocket
# ──────────────────────────────────────────────────────────────────────────────
#
# When activation_mode is 'live' or 'fast-forward', LTP data comes from the
# broker's WebSocket (Zerodha KiteTicker, Angel SmartWebSocket, etc.) instead
# of option_chain_historical_data.
#
# Broker WS emits on every price change → we get a broker_ltp_map:
#   { composite_token: ltp }
#   composite_token = make_option_token(underlying, expiry, strike, option_type)
#   Example: 'NIFTY_2025-11-04_24500_CE' → 156.50
#
# process_broker_tick() is identical to process_tick() in §16 except:
#   - current_price  = broker_ltp_map.get(token)          ← broker WS (live/ff)
#   - fallback       = resolve_chain_price(chain_doc)     ← DB if token missing
#
# Usage:
#   # On each broker WS tick:
#   broker_map = {tick['token']: tick['last_price'] for tick in broker_ticks}
#   ctx = TickContext(db=db, trade_date=trade_date, now_ts=now_iso(),
#                    activation_mode='live')
#   result = process_broker_tick(ctx, running_trades, broker_map)
# ─────────────────────────────────────────────────────────────────────────────

def resolve_broker_ltp(
    broker_ltp_map: dict[str, float],
    underlying: str,
    expiry: str,
    strike: Any,
    option_type: str,
    leg: dict | None = None,
) -> float:
    """
    Look up current LTP from broker_ltp_map using composite token key.

    Try order:
      1. leg.token (e.g. 'NSE_20251104_24500_CE' — broker's own token string)
      2. make_option_token(underlying, expiry, strike, option_type)
         (our canonical composite key: 'NIFTY_2025-11-04_24500_CE')
      3. Return 0.0 if not found — caller will fall back to DB chain doc.

    Parameters
    ----------
    broker_ltp_map: dict emitted from broker WS on_ticks, normalised to
                    {token_str: ltp_float} by the caller.
    leg:            optional leg dict — may have a broker token stored as leg.token
    """
    if not broker_ltp_map:
        return 0.0
    # 1. Try stored broker token (e.g. numeric converted to string, or exchange token)
    if leg:
        stored_tok = str(leg.get('token') or '').strip()
        if stored_tok and stored_tok in broker_ltp_map:
            return safe_float(broker_ltp_map[stored_tok])
    # 2. Try composite canonical key
    composite = make_option_token(underlying, expiry, strike, option_type)
    if composite in broker_ltp_map:
        return safe_float(broker_ltp_map[composite])
    return 0.0


def process_broker_tick(
    ctx: TickContext,
    running_trades: list[dict],
    broker_ltp_map: dict[str, float],
) -> TickResult:
    """
    Broker-tick processor for live trade and fast-forward modes.

    Identical to process_tick() (§16) in all SL/Target/Trail/Overall/Broker
    logic, but LTP source is broker_ltp_map instead of option_chain DB.

    Parameters
    ----------
    ctx:            TickContext — db, trade_date, now_ts, activation_mode.
    running_trades: list of trade docs from algo_trades (pre-loaded by caller).
    broker_ltp_map: { token_str: ltp_float } — from broker WS on_ticks callback.
                    token_str can be the broker's own token or our composite key.

    LTP resolution per leg:
      1. broker_ltp_map[leg.token]  → direct broker token match
      2. broker_ltp_map[make_option_token(...)]  → composite key match
      3. Fallback: resolve_chain_price(get_chain_at_time(...))  → DB query
         (used when broker hasn't emitted a tick for that token yet, or
          when running in fast-forward mode with sparse broker data)

    Returns TickResult — same structure as process_tick().
    """
    open_positions: list[dict]      = []
    strategy_map:   dict[str, dict] = {}

    now_dt = parse_timestamp(ctx.now_ts)

    for trade in running_trades:
        trade_id         = str(trade.get('_id') or '')
        underlying       = str((trade.get('config') or {}).get('Ticker') or trade.get('ticker') or '')
        strategy_cfg     = trade.get('strategy') or trade.get('config') or {}
        all_leg_configs  = resolve_trade_leg_configs(trade)

        # Exit time check
        raw_exit_time  = str(trade.get('exit_time') or '')
        exit_time_hhmm = raw_exit_time[11:16] if len(raw_exit_time) >= 16 else raw_exit_time[:5]
        now_hhmm       = ctx.now_ts[11:16] if len(ctx.now_ts) >= 16 else ''
        past_exit      = bool(exit_time_hhmm and now_hhmm and now_hhmm >= exit_time_hhmm)

        # Load legs (dict entries + position history string refs)
        legs = [l for l in (trade.get('legs') or []) if isinstance(l, dict)]
        dict_leg_ids = {str(l.get('id') or '') for l in legs if l.get('id')}
        if any(isinstance(item, str) for item in (trade.get('legs') or [])):
            hist_col = ctx.db._db[COL_POSITIONS_HIST]
            for hdoc in hist_col.find({'trade_id': trade_id, 'status': OPEN_LEG_STATUS}):
                h_leg_id = str(hdoc.get('leg_id') or hdoc.get('id') or '')
                if not h_leg_id or h_leg_id in dict_leg_ids:
                    continue
                hdoc['expiry_date'] = normalize_expiry(str(hdoc.get('expiry_date') or ''))
                hdoc['id'] = h_leg_id
                legs.append(hdoc)

        # ── Per-leg loop ──────────────────────────────────────────────────
        for leg_index, leg in enumerate(legs):
            if int(leg.get('status') or 0) != OPEN_LEG_STATUS:
                continue
            entry_trade = leg.get('entry_trade') if isinstance(leg.get('entry_trade'), dict) else {}
            if not entry_trade:
                continue  # pending, not yet entered

            leg_id      = str(leg.get('id') or '')
            leg_cfg     = resolve_leg_cfg(leg_id, leg, all_leg_configs)
            entry_price = safe_float(entry_trade.get('price') or entry_trade.get('trigger_price'))
            lot_size    = safe_int(leg.get('lot_size'), 1)
            lots        = safe_int(leg.get('quantity') or entry_trade.get('quantity'))
            qty         = max(0, lots) * max(1, lot_size)
            expiry      = normalize_expiry(str(leg.get('expiry_date') or ''))
            strike      = leg.get('strike')
            option_type = str(leg.get('option') or '')
            sell_pos    = is_sell(str(leg.get('position') or ''))

            # ── Force-exit at exit_time ───────────────────────────────────
            if past_exit:
                # Try broker LTP first; fall back to chain doc
                exit_price = resolve_broker_ltp(broker_ltp_map, underlying, expiry, strike, option_type, leg)
                if not exit_price:
                    chain_doc  = get_chain_at_time(ctx.db, underlying, expiry, strike, option_type, ctx.now_ts, ctx.market_cache)
                    exit_price = resolve_chain_price(chain_doc) or entry_price
                close_leg_in_db(ctx.db, trade_id, leg_index, exit_price, 'exit_time', ctx.now_ts, leg_id=leg_id)
                ctx.actions_taken.append(f'{trade_id}/{leg_id}: exit_time @ {exit_price}')
                continue

            # ── Get current LTP — broker first, then DB fallback ─────────
            current_price = resolve_broker_ltp(broker_ltp_map, underlying, expiry, strike, option_type, leg)
            if not current_price:
                chain_doc     = get_chain_at_time(ctx.db, underlying, expiry, strike, option_type, ctx.now_ts, ctx.market_cache)
                current_price = resolve_chain_price(chain_doc)
            if not current_price:
                continue  # no price from broker or DB — skip this tick

            pnl = ((entry_price - current_price) if sell_pos else (current_price - entry_price)) * qty
            sl_config = leg_cfg.get('LegStopLoss') or {}
            tp_config = leg_cfg.get('LegTarget') or {}
            trail_config = get_trail_config(leg_cfg)
            stored_sl = safe_float(leg.get('current_sl_price') or leg.get('sl_price') or 0) or None
            next_sl = stored_sl
            if stored_sl:
                next_sl = compute_next_trail_sl(leg_cfg, entry_price, current_price, stored_sl, sell_pos)
            tp_price = calc_tp_price(entry_price, sell_pos, tp_config)

            print(
                '[FEATURE CHECK]',
                f'listen_time={ctx.now_ts}',
                f'trade_id={trade_id}',
                f'leg_id={leg_id}',
                f'strategy_name={str(trade.get("name") or "-")}',
                f'option={option_type}',
                f'position={str(leg.get("position") or "")}',
                f'entry_price={entry_price}',
                f'ltp={current_price}',
                f'stored_sl={stored_sl}',
                f'next_sl={next_sl}',
                f'tp={tp_price}',
                f'sl_enabled={bool(sl_config)}',
                f'tg_enabled={bool(tp_config)}',
                f'tsl_enabled={bool(trail_config)}',
            )

            # ── SL check ─────────────────────────────────────────────────
            sl_hit, sl_price = check_leg_sl(leg_cfg, entry_price, current_price, stored_sl, sell_pos)
            if sl_hit:
                close_leg_in_db(ctx.db, trade_id, leg_index, current_price, 'stoploss', ctx.now_ts, leg_id=leg_id)
                ctx.actions_taken.append(
                    f'{trade_id}/{leg_id}: SL hit @ {sl_price} - ltp - {current_price}'
                )
                result = handle_leg_reentry(ctx.db, trade, leg, leg_cfg, 'stoploss', ctx.now_ts)
                if result:
                    ctx.actions_taken.append(result)
                continue

            # ── Target check ──────────────────────────────────────────────
            stored_tp = safe_float(leg.get('current_tp_price') or leg.get('tp_price') or 0) or None
            tp_hit, tp_price = check_leg_target(leg_cfg, entry_price, current_price, stored_tp, sell_pos)
            if tp_hit:
                close_leg_in_db(ctx.db, trade_id, leg_index, current_price, 'target', ctx.now_ts, leg_id=leg_id)
                ctx.actions_taken.append(
                    f'{trade_id}/{leg_id}: TP hit @ {tp_price} - ltp - {current_price}'
                )
                result = handle_leg_reentry(ctx.db, trade, leg, leg_cfg, 'target', ctx.now_ts)
                if result:
                    ctx.actions_taken.append(result)
                continue

            # ── Trail SL update ───────────────────────────────────────────
            if stored_sl:
                new_sl = compute_next_trail_sl(leg_cfg, entry_price, current_price, stored_sl, sell_pos)
                if new_sl != stored_sl:
                    update_leg_sl_in_db(ctx.db, trade_id, leg_index, new_sl, current_price, leg_id=leg_id)
                    ctx.actions_taken.append(f'{trade_id}/{leg_id}: trail SL {stored_sl}→{new_sl}')

            # ── Open position snapshot ────────────────────────────────────
            strat_entry = strategy_map.setdefault(trade_id, {
                'trade_id': trade_id, 'open_positions': [], 'total_pnl': 0.0,
            })
            strat_entry['open_positions'].append({
                'leg_id':        leg_id,
                'pnl':           round(pnl, 2),
                'ltp':           current_price,
                'current_price': current_price,
                'entry_price':   entry_price,
                'quantity':      qty,
                'source':        'broker_ws',  # marks this came from live broker tick
            })
            strat_entry['total_pnl'] = round(strat_entry['total_pnl'] + pnl, 2)
            open_positions.append({'trade_id': trade_id, 'leg_id': leg_id, 'ltp': current_price, 'pnl': round(pnl, 2)})

        # ── Overall SL / Target / Trail / LockAndTrail ────────────────────
        if not past_exit:
            strat_entry  = strategy_map.get(trade_id)
            open_leg_ids = {p['leg_id'] for p in (strat_entry or {}).get('open_positions', [])}
            current_mtm, _ = compute_strategy_mtm(
                ctx.db, trade_id, ctx.now_ts,
                (strat_entry or {}).get('open_positions', []),
            )
            ctx.trade_mtm_map[trade_id] = current_mtm

            peak_mtm  = max(safe_float(trade.get('peak_mtm'), current_mtm), current_mtm)
            ore_done  = safe_int(trade.get('overall_sl_reentry_done'))
            ort_done  = safe_int(trade.get('overall_tgt_reentry_done'))
            dyn_sl    = safe_float(trade.get('current_overall_sl_threshold'))
            eff_sl    = safe_float(resolve_overall_cycle_value(parse_overall_sl(strategy_cfg)[1], ore_done))
            eff_tgt   = safe_float(resolve_overall_cycle_value(parse_overall_tgt(strategy_cfg)[1], ort_done))
            last_overall_event_at = str(trade.get('last_overall_event_at') or '').strip().replace(' ', 'T')[:19]
            last_overall_event_reason = str(trade.get('last_overall_event_reason') or '').strip()
            skip_same_tick_overall = bool(
                open_leg_ids
                and last_overall_event_at
                and last_overall_event_at == str(ctx.now_ts or '').strip()[:19]
                and last_overall_event_reason in {'overall_sl', 'overall_target'}
            )

            sync_overall_feature_status(ctx.db, trade, ctx.now_ts,
                                        current_mtm=current_mtm,
                                        overall_sl_done=ore_done,
                                        overall_tgt_done=ort_done)

            print('[OVERALL CHECK]', {
                'mode':           ctx.activation_mode,
                'trade_id':       trade_id,
                'strategy_name':  str(trade.get('name') or ''),
                'timestamp':      ctx.now_ts,
                'current_mtm':    round(current_mtm, 2),
                'overall_sl_type': str((parse_overall_sl(strategy_cfg)[0] or {}).get('Type') or ''),
                'overall_sl_base': round(safe_float(parse_overall_sl(strategy_cfg)[1]), 2),
                'overall_sl_current': round(eff_sl, 2),
                'overall_sl_reentry_type': str((strategy_cfg.get('OverallReentryOnSL') or {}).get('Type') or 'None'),
                'overall_sl_reentry_done': ore_done,
                'overall_sl_reentry_count': safe_int((strategy_cfg.get('OverallReentryOnSL') or {}).get('Value') or 0),
                'overall_tgt_type': str((parse_overall_tgt(strategy_cfg)[0] or {}).get('Type') or ''),
                'overall_tgt_base': round(safe_float(parse_overall_tgt(strategy_cfg)[1]), 2),
                'overall_tgt_current': round(eff_tgt, 2),
                'overall_tgt_reentry_type': str((strategy_cfg.get('OverallReentryOnTarget') or {}).get('Type') or 'None'),
                'overall_tgt_reentry_done': ort_done,
                'overall_tgt_reentry_count': safe_int((strategy_cfg.get('OverallReentryOnTarget') or {}).get('Value') or 0),
                'dynamic_sl_threshold': round(safe_float(dyn_sl), 2),
                'ltp_source':     'broker_ws',
            })
            print('[OVERALL SL DEBUG]', {
                'mode': ctx.activation_mode,
                'trade_id': trade_id,
                'strategy_name': str(trade.get('name') or ''),
                'timestamp': ctx.now_ts,
                'current_mtm': round(current_mtm, 2),
                'base_overall_sl': round(safe_float(parse_overall_sl(strategy_cfg)[1]), 2),
                'reentry_done': ore_done,
                'cycle_number': ore_done + 1,
                'cycle_overall_sl': round(eff_sl, 2),
                'dynamic_sl_threshold': round(safe_float(dyn_sl), 2),
                'checked_stoploss': round(safe_float(dyn_sl or eff_sl), 2),
                'would_hit_cycle_sl': bool(eff_sl > 0 and current_mtm <= -eff_sl),
                'would_hit_dynamic_sl': bool(dyn_sl > 0 and current_mtm <= -dyn_sl),
                'skip_same_tick_overall': skip_same_tick_overall,
                'last_overall_event_at': last_overall_event_at,
                'last_overall_event_reason': last_overall_event_reason,
            })

            if skip_same_tick_overall:
                ctx.actions_taken.append(
                    f'{trade_id}: skipped same-tick overall recheck after {last_overall_event_reason}'
                )
                continue

            def _close_all(reason: str) -> None:
                for p in list((strat_entry or {}).get('open_positions', [])):
                    _lid = p.get('leg_id', '')
                    for i, l in enumerate(trade.get('legs') or []):
                        if isinstance(l, dict) and str(l.get('id') or '') == _lid:
                            close_leg_in_db(ctx.db, trade_id, i, p.get('ltp', 0), reason, ctx.now_ts, leg_id=_lid)
                for hdoc in ctx.db._db[COL_POSITIONS_HIST].find({'trade_id': trade_id, 'status': OPEN_LEG_STATUS}):
                    h_lid = str(hdoc.get('leg_id') or '')
                    ltp   = safe_float(hdoc.get('last_saw_price'))
                    ctx.db._db[COL_POSITIONS_HIST].update_one(
                        {'trade_id': trade_id, 'leg_id': h_lid},
                        {'$set': {
                            'exit_trade':     build_exit_trade_payload(ltp, reason, ctx.now_ts),
                            'status':         CLOSED_LEG_STATUS,
                            'last_saw_price': ltp,
                        }},
                    )

            # ── 1. Overall SL ─────────────────────────────────────────────
            osl_hit = (eff_sl > 0 and current_mtm <= -eff_sl) or (dyn_sl > 0 and current_mtm <= -dyn_sl)
            if osl_hit and not open_leg_ids:
                osl_hit = False

            if osl_hit:
                _close_all('overall_sl')
                ctx.actions_taken.append(f'{trade_id}: overall SL hit mtm={current_mtm}')
                ctx.hit_trade_ids.append(trade_id)
                ctx.hit_ltp_snapshots[trade_id] = list((strategy_map.get(trade_id) or {}).get('open_positions') or [])
                ore_type, ore_count = parse_overall_reentry_sl(strategy_cfg)
                if ore_type != 'None' and ore_count > 0 and ore_done < ore_count:
                    new_ids = requeue_all_legs_for_overall_reentry(ctx.db, trade, trade_id, strategy_cfg, ctx.now_ts, reentry_type=ore_type)
                    if new_ids:
                        next_cycle_sl = resolve_overall_cycle_value(
                            parse_overall_sl(strategy_cfg)[1],
                            ore_done + 1,
                        )
                        ctx.db._db[COL_ALGO_TRADES].update_one(
                            {'_id': trade_id},
                            {'$set': {
                                'overall_sl_reentry_done': ore_done + 1,
                                'peak_mtm': 0.0,
                                'current_overall_sl_threshold': next_cycle_sl,
                            }},
                        )
                        ctx.actions_taken.append(f'{trade_id}: overall SL reentry queued legs={new_ids}')
                    else:
                        square_off_trade(ctx.db, trade, ctx.now_ts, market_cache=ctx.market_cache)
                else:
                    square_off_trade(ctx.db, trade, ctx.now_ts, market_cache=ctx.market_cache)
                continue

            # ── 2. Overall Target ─────────────────────────────────────────
            if open_leg_ids and eff_tgt > 0 and current_mtm >= eff_tgt:
                _close_all('overall_target')
                ctx.actions_taken.append(f'{trade_id}: overall Target hit mtm={current_mtm}')
                ctx.hit_trade_ids.append(trade_id)
                ctx.hit_ltp_snapshots[trade_id] = list((strategy_map.get(trade_id) or {}).get('open_positions') or [])
                ort_type, ort_count = parse_overall_reentry_tgt(strategy_cfg)
                if ort_type != 'None' and ort_count > 0 and ort_done < ort_count:
                    new_ids = requeue_all_legs_for_overall_reentry(ctx.db, trade, trade_id, strategy_cfg, ctx.now_ts, reentry_type=ort_type)
                    if new_ids:
                        reset_sl_threshold = resolve_overall_cycle_value(
                            parse_overall_sl(strategy_cfg)[1],
                            ore_done,
                        )
                        ctx.db._db[COL_ALGO_TRADES].update_one(
                            {'_id': trade_id},
                            {'$set': {
                                'overall_tgt_reentry_done': ort_done + 1,
                                'peak_mtm': 0.0,
                                'current_overall_sl_threshold': reset_sl_threshold,
                            }},
                        )
                        ctx.actions_taken.append(f'{trade_id}: overall Target reentry queued legs={new_ids}')
                    else:
                        square_off_trade(ctx.db, trade, ctx.now_ts, market_cache=ctx.market_cache)
                else:
                    square_off_trade(ctx.db, trade, ctx.now_ts, market_cache=ctx.market_cache)
                continue

            # ── 3. LockAndTrail ───────────────────────────────────────────
            if open_leg_ids:
                lock_cfg = parse_lock_and_trail(strategy_cfg)
                lock_exit, lock_floor = check_lock_and_trail(lock_cfg, current_mtm, peak_mtm)
                if lock_exit:
                    _close_all('lock_and_trail')
                    square_off_trade(ctx.db, trade, ctx.now_ts, market_cache=ctx.market_cache)
                    ctx.actions_taken.append(f'{trade_id}: LockAndTrail exit mtm={current_mtm} floor={lock_floor}')
                    continue

            # ── 4. Overall Trail SL threshold update ──────────────────────
            if open_leg_ids:
                new_thresh = compute_next_overall_trail_sl(strategy_cfg, dyn_sl or eff_sl, safe_float(parse_overall_sl(strategy_cfg)[1]), peak_mtm)
                if new_thresh != (dyn_sl or eff_sl):
                    ctx.db._db[COL_ALGO_TRADES].update_one(
                        {'_id': trade_id},
                        {'$set': {'peak_mtm': peak_mtm, 'current_overall_sl_threshold': new_thresh}},
                    )
                    ctx.actions_taken.append(f'{trade_id}: overall trail SL {dyn_sl}→{new_thresh}')
                elif peak_mtm > safe_float(trade.get('peak_mtm')):
                    ctx.db._db[COL_ALGO_TRADES].update_one({'_id': trade_id}, {'$set': {'peak_mtm': peak_mtm}})

        elif past_exit:
            mark_trade_squared_off(ctx.db, trade_id)

    # ── Broker-level SL/Target check ─────────────────────────────────────
    broker_hit_ids, broker_ltp_snaps = check_broker_sl_target(
        ctx, running_trades, strategy_map, []
    )
    for tid in broker_hit_ids:
        if tid not in ctx.hit_trade_ids:
            ctx.hit_trade_ids.append(tid)
    ctx.hit_ltp_snapshots.update(broker_ltp_snaps)

    return TickResult(
        actions_taken=ctx.actions_taken,
        hit_trade_ids=ctx.hit_trade_ids,
        hit_ltp_snapshots=ctx.hit_ltp_snapshots,
        open_positions=open_positions,
        checked_at=ctx.now_ts,
    )


# ──────────────────────────────────────────────────────────────────────────────
# END OF trading_core.py
#
# How to use across execution modes:
#
#   from features.trading_core import TickContext, process_tick, process_pending_entries
#
#   # 1. Pre-load market cache once per trade_date
#   records = load_running_trades(db, trade_date, activation_mode)
#   cache   = preload_market_cache(db, trade_date, records)
#
#   # 2. For each candle minute (backtest loop or live timer):
#   for now_ts in candle_timestamps:
#       ctx    = TickContext(db=db, trade_date=trade_date,
#                            now_ts=now_ts, activation_mode=mode,
#                            market_cache=cache)
#
#       # 2a. Try to enter pending legs
#       entries = process_pending_entries(ctx, records)
#
#       # 2b. Reload after entries (new legs may have entered)
#       records = load_running_trades(db, trade_date, activation_mode)
#       broadcast(result)
# ──────────────────────────────────────────────────────────────────────────────
