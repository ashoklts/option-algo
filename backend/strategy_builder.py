"""
Strategy Builder
Assembles the complete AlgoTest backtest/live request payload.

Actual request structure:
{
  "strategy_id": null,
  "name": null,
  "start_date": "2025-10-01",
  "end_date":   "2025-10-30",
  "strategy": {
    "Ticker":                    "NIFTY",
    "TakeUnderlyingFromCashOrNot": "True",
    "TrailSLtoBreakeven":        "False",
    "SquareOffAllLegs":          "False",
    "EntryIndicators":           { ... },
    "ExitIndicators":            { ... },
    "StrategyType":              "StrategyType.IntradaySameDay",
    "MaxPositionInADay":         1,
    "ReentryTimeRestriction":    "None",
    "SkipInitialCandles":        0,
    "ListOfLegConfigs":          [ ...legs... ],
    "IdleLegConfigs":            { ...lazy_legs... },
    "OverallMomentum":           { "Type": "None", "Value": 0 },
    "OverallSL":                 { "Type": "None", "Value": 0 },
    "OverallTgt":                { "Type": "None", "Value": 0 },
    "OverallTrailSL":            { "Type": "None", "Value": {} },
    "LockAndTrail":              { "Type": "None", "Value": {} },
    "OverallReentrySL":          { "Type": "None", "Value": {} },
    "OverallReentryTgt":         { "Type": "None", "Value": {} },
    "WeeklyOldRegime":           true
  },
  "attributes": { "template": "Custom", "positional": "False" },
  "source": "WEB"
}
"""

import json
from enum import Enum
from typing import Optional

from features.leg_builder import (
    build_leg, stop_loss, target_profit, trail_sl, momentum, reentry, lot_config,
    PositionType, InstrumentKind, ExpiryKind, StrikeType, EntryType,
    LegTgtSLType, TrailSLType, MomentumType, ReentryType,
    strike_by_type, strike_by_closest_premium, strike_by_premium_range,
    strike_by_pct_of_atm, strike_by_straddle_width, strike_by_delta_range,
    DISABLED, DISABLED_OBJ,
)
from features.entry_exit_timing import intraday_session, IntradaySession, Presets
from features.reentry_lazy_leg import (
    re_asap, re_asap_reverse, re_cost, re_cost_reverse,
    re_momentum, re_momentum_reverse, re_lazy_leg,
    LazyLegRegistry,
)
from features.overall_momentum import overall_momentum, DISABLED_MOMENTUM


# ─── Strategy-level Enums ─────────────────────────────────────────────────────

class StrategyType(str, Enum):
    INTRADAY_SAME_DAY   = "StrategyType.IntradaySameDay"
    POSITIONAL          = "StrategyType.Positional"
    BTST                = "StrategyType.BTST"


class Ticker(str, Enum):
    NIFTY     = "NIFTY"
    BANKNIFTY = "BANKNIFTY"
    FINNIFTY  = "FINNIFTY"
    SENSEX    = "SENSEX"
    MIDCPNIFTY = "MIDCPNIFTY"


class OverallSLType(str, Enum):
    MTM                = "OverallTgtSLType.MTM"
    PREMIUM_PERCENTAGE = "OverallTgtSLType.PremiumPercentage"
    PERCENTAGE         = "OverallTgtSLType.PremiumPercentage"


class OverallTgtType(str, Enum):
    MTM                = "OverallTgtSLType.MTM"
    PREMIUM_PERCENTAGE = "OverallTgtSLType.PremiumPercentage"
    PERCENTAGE         = "OverallTgtSLType.PremiumPercentage"


class OverallTrailSLType(str, Enum):
    POINTS     = "TrailStopLossType.Points"
    PERCENTAGE = "TrailStopLossType.Percentage"


# ─── Overall SL / Target / Trail helpers ─────────────────────────────────────

# When any overall field is disabled — exact values from actual API
_DISABLED_OVERALL        = {"Type": "None", "Value": 0}   # OverallSL, OverallTgt, OverallMomentum
_DISABLED_OVERALL_OBJECT = {"Type": "None", "Value": {}}  # OverallTrailSL, LockAndTrail, OverallReentry*


def overall_sl(sl_type: OverallSLType, value: float) -> dict:
    """e.g. overall_sl(OverallSLType.MTM, 2500)"""
    return {"Type": sl_type.value, "Value": value}


def overall_target(tgt_type: OverallTgtType, value: float) -> dict:
    """e.g. overall_target(OverallTgtType.MTM, 5000)"""
    return {"Type": tgt_type.value, "Value": value}


def overall_trail_sl(
    trail_type: OverallTrailSLType,
    instrument_move: float,
    sl_move: float,
) -> dict:
    return {
        "Type": trail_type.value,
        "Value": {"InstrumentMove": instrument_move, "StopLossMove": sl_move},
    }


def overall_reentry(re_type: ReentryType, count: int = 1) -> dict:
    return {"Type": re_type.value, "Value": {"ReentryCount": count}}


# ─── Strategy Builder ────────────────────────────────────────────────────────

def build_strategy(
    # ── Date range ───────────────────────────────
    start_date: str,                                # "2025-10-01"
    end_date:   str,                                # "2025-10-30"

    # ── Legs ─────────────────────────────────────
    legs:       list,                               # list of build_leg() dicts
    idle_legs:  dict              = None,           # { "lazy1": build_leg(...), ... }

    # ── Timing ───────────────────────────────────
    session:    IntradaySession   = None,           # intraday_session((9,35),(15,15))
    entry_hour: int               = 9,
    entry_min:  int               = 35,
    exit_hour:  int               = 15,
    exit_min:   int               = 15,

    # ── Instrument ───────────────────────────────
    ticker:     Ticker            = Ticker.NIFTY,

    # ── Strategy settings ────────────────────────
    strategy_type: StrategyType   = StrategyType.INTRADAY_SAME_DAY,
    max_positions_per_day: int    = 1,
    skip_initial_candles:  int    = 0,
    reentry_time_restriction: str = "None",
    trail_sl_to_breakeven: bool   = False,
    square_off_all_legs:   bool   = False,
    take_underlying_from_cash: bool = True,
    weekly_old_regime:     bool   = True,

    # ── Overall SL / Target / Trail ──────────────
    overall_sl_config:       dict = None,
    overall_tgt_config:      dict = None,
    overall_trail_sl_config: dict = None,
    lock_and_trail:          dict = None,
    overall_reentry_sl:      dict = None,
    overall_reentry_tgt:     dict = None,
    overall_momentum:        dict = None,

    # ── Meta ─────────────────────────────────────
    strategy_id: Optional[str]    = None,
    name:        Optional[str]    = None,
    positional:  bool             = False,
) -> dict:
    """
    Build the complete AlgoTest backtest/live request payload.

    Parameters
    ----------
    start_date  : "YYYY-MM-DD"
    end_date    : "YYYY-MM-DD"
    legs        : list of build_leg() dicts
    idle_legs   : dict of lazy leg configs { name: build_leg(...) }
    session     : IntradaySession object  (overrides entry/exit hour/min if provided)
    entry_hour/min : entry time (used only if session is None)
    exit_hour/min  : exit time  (used only if session is None)
    ticker      : NIFTY / BANKNIFTY / etc.
    strategy_type : IntradaySameDay / Positional / BTST
    overall_sl_config    : overall_sl(...)    or None (disabled)
    overall_tgt_config   : overall_target(...)or None (disabled)
    overall_trail_sl_config : overall_trail_sl(...) or None (disabled)
    ...

    Returns
    -------
    dict — full request payload ready to send to AlgoTest API
    """

    # Resolve timing
    if session is not None:
        entry_indicators = session.entry_indicators
        exit_indicators  = session.exit_indicators
    else:
        s = intraday_session((entry_hour, entry_min), (exit_hour, exit_min))
        entry_indicators = s.entry_indicators
        exit_indicators  = s.exit_indicators

    return {
        "strategy_id": strategy_id,
        "name":        name,
        "start_date":  start_date,
        "end_date":    end_date,
        "strategy": {
            "Ticker":                      ticker.value,
            "TakeUnderlyingFromCashOrNot": str(take_underlying_from_cash),
            "TrailSLtoBreakeven":          str(trail_sl_to_breakeven),
            "SquareOffAllLegs":            str(square_off_all_legs),
            "EntryIndicators":             entry_indicators,
            "ExitIndicators":              exit_indicators,
            "StrategyType":                strategy_type.value,
            "MaxPositionInADay":           max_positions_per_day,
            "ReentryTimeRestriction":      reentry_time_restriction,
            "SkipInitialCandles":          skip_initial_candles,
            "ListOfLegConfigs":            legs,
            "IdleLegConfigs":              idle_legs or {},
            "OverallMomentum":             overall_momentum        or _DISABLED_OVERALL,
            "OverallSL":                   overall_sl_config       or _DISABLED_OVERALL,
            "OverallTgt":                  overall_tgt_config      or _DISABLED_OVERALL,
            "OverallTrailSL":              overall_trail_sl_config or _DISABLED_OVERALL_OBJECT,
            "LockAndTrail":                lock_and_trail          or _DISABLED_OVERALL_OBJECT,
            "OverallReentrySL":            overall_reentry_sl      or _DISABLED_OVERALL_OBJECT,
            "OverallReentryTgt":           overall_reentry_tgt     or _DISABLED_OVERALL_OBJECT,
            "WeeklyOldRegime":             weekly_old_regime,
        },
        "attributes": {
            "template":   "Custom",
            "positional": str(positional),
        },
        "source": "WEB",
    }


# ─── Demo ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    print("=" * 65)
    print("Strategy Builder Demo")
    print("=" * 65)

    # ── Example 1: Exact match to the actual API request ─────────
    print("\n1. Short Straddle — ATM CE + PE, SL 30%, Target 25% (Oct 2025)")
    print("-" * 65)

    ce = build_leg(
        position=PositionType.SELL,
        instrument=InstrumentKind.CE,
        expiry=ExpiryKind.WEEKLY,
        lots=1,
        entry_type=EntryType.STRIKE_TYPE,
        strike_parameter=strike_by_type(StrikeType.ATM),
        leg_stoploss=stop_loss(LegTgtSLType.PERCENTAGE, 30),
        leg_target=target_profit(LegTgtSLType.PERCENTAGE, 25),
    )

    pe = build_leg(
        position=PositionType.SELL,
        instrument=InstrumentKind.PE,
        expiry=ExpiryKind.WEEKLY,
        lots=1,
        entry_type=EntryType.STRIKE_TYPE,
        strike_parameter=strike_by_type(StrikeType.ATM),
        leg_stoploss=stop_loss(LegTgtSLType.PERCENTAGE, 30),
        leg_target=target_profit(LegTgtSLType.PERCENTAGE, 25),
    )

    request = build_strategy(
        start_date="2025-10-01",
        end_date="2025-10-30",
        legs=[ce, pe],
        entry_hour=9, entry_min=35,
        exit_hour=15, exit_min=15,
        ticker=Ticker.NIFTY,
        strategy_type=StrategyType.INTRADAY_SAME_DAY,
    )

    print(json.dumps(request, indent=2))

    # ── Example 2: With Overall SL + Trail + Momentum ────────────
    print("\n\n2. Same strategy + Overall SL 20% + Overall Trail + Overall Momentum")
    print("-" * 65)

    request2 = build_strategy(
        start_date="2025-10-01",
        end_date="2025-10-30",
        legs=[ce, pe],
        session=Presets.STANDARD,          # 9:35 → 15:15
        ticker=Ticker.BANKNIFTY,
        strategy_type=StrategyType.INTRADAY_SAME_DAY,
        overall_sl_config=overall_sl(OverallSLType.PERCENTAGE, 20),
        overall_tgt_config=overall_target(OverallTgtType.PERCENTAGE, 15),
        overall_trail_sl_config=overall_trail_sl(OverallTrailSLType.PERCENTAGE, 10, 5),
        overall_momentum=overall_momentum(MomentumType.PERCENTAGE_DOWN, 8),  # ← enter only when combined premium falls 8%
        max_positions_per_day=2,
    )

    print(json.dumps(request2, indent=2))

    # ── Example 3: With Lazy Legs (IdleLegConfigs) ────────────────
    print("\n\n3. With Lazy Leg")
    print("-" * 65)

    lazy_leg = build_leg(
        position=PositionType.BUY,
        instrument=InstrumentKind.CE,
        expiry=ExpiryKind.WEEKLY,
        lots=1,
        entry_type=EntryType.CLOSEST_PREMIUM,
        strike_parameter=strike_by_closest_premium(30),
    )

    ce_with_lazy = build_leg(
        position=PositionType.SELL,
        instrument=InstrumentKind.CE,
        expiry=ExpiryKind.WEEKLY,
        lots=1,
        entry_type=EntryType.STRIKE_TYPE,
        strike_parameter=strike_by_type(StrikeType.ATM),
        leg_stoploss=stop_loss(LegTgtSLType.PERCENTAGE, 30),
        leg_reentry_sl=reentry(ReentryType.NEXT_LEG, next_leg_ref="lazy1"),
    )

    request3 = build_strategy(
        start_date="2025-10-01",
        end_date="2025-10-30",
        legs=[ce_with_lazy],
        idle_legs={"lazy1": {**lazy_leg, "id": "lazy1"}},
        session=Presets.STANDARD,
        ticker=Ticker.NIFTY,
    )

    print(json.dumps(request3, indent=2))
