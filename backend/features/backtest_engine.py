"""
backtest_engine.py
──────────────────
Bulk-load all data once → process fully in-memory.
No per-candle DB queries → fast execution.
"""

from datetime import datetime, timedelta
from typing import Optional, Tuple
from collections import defaultdict

try:
    from .mongo_data        import MongoData
    from .lazy_leg          import process_lazy_legs
    from .expiry_config     import get_expiry_weekday_from_rules
    from .range_breakout    import (
        parse_range_breakout,
        compute_range,
        compute_btst_range,
        compute_positional_range,
        compute_dte,
        find_day_by_dte,
        find_breakout_entry,
    )
    from .overall_settings  import (
        parse_overall_sl,
        parse_overall_tgt,
        parse_overall_reentry_sl,
        parse_overall_reentry_tgt,
        parse_lock_and_trail,
        parse_overall_trail_sl,
        find_overall_sl_exit_time,
        find_overall_tgt_exit_time,
        find_lock_exit_time,
        find_lock_trail_exit_time,
        find_trail_sl_exit_time,
        resolve_all_exits,
        run_overall_reentry,
        run_overall_reentry_tgt,
    )
    from .debug_flags       import debug_print
except ImportError:
    from mongo_data        import MongoData
    from lazy_leg          import process_lazy_legs
    from expiry_config     import get_expiry_weekday_from_rules
    from range_breakout    import (
        parse_range_breakout,
        compute_range,
        compute_btst_range,
        compute_positional_range,
        compute_dte,
        find_day_by_dte,
        find_breakout_entry,
    )
    from overall_settings  import (
        parse_overall_sl,
        parse_overall_tgt,
        parse_overall_reentry_sl,
        parse_overall_reentry_tgt,
        parse_lock_and_trail,
        parse_overall_trail_sl,
        find_overall_sl_exit_time,
        find_overall_tgt_exit_time,
        find_lock_exit_time,
        find_lock_trail_exit_time,
        find_trail_sl_exit_time,
        resolve_all_exits,
        run_overall_reentry,
        run_overall_reentry_tgt,
    )
    from debug_flags       import debug_print


# ─── Instrument Config ────────────────────────────────────────────────────────

STRIKE_STEPS = {
    "NIFTY":      50,
    "BANKNIFTY":  100,
    "FINNIFTY":   50,
    "MIDCPNIFTY": 25,
    "SENSEX":     100,
}


# ─── In-Memory Index ──────────────────────────────────────────────────────────

class DataIndex:
    """
    Built once from bulk-loaded candles.
    All lookups are O(1) dict access — no DB calls during backtest.

    candle_index  : (date, time, expiry, strike, type)  → close price
    spot_index    : (date, time)                         → spot price
    expiry_index  : date                                 → sorted list of expiries
    time_index    : (date, expiry, strike, type)         → sorted list of "HH:MM" strings
    """

    def __init__(self, raw_candles: list):
        self.candle_index:  dict = {}
        self.high_index:    dict = {}
        self.low_index:     dict = {}
        self.spot_index:    dict = {}
        self.expiry_index:  dict = defaultdict(set)
        self._time_map:     dict = defaultdict(list)
        self._all_times:    dict = defaultdict(set)
        self.strikes_index: dict = defaultdict(set)
        
        c_i = self.candle_index
        h_i = self.high_index
        l_i = self.low_index
        s_i = self.spot_index
        e_i = self.expiry_index
        t_m = self._time_map
        a_t = self._all_times
        st_i = self.strikes_index

        for c in raw_candles:
            try:
                ts = c["timestamp"]
                date_str = ts[:10]
                time_str = ts[11:16]

                expiry = c["expiry"]
                strike = int(c["strike"])
                otype  = c["type"]
                close  = float(c["close"])
                spot   = float(c.get("spot_price", 0))
                high   = float(c.get("high", close))
                low    = float(c.get("low",  close))

                key = (date_str, time_str, expiry, strike, otype)
                
                c_i[key] = close
                h_i[key] = high
                l_i[key] = low
                s_i[(date_str, time_str)] = spot
                e_i[date_str].add(expiry)
                t_m[(date_str, expiry, strike, otype)].append(time_str)
                a_t[date_str].add(time_str)
                st_i[(date_str, time_str, expiry, otype)].add(strike)
            except KeyError:
                pass

        self.expiry_index  = {d: sorted(v) for d, v in self.expiry_index.items()}
        self._time_map     = {k: sorted(set(v)) for k, v in self._time_map.items()}
        self._all_times    = {d: sorted(v)      for d, v in self._all_times.items()}
        self.strikes_index = {k: sorted(v)      for k, v in self.strikes_index.items()}

    def get_close(self, date: str, time: str, expiry: str,
                  strike: int, otype: str) -> Optional[float]:
        return self.candle_index.get((date, time, expiry, strike, otype))

    def get_spot(self, date: str, time: str) -> Optional[float]:
        return self.spot_index.get((date, time))

    def get_expiries(self, date: str) -> list:
        return self.expiry_index.get(date, [])

    def get_candles_range(self, date: str, start_time: str, end_time: str,
                          expiry: str, strike: int, otype: str) -> list:
        times = self._time_map.get((date, expiry, strike, otype), [])
        result = []
        for t in times:
            if start_time <= t <= end_time:
                key = (date, t, expiry, strike, otype)
                result.append({
                    "time":  t,
                    "close": self.candle_index[key],
                    "high":  self.high_index.get(key, self.candle_index[key]),
                    "low":   self.low_index.get(key,  self.candle_index[key]),
                })
        return result


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _extract_time(indicators: dict) -> Tuple[int, int]:
    for node in indicators.get("Value", []):
        val = node.get("Value", {})
        if val.get("IndicatorName") == "IndicatorType.TimeIndicator":
            p = val.get("Parameters", {})
            return int(p["Hour"]), int(p["Minute"])
    return 9, 15


def _add_one_minute(time_str: str) -> str:
    h, m = int(time_str[:2]), int(time_str[3:])
    m += 1
    if m >= 60:
        h, m = h + 1, 0
    return f"{h:02d}:{m:02d}"


def _get_trading_days(start: str, end: str, holidays: set) -> list:
    days   = []
    cur    = datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end,   "%Y-%m-%d").date()
    while cur <= end_dt:
        if cur.weekday() < 5 and cur.strftime("%Y-%m-%d") not in holidays:
            days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return days


def _find_atm(spot: float, step: int) -> int:
    return round(spot / step) * step


_CE_OFFSETS = {
    "StrikeType.ATM":  0,
    "StrikeType.OTM1": 1,  "StrikeType.OTM2": 2,  "StrikeType.OTM3": 3,
    "StrikeType.OTM4": 4,  "StrikeType.OTM5": 5,
    "StrikeType.ITM1":-1,  "StrikeType.ITM2":-2,  "StrikeType.ITM3":-3,
    "StrikeType.ATMp1": 1, "StrikeType.ATMp2": 2,
    "StrikeType.ATMm1":-1, "StrikeType.ATMm2":-2,
}
_PE_OFFSETS = {k: -v for k, v in _CE_OFFSETS.items()}
_PE_OFFSETS["StrikeType.ATM"] = 0


def _resolve_strike(spot: float, param: str, otype: str, step: int) -> int:
    """ATM / OTM / ITM offset-based strike resolution."""
    atm    = _find_atm(spot, step)
    table  = _CE_OFFSETS if otype == "CE" else _PE_OFFSETS
    offset = table.get(param, 0)
    return atm + (offset * step)


def _resolve_strike_by_premium(
    idx, day: str, time_str: str,
    expiry: str, otype: str,
    target_premium: float,
) -> Optional[int]:
    """
    Premium-based strike resolution.
    Scans all available strikes at the given time and returns
    the strike whose current premium is closest to target_premium.
    """
    strikes = idx.strikes_index.get((day, time_str, expiry, otype), [])
    if not strikes:
        return None

    best_strike = None
    best_diff   = float("inf")
    for s in strikes:
        price = idx.get_close(day, time_str, expiry, s, otype)
        if price is None:
            continue
        diff = abs(price - target_premium)
        if diff < best_diff:
            best_diff   = diff
            best_strike = s
    return best_strike


_WEEKDAY_MAP = {
    "Monday": 0, "Tuesday": 1, "Wednesday": 2,
    "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6,
}


def _resolve_expiry(date_str: str, kind: str, expiries: list,
                    expiry_weekday: Optional[str] = None) -> Optional[str]:
    """
    Pick the correct expiry from the sorted `expiries` list.

    For Weekly / NextWeekly types, uses `expiry_weekday` (e.g. "Thursday")
    to filter expiries to only those falling on the correct weekday.
    This handles historical expiry-day changes (NIFTY Thu→Tue, etc.).

    Falls back to positional selection (expiries[0], expiries[1]) when
    `expiry_weekday` is None or no matching expiry is found.
    """
    if not expiries:
        return None

    if kind in ("ExpiryType.Weekly", "ExpiryType.NextWeekly"):
        if expiry_weekday:
            target_wd = _WEEKDAY_MAP.get(expiry_weekday, 3)   # default Thursday
            weekly = [
                e for e in expiries
                if datetime.strptime(e, "%Y-%m-%d").weekday() == target_wd
            ]
        else:
            weekly = expiries   # no filter — use all expiries in order

        if kind == "ExpiryType.Weekly":
            return weekly[0] if weekly else expiries[0]
        else:   # NextWeekly
            return weekly[1] if len(weekly) > 1 else (weekly[0] if weekly else expiries[0])

    cur_mo = date_str[:7]
    this   = [e for e in expiries if e[:7] == cur_mo]
    if kind == "ExpiryType.Monthly":
        return this[-1] if this else expiries[0]
    if kind == "ExpiryType.NextMonthly":
        yr, mo = int(date_str[:4]), int(date_str[5:7])
        nxt    = f"{yr}-{mo+1:02d}" if mo < 12 else f"{yr+1}-01"
        nxt_ex = [e for e in expiries if e[:7] == nxt]
        return nxt_ex[-1] if nxt_ex else None
    return expiries[0]


def _calc_trigger_price(entry_price: float, entry_spot: float, position: str,
                        sl_or_tgt_type: str, val: float, is_sl: bool) -> Optional[float]:
    """
    Compute the SL or Target trigger level.
    is_sl=True  → SL  (SELL: price rises, BUY: price falls)
    is_sl=False → Tgt (SELL: price falls, BUY: price rises)
    """
    if sl_or_tgt_type == "None" or val <= 0:
        return None
    is_underlying = "Underlying" in sl_or_tgt_type
    is_pct        = "Percentage"  in sl_or_tgt_type
    base          = entry_spot if is_underlying else entry_price

    if position == "SELL":
        # SL: price goes UP; Target: price goes DOWN
        if is_sl:
            return base * (1 + val / 100) if is_pct else base + val
        else:
            return base * (1 - val / 100) if is_pct else base - val
    else:  # BUY
        # SL: price goes DOWN; Target: price goes UP
        if is_sl:
            return base * (1 - val / 100) if is_pct else base - val
        else:
            return base * (1 + val / 100) if is_pct else base + val


def _check_sl_target(candles, entry_price, entry_spot, position,
                     sl_type, sl_val, tgt_type, tgt_val,
                     idx, day,
                     trail_type="None", trail_x=0.0, trail_y=0.0):
    """
    Scan candles for SL (with optional TSL) or Target trigger.

    Premium-based (Points / Percentage):
      SELL SL     → candle_high >= sl_px   → exit at sl_px
      SELL Target → candle_low  <= tgt_px  → exit at tgt_px
      BUY  SL     → candle_low  <= sl_px   → exit at sl_px
      BUY  Target → candle_high >= tgt_px  → exit at tgt_px

    Underlying-based (UnderlyingPoints / UnderlyingPercentage):
      SELL SL     → spot >= sl_px   → exit at option close
      SELL Target → spot <= tgt_px  → exit at option close
      BUY  SL     → spot <= sl_px   → exit at option close
      BUY  Target → spot >= tgt_px  → exit at option close

    TSL (trail_type != "None"):
      SELL: favorable = entry_price - candle_low  → SL moves DOWN (ratchet)
      BUY : favorable = candle_high - entry_price → SL moves UP   (ratchet)
      steps   = int(favorable // trail_x_pts)
      new_sl  = initial_sl ± (steps * trail_y_pts)   [absolute, not incremental]
    """
    is_underlying_sl  = sl_type  != "None" and "Underlying" in sl_type
    is_underlying_tgt = tgt_type != "None" and "Underlying" in tgt_type

    sl_px  = _calc_trigger_price(entry_price, entry_spot, position, sl_type,  sl_val,  is_sl=True)
    tgt_px = _calc_trigger_price(entry_price, entry_spot, position, tgt_type, tgt_val, is_sl=False)

    # ── TSL setup ─────────────────────────────────────────────────────────────
    use_trail = trail_type != "None" and trail_x > 0 and trail_y > 0 and sl_px is not None
    cur_sl_px = sl_px   # mutable SL level (updated per candle by TSL)

    if use_trail:
        if "Percentage" in trail_type:
            # % always relative to entry_price (fixed reference)
            trail_x_pts = entry_price * trail_x / 100
            trail_y_pts = entry_price * trail_y / 100
        else:
            trail_x_pts = trail_x
            trail_y_pts = trail_y

    for c in candles:
        time_str = c["time"]

        # ── Update TSL (before SL check — favorable extreme drives ratchet) ──
        if use_trail and cur_sl_px is not None:
            if position == "SELL":
                favorable = entry_price - c["low"]    # price fell = good for SELL
            else:
                favorable = c["high"] - entry_price   # price rose = good for BUY

            if favorable > 0:
                steps  = int(favorable // trail_x_pts)
                new_sl = (sl_px - steps * trail_y_pts if position == "SELL"
                          else sl_px + steps * trail_y_pts)
                # Ratchet: SL only moves in the favorable direction, never reverses
                if position == "SELL":
                    cur_sl_px = min(cur_sl_px, new_sl)   # SL only moves DOWN
                else:
                    cur_sl_px = max(cur_sl_px, new_sl)   # SL only moves UP

        # ── Check SL (cur_sl_px may have been updated by TSL above) ──────────
        if position == "SELL":
            if cur_sl_px is not None:
                if is_underlying_sl:
                    spot = idx.get_spot(day, time_str)
                    if spot and spot >= cur_sl_px:
                        return round(c["close"], 2), time_str, "SL"
                elif c["high"] >= cur_sl_px:
                    return round(cur_sl_px, 2), time_str, "SL"
            if tgt_px is not None:
                if is_underlying_tgt:
                    spot = idx.get_spot(day, time_str)
                    if spot and spot <= tgt_px:
                        return round(c["close"], 2), time_str, "Target"
                elif c["low"] <= tgt_px:
                    return round(tgt_px, 2), time_str, "Target"
        else:  # BUY
            if cur_sl_px is not None:
                if is_underlying_sl:
                    spot = idx.get_spot(day, time_str)
                    if spot and spot <= cur_sl_px:
                        return round(c["close"], 2), time_str, "SL"
                elif c["low"] <= cur_sl_px:
                    return round(cur_sl_px, 2), time_str, "SL"
            if tgt_px is not None:
                if is_underlying_tgt:
                    spot = idx.get_spot(day, time_str)
                    if spot and spot >= tgt_px:
                        return round(c["close"], 2), time_str, "Target"
                elif c["high"] >= tgt_px:
                    return round(tgt_px, 2), time_str, "Target"

    return None, None, None


def _calc_pnl(position, entry, exit_, lots, lot_size):
    diff = (entry - exit_) if position == "SELL" else (exit_ - entry)
    return round(diff * lots * lot_size, 2)


def _pick_strike(idx, day, time_str, expiry, otype, spot,
                 entry_type, strike_param, step) -> Optional[int]:
    """
    Unified strike picker — works for both entry types.
    EntryByStrikeType   : ATM/OTM/ITM offset from spot
    EntryByPremium      : find strike closest to target premium
    EntryByPremiumRange : use the mid-point of the premium band

    Note:
    The current candle cache does not expose option greeks, so delta-based
    selectors are approximated using ATM instead of failing the whole run.
    """
    if entry_type == "EntryType.EntryByPremium":
        target = float(strike_param)
        return _resolve_strike_by_premium(idx, day, time_str, expiry, otype, target)

    if entry_type == "EntryType.EntryByPremiumRange" and isinstance(strike_param, dict):
        lower = float(strike_param.get("LowerRange", 0) or 0)
        upper = float(strike_param.get("UpperRange", lower) or lower)
        target = (lower + upper) / 2
        return _resolve_strike_by_premium(idx, day, time_str, expiry, otype, target)

    if entry_type in ("EntryType.EntryByDelta", "EntryType.EntryByDeltaRange"):
        return _resolve_strike(spot, "StrikeType.ATM", otype, step)

    if entry_type == "EntryType.EntryByAtmMultiplier":
        try:
            scaled_spot = float(spot) * float(strike_param)
            return _find_atm(scaled_spot, step)
        except Exception:
            return _resolve_strike(spot, "StrikeType.ATM", otype, step)

    if entry_type in ("EntryType.EntryByStraddlePrice", "EntryType.EntryByPremiumCloseToStraddle") and isinstance(strike_param, dict):
        strike_kind = strike_param.get("StrikeKind", "StrikeType.ATM")
        return _resolve_strike(spot, strike_kind, otype, step)

    if isinstance(strike_param, str):
        return _resolve_strike(spot, strike_param, otype, step)

    return _resolve_strike(spot, "StrikeType.ATM", otype, step)


def _flip_position(position: str) -> str:
    return "BUY" if position == "SELL" else "SELL"


def _find_momentum_entry(idx, day: str, scan_start: str, exit_time: str,
                         expiry: str, strike: int, otype: str,
                         base_price: float, momentum_type: str,
                         momentum_val: float):
    """
    Scan for momentum trigger.

    Premium-based  (PointsUp / PercentageUp)         → check candle HIGH  >= target
    Underlying-based (UnderlyingPointsUp / UnderlyingPercentageUp) → check spot  >= target

    Returns (trigger_time, target_price) or (None, None).
    """
    is_underlying = "Underlying" in momentum_type
    is_pct        = "Percentage" in momentum_type

    if "Up" in momentum_type:
        target = (base_price * (1 + momentum_val / 100) if is_pct
                  else base_price + momentum_val)

        if is_underlying:
            for t in idx._all_times.get(day, []):
                if t < scan_start or t > exit_time:
                    continue
                spot = idx.get_spot(day, t)
                if spot and spot >= target:
                    return t, round(target, 2)
        else:
            for c in idx.get_candles_range(day, scan_start, exit_time, expiry, strike, otype):
                if c["high"] >= target:
                    return c["time"], round(target, 2)

    else:  # Down
        target = (base_price * (1 - momentum_val / 100) if is_pct
                  else base_price - momentum_val)

        if is_underlying:
            for t in idx._all_times.get(day, []):
                if t < scan_start or t > exit_time:
                    continue
                spot = idx.get_spot(day, t)
                if spot and spot <= target:
                    return t, round(target, 2)
        else:
            for c in idx.get_candles_range(day, scan_start, exit_time, expiry, strike, otype):
                if c["low"] <= target:
                    return c["time"], round(target, 2)

    return None, None


def _find_momentum_reentry(candles: list, base_price: float,
                           momentum_type: str, momentum_val: float):
    """
    Scan candles from base_price and return (time, price) when momentum is achieved.

    PointsUp / PercentageUp    → wait for price to RISE by X
    PointsDown / PercentageDown → wait for price to FALL by X
    """
    if "Up" in momentum_type:
        target = (base_price * (1 + momentum_val / 100)
                  if "Percentage" in momentum_type
                  else base_price + momentum_val)
        for c in candles:
            if c["close"] >= target:
                return c["time"], round(c["close"], 2)
    else:  # Down
        target = (base_price * (1 - momentum_val / 100)
                  if "Percentage" in momentum_type
                  else base_price - momentum_val)
        for c in candles:
            if c["close"] <= target:
                return c["time"], round(c["close"], 2)
    return None, None


def _find_at_cost_reentry(candles: list, cost_price: float,
                           position: str, exit_reason: str):
    """
    Scan candles and return (time, price) when price returns to cost_price.

    BUY  + SL hit     → price fell below cost → wait for price to RISE back  (>= cost)
    SELL + SL hit     → price rose above cost → wait for price to FALL back  (<= cost)
    BUY  + Target hit → price rose above cost → wait for price to FALL back  (<= cost)
    SELL + Target hit → price fell below cost → wait for price to RISE back  (>= cost)
    """
    wait_for_rise = (
        (position == "BUY"  and exit_reason == "SL") or
        (position == "SELL" and exit_reason == "Target")
    )
    for c in candles:
        price = c["close"]
        if wait_for_rise  and price >= cost_price:
            return c["time"], round(price, 2)
        if not wait_for_rise and price <= cost_price:
            return c["time"], round(price, 2)
    return None, None


def _calc_momentum_target(base_price: float, momentum_type: str, momentum_val: float) -> float:
    if "Up" in momentum_type:
        return base_price * (1 + momentum_val / 100) if "Percentage" in momentum_type else base_price + momentum_val
    else:
        return base_price * (1 - momentum_val / 100) if "Percentage" in momentum_type else base_price - momentum_val


def _process_leg(idx, day, entry_time, exit_time,
                 expiry, initial_strike, otype, position,
                 sl_type, sl_val, tgt_type, tgt_val,
                 reentry_sl_count: int, reentry_tp_count: int,
                 reentry_sl_type: str, reentry_tp_type: str,
                 lots: int, lot_size: int,
                 entry_type: str, strike_param: str, step: int,
                 momentum_type: str = "None", momentum_val: float = 0,
                 override_entry_px: float = None,
                 override_base_px: float = None,
                 strategy_entry_time: str = None,
                 trail_type: str = "None", trail_x: float = 0.0,
                 trail_y: float = 0.0,
                 reentry_sl_next_ref: str = None,
                 reentry_tp_next_ref: str = None) -> dict:
    """
    Process a single leg with re-entry support.

    ReentryType.Immediate        → same position, new ATM strike
    ReentryType.ImmediateReverse → flip position (BUY↔SELL), new ATM strike
    Both: same option type (CE stays CE), instant re-entry at exit time
    """
    sub_trades            = []
    sl_left               = reentry_sl_count
    tp_left               = reentry_tp_count
    reentry_number        = 0        # 0 = Initial, 1 = first reentry, 2 = second, …
    cur_time              = entry_time
    cur_strike            = initial_strike
    cur_position          = position
    total_pnl             = 0.0
    forced_entry_price    = None   # used for AtCost: override entry with cost price
    next_leg_ref          = None   # set when ReentryType.NextLeg is triggered
    next_leg_trigger_time = None
    # listen_time = when momentum base price is measured (shifts to exit_at after each trade)
    listen_time   = strategy_entry_time or entry_time

    while True:
        is_first = len(sub_trades) == 0

        # AtCost re-entry: use the original cost price, not the candle close
        if forced_entry_price is not None:
            entry_price        = forced_entry_price
            forced_entry_price = None   # consume it
        else:
            entry_price = idx.get_close(day, cur_time, expiry, cur_strike, otype)
        if entry_price is None:
            break

        # Spot at entry — needed for Underlying SL/Target calculation
        entry_spot = idx.get_spot(day, cur_time) or 0.0

        # Pre-compute SL / Target trigger levels for display
        sl_display  = _calc_trigger_price(entry_price, entry_spot, cur_position,
                                          sl_type,  sl_val,  is_sl=True)
        tgt_display = _calc_trigger_price(entry_price, entry_spot, cur_position,
                                          tgt_type, tgt_val, is_sl=False)

        # Momentum display info — for ALL sub_trades when LegMomentum is active
        if momentum_type != "None" and momentum_val > 0:
            if is_first and override_base_px is not None:
                base_px = override_base_px   # pre-computed in run_backtest
            else:
                base_px = idx.get_close(day, listen_time, expiry, cur_strike, otype)
            if base_px is not None:
                sub_base_price   = round(base_px, 2)
                sub_target_price = round(_calc_momentum_target(base_px, momentum_type, momentum_val), 2)
            else:
                sub_base_price = sub_target_price = None
        else:
            sub_base_price = sub_target_price = None

        scan_start = _add_one_minute(cur_time)

        if scan_start > exit_time:
            exit_price  = entry_price
            exit_at     = cur_time
            exit_reason = "Time Exit"
        else:
            candles = idx.get_candles_range(day, scan_start, exit_time, expiry, cur_strike, otype)
            exit_price, exit_at, exit_reason = _check_sl_target(
                candles, entry_price, entry_spot, cur_position,
                sl_type, sl_val, tgt_type, tgt_val,
                idx, day,
                trail_type=trail_type, trail_x=trail_x, trail_y=trail_y,
            )
            if exit_price is None:
                exit_price  = idx.get_close(day, exit_time, expiry, cur_strike, otype) or entry_price
                exit_at     = exit_time
                exit_reason = "Time Exit"

        pnl        = _calc_pnl(cur_position, entry_price, exit_price, lots, lot_size)
        total_pnl += pnl

        if is_first:
            re_type = "Initial"
        elif exit_reason == "SL":
            re_type = reentry_sl_type.replace("ReentryType.", "")
        else:
            re_type = reentry_tp_type.replace("ReentryType.", "")

        trade = {
            "entry_date":          day,
            "entry_time":          cur_time,
            "entry_action":        cur_position,
            "entry_price":         round(entry_price, 2),
            "entry_spot":          round(entry_spot, 2),
            "sl_price":            round(sl_display,  2) if sl_display  is not None else None,
            "tgt_price":           round(tgt_display, 2) if tgt_display is not None else None,
            "strike":              cur_strike,
            "option_type":         otype,
            "exit_date":           day,
            "exit_time":           exit_at,
            "exit_action":         _flip_position(cur_position),
            "exit_price":          round(exit_price, 2),
            "exit_reason":         exit_reason,
            "reentry_type":        re_type,
            "reentry_number":      reentry_number,
            "pnl":                 pnl,
        }

        # Momentum info — shown for ALL sub_trades when LegMomentum is active
        if sub_base_price is not None:
            # ATM at listen_time
            spot_listen    = idx.get_spot(day, listen_time)
            atm_strike     = _find_atm(spot_listen, step) if spot_listen else None
            atm_px_listen  = idx.get_close(day, listen_time, expiry, atm_strike, otype) if atm_strike else None

            # Spot & ATM at actual entry time
            spot_entry     = idx.get_spot(day, cur_time)
            atm_px_entry   = idx.get_close(day, cur_time, expiry, atm_strike, otype) if atm_strike else None

            trade["momentum_type"]              = momentum_type.replace("MomentumType.", "")
            trade["momentum_value"]             = momentum_val
            trade["momentum_listen_time"]       = listen_time
            trade["spot_at_listen_time"]        = round(spot_listen,   2) if spot_listen  else None
            trade["atm_strike"]                 = atm_strike
            trade["atm_price_at_listen_time"]   = round(atm_px_listen, 2) if atm_px_listen else None
            trade["momentum_base_price"]        = sub_base_price
            trade["momentum_target_price"]      = sub_target_price
            trade["spot_at_entry_time"]         = round(spot_entry,    2) if spot_entry   else None
            trade["atm_price_at_entry_time"]    = round(atm_px_entry,  2) if atm_px_entry  else None

        sub_trades.append(trade)

        # Shift listen_time for next re-entry's momentum base measurement
        listen_time = exit_at

        # ── Re-entry decision ────────────────────────────────────────────────
        is_sl_reentry   = exit_reason == "SL"     and sl_left > 0
        is_tp_reentry   = exit_reason == "Target" and tp_left > 0
        # NextLeg triggers on SL/Target hit regardless of count
        is_sl_next_leg  = exit_reason == "SL"     and "NextLeg" in reentry_sl_type and bool(reentry_sl_next_ref)
        is_tp_next_leg  = exit_reason == "Target" and "NextLeg" in reentry_tp_type and bool(reentry_tp_next_ref)

        if (is_sl_reentry or is_tp_reentry or is_sl_next_leg or is_tp_next_leg) and exit_at <= exit_time:
            re_type = reentry_sl_type if (is_sl_reentry or is_sl_next_leg) else reentry_tp_type

            # ── NextLeg: spawn a lazy leg instead of re-entering ─────────────
            if "NextLeg" in re_type:
                next_leg_ref          = (reentry_sl_next_ref if (is_sl_reentry or is_sl_next_leg)
                                         else reentry_tp_next_ref)
                next_leg_trigger_time = exit_at
                # tag the exiting sub_trade with the lazy leg it triggered
                if sub_trades:
                    sub_trades[-1]["reentry_type"]       = f"NextLeg({next_leg_ref})"
                    sub_trades[-1]["triggered_lazy_leg"] = next_leg_ref
                break

            if "AtCost" in re_type:
                # ── RE-COST: same strike, wait for price to return to entry price ──
                wait_candles = idx.get_candles_range(
                    day, _add_one_minute(exit_at), exit_time,
                    expiry, cur_strike, otype,
                )
                cost_time, _ = _find_at_cost_reentry(
                    wait_candles, entry_price, cur_position, exit_reason
                )
                if cost_time is None:
                    break   # price never returned, no re-entry

                forced_entry_price = entry_price   # re-enter at original cost, not candle close
                cur_time = cost_time   # enter at the candle where price crossed cost
                # cur_strike stays same
                if "Reverse" in re_type:
                    cur_position = _flip_position(cur_position)   # AtCostReverse

            elif "LikeOriginal" in re_type:
                # ── RE-MOMENTUM: new ATM + wait for momentum breakout ────────────
                new_spot = idx.get_spot(day, exit_at)
                if new_spot is None:
                    break
                new_strike = _pick_strike(
                    idx, day, exit_at, expiry, otype, new_spot,
                    entry_type, strike_param, step,
                )
                if new_strike is None:
                    break

                # LikeOriginal / LikeOriginalReverse — new ATM, instant entry
                # LegMomentum is NOT considered here (AlgoTest removed this)
                cur_strike = new_strike
                cur_time   = exit_at

                if "Reverse" in re_type:
                    cur_position = _flip_position(cur_position)

            else:
                # ── RE-ASAP / ImmediateReverse: new ATM strike, instant re-entry ──
                new_spot = idx.get_spot(day, exit_at)
                if new_spot is None:
                    break
                new_strike = _pick_strike(
                    idx, day, exit_at, expiry, otype, new_spot,
                    entry_type, strike_param, step,
                )
                if new_strike is None:
                    break
                cur_strike = new_strike
                cur_time   = exit_at   # instant

                if "Reverse" in re_type:
                    cur_position = _flip_position(cur_position)

            if is_sl_reentry:
                sl_left -= 1
            else:
                tp_left -= 1
            reentry_number += 1
        else:
            break

    return {
        "sub_trades":             sub_trades,
        "total_leg_pnl":          round(total_pnl, 2),
        "entry_time":             sub_trades[0]["entry_time"]   if sub_trades else entry_time,
        "entry_price":            sub_trades[0]["entry_price"]  if sub_trades else 0,
        "exit_time":              sub_trades[-1]["exit_time"]   if sub_trades else exit_time,
        "exit_price":             sub_trades[-1]["exit_price"]  if sub_trades else 0,
        "exit_reason":            sub_trades[-1]["exit_reason"] if sub_trades else "Time Exit",
        "reentries":              len(sub_trades) - 1,
        "next_leg_ref":           next_leg_ref,
        "next_leg_trigger_time":  next_leg_trigger_time,
    }


def _summary(trades: list) -> dict:
    if not trades:
        return {}
    pnls      = [t["total_pnl"] for t in trades]
    wins      = [p for p in pnls if p > 0]
    losses    = [p for p in pnls if p <= 0]
    n         = len(pnls)
    avg_win   = sum(wins)   / len(wins)   if wins   else 0.0
    avg_loss  = sum(losses) / len(losses) if losses else 0.0
    win_rate  = len(wins)   / n
    loss_rate = len(losses) / n

    cum = peak = max_dd = 0.0
    for p in pnls:
        cum   += p
        peak   = max(peak, cum)
        max_dd = max(max_dd, peak - cum)

    mws = mls = cw = cl = 0
    for p in pnls:
        if p > 0:
            cw += 1; cl = 0
        else:
            cl += 1; cw = 0
        mws = max(mws, cw)
        mls = max(mls, cl)

    overall  = round(sum(pnls), 2)
    rr       = round(avg_win / abs(avg_loss), 4) if avg_loss != 0 else 0
    romd     = round(overall / max_dd, 4)        if max_dd   != 0 else "nan"
    exp      = round((win_rate * avg_win) + (loss_rate * avg_loss), 2)

    return {
        "NumberOfTrades":               n,
        "WinningRatio":                 round(win_rate  * 100, 2),
        "LosingRatio":                  round(loss_rate * 100, 2),
        "OverallProfit":                overall,
        "AverageProfitPerTrade":        round(overall / n, 2),
        "AverageProfitPerWinningTrade": round(avg_win,  2),
        "AverageProfitPerLosingTrade":  round(avg_loss, 2),
        "MaximumProfitInSingleTrade":   round(max(pnls), 2),
        "MinimumProfitInSingleTrade":   round(min(pnls), 2),
        "MaximumWinningStreak":         mws,
        "MaximumLosingStreak":          mls,
        "MaximumDrawdown":              round(max_dd, 2),
        "ReturnOverMaximumDrawdown":    romd,
        "Expectancy":                   exp,
        "RewardToRiskRatio":            rr,
    }


from collections import OrderedDict
import os
import pathlib

try:
    import pyarrow as _pa
    import pyarrow.parquet as _pq
    _PARQUET_OK = True
except ImportError:
    _PARQUET_OK = False

# ─── Cache Mode Toggle ────────────────────────────────────────────────────────
# REDIS_MEMORY = True  → DataIndex stored in Redis (RAM). Lazy-loaded on first
#                         access per day, stays in Redis for the server session.
#                         Speed: ~5ms/day (vs ~60ms pkl5 disk).
#                         Requires: pip install redis  +  Redis server running.
# REDIS_MEMORY = False → Current pkl5 disk cache (~60ms/day). No extra setup.

REDIS_MEMORY = False  # Redis is slower than pkl5 (pickle.loads bottleneck same regardless of source)

_redis_client = None   # initialized on first use (lazy)

def _get_redis():
    """Return Redis client, connecting once per server process."""
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        _redis_client = r
        return r
    except Exception as e:
        raise RuntimeError(
            f"Redis not available: {e}\n"
            "Fix: sudo apt install redis-server && sudo systemctl start redis\n"
            "     pip install redis\n"
            "Or set REDIS_MEMORY = False in backtest_engine.py"
        )

# ─── Single-file Pickle5 DataIndex Cache (REDIS_MEMORY = False) ───────────────
# Cold run:  MongoDB (276ms) + DataIndex build (250ms) + save .pkl5 = ~800ms
# Warm run:  load .pkl5 (all 6 dicts, protocol=5)                   = ~60ms/day
# 1 year cached: ~60ms × 246 days = ~15s
#
# ─── Redis In-Memory Cache (REDIS_MEMORY = True) ──────────────────────────────
# First access per day (this server session): pkl5 → Redis             ~60ms
# Subsequent accesses same session:           Redis RAM lookup          ~5ms/day
# 1 year second backtest same session:        ~5ms × 246 days = ~1.2s
# Note: Redis data lost on server restart → reloads from pkl5 on next run

_CACHE_DIR = pathlib.Path.home() / ".backtest_cache"

import pickle as _pickle


def _cache_dir(underlying: str) -> pathlib.Path:
    d = _CACHE_DIR / underlying
    d.mkdir(parents=True, exist_ok=True)
    return d


def _pkl5_path(underlying: str, date: str) -> pathlib.Path:
    return _cache_dir(underlying) / f"{date}.pkl5"


# Keep these so warm_cache.py still works
def _parquet_path(underlying: str, date: str) -> pathlib.Path:
    return _cache_dir(underlying) / f"{date}.parquet"

def _idx_pkl_path(underlying: str, date: str) -> pathlib.Path:
    return _cache_dir(underlying) / f"{date}.idx.pkl"


def _build_index_from_raw(raw: list) -> "DataIndex":
    """Build DataIndex from raw MongoDB candle list."""
    idx = DataIndex.__new__(DataIndex)
    idx.candle_index  = {}
    idx.high_index    = {}
    idx.low_index     = {}
    idx.spot_index    = {}
    _ei = defaultdict(set)
    _at = defaultdict(set)
    _tm = defaultdict(list)
    _si = defaultdict(set)

    for c in raw:
        try:
            ts       = c["timestamp"]
            date_str = ts[:10];  time_str = ts[11:16]
            expiry   = c["expiry"]
            strike   = int(c["strike"])
            otype    = c["type"]
            close    = float(c["close"])
            spot     = float(c.get("spot_price", 0))
            high     = float(c.get("high", close))
            low      = float(c.get("low",  close))
            key = (date_str, time_str, expiry, strike, otype)
            idx.candle_index[key] = close
            idx.high_index[key]   = high
            idx.low_index[key]    = low
            idx.spot_index[(date_str, time_str)] = spot
            _ei[date_str].add(expiry)
            _at[date_str].add(time_str)
            _tm[(date_str, expiry, strike, otype)].append(time_str)
            _si[(date_str, time_str, expiry, otype)].add(strike)
        except KeyError:
            pass

    # If no real high/low data, share candle_index reference (saves ~32ms on load)
    if idx.high_index == idx.candle_index:
        idx.high_index = idx.candle_index
        idx.low_index  = idx.candle_index

    idx.expiry_index  = {k: sorted(v) for k, v in _ei.items()}
    idx._all_times    = {k: sorted(v) for k, v in _at.items()}
    idx._time_map     = {k: sorted(set(v)) for k, v in _tm.items()}
    idx.strikes_index = {k: sorted(v) for k, v in _si.items()}
    return idx


def _save_pkl5(idx: "DataIndex", path: pathlib.Path):
    """Save full DataIndex as a single pickle5 file (~4.5MB, loads in ~40ms)."""
    # Detect whether high/low are real data or aliases of candle_index
    has_hl = idx.high_index is not idx.candle_index
    data = {
        'candle_index':  idx.candle_index,
        'spot_index':    idx.spot_index,
        'expiry_index':  idx.expiry_index,
        '_all_times':    idx._all_times,
        '_time_map':     idx._time_map,
        'strikes_index': idx.strikes_index,
        'has_hl':        has_hl,
    }
    if has_hl:
        data['high_index'] = idx.high_index
        data['low_index']  = idx.low_index
    with open(path, 'wb') as f:
        _pickle.dump(data, f, protocol=5)


def _load_pkl5(path: pathlib.Path) -> "DataIndex":
    """Load DataIndex from .pkl5 file in ~40ms."""
    with open(path, 'rb') as f:
        d = _pickle.load(f)
    idx = DataIndex.__new__(DataIndex)
    idx.candle_index  = d['candle_index']
    idx.spot_index    = d['spot_index']
    idx.expiry_index  = d['expiry_index']
    idx._all_times    = d['_all_times']
    idx._time_map     = d['_time_map']
    idx.strikes_index = d['strikes_index']
    if d.get('has_hl'):
        idx.high_index = d['high_index']
        idx.low_index  = d['low_index']
    else:
        idx.high_index = idx.candle_index
        idx.low_index  = idx.candle_index
    return idx


# compat shim: warm_cache.py calls these
def _save_idx_pkl(idx: "DataIndex", path: pathlib.Path):
    _save_pkl5(idx, path.with_suffix('.pkl5'))

def _raw_to_parquet(raw: list, path: pathlib.Path):
    pass  # no longer needed — pkl5 replaces parquet

def _dataindex_from_df(df):
    pass  # no longer needed


def _load_index_cached(db, underlying: str, date: str) -> "Optional[DataIndex]":
    """
    Load DataIndex from cache or MongoDB.

    REDIS_MEMORY=True  fast path: Redis RAM lookup (~5ms). On miss: loads pkl5
                       → stores in Redis → returns. Redis persists for server session.
    REDIS_MEMORY=False fast path: pkl5 disk load (~60ms/day).
    Legacy path: old parquet+.idx.pkl → upgraded to .pkl5 on first access.
    Cold path: MongoDB → build DataIndex → save .pkl5 (~800ms first time).
    """
    # ── Redis fast path ───────────────────────────────────────────────────────
    if REDIS_MEMORY:
        import pickle as _pkl
        r   = _get_redis()
        key = f"di:{underlying}:{date}"
        try:
            raw_bytes = r.get(key)
            if raw_bytes:
                d   = _pkl.loads(raw_bytes)
                idx = DataIndex.__new__(DataIndex)
                idx.candle_index  = d['candle_index']
                idx.spot_index    = d['spot_index']
                idx.expiry_index  = d['expiry_index']
                idx._all_times    = d['_all_times']
                idx._time_map     = d['_time_map']
                idx.strikes_index = d['strikes_index']
                if d.get('has_hl'):
                    idx.high_index = d['high_index']
                    idx.low_index  = d['low_index']
                else:
                    idx.high_index = idx.candle_index
                    idx.low_index  = idx.candle_index
                return idx
        except Exception:
            pass  # Redis error → fall through to pkl5

        # Miss: load from pkl5 and push to Redis
        pkl5 = _pkl5_path(underlying, date)
        if pkl5.exists():
            try:
                with open(pkl5, 'rb') as f:
                    d = _pkl.load(f)
                idx = DataIndex.__new__(DataIndex)
                idx.candle_index  = d['candle_index']
                idx.spot_index    = d['spot_index']
                idx.expiry_index  = d['expiry_index']
                idx._all_times    = d['_all_times']
                idx._time_map     = d['_time_map']
                idx.strikes_index = d['strikes_index']
                if d.get('has_hl'):
                    idx.high_index = d['high_index']
                    idx.low_index  = d['low_index']
                else:
                    idx.high_index = idx.candle_index
                    idx.low_index  = idx.candle_index
                # Store in Redis (no TTL — persists until server restart)
                try:
                    r.set(key, _pkl.dumps(d, protocol=5))
                except Exception:
                    pass
                return idx
            except Exception:
                pkl5.unlink(missing_ok=True)

        # pkl5 missing — cold path (MongoDB)
        raw = db.load_day(date, underlying)
        if not raw:
            return None
        idx = _build_index_from_raw(raw)
        try:
            _save_pkl5(idx, _pkl5_path(underlying, date))
            d = {
                'candle_index': idx.candle_index, 'spot_index': idx.spot_index,
                'expiry_index': idx.expiry_index, '_all_times': idx._all_times,
                '_time_map': idx._time_map, 'strikes_index': idx.strikes_index,
                'has_hl': idx.high_index is not idx.candle_index,
            }
            r.set(key, _pkl.dumps(d, protocol=5))
        except Exception:
            pass
        return idx

    # ── pkl5 disk path (REDIS_MEMORY = False) ─────────────────────────────────
    pkl5 = _pkl5_path(underlying, date)

    # ── Fast path ────────────────────────────────────────────────────────────
    if pkl5.exists():
        try:
            return _load_pkl5(pkl5)
        except Exception:
            pkl5.unlink(missing_ok=True)

    # ── Legacy upgrade: old parquet + .idx.pkl → rebuild as .pkl5 ────────────
    pq_path  = _parquet_path(underlying, date)
    old_pkl  = _idx_pkl_path(underlying, date)
    if _PARQUET_OK and pq_path.exists():
        try:
            import pyarrow.parquet as _pq2
            df = _pq2.read_table(str(pq_path)).to_pandas()
            date_strs = df['date_str'].tolist(); time_strs = df['time_str'].tolist()
            expiries  = df['expiry'].tolist();   strikes   = df['strike'].tolist()
            types     = df['type'].tolist();     closes    = df['close'].tolist()
            spots     = df['spot_price'].tolist()
            highs = df['high'].tolist() if 'high' in df.columns else closes
            lows  = df['low'].tolist()  if 'low'  in df.columns else closes
            keys  = list(zip(date_strs, time_strs, expiries, strikes, types))
            idx   = DataIndex.__new__(DataIndex)
            idx.candle_index = dict(zip(keys, closes))
            if highs is closes:
                idx.high_index = idx.candle_index
                idx.low_index  = idx.candle_index
            else:
                idx.high_index = dict(zip(keys, highs))
                idx.low_index  = dict(zip(keys, lows))
            idx.spot_index = dict(zip(zip(date_strs, time_strs), spots))
            # expiry/time/strikes from old pkl if present, else rebuild
            if old_pkl.exists():
                with open(old_pkl, 'rb') as f:
                    sv = _pickle.load(f)
                idx.expiry_index  = sv['expiry_index']
                idx._all_times    = sv['_all_times']
                idx._time_map     = sv['_time_map']
                idx.strikes_index = sv['strikes_index']
            else:
                _ei=defaultdict(set); _at=defaultdict(set)
                _tm=defaultdict(list); _si=defaultdict(set)
                for d,t,e,s,o in zip(date_strs,time_strs,expiries,strikes,types):
                    _ei[d].add(e); _at[d].add(t)
                    _tm[(d,e,s,o)].append(t); _si[(d,t,e,o)].add(s)
                idx.expiry_index  = {k:sorted(v) for k,v in _ei.items()}
                idx._all_times    = {k:sorted(v) for k,v in _at.items()}
                idx._time_map     = {k:sorted(set(v)) for k,v in _tm.items()}
                idx.strikes_index = {k:sorted(v) for k,v in _si.items()}
            # Save as pkl5 for next time, clean up old files
            try:
                _save_pkl5(idx, pkl5)
                pq_path.unlink(missing_ok=True)
                old_pkl.unlink(missing_ok=True)
            except Exception:
                pass
            return idx
        except Exception:
            pq_path.unlink(missing_ok=True)

    # ── Cold path: query MongoDB ──────────────────────────────────────────────
    raw = db.load_day(date, underlying)
    if not raw:
        return None

    idx = _build_index_from_raw(raw)
    try:
        _save_pkl5(idx, pkl5)
    except Exception:
        pass
    return idx


# ─── Main ─────────────────────────────────────────────────────────────────────

def run_backtest(request: dict, on_progress=None) -> dict:
    """
    on_progress(completed: int, total: int, day: str) — called after each trading day.
    Use this for progress tracking in long-running backtests.
    """
    db         = MongoData()
    start_date = request["start_date"]
    end_date   = request["end_date"]
    strategy   = request["strategy"]
    underlying = strategy["Ticker"]
    step       = STRIKE_STEPS.get(underlying, 50)
    legs       = strategy["ListOfLegConfigs"]

    entry_h, entry_m = _extract_time(strategy["EntryIndicators"])
    exit_h,  exit_m  = _extract_time(strategy["ExitIndicators"])
    entry_time  = f"{entry_h:02d}:{entry_m:02d}"
    exit_time   = f"{exit_h:02d}:{exit_m:02d}"

    # ── Overall configs (parsed once, applied per day) ───────────────────────
    overall_sl_type,      overall_sl_val       = parse_overall_sl(strategy)
    overall_tgt_type,     overall_tgt_val      = parse_overall_tgt(strategy)
    overall_re_type,      overall_re_count     = parse_overall_reentry_sl(strategy)
    overall_re_tgt_type,  overall_re_tgt_count = parse_overall_reentry_tgt(strategy)
    lock_type, lock_trigger, lock_floor, lock_trail_for_every, lock_trail_by = \
        parse_lock_and_trail(strategy)
    trail_sl_type, trail_sl_for_every, trail_sl_by = parse_overall_trail_sl(strategy)
    rb_type, rb_condition, rb_start, rb_end, rb_start_dte, rb_end_dte = \
        parse_range_breakout(strategy)

    # ── 1. Load metadata (holidays, lot size) — no candle data yet ───────────
    holidays      = db.get_holidays()
    lot_size      = db.get_lot_size(start_date, underlying)
    trading_days  = _get_trading_days(start_date, end_date, holidays)
    expiry_rules  = db.get_expiry_rules(underlying)   # loaded once from DB
    total_days   = len(trading_days)

    trades = []
    is_btst       = "BTST"       in rb_type
    is_positional = "Positional" in rb_type
    total_steps   = total_days

    if on_progress and total_steps > 0:
        on_progress(0, total_steps, "Initializing")

    for day_idx, day in enumerate(trading_days):
        idx = _load_index_cached(db, underlying, day)
        if not idx:
            if on_progress:
                on_progress(day_idx + 1, total_steps,
                            f"Processing {day_idx + 1}/{total_days}: {day}")
            continue

        # ── Expiry weekday for this day (from DB rules loaded once at start) ───
        expiry_weekday = get_expiry_weekday_from_rules(expiry_rules, day)

        # ── BTST: load previous day from cache (fast on repeat runs) ─────────
        prev_trading_day = trading_days[day_idx - 1] if day_idx > 0 else None
        prev_idx: Optional[DataIndex] = None
        if is_btst and prev_trading_day:
            prev_idx = _load_index_cached(db, underlying, prev_trading_day)

        # ── Positional ORB: DTE check + load all range days ───────────────────
        positional_start_day  = None
        positional_range_data = []   # [(day_str, DataIndex)]
        if is_positional:
            expiries_today = idx.get_expiries(day)
            if not expiries_today:
                continue
            # Use first leg's expiry kind for DTE resolution
            first_expiry_kind = legs[0].get("ExpiryKind", "ExpiryType.Weekly") if legs else "ExpiryType.Weekly"
            pos_expiry = _resolve_expiry(day, first_expiry_kind, expiries_today, expiry_weekday)
            if pos_expiry is None:
                continue
            dte_today = compute_dte(day, pos_expiry, trading_days)
            if dte_today != rb_end_dte:
                continue   # not a Positional ORB trade day

            positional_start_day = find_day_by_dte(rb_start_dte, pos_expiry, trading_days)
            if positional_start_day is None or positional_start_day > day:
                continue

            # Load all range days from cache (fast on repeat runs)
            range_day_list = sorted(d for d in trading_days
                                    if positional_start_day <= d <= day)
            for rd in range_day_list:
                rd_idx = idx if rd == day else _load_index_cached(db, underlying, rd)
                if rd_idx:
                    positional_range_data.append((rd, rd_idx))

        spot = idx.get_spot(day, entry_time)
        if spot is None:
            continue

        expiries = idx.get_expiries(day)
        if not expiries:
            continue

        # ── Overall SL: static or trailing ───────────────────────────────────
        if trail_sl_type != "None" and overall_sl_type != "None":
            overall_sl_exit_time = find_trail_sl_exit_time(
                idx, day, entry_time, exit_time,
                legs, expiries, step, lot_size,
                overall_sl_type, overall_sl_val,
                trail_sl_type, trail_sl_for_every, trail_sl_by, spot,
            )
        else:
            overall_sl_exit_time = find_overall_sl_exit_time(
                idx, day, entry_time, exit_time,
                legs, expiries, step, lot_size,
                overall_sl_type, overall_sl_val, spot,
            )

        # ── Overall Target ────────────────────────────────────────────────────
        overall_tgt_exit_time = find_overall_tgt_exit_time(
            idx, day, entry_time, exit_time,
            legs, expiries, step, lot_size,
            overall_tgt_type, overall_tgt_val, spot,
        )

        # ── Lock / Lock and Trail ─────────────────────────────────────────────
        if lock_type == "Lock":
            lock_exit_time = find_lock_exit_time(
                idx, day, entry_time, exit_time,
                legs, expiries, step, lot_size,
                lock_trigger, lock_floor, spot,
            )
        elif lock_type == "LockAndTrail":
            lock_exit_time = find_lock_trail_exit_time(
                idx, day, entry_time, exit_time,
                legs, expiries, step, lot_size,
                lock_trigger, lock_floor,
                lock_trail_for_every, lock_trail_by, spot,
            )
        else:
            lock_exit_time = None

        # ── Resolve: earliest exit wins; SL > TGT > Lock on tie ──────────────
        overall_sl_exit_time, overall_tgt_exit_time, lock_exit_time, effective_exit = \
            resolve_all_exits(overall_sl_exit_time, overall_tgt_exit_time, lock_exit_time, exit_time)

        # Which trigger actually caused the effective exit?
        sl_caused_exit  = (overall_sl_exit_time  is not None and effective_exit == overall_sl_exit_time)
        tgt_caused_exit = (overall_tgt_exit_time is not None and effective_exit == overall_tgt_exit_time)

        day_trade = {
            "date":                  day,
            "entry_time":            entry_time,
            "exit_time":             effective_exit,
            "spot_at_entry":         round(spot, 2),
            "legs":                  [],
            "total_pnl":             0.0,
            "overall_sl_exit":       overall_sl_exit_time  is not None,
            "overall_sl_exit_time":  overall_sl_exit_time,
            "overall_tgt_exit":      overall_tgt_exit_time is not None,
            "overall_tgt_exit_time": overall_tgt_exit_time,
            "lock_exit":             lock_exit_time        is not None,
            "lock_exit_time":        lock_exit_time,
        }
        valid = True

        for leg in legs:
            position     = "SELL" if "Sell" in leg["PositionType"] else "BUY"
            otype        = "CE"   if "CE"   in leg["InstrumentKind"] else "PE"
            expiry_kind  = leg.get("ExpiryKind",      "ExpiryType.Weekly")
            entry_type   = leg.get("EntryType",       "EntryType.EntryByStrikeType")
            strike_param = leg.get("StrikeParameter", "StrikeType.ATM")
            lots         = int(leg["LotConfig"]["Value"])
            sl_type       = leg["LegStopLoss"]["Type"]
            sl_val        = float(leg["LegStopLoss"]["Value"])
            tgt_type      = leg["LegTarget"]["Type"]
            tgt_val       = float(leg["LegTarget"]["Value"])
            momentum_type = leg.get("LegMomentum", {}).get("Type",  "None")
            momentum_val  = float(leg.get("LegMomentum", {}).get("Value", 0))
            trail_sl      = leg.get("LegTrailSL", {})
            trail_type    = trail_sl.get("Type", "None")
            trail_x       = float(trail_sl.get("Value", {}).get("InstrumentMove", 0))
            trail_y       = float(trail_sl.get("Value", {}).get("StopLossMove",   0))

            # Re-entry config
            re_sl  = leg.get("LegReentrySL", {})
            re_tp  = leg.get("LegReentryTP", {})
            reentry_sl_type  = re_sl.get("Type", "None")
            reentry_tp_type  = re_tp.get("Type", "None")
            _re_sl_val_cnt = re_sl.get("Value", {})
            _re_tp_val_cnt = re_tp.get("Value", {})
            reentry_sl_count = int(_re_sl_val_cnt.get("ReentryCount", 0) if isinstance(_re_sl_val_cnt, dict) else 0) \
                               if reentry_sl_type != "None" else 0
            reentry_tp_count = int(_re_tp_val_cnt.get("ReentryCount", 0) if isinstance(_re_tp_val_cnt, dict) else 0) \
                               if reentry_tp_type != "None" else 0
            _re_sl_val = re_sl.get("Value", {})
            _re_tp_val = re_tp.get("Value", {})
            reentry_sl_next_ref = _re_sl_val.get("NextLegRef") if isinstance(_re_sl_val, dict) else None
            reentry_tp_next_ref = _re_tp_val.get("NextLegRef") if isinstance(_re_tp_val, dict) else None

            expiry = _resolve_expiry(day, expiry_kind, expiries, expiry_weekday)
            if expiry is None:
                valid = False; break

            actual_entry_time  = entry_time
            override_entry_px  = None
            override_base_px   = None

            if rb_type != "None":
                _mode = ("Positional" if is_positional else "BTST" if is_btst else "ORB")
                tag   = f"[{_mode} {day} leg={leg.get('id')}]"

                if is_positional:
                    # ── Positional ORB: DTE-based multi-day range ─────────────
                    if not positional_range_data:
                        debug_print(f"{tag} SKIP: no range day data")
                        valid = False; break

                    # Strike at start_dte_day's start_time
                    start_day_idx = next(
                        (di for d, di in positional_range_data if d == positional_start_day),
                        None
                    )
                    if start_day_idx is None:
                        debug_print(f"{tag} SKIP: start_day {positional_start_day} not loaded")
                        valid = False; break

                    rb_spot = start_day_idx.get_spot(positional_start_day, rb_start) or spot
                    strike  = _pick_strike(start_day_idx, positional_start_day, rb_start,
                                           expiry, otype, rb_spot,
                                           entry_type, strike_param, step)
                    if strike is None:
                        debug_print(f"{tag} SKIP: strike not resolved at {rb_start} on {positional_start_day}")
                        valid = False; break

                    r_high, r_low = compute_positional_range(
                        positional_range_data, rb_start, rb_end, day,
                        rb_type, expiry, strike, otype,
                    )

                elif is_btst:
                    # ── BTST ORB: strike on Day 1, range spans Day 1 + Day 2 ──
                    if prev_idx is None:
                        debug_print(f"{tag} SKIP: no previous day data (day_idx={day_idx})")
                        valid = False; break

                    rb_spot = prev_idx.get_spot(prev_trading_day, rb_start) or spot
                    strike  = _pick_strike(prev_idx, prev_trading_day, rb_start,
                                           expiry, otype, rb_spot,
                                           entry_type, strike_param, step)
                    if strike is None:
                        debug_print(f"{tag} SKIP: strike not resolved at {rb_start} on {prev_trading_day}")
                        valid = False; break

                    r_high, r_low = compute_btst_range(
                        prev_idx, idx, prev_trading_day, day,
                        rb_start, rb_end, rb_type, expiry, strike, otype,
                    )

                else:
                    # ── Same-day ORB: strike at range_start on today ──────────
                    rb_spot = idx.get_spot(day, rb_start) or spot
                    strike  = _pick_strike(idx, day, rb_start, expiry, otype, rb_spot,
                                           entry_type, strike_param, step)
                    if strike is None:
                        debug_print(f"{tag} SKIP: strike not resolved at {rb_start}")
                        valid = False; break

                    r_high, r_low = compute_range(
                        idx, day, rb_start, rb_end,
                        rb_type, expiry, strike, otype,
                    )

                if r_high is None or r_low is None:
                    debug_print(f"{tag} SKIP: no range data")
                    valid = False; break

                debug_print(f"{tag} range={rb_start}→{rb_end} "
                      f"high={r_high:.2f} low={r_low:.2f} condition={rb_condition}")

                # Breakout scan always on current trade day
                rb_entry_time, rb_entry_price = find_breakout_entry(
                    idx, day, rb_end, effective_exit,
                    expiry, strike, otype,
                    rb_type, rb_condition, r_high, r_low,
                )
                if rb_entry_time is None:
                    debug_print(f"{tag} SKIP: breakout never triggered")
                    valid = False; break

                debug_print(f"{tag} breakout at {rb_entry_time} entry_px={rb_entry_price:.2f}")

                actual_entry_time = rb_entry_time
                override_entry_px = rb_entry_price

            else:
                # ── Normal flow: strike at entry_time + optional momentum ─────
                strike = _pick_strike(idx, day, entry_time, expiry, otype, spot,
                                      entry_type, strike_param, step)
                if strike is None:
                    valid = False; break

                if momentum_type != "None" and momentum_val > 0:
                    if "Underlying" in momentum_type:
                        base_px = idx.get_spot(day, entry_time)
                    else:
                        base_px = idx.get_close(day, entry_time, expiry, strike, otype)
                    if base_px is None:
                        valid = False; break

                    mom_time, mom_px = _find_momentum_entry(
                        idx, day, _add_one_minute(entry_time), exit_time,
                        expiry, strike, otype,
                        base_px, momentum_type, momentum_val,
                    )
                    if mom_time is None:
                        valid = False; break   # momentum not achieved → no trade today

                    actual_entry_time = mom_time
                    override_entry_px = mom_px
                    override_base_px  = base_px
                else:
                    if idx.get_close(day, entry_time, expiry, strike, otype) is None:
                        valid = False; break

            # Process leg (with re-entry support)
            result = _process_leg(
                idx, day, actual_entry_time, effective_exit,
                expiry, strike, otype, position,
                sl_type, sl_val, tgt_type, tgt_val,
                reentry_sl_count, reentry_tp_count,
                reentry_sl_type, reentry_tp_type,
                lots, lot_size,
                entry_type, strike_param, step,
                momentum_type, momentum_val,
                override_entry_px=override_entry_px,
                override_base_px=override_base_px,
                strategy_entry_time=entry_time,
                trail_type=trail_type, trail_x=trail_x, trail_y=trail_y,
                reentry_sl_next_ref=reentry_sl_next_ref,
                reentry_tp_next_ref=reentry_tp_next_ref,
            )

            parent_leg_entry = {
                "id":                leg["id"],
                "expiry":            expiry,
                "strike":            strike,
                "type":              otype,
                "position":          position,
                "entry_time":        result["entry_time"],
                "entry_price":       result["entry_price"],
                "exit_time":         result["exit_time"],
                "exit_price":        result["exit_price"],
                "exit_reason":       result["exit_reason"],
                "reentries":         result["reentries"],
                "lots":              lots,
                "lot_size":          lot_size,
                "pnl":               result["total_leg_pnl"],
                "sub_trades":        result["sub_trades"],
                "range_breakout":    rb_type != "None",
            }

            # ── Lazy Leg: merge into parent leg's sub_trades ──────────────────
            idle_configs = strategy.get("IdleLegConfigs", {})
            debug_print(f"[Leg {leg['id']} {day}] exit_reason={result['exit_reason']} next_leg_ref={result.get('next_leg_ref')} trigger_time={result.get('next_leg_trigger_time')}")
            if idle_configs and result.get("next_leg_ref"):
                lazy_legs = process_lazy_legs(
                    idx, day, effective_exit, expiries,
                    result["next_leg_ref"],
                    result["next_leg_trigger_time"],
                    idle_configs, lot_size, step,
                )
                for ll in lazy_legs:
                    # tag each sub_trade with lazy leg id and add to parent
                    for st in ll.get("sub_trades", []):
                        st["lazy_leg_id"]  = ll["id"]
                        # preserve inner reentry_number; prefix type with Lazy(id)
                        inner_type = st.get("reentry_type", "Initial")
                        st["reentry_type"] = f"Lazy({ll['id']})" if inner_type == "Initial" else f"Lazy({ll['id']})/{inner_type}"
                        parent_leg_entry["sub_trades"].append(st)
                    # merge lazy leg pnl into parent leg pnl
                    parent_leg_entry["pnl"] = round(
                        parent_leg_entry["pnl"] + ll["pnl"], 2
                    )
                    # extend exit_time to lazy leg's exit if later
                    if ll["exit_time"] and ll["exit_time"] > parent_leg_entry["exit_time"]:
                        parent_leg_entry["exit_time"] = ll["exit_time"]
                        parent_leg_entry["exit_price"] = ll["exit_price"]
                        parent_leg_entry["exit_reason"] = ll["exit_reason"]

            day_trade["legs"].append(parent_leg_entry)

        if valid and day_trade["legs"]:
            day_trade["total_pnl"] = round(sum(l["pnl"] for l in day_trade["legs"]), 2)

            # ── Overall Re-entry on SL ────────────────────────────────────────
            if (sl_caused_exit and
                    overall_re_type != "None" and
                    overall_re_count > 0 and
                    overall_sl_exit_time < exit_time):

                reentry_legs = run_overall_reentry(
                    idx           = idx,
                    day           = day,
                    trigger_time  = overall_sl_exit_time,
                    exit_time     = exit_time,
                    leg_configs   = legs,
                    expiries      = expiries,
                    step          = step,
                    lot_size      = lot_size,
                    idle_configs  = strategy.get("IdleLegConfigs", {}),
                    overall_sl_type  = overall_sl_type,
                    overall_sl_val   = overall_sl_val,
                    reentry_type   = overall_re_type,
                    reentries_left = overall_re_count,
                    cycle_number  = 1,
                )

                day_trade["legs"].extend(reentry_legs)
                day_trade["total_pnl"] = round(
                    sum(l["pnl"] for l in day_trade["legs"]), 2
                )

            # ── Overall Re-entry on Target ────────────────────────────────────
            if (tgt_caused_exit and
                    overall_re_tgt_type != "None" and
                    overall_re_tgt_count > 0 and
                    overall_tgt_exit_time < exit_time):

                tgt_reentry_legs = run_overall_reentry_tgt(
                    idx              = idx,
                    day              = day,
                    trigger_time     = overall_tgt_exit_time,
                    exit_time        = exit_time,
                    leg_configs      = legs,
                    expiries         = expiries,
                    step             = step,
                    lot_size         = lot_size,
                    idle_configs     = strategy.get("IdleLegConfigs", {}),
                    overall_sl_type  = overall_sl_type,
                    overall_sl_val   = overall_sl_val,
                    overall_tgt_type = overall_tgt_type,
                    overall_tgt_val  = overall_tgt_val,
                    reentry_type     = overall_re_tgt_type,
                    reentries_left   = overall_re_tgt_count,
                    cycle_number     = 1,
                )

                day_trade["legs"].extend(tgt_reentry_legs)
                day_trade["total_pnl"] = round(
                    sum(l["pnl"] for l in day_trade["legs"]), 2
                )

            trades.append(day_trade)

        if on_progress:
            on_progress(day_idx + 1, total_steps,
                        f"Processing {day_idx + 1}/{total_days}: {day}")

    db.close()

    return {
        "trades":  trades,
        "summary": _summary(trades),
        "meta": {
            "underlying":             underlying,
            "start_date":             start_date,
            "end_date":               end_date,
            "entry_time":             entry_time,
            "exit_time":              exit_time,
            "trading_days_processed": len(trading_days),
            "trades_executed":        len(trades),
            "lot_size":               lot_size,
            "candles_loaded":         sum(len(t["legs"]) for t in trades),
        },
    }


if __name__ == "__main__":
    import json, pathlib
    req_path = pathlib.Path(__file__).parent.parent / "current_backtest_request.json"
    with open(req_path) as f:
        req = json.load(f)
    result = run_backtest(req)
    debug_print(json.dumps(result["summary"], indent=2))
    debug_print(f"Trades: {len(result['trades'])}, Candles loaded: {result['meta']['candles_loaded']}")
