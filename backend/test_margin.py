"""
test_margin.py
──────────────
Verify span_margin.py results against Kite's basket-margin API.

Run:
    python3 test_margin.py
"""

from __future__ import annotations

import json
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from features.span_margin import (
    SpanMarginCalculator,
    SpanPosition,
    calculate_margin,
)

# ─── Test portfolios ──────────────────────────────────────────────────────────

# Short Straddle (most common)
SHORT_STRADDLE = [
    SpanPosition("NIFTY", "CE", "29MAY2025", 24500.0, "SELL", 1, 25, 150.0),
    SpanPosition("NIFTY", "PE", "29MAY2025", 24500.0, "SELL", 1, 25, 140.0),
]

# Bull Put Spread (hedge reduces margin)
BULL_PUT_SPREAD = [
    SpanPosition("NIFTY", "PE", "29MAY2025", 24000.0, "SELL", 1, 25, 120.0),
    SpanPosition("NIFTY", "PE", "29MAY2025", 23500.0, "BUY",  1, 25,  60.0),
]

# Iron Condor
IRON_CONDOR = [
    SpanPosition("NIFTY", "CE", "29MAY2025", 25000.0, "SELL", 1, 25,  90.0),
    SpanPosition("NIFTY", "CE", "29MAY2025", 25500.0, "BUY",  1, 25,  40.0),
    SpanPosition("NIFTY", "PE", "29MAY2025", 24000.0, "SELL", 1, 25,  80.0),
    SpanPosition("NIFTY", "PE", "29MAY2025", 23500.0, "BUY",  1, 25,  35.0),
]

# Futures + Short Option (hedged)
FUT_HEDGE = [
    SpanPosition("NIFTY", "FUT", "29MAY2025", 0.0, "SELL", 1, 25, 24480.0),
    SpanPosition("NIFTY", "CE", "29MAY2025", 24500.0, "BUY", 1, 25, 150.0),
]

# BANKNIFTY short strangle
BANKNIFTY_STRANGLE = [
    SpanPosition("BANKNIFTY", "CE", "28MAY2025", 55000.0, "SELL", 1, 15, 200.0),
    SpanPosition("BANKNIFTY", "PE", "28MAY2025", 52000.0, "SELL", 1, 15, 180.0),
]

# Pure futures
NIFTY_FUT = [
    SpanPosition("NIFTY", "FUT", "29MAY2025", 0.0, "SELL", 1, 25, 24480.0),
]

BANKNIFTY_FUT = [
    SpanPosition("BANKNIFTY", "FUT", "28MAY2025", 0.0, "BUY", 1, 15, 54800.0),
]


def run_test(name: str, positions: list[SpanPosition], kite_margin: float | None = None):
    result = calculate_margin(positions)
    print(f"\n{'─'*60}")
    print(f"  {name}")
    print(f"{'─'*60}")
    print(f"  Source        : {'SPAN file (' + result.span_file_date + ')' if result.from_span_file else 'Fallback %'}")
    print(f"  SPAN margin   : ₹{result.span_margin:>12,.2f}")
    print(f"  Exposure      : ₹{result.exposure_margin:>12,.2f}")
    print(f"  Total margin  : ₹{result.total_margin:>12,.2f}")
    print(f"  Premium rcvd  : ₹{result.premium_received:>12,.2f}")
    print(f"  Net margin    : ₹{result.net_margin:>12,.2f}")
    if kite_margin is not None:
        diff = abs(result.total_margin - kite_margin)
        pct  = diff / kite_margin * 100 if kite_margin else 0
        status = "✓ MATCH" if pct < 2.0 else "✗ DIFF"
        print(f"  Kite margin   : ₹{kite_margin:>12,.2f}  [{status} {pct:.1f}%]")

    for leg in result.legs:
        direction = "SELL" if leg.transaction_type == "SELL" else "BUY "
        print(f"    {direction} {leg.quantity}L {leg.underlying} {leg.expiry} "
              f"{leg.strike or 'FUT'} {leg.instrument_type}  "
              f"exp=₹{leg.exposure_margin:,.0f}"
              + (" [SOMC]" if leg.somc_applied else ""))


if __name__ == "__main__":
    print("=" * 60)
    print("  NSE SPAN Margin Calculator — Verification")
    print("=" * 60)

    calc = calculate_margin.__globals__["get_calculator"]()
    loaded = calc.ensure_span_loaded()
    print(f"\n  SPAN file loaded: {loaded}")
    if loaded:
        print(f"  Risk arrays    : {len(calc._parser.risk_arrays)}")
        print(f"  Commodities    : {len(calc._parser.commodities)}")
        print(f"  Underlying map : {calc._cc_to_sym}")

    # ── Run tests ──────────────────────────────────────────────────────────
    # Fill in kite_margin values after checking in Kite basket margin calculator

    run_test("NIFTY Futures (1 lot SELL)",          NIFTY_FUT,          kite_margin=None)
    run_test("BANKNIFTY Futures (1 lot BUY)",        BANKNIFTY_FUT,      kite_margin=None)
    run_test("NIFTY Short Straddle",                 SHORT_STRADDLE,     kite_margin=None)
    run_test("NIFTY Bull Put Spread (hedged)",        BULL_PUT_SPREAD,    kite_margin=None)
    run_test("NIFTY Iron Condor",                    IRON_CONDOR,        kite_margin=None)
    run_test("NIFTY Futures + Long CE (hedged)",     FUT_HEDGE,          kite_margin=None)
    run_test("BANKNIFTY Short Strangle",             BANKNIFTY_STRANGLE, kite_margin=None)

    print("\n" + "=" * 60)
    print("  Done. Fill kite_margin= values to verify accuracy.")
    print("=" * 60)
