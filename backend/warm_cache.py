"""
warm_cache.py
─────────────
Pre-build .pkl5 cache files for a date range so that
backtest runs fast (~10s/year) instead of slow (~3min first-run).

Usage:
    python3 warm_cache.py --underlying NIFTY --start 2025-01-01 --end 2025-12-31
    python3 warm_cache.py --underlying NIFTY --start 2025-01-01 --end 2025-12-31 --force
"""

import argparse
import time
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from features.mongo_data import MongoData
from features.backtest_engine import (
    _get_trading_days,
    _pkl5_path,
    _build_index_from_raw,
    _save_pkl5,
)


def warm(underlying: str, start: str, end: str, force: bool = False):
    db = MongoData()
    holidays = db.get_holidays()
    days = _get_trading_days(start, end, holidays)

    total = len(days)
    already_cached = 0
    built = 0
    skipped = 0
    errors = 0

    print(f"Underlying  : {underlying}")
    print(f"Date range  : {start} → {end}")
    print(f"Trading days: {total}")
    print(f"Force rebuild: {force}")
    print("─" * 50)

    t_start = time.perf_counter()

    for i, day in enumerate(days, 1):
        pkl5 = _pkl5_path(underlying, day)

        if not force and pkl5.exists():
            already_cached += 1
            elapsed = time.perf_counter() - t_start
            remaining = (elapsed / i) * (total - i)
            print(f"  [{i:3d}/{total}] {day}  CACHED  (eta {remaining:.0f}s)", end="\r")
            continue

        t0 = time.perf_counter()
        raw = db.load_day(day, underlying)
        if not raw:
            skipped += 1
            print(f"  [{i:3d}/{total}] {day}  no data — skipped")
            continue

        try:
            idx = _build_index_from_raw(raw)
            _save_pkl5(idx, pkl5)
            elapsed_day = (time.perf_counter() - t0) * 1000
            built += 1

            total_elapsed = time.perf_counter() - t_start
            remaining = (total_elapsed / i) * (total - i)
            print(f"  [{i:3d}/{total}] {day}  {elapsed_day:.0f}ms  (eta {remaining:.0f}s)")
        except Exception as e:
            errors += 1
            print(f"  [{i:3d}/{total}] {day}  ERROR: {e}")

    total_elapsed = time.perf_counter() - t_start
    print()
    print("─" * 50)
    print(f"Done in {total_elapsed:.1f}s")
    print(f"  Already cached : {already_cached}")
    print(f"  Newly built    : {built}")
    print(f"  No data/skipped: {skipped}")
    print(f"  Errors         : {errors}")
    print()
    if built + already_cached > 0:
        print(f"Next backtest for this range: ~{(built + already_cached) * 0.040:.1f}s  (40ms/day cached)")

    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pre-warm backtest cache")
    parser.add_argument("--underlying", default="NIFTY",
                        help="Underlying symbol (default: NIFTY)")
    parser.add_argument("--start", required=True,
                        help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True,
                        help="End date YYYY-MM-DD")
    parser.add_argument("--force", action="store_true",
                        help="Rebuild even if cache already exists")
    args = parser.parse_args()
    warm(args.underlying, args.start, args.end, args.force)
