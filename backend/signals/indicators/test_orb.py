from __future__ import annotations

import pytest

from .orb import calculate_opening_range, detect_breakout, is_opening_range_valid, orb_from_candles


def _candle(ts, o, h, l, c):
    return {"timestamp": ts, "open": o, "high": h, "low": l, "close": c, "volume": 1, "oi": 1}


def test_opening_range_is_first_bucket():
    day = [
        _candle("2026-01-05T09:15:00", 100, 105, 98, 102),
        _candle("2026-01-05T09:25:00", 102, 110, 101, 108),
    ]
    rng = calculate_opening_range(day)
    assert rng == {"open": 100.0, "high": 105.0, "low": 98.0}


def test_opening_range_validity_filter():
    narrow = {"open": 100.0, "high": 100.5, "low": 99.5}
    wide = {"open": 100.0, "high": 110.0, "low": 90.0}
    assert is_opening_range_valid(narrow, max_diff_pct=2.0) is True
    assert is_opening_range_valid(wide, max_diff_pct=2.0) is False


def test_detect_breakout_high_side():
    day = [
        _candle("2026-01-05T09:15:00", 100, 105, 98, 102),
        _candle("2026-01-05T09:25:00", 102, 104, 101, 103),  # no breakout yet
        _candle("2026-01-05T09:35:00", 103, 108, 102, 107),  # breaks above 105
    ]
    opening_range = calculate_opening_range(day)
    index = detect_breakout(day, opening_range, "High")
    assert index == 2


def test_detect_breakout_returns_none_when_no_breakout():
    day = [
        _candle("2026-01-05T09:15:00", 100, 105, 98, 102),
        _candle("2026-01-05T09:25:00", 102, 104, 101, 103),
    ]
    opening_range = calculate_opening_range(day)
    assert detect_breakout(day, opening_range, "High") is None


def test_orb_from_candles_groups_by_day_and_resamples():
    candles = []
    for minute in range(20):
        ts = f"2026-01-05T09:{15 + minute:02d}:00"
        candles.append(_candle(ts, 100 + minute, 100 + minute + 1, 100 + minute - 1, 100 + minute + 0.5))
    for minute in range(20):
        ts = f"2026-01-06T09:{15 + minute:02d}:00"
        candles.append(_candle(ts, 200 + minute, 200 + minute + 1, 200 + minute - 1, 200 + minute + 0.5))

    results = orb_from_candles(candles, candle_interval="10 minutes", max_diff_pct=50.0, breakout_side="High")
    assert len(results) == 2
    assert results[0]["date"] == "2026-01-05"
    assert results[1]["date"] == "2026-01-06"
    assert results[0]["valid"] is True
