from __future__ import annotations

import numpy as np
import pytest

from .atr import atr_from_candles, calculate_atr, calculate_true_range


def test_true_range_hand_calculated():
    high = [10, 12, 11]
    low = [8, 9, 7]
    close = [9, 11, 9]
    tr = calculate_true_range(high, low, close)

    assert tr[0] == pytest.approx(10 - 8)  # no previous close
    assert tr[1] == pytest.approx(max(12 - 9, abs(12 - 9), abs(9 - 9)))
    assert tr[2] == pytest.approx(max(11 - 7, abs(11 - 11), abs(7 - 11)))


def test_atr_is_wilder_smoothed_true_range():
    rng = np.random.default_rng(1)
    high = rng.uniform(100, 110, size=50)
    low = high - rng.uniform(1, 5, size=50)
    close = low + rng.uniform(0, (high - low))

    tr = calculate_true_range(high, low, close)
    atr = calculate_atr(high, low, close, period=14)

    assert np.isnan(atr[12])
    assert atr[13] == pytest.approx(tr[:14].mean())  # seed = SMA of first 14 TRs


def test_atr_from_candles_matches_direct_call():
    candles = [
        {"open": 10, "high": 12, "low": 9, "close": 11, "volume": 100, "oi": 1},
        {"open": 11, "high": 13, "low": 10, "close": 12, "volume": 100, "oi": 1},
        {"open": 12, "high": 14, "low": 11, "close": 13, "volume": 100, "oi": 1},
    ]
    result = atr_from_candles(candles, period=2)
    direct = calculate_atr([12, 13, 14], [9, 10, 11], [11, 12, 13], period=2)
    np.testing.assert_allclose(result, direct, equal_nan=True)
