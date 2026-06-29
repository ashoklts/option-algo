from __future__ import annotations

import numpy as np

from .supertrend import calculate_supertrend, supertrend_from_candles


def test_strong_uptrend_settles_into_uptrend_direction():
    n = 60
    close = np.linspace(100, 160, n)
    high = close + 1
    low = close - 1

    result = calculate_supertrend(high, low, close, period=10, multiplier=3.0)
    valid = ~np.isnan(result["direction"])

    assert result["direction"][valid][-1] == -1.0  # -1 = uptrend (supertrend tracks lower band)
    assert result["supertrend"][valid][-1] < close[-1]


def test_strong_downtrend_settles_into_downtrend_direction():
    n = 60
    close = np.linspace(160, 100, n)
    high = close + 1
    low = close - 1

    result = calculate_supertrend(high, low, close, period=10, multiplier=3.0)
    valid = ~np.isnan(result["direction"])

    assert result["direction"][valid][-1] == 1.0  # 1 = downtrend (supertrend tracks upper band)
    assert result["supertrend"][valid][-1] > close[-1]


def test_band_only_moves_in_trend_favour_once_established():
    n = 60
    close = np.linspace(100, 160, n)
    high = close + 1
    low = close - 1

    result = calculate_supertrend(high, low, close, period=10, multiplier=3.0)
    lower_band_region = result["supertrend"][~np.isnan(result["supertrend"])]
    # In a clean uptrend the trailing stop should be non-decreasing once it locks onto the trend.
    tail = lower_band_region[-20:]
    assert np.all(np.diff(tail) >= -1e-9)


def test_supertrend_from_candles_matches_direct_call():
    candles = [{"open": c, "high": c + 1, "low": c - 1, "close": c, "volume": 1, "oi": 1} for c in np.linspace(100, 120, 30)]
    result = supertrend_from_candles(candles, period=10, multiplier=3.0)
    direct = calculate_supertrend(
        [c["high"] for c in candles], [c["low"] for c in candles], [c["close"] for c in candles], period=10, multiplier=3.0
    )
    np.testing.assert_allclose(result["supertrend"], direct["supertrend"], equal_nan=True)
