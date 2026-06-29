from __future__ import annotations

import numpy as np

from .chandelier_exit import calculate_chandelier_exit, chandelier_exit_from_candles


def test_uptrend_settles_into_long_direction_with_rising_long_stop():
    n = 60
    close = np.linspace(100, 160, n)
    high = close + 1
    low = close - 1

    result = calculate_chandelier_exit(high, low, close, period=10, mult=3.0)
    valid = ~np.isnan(result["direction"])

    assert result["direction"][valid][-1] == 1.0
    tail = result["long_stop"][valid][-20:]
    assert np.all(np.diff(tail) >= -1e-9)  # ratchets, never gives back ground


def test_downtrend_settles_into_short_direction():
    n = 60
    close = np.linspace(160, 100, n)
    high = close + 1
    low = close - 1

    result = calculate_chandelier_exit(high, low, close, period=10, mult=3.0)
    valid = ~np.isnan(result["direction"])
    assert result["direction"][valid][-1] == -1.0


def test_chandelier_exit_from_candles_matches_direct_call():
    candles = [{"open": c, "high": c + 1, "low": c - 1, "close": c, "volume": 1, "oi": 1} for c in np.linspace(100, 130, 40)]
    result = chandelier_exit_from_candles(candles, period=10, mult=3.0)
    direct = calculate_chandelier_exit(
        [c["high"] for c in candles], [c["low"] for c in candles], [c["close"] for c in candles], period=10, mult=3.0
    )
    np.testing.assert_allclose(result["long_stop"], direct["long_stop"], equal_nan=True)
