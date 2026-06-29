from __future__ import annotations

import numpy as np

from .half_trend import calculate_half_trend, half_trend_from_candles


def test_trend_is_always_zero_or_one():
    rng = np.random.default_rng(9)
    high = 100 + np.cumsum(rng.normal(0, 1, size=200))
    low = high - rng.uniform(1, 3, size=200)
    close = low + rng.uniform(0, 1, size=200) * (high - low)

    result = calculate_half_trend(high, low, close, amplitude=2, channel_deviation=2.0, atr_period=20)
    valid_trend = result["trend"][~np.isnan(result["trend"])]
    assert set(np.unique(valid_trend)).issubset({0.0, 1.0})


def test_half_trend_line_stays_within_plausible_price_envelope():
    n = 100
    close = np.linspace(100, 160, n)
    high = close + 1
    low = close - 1

    result = calculate_half_trend(high, low, close, amplitude=2, channel_deviation=2.0, atr_period=20)
    valid = result["half_trend"][~np.isnan(result["half_trend"])]
    assert np.all(valid >= low.min() - 5)
    assert np.all(valid <= high.max() + 5)


def test_half_trend_from_candles_matches_direct_call():
    candles = [{"open": c, "high": c + 1, "low": c - 1, "close": c, "volume": 1, "oi": 1} for c in np.linspace(100, 130, 60)]
    result = half_trend_from_candles(candles, amplitude=2, channel_deviation=2.0, atr_period=20)
    direct = calculate_half_trend(
        [c["high"] for c in candles], [c["low"] for c in candles], [c["close"] for c in candles],
        amplitude=2, channel_deviation=2.0, atr_period=20,
    )
    np.testing.assert_allclose(result["half_trend"], direct["half_trend"], equal_nan=True)
