from __future__ import annotations

import numpy as np
import pytest

from .trend_intensity_index import calculate_trend_intensity_index, trend_intensity_index_from_candles


def test_output_is_bounded_zero_to_hundred():
    rng = np.random.default_rng(12)
    prices = 100 + np.cumsum(rng.normal(0, 1, size=100))
    result = calculate_trend_intensity_index(prices, period=20)
    valid = result[~np.isnan(result)]
    assert np.all(valid >= 0.0)
    assert np.all(valid <= 100.0)


def test_strong_uptrend_gives_high_tii():
    prices = np.linspace(100, 200, 60)  # monotonically rising -> all deviations should skew positive
    result = calculate_trend_intensity_index(prices, period=20)
    valid = result[~np.isnan(result)]
    assert valid[-1] > 80.0


def test_period_must_be_at_least_two():
    with pytest.raises(ValueError):
        calculate_trend_intensity_index([1, 2, 3], period=1)


def test_trend_intensity_index_from_candles_matches_direct_call():
    candles = [{"open": c, "high": c + 1, "low": c - 1, "close": c, "volume": 1, "oi": 1} for c in np.linspace(100, 130, 60)]
    result = trend_intensity_index_from_candles(candles, period=20, value_path="Close")
    direct = calculate_trend_intensity_index([c["close"] for c in candles], period=20)
    np.testing.assert_allclose(result, direct, equal_nan=True)
