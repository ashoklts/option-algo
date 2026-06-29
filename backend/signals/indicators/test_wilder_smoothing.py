from __future__ import annotations

import numpy as np
import pytest

from .wilder_smoothing import (
    calculate_moving_average_for_atr,
    calculate_moving_average_for_rsi,
    calculate_smma,
    calculate_wilder_smoothing,
    wilder_smoothing_from_candles,
)


def _reference_wilder_smoothing(values, period):
    """Deliberately separate, naive loop implementation used only to cross-check
    the production code against an independently-written formula."""
    n = len(values)
    out = [float("nan")] * n
    if n < period:
        return out

    out[period - 1] = sum(values[:period]) / period
    alpha = 1.0 / period
    for i in range(period, n):
        out[i] = alpha * values[i] + (1 - alpha) * out[i - 1]
    return out


def test_seed_is_sma_of_first_window_not_first_bar():
    prices = [10, 20, 30, 40, 50]
    result = calculate_wilder_smoothing(prices, period=3)

    assert np.isnan(result[0])
    assert np.isnan(result[1])
    assert result[2] == pytest.approx((10 + 20 + 30) / 3)

    alpha = 1.0 / 3
    assert result[3] == pytest.approx(alpha * 40 + (1 - alpha) * result[2])
    assert result[4] == pytest.approx(alpha * 50 + (1 - alpha) * result[3])


def test_matches_independent_reference_loop():
    rng = np.random.default_rng(13)
    prices = rng.uniform(50, 500, size=2000).tolist()
    period = 14

    manual = calculate_wilder_smoothing(prices, period)
    reference = _reference_wilder_smoothing(prices, period)

    np.testing.assert_allclose(manual, reference, rtol=0, atol=1e-9, equal_nan=True)


def test_insufficient_data_returns_all_nan():
    result = calculate_wilder_smoothing([1, 2, 3], period=5)
    assert np.all(np.isnan(result))


def test_period_must_be_positive():
    with pytest.raises(ValueError):
        calculate_wilder_smoothing([1, 2, 3], period=0)


def test_catalog_aliases_are_the_same_function():
    assert calculate_smma is calculate_wilder_smoothing
    assert calculate_moving_average_for_atr is calculate_wilder_smoothing
    assert calculate_moving_average_for_rsi is calculate_wilder_smoothing


def test_wilder_smoothing_from_candles_uses_value_path():
    candles = [
        {"open": 1, "high": 2, "low": 0, "close": 1, "volume": 100, "oi": 10},
        {"open": 2, "high": 3, "low": 1, "close": 2, "volume": 100, "oi": 10},
        {"open": 3, "high": 4, "low": 2, "close": 3, "volume": 100, "oi": 10},
    ]
    result = wilder_smoothing_from_candles(candles, period=3, value_path="Close")
    assert result[2] == pytest.approx((1 + 2 + 3) / 3)
