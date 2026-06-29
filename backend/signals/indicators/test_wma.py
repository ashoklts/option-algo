from __future__ import annotations

import numpy as np
import pytest

from .wma import calculate_wma, wma_from_candles


def _reference_wma(values, period):
    """Deliberately separate, naive loop implementation used only to cross-check
    the vectorised production code against an independently-written formula."""
    n = len(values)
    out = [float("nan")] * n
    for end in range(period - 1, n):
        window = values[end - period + 1: end + 1]
        weights = list(range(1, period + 1))
        weighted_sum = sum(v * w for v, w in zip(window, weights))
        out[end] = weighted_sum / sum(weights)
    return out


def test_matches_hand_calculated_values():
    prices = [10, 20, 30]
    result = calculate_wma(prices, period=3)
    # weights: oldest=1, middle=2, newest=3 ; norm = 6
    expected = (10 * 1 + 20 * 2 + 30 * 3) / 6
    assert np.isnan(result[0])
    assert np.isnan(result[1])
    assert result[2] == pytest.approx(expected)


def test_matches_independent_reference_loop():
    rng = np.random.default_rng(11)
    prices = rng.uniform(50, 500, size=500).tolist()
    period = 14

    manual = calculate_wma(prices, period)
    reference = _reference_wma(prices, period)

    np.testing.assert_allclose(manual, reference, rtol=0, atol=1e-9, equal_nan=True)


def test_insufficient_data_returns_all_nan():
    result = calculate_wma([1, 2, 3], period=5)
    assert np.all(np.isnan(result))


def test_period_must_be_positive():
    with pytest.raises(ValueError):
        calculate_wma([1, 2, 3], period=0)


def test_wma_from_candles_uses_value_path():
    candles = [
        {"open": 1, "high": 2, "low": 0, "close": 1, "volume": 100, "oi": 10},
        {"open": 2, "high": 3, "low": 1, "close": 2, "volume": 100, "oi": 10},
        {"open": 3, "high": 4, "low": 2, "close": 3, "volume": 100, "oi": 10},
    ]
    result = wma_from_candles(candles, period=3, value_path="Close")
    expected = (1 * 1 + 2 * 2 + 3 * 3) / 6
    assert result[2] == pytest.approx(expected)
