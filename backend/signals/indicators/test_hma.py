from __future__ import annotations

import numpy as np
import pytest

from .hma import _pine_round, calculate_hma, hma_from_candles
from .wma import calculate_wma


def test_pine_round_ties_away_from_zero_unlike_python_builtin():
    assert _pine_round(4.5) == 5
    assert _pine_round(2.5) == 3
    assert round(4.5) == 4  # Python's banker's rounding - the divergence we must avoid


def test_matches_manual_composition_of_wma():
    rng = np.random.default_rng(3)
    prices = rng.uniform(100, 200, size=200)
    period = 9  # half=4.5 -> rounds to 5, sqrt(9)=3 exactly

    manual = calculate_hma(prices, period)

    half_period = _pine_round(period / 2.0)
    sqrt_period = _pine_round(period ** 0.5)
    expected = calculate_wma(2.0 * calculate_wma(prices, half_period) - calculate_wma(prices, period), sqrt_period)

    np.testing.assert_allclose(manual, expected, rtol=0, atol=1e-9, equal_nan=True)


def test_warms_up_before_producing_values():
    result = calculate_hma([1, 2, 3], period=9)
    assert np.all(np.isnan(result))


def test_period_must_be_positive():
    with pytest.raises(ValueError):
        calculate_hma([1, 2, 3], period=0)


def test_hma_from_candles_uses_value_path():
    rng = np.random.default_rng(5)
    closes = rng.uniform(100, 200, size=30)
    candles = [{"open": c, "high": c, "low": c, "close": c, "volume": 1, "oi": 1} for c in closes]

    result = hma_from_candles(candles, period=9, value_path="Close")
    expected = calculate_hma(closes, period=9)
    np.testing.assert_allclose(result, expected, rtol=0, atol=1e-9, equal_nan=True)
