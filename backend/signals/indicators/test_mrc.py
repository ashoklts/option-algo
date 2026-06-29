from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from .mrc import calculate_linreg, calculate_mrc, mrc_from_candles


def test_linreg_matches_pandas_independent_oracle():
    rng = np.random.default_rng(10)
    prices = rng.uniform(100, 200, size=200)
    period = 14

    manual = calculate_linreg(prices, period)

    def _ols_endpoint(window):
        x = np.arange(len(window))
        slope, intercept = np.polyfit(x, window, 1)
        return intercept + slope * (len(window) - 1)

    oracle = pd.Series(prices).rolling(period).apply(_ols_endpoint, raw=True).to_numpy()
    np.testing.assert_allclose(manual, oracle, rtol=0, atol=1e-9, equal_nan=True)


def test_linreg_of_a_straight_line_is_exact():
    prices = [10, 12, 14, 16, 18]  # perfectly linear, slope=2
    result = calculate_linreg(prices, period=5)
    assert result[-1] == pytest.approx(18.0)


def test_period_must_be_at_least_two():
    with pytest.raises(ValueError):
        calculate_linreg([1, 2, 3], period=1)


def test_mrc_bands_are_basis_plus_minus_mult_atr():
    rng = np.random.default_rng(11)
    high = 100 + np.cumsum(rng.normal(0, 1, size=60))
    low = high - rng.uniform(1, 3, size=60)
    close = low + rng.uniform(0, 1, size=60) * (high - low)

    result = calculate_mrc(high, low, close, period=14, mult=2.0)
    np.testing.assert_allclose(result["upper"] - result["basis"], result["basis"] - result["lower"], equal_nan=True)


def test_mrc_from_candles_matches_direct_call():
    candles = [{"open": c, "high": c + 1, "low": c - 1, "close": c, "volume": 1, "oi": 1} for c in np.linspace(100, 130, 40)]
    result = mrc_from_candles(candles, period=14, mult=2.0)
    direct = calculate_mrc(
        [c["high"] for c in candles], [c["low"] for c in candles], [c["close"] for c in candles], period=14, mult=2.0
    )
    np.testing.assert_allclose(result["basis"], direct["basis"], equal_nan=True)
