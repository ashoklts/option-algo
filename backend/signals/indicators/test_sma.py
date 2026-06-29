from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from .sma import calculate_sma, sma_from_candles


def test_matches_hand_calculated_values():
    prices = [10, 11, 12, 13, 14]
    result = calculate_sma(prices, period=3)

    assert np.isnan(result[0])
    assert np.isnan(result[1])
    assert result[2] == pytest.approx((10 + 11 + 12) / 3)
    assert result[3] == pytest.approx((11 + 12 + 13) / 3)
    assert result[4] == pytest.approx((12 + 13 + 14) / 3)


def test_matches_pandas_rolling_mean_independent_oracle():
    rng = np.random.default_rng(42)
    prices = rng.uniform(100, 200, size=5000)
    period = 21

    manual = calculate_sma(prices, period)
    oracle = pd.Series(prices).rolling(window=period).mean().to_numpy()

    np.testing.assert_allclose(manual, oracle, rtol=0, atol=1e-9, equal_nan=True)


def test_insufficient_data_returns_all_nan():
    result = calculate_sma([1, 2, 3], period=5)
    assert result.shape == (3,)
    assert np.all(np.isnan(result))


def test_period_must_be_positive():
    with pytest.raises(ValueError):
        calculate_sma([1, 2, 3], period=0)


def test_period_of_one_returns_input_unchanged():
    prices = [5, 6, 7]
    result = calculate_sma(prices, period=1)
    np.testing.assert_allclose(result, prices)


def test_sma_from_candles_uses_value_path():
    candles = [
        {"open": 1, "high": 2, "low": 0, "close": 1, "volume": 100, "oi": 10},
        {"open": 2, "high": 3, "low": 1, "close": 2, "volume": 100, "oi": 10},
        {"open": 3, "high": 4, "low": 2, "close": 3, "volume": 100, "oi": 10},
    ]

    close_result = sma_from_candles(candles, period=3, value_path="Close")
    assert close_result[2] == pytest.approx((1 + 2 + 3) / 3)

    hlc3_result = sma_from_candles(candles, period=3, value_path="HLC3")
    hlc3_values = [(2 + 0 + 1) / 3, (3 + 1 + 2) / 3, (4 + 2 + 3) / 3]
    assert hlc3_result[2] == pytest.approx(sum(hlc3_values) / 3)
