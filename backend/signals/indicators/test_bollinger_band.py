from __future__ import annotations

import numpy as np
import pytest

from .bollinger_band import bollinger_bands_from_candles, calculate_bollinger_bands


def test_matches_hand_calculated_population_stdev():
    prices = [10, 12, 14]
    result = calculate_bollinger_bands(prices, period=3, mult=2.0)

    mean = sum(prices) / 3
    pop_var = sum((p - mean) ** 2 for p in prices) / 3  # ddof=0, NOT /2
    pop_std = pop_var ** 0.5

    assert result["basis"][2] == pytest.approx(mean)
    assert result["upper"][2] == pytest.approx(mean + 2 * pop_std)
    assert result["lower"][2] == pytest.approx(mean - 2 * pop_std)


def test_ddof_is_population_not_sample():
    # Sanity check that we did not accidentally use pandas-style ddof=1.
    prices = [1, 2, 3, 4]
    result = calculate_bollinger_bands(prices, period=4, mult=1.0)
    pop_std = np.std(prices, ddof=0)
    sample_std = np.std(prices, ddof=1)
    assert result["upper"][3] == pytest.approx(result["basis"][3] + pop_std)
    assert result["upper"][3] != pytest.approx(result["basis"][3] + sample_std)


def test_insufficient_data_returns_nan_bands():
    result = calculate_bollinger_bands([1, 2], period=5)
    assert np.all(np.isnan(result["upper"]))
    assert np.all(np.isnan(result["lower"]))


def test_bollinger_bands_from_candles_uses_value_path():
    candles = [
        {"open": 1, "high": 2, "low": 0, "close": 10, "volume": 1, "oi": 1},
        {"open": 1, "high": 2, "low": 0, "close": 12, "volume": 1, "oi": 1},
        {"open": 1, "high": 2, "low": 0, "close": 14, "volume": 1, "oi": 1},
    ]
    result = bollinger_bands_from_candles(candles, period=3, value_path="Close")
    assert result["basis"][2] == pytest.approx(12.0)
