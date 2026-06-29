from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from .ema import calculate_ema, ema_from_candles


def test_first_bar_seeds_with_source_not_sma():
    prices = [10, 20, 30, 40, 50]
    result = calculate_ema(prices, period=3)

    assert result[0] == pytest.approx(10.0)
    alpha = 2.0 / 4.0
    assert result[1] == pytest.approx(alpha * 20 + (1 - alpha) * 10)
    assert result[2] == pytest.approx(alpha * 30 + (1 - alpha) * result[1])


def test_matches_pandas_ewm_adjust_false_independent_oracle():
    rng = np.random.default_rng(7)
    prices = rng.uniform(100, 200, size=5000)
    period = 21

    manual = calculate_ema(prices, period)
    oracle = pd.Series(prices).ewm(span=period, adjust=False).mean().to_numpy()

    np.testing.assert_allclose(manual, oracle, rtol=0, atol=1e-9)


def test_no_nan_warmup_unlike_sma():
    result = calculate_ema([1, 2, 3], period=10)
    assert not np.any(np.isnan(result))


def test_period_must_be_positive():
    with pytest.raises(ValueError):
        calculate_ema([1, 2, 3], period=0)


def test_ema_from_candles_uses_value_path():
    candles = [
        {"open": 1, "high": 2, "low": 0, "close": 1, "volume": 100, "oi": 10},
        {"open": 2, "high": 3, "low": 1, "close": 2, "volume": 100, "oi": 10},
        {"open": 3, "high": 4, "low": 2, "close": 3, "volume": 100, "oi": 10},
    ]
    result = ema_from_candles(candles, period=3, value_path="Close")
    assert result[0] == pytest.approx(1.0)
