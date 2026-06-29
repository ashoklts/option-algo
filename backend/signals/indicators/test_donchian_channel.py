from __future__ import annotations

import numpy as np
import pytest

from .donchian_channel import calculate_donchian_channel, donchian_channel_from_candles


def test_matches_hand_calculated_values():
    high = [10, 15, 12]
    low = [5, 8, 9]
    result = calculate_donchian_channel(high, low, period=3)

    assert np.isnan(result["upper"][0])
    assert np.isnan(result["upper"][1])
    assert result["upper"][2] == pytest.approx(15)
    assert result["lower"][2] == pytest.approx(5)
    assert result["basis"][2] == pytest.approx((15 + 5) / 2)


def test_insufficient_data_returns_nan():
    result = calculate_donchian_channel([1, 2], [1, 2], period=5)
    assert np.all(np.isnan(result["upper"]))


def test_donchian_channel_from_candles_uses_high_low():
    candles = [
        {"open": 1, "high": 10, "low": 5, "close": 7, "volume": 1, "oi": 1},
        {"open": 1, "high": 15, "low": 8, "close": 12, "volume": 1, "oi": 1},
        {"open": 1, "high": 12, "low": 9, "close": 10, "volume": 1, "oi": 1},
    ]
    result = donchian_channel_from_candles(candles, period=3)
    assert result["upper"][2] == pytest.approx(15)
    assert result["lower"][2] == pytest.approx(5)
