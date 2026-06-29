from __future__ import annotations

import numpy as np

from .parabolic_sar import calculate_parabolic_sar, parabolic_sar_from_candles


def test_settles_below_price_in_a_clean_uptrend():
    n = 60
    close = np.linspace(100, 160, n)
    high = close + 1
    low = close - 1

    sar = calculate_parabolic_sar(high, low)
    assert np.all(sar[-20:] < close[-20:])  # SAR should sit below price once locked into an uptrend


def test_settles_above_price_in_a_clean_downtrend():
    n = 60
    close = np.linspace(160, 100, n)
    high = close + 1
    low = close - 1

    sar = calculate_parabolic_sar(high, low)
    assert np.all(sar[-20:] > close[-20:])  # SAR should sit above price once locked into a downtrend


def test_short_series_returns_nan():
    sar = calculate_parabolic_sar([10], [9])
    assert sar.shape == (1,)
    assert np.isnan(sar[0])


def test_acceleration_factor_capped_at_maximum():
    # A long, smooth, one-directional move should let AF ramp up to the cap
    # without errors and without the SAR ever overshooting price the wrong way.
    n = 100
    close = np.linspace(100, 300, n)
    high = close + 0.5
    low = close - 0.5
    sar = calculate_parabolic_sar(high, low, acceleration=0.02, maximum=0.2)
    assert np.all(sar[-10:] < low[-10:])


def test_parabolic_sar_from_candles_matches_direct_call():
    candles = [{"open": c, "high": c + 1, "low": c - 1, "close": c, "volume": 1, "oi": 1} for c in np.linspace(100, 120, 30)]
    result = parabolic_sar_from_candles(candles)
    direct = calculate_parabolic_sar([c["high"] for c in candles], [c["low"] for c in candles])
    np.testing.assert_allclose(result, direct, equal_nan=True)
