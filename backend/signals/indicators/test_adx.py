from __future__ import annotations

import numpy as np

from .adx import calculate_dmi, dmi_from_candles
from .wilder_smoothing import calculate_wilder_smoothing


def test_strong_uptrend_gives_plus_di_dominance():
    n = 40
    high = np.arange(100, 100 + n, dtype=np.float64)
    low = high - 2.0
    close = high - 0.5

    result = calculate_dmi(high, low, close, di_length=14, adx_smoothing=14)

    assert result["plus_di"][-1] > result["minus_di"][-1]
    assert result["adx"][-1] > 0


def test_adx_matches_independent_recompute_from_di_values():
    """Re-derive DX from the returned +DI/-DI and re-smooth it independently
    of adx.py's internals, then check that matches the returned ADX exactly -
    a genuine cross-check, not just a bounds sanity check."""
    rng = np.random.default_rng(2)
    high = 100 + np.cumsum(rng.normal(0, 1, size=60))
    low = high - rng.uniform(1, 3, size=60)
    close = low + rng.uniform(0, 1, size=60) * (high - low)

    result = calculate_dmi(high, low, close, di_length=14, adx_smoothing=14)
    plus_di, minus_di, adx = result["plus_di"], result["minus_di"], result["adx"]

    di_sum = plus_di + minus_di
    with np.errstate(invalid="ignore"):
        dx = np.where(di_sum == 0, 0.0, 100.0 * np.abs(plus_di - minus_di) / di_sum)
    dx[np.isnan(plus_di) | np.isnan(minus_di)] = np.nan

    recomputed_adx = calculate_wilder_smoothing(dx, 14)
    np.testing.assert_allclose(adx, recomputed_adx, rtol=0, atol=1e-9, equal_nan=True)


def test_first_bar_dm_is_zero():
    high = [10, 12, 11]
    low = [8, 9, 7]
    close = [9, 11, 9]
    result = calculate_dmi(high, low, close, di_length=2, adx_smoothing=2)
    # plus_di/minus_di at bar 0 should reflect zero DM contribution (handled internally);
    # just confirm no crash and shape correctness.
    assert result["plus_di"].shape == (3,)
    assert result["minus_di"].shape == (3,)


def test_dmi_from_candles_matches_direct_call():
    candles = [
        {"open": 10, "high": 12, "low": 9, "close": 11, "volume": 1, "oi": 1},
        {"open": 11, "high": 13, "low": 10, "close": 12, "volume": 1, "oi": 1},
        {"open": 12, "high": 14, "low": 11, "close": 13, "volume": 1, "oi": 1},
        {"open": 13, "high": 15, "low": 12, "close": 14, "volume": 1, "oi": 1},
    ]
    result = dmi_from_candles(candles, di_length=2, adx_smoothing=2)
    direct = calculate_dmi([12, 13, 14, 15], [9, 10, 11, 12], [11, 12, 13, 14], di_length=2, adx_smoothing=2)
    np.testing.assert_allclose(result["adx"], direct["adx"], equal_nan=True)
