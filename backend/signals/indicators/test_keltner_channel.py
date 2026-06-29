from __future__ import annotations

import numpy as np

from .keltner_channel import calculate_keltner_channel, keltner_channel_from_candles
from .atr import calculate_atr
from .ema import calculate_ema


def test_basis_is_ema_and_bands_are_atr_multiples():
    rng = np.random.default_rng(8)
    high = 100 + np.cumsum(rng.normal(0, 1, size=60))
    low = high - rng.uniform(1, 3, size=60)
    close = low + rng.uniform(0, 1, size=60) * (high - low)

    result = calculate_keltner_channel(high, low, close, period=20, mult=2.0)

    expected_basis = calculate_ema(close, 20)
    expected_atr = calculate_atr(high, low, close, 20)

    np.testing.assert_allclose(result["basis"], expected_basis, equal_nan=True)
    np.testing.assert_allclose(result["upper"], expected_basis + 2.0 * expected_atr, equal_nan=True)
    np.testing.assert_allclose(result["lower"], expected_basis - 2.0 * expected_atr, equal_nan=True)


def test_custom_source_used_for_basis_not_close():
    high = list(range(100, 130))
    low = [h - 2 for h in high]
    close = [h - 1 for h in high]
    custom_source = [h - 5 for h in high]  # deliberately different from close

    result = calculate_keltner_channel(high, low, close, period=10, mult=1.0, source=custom_source)
    expected_basis = calculate_ema(custom_source, 10)
    np.testing.assert_allclose(result["basis"], expected_basis, equal_nan=True)


def test_keltner_channel_from_candles_matches_direct_call():
    candles = [{"open": c, "high": c + 1, "low": c - 1, "close": c, "volume": 1, "oi": 1} for c in np.linspace(100, 120, 30)]
    result = keltner_channel_from_candles(candles, period=10, mult=2.0)
    direct = calculate_keltner_channel(
        [c["high"] for c in candles], [c["low"] for c in candles], [c["close"] for c in candles], period=10, mult=2.0
    )
    np.testing.assert_allclose(result["basis"], direct["basis"], equal_nan=True)
