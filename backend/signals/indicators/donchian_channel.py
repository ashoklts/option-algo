from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_ohlc


def calculate_donchian_channel(high: Sequence[float], low: Sequence[float], period: int) -> dict[str, np.ndarray]:
    """
    Donchian Channel: upper = highest(high, period), lower = lowest(low, period)
    (each window includes the current bar, matching ta.highest/ta.lowest),
    basis = midpoint of the two. NaN for the first (period - 1) bars.
    """
    if period < 1:
        raise ValueError("period must be >= 1")

    high_arr = np.asarray(high, dtype=np.float64)
    low_arr = np.asarray(low, dtype=np.float64)
    n = high_arr.shape[0]

    upper = np.full(n, np.nan, dtype=np.float64)
    lower = np.full(n, np.nan, dtype=np.float64)

    if n >= period:
        upper[period - 1:] = np.lib.stride_tricks.sliding_window_view(high_arr, period).max(axis=-1)
        lower[period - 1:] = np.lib.stride_tricks.sliding_window_view(low_arr, period).min(axis=-1)

    basis = (upper + lower) / 2.0
    return {"upper": upper, "lower": lower, "basis": basis}


def donchian_channel_from_candles(
    candles: Sequence[Mapping[str, Any]],
    period: int,
    candle_interval: str | None = None,
) -> dict[str, np.ndarray]:
    """
    Donchian Channel computed directly off raw OHLCV candles. Pass
    `candle_interval` (e.g. "5 minutes", "1 hour") when `candles` are
    1-minute bars and need aggregating first.
    """
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    ohlc = extract_ohlc(candles)
    return calculate_donchian_channel(ohlc["high"], ohlc["low"], period)
