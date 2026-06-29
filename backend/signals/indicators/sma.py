from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_value_series


def calculate_sma(values: Sequence[float], period: int) -> np.ndarray:
    """
    Simple Moving Average. output[i] is the mean of values[i - period + 1 : i + 1];
    the first (period - 1) entries are NaN rather than a partial-window average,
    since a short window changes the value enough to flip a live signal.

    Each window is summed independently instead of via a running cumulative sum,
    so floating-point error can't accumulate across a long candle history.
    """
    if period < 1:
        raise ValueError("period must be >= 1")

    prices = np.asarray(values, dtype=np.float64)
    n = prices.shape[0]

    sma = np.full(n, np.nan, dtype=np.float64)
    if n < period:
        return sma

    windows = np.lib.stride_tricks.sliding_window_view(prices, window_shape=period)
    sma[period - 1:] = windows.mean(axis=-1)
    return sma


def sma_from_candles(
    candles: Sequence[Mapping[str, Any]],
    period: int,
    value_path: str = "Close",
    candle_interval: str | None = None,
) -> np.ndarray:
    """
    SMA computed directly off raw OHLCV candles using the indicator's
    `valuePaths` field. Pass `candle_interval` (e.g. "5 minutes", "1 hour")
    when `candles` are 1-minute bars and need aggregating to that interval
    first; omit it if `candles` are already at the target interval.
    """
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    prices = extract_value_series(candles, value_path)
    return calculate_sma(prices, period)
