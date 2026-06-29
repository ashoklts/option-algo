from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_value_series


def calculate_wma(values: Sequence[float], period: int) -> np.ndarray:
    """
    Weighted Moving Average, matching TradingView's ta.wma: within each
    window the most recent bar gets weight `period`, the oldest bar in the
    window gets weight 1, linearly in between. NaN for the first
    (period - 1) bars, same as SMA, since a full window is required.
    """
    if period < 1:
        raise ValueError("period must be >= 1")

    prices = np.asarray(values, dtype=np.float64)
    n = prices.shape[0]

    wma = np.full(n, np.nan, dtype=np.float64)
    if n < period:
        return wma

    weights = np.arange(1, period + 1, dtype=np.float64)  # oldest..newest -> 1..period
    weight_sum = weights.sum()

    windows = np.lib.stride_tricks.sliding_window_view(prices, window_shape=period)
    wma[period - 1:] = windows @ weights / weight_sum
    return wma


def wma_from_candles(
    candles: Sequence[Mapping[str, Any]],
    period: int,
    value_path: str = "Close",
    candle_interval: str | None = None,
) -> np.ndarray:
    """
    WMA computed directly off raw OHLCV candles using the indicator's
    `valuePaths` field. Pass `candle_interval` (e.g. "5 minutes", "1 hour")
    when `candles` are 1-minute bars and need aggregating to that interval
    first; omit it if `candles` are already at the target interval.
    """
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    prices = extract_value_series(candles, value_path)
    return calculate_wma(prices, period)
