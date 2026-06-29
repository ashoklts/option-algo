from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_value_series
from .sma import calculate_sma


def calculate_bollinger_bands(values: Sequence[float], period: int, mult: float = 2.0) -> dict[str, np.ndarray]:
    """
    Bollinger Bands, matching TradingView's ta.bb: basis is a plain SMA, and
    the bands are basis +/- mult * population standard deviation (ddof=0 -
    divide by `period`, NOT `period - 1`). numpy's default ddof=0 already
    matches this, but it's easy to get wrong if you reach for pandas/numpy
    defaults elsewhere, since pandas' default .std() is ddof=1 (sample).
    """
    if period < 1:
        raise ValueError("period must be >= 1")

    prices = np.asarray(values, dtype=np.float64)
    n = prices.shape[0]

    basis = calculate_sma(prices, period)
    upper = np.full(n, np.nan, dtype=np.float64)
    lower = np.full(n, np.nan, dtype=np.float64)

    if n >= period:
        windows = np.lib.stride_tricks.sliding_window_view(prices, window_shape=period)
        stdev = windows.std(axis=-1, ddof=0)
        upper[period - 1:] = basis[period - 1:] + mult * stdev
        lower[period - 1:] = basis[period - 1:] - mult * stdev

    return {"basis": basis, "upper": upper, "lower": lower}


def bollinger_bands_from_candles(
    candles: Sequence[Mapping[str, Any]],
    period: int,
    mult: float = 2.0,
    value_path: str = "Close",
    candle_interval: str | None = None,
) -> dict[str, np.ndarray]:
    """
    Bollinger Bands computed directly off raw OHLCV candles using the
    indicator's `valuePaths` field. Pass `candle_interval` (e.g. "5 minutes",
    "1 hour") when `candles` are 1-minute bars and need aggregating first.
    """
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    prices = extract_value_series(candles, value_path)
    return calculate_bollinger_bands(prices, period, mult)
