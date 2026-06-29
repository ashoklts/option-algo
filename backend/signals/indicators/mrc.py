from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_ohlc, extract_value_series
from .atr import calculate_atr

# UNVERIFIED AGAINST A LIVE TRADINGVIEW CHART. "Mean Reversion Channel" is a
# community indicator with the LEAST consensus of any indicator in this
# module - different published versions use different basis lines (linear
# regression vs SMA vs EMA) and different band logic (ATR multiples vs plain
# stdev). This app's UI only exposes (noOfCandles, multiplier, valuePaths) -
# the same shape as Keltner Channel - so this implementation picks the most
# commonly cited structure for that shape: a linear-regression basis with a
# single ATR-multiple band. If your TradingView reference disagrees, the
# basis line type is the first thing to check, before the band math.


def calculate_linreg(values: Sequence[float], period: int) -> np.ndarray:
    """
    Rolling linear regression value at the most recent point of each window
    (ordinary least squares, x = 0..period-1 oldest-to-newest) - matches
    TradingView's ta.linreg(source, period, 0).
    """
    if period < 2:
        raise ValueError("period must be >= 2 for linear regression")

    prices = np.asarray(values, dtype=np.float64)
    n = prices.shape[0]
    result = np.full(n, np.nan, dtype=np.float64)
    if n < period:
        return result

    x = np.arange(period, dtype=np.float64)
    sum_x = x.sum()
    sum_x2 = (x ** 2).sum()
    denom = period * sum_x2 - sum_x ** 2

    windows = np.lib.stride_tricks.sliding_window_view(prices, period)
    sum_y = windows.sum(axis=-1)
    sum_xy = windows @ x

    slope = (period * sum_xy - sum_x * sum_y) / denom
    intercept = (sum_y - slope * sum_x) / period
    result[period - 1:] = intercept + slope * (period - 1)
    return result


def calculate_mrc(
    high: Sequence[float],
    low: Sequence[float],
    close: Sequence[float],
    period: int,
    mult: float,
    source: Sequence[float] | None = None,
) -> dict[str, np.ndarray]:
    close_arr = np.asarray(close, dtype=np.float64)
    src = np.asarray(source, dtype=np.float64) if source is not None else close_arr

    basis = calculate_linreg(src, period)
    atr = calculate_atr(high, low, close_arr, period)

    return {"basis": basis, "upper": basis + mult * atr, "lower": basis - mult * atr}


def mrc_from_candles(
    candles: Sequence[Mapping[str, Any]],
    period: int,
    mult: float,
    value_path: str = "Close",
    candle_interval: str | None = None,
) -> dict[str, np.ndarray]:
    """MRC computed directly off raw OHLCV candles - see module docstring for the accuracy caveat."""
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    ohlc = extract_ohlc(candles)
    source = extract_value_series(candles, value_path)
    return calculate_mrc(ohlc["high"], ohlc["low"], ohlc["close"], period, mult, source)
