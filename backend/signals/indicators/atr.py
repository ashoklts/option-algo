from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_ohlc
from .wilder_smoothing import calculate_wilder_smoothing


def calculate_true_range(high: Sequence[float], low: Sequence[float], close: Sequence[float]) -> np.ndarray:
    """
    True Range, matching TradingView's ta.tr(true): bar 0 (no previous close)
    is just high-low; every bar after that takes the largest of high-low,
    |high - prevClose|, |low - prevClose|.
    """
    high_arr = np.asarray(high, dtype=np.float64)
    low_arr = np.asarray(low, dtype=np.float64)
    close_arr = np.asarray(close, dtype=np.float64)
    n = high_arr.shape[0]

    tr = np.empty(n, dtype=np.float64)
    if n == 0:
        return tr

    tr[0] = high_arr[0] - low_arr[0]
    if n > 1:
        prev_close = close_arr[:-1]
        tr[1:] = np.maximum(
            high_arr[1:] - low_arr[1:],
            np.maximum(np.abs(high_arr[1:] - prev_close), np.abs(low_arr[1:] - prev_close)),
        )
    return tr


def calculate_atr(high: Sequence[float], low: Sequence[float], close: Sequence[float], period: int) -> np.ndarray:
    """Average True Range: Wilder smoothing (ta.rma) of True Range - matches TradingView's ta.atr."""
    tr = calculate_true_range(high, low, close)
    return calculate_wilder_smoothing(tr, period)


def atr_from_candles(
    candles: Sequence[Mapping[str, Any]],
    period: int,
    candle_interval: str | None = None,
) -> np.ndarray:
    """
    ATR computed directly off raw OHLCV candles. Pass `candle_interval` (e.g.
    "5 minutes", "1 hour") when `candles` are 1-minute bars and need
    aggregating first; omit it if `candles` are already at the target interval.
    """
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    ohlc = extract_ohlc(candles)
    return calculate_atr(ohlc["high"], ohlc["low"], ohlc["close"], period)
