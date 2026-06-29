from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_ohlc, extract_value_series
from .atr import calculate_atr
from .ema import calculate_ema


def calculate_keltner_channel(
    high: Sequence[float],
    low: Sequence[float],
    close: Sequence[float],
    period: int = 20,
    mult: float = 2.0,
    source: Sequence[float] | None = None,
) -> dict[str, np.ndarray]:
    """
    Keltner Channel, matching TradingView's default built-in: basis is an EMA
    of `source` (defaults to close), bands are basis +/- mult * ATR(period) -
    the default "Bands Style" is Average True Range, not a plain stdev like
    Bollinger. Reuses ema.py and atr.py rather than re-deriving either.
    """
    close_arr = np.asarray(close, dtype=np.float64)
    src = np.asarray(source, dtype=np.float64) if source is not None else close_arr

    basis = calculate_ema(src, period)
    atr = calculate_atr(high, low, close_arr, period)

    return {"basis": basis, "upper": basis + mult * atr, "lower": basis - mult * atr}


def keltner_channel_from_candles(
    candles: Sequence[Mapping[str, Any]],
    period: int = 20,
    mult: float = 2.0,
    value_path: str = "Close",
    candle_interval: str | None = None,
) -> dict[str, np.ndarray]:
    """
    Keltner Channel computed directly off raw OHLCV candles, with the basis
    EMA using the indicator's `valuePaths` field. Pass `candle_interval`
    (e.g. "5 minutes", "1 hour") when `candles` are 1-minute bars and need
    aggregating first.
    """
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    ohlc = extract_ohlc(candles)
    source = extract_value_series(candles, value_path)
    return calculate_keltner_channel(ohlc["high"], ohlc["low"], ohlc["close"], period, mult, source)
