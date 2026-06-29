from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_ohlc
from .wilder_smoothing import calculate_wilder_smoothing


def _shift_forward(series: np.ndarray, offset: int) -> np.ndarray:
    n = series.shape[0]
    if offset == 0:
        return series.copy()
    out = np.full(n, np.nan, dtype=np.float64)
    if 0 < offset < n:
        out[offset:] = series[: n - offset]
    return out


def calculate_williams_alligator(
    high: Sequence[float],
    low: Sequence[float],
    jaw_length: int = 13,
    jaw_offset: int = 8,
    teeth_length: int = 8,
    teeth_offset: int = 5,
    lips_length: int = 5,
    lips_offset: int = 3,
) -> dict[str, np.ndarray]:
    """
    Williams Alligator (Bill Williams). Each line is a Smoothed Moving Average
    of the median price (high+low)/2 - SMMA here is exactly Wilder's
    smoothing, so this reuses wilder_smoothing.py rather than re-deriving it -
    then shifted forward by its own offset (default Jaw 13/8, Teeth 8/5,
    Lips 5/3, matching Bill Williams' published defaults).

    As with ichimoku.py's Senkou spans, the shift here is applied to the
    series itself so jaw[i]/teeth[i]/lips[i] is the value actively overlaying
    bar i right now, rather than Pine's chart-only plot offset.
    """
    high_arr = np.asarray(high, dtype=np.float64)
    low_arr = np.asarray(low, dtype=np.float64)
    median_price = (high_arr + low_arr) / 2.0

    jaw_raw = calculate_wilder_smoothing(median_price, jaw_length)
    teeth_raw = calculate_wilder_smoothing(median_price, teeth_length)
    lips_raw = calculate_wilder_smoothing(median_price, lips_length)

    return {
        "jaw": _shift_forward(jaw_raw, jaw_offset),
        "teeth": _shift_forward(teeth_raw, teeth_offset),
        "lips": _shift_forward(lips_raw, lips_offset),
    }


def williams_alligator_from_candles(
    candles: Sequence[Mapping[str, Any]],
    jaw_length: int = 13,
    jaw_offset: int = 8,
    teeth_length: int = 8,
    teeth_offset: int = 5,
    lips_length: int = 5,
    lips_offset: int = 3,
    candle_interval: str | None = None,
) -> dict[str, np.ndarray]:
    """
    Williams Alligator computed directly off raw OHLCV candles. Pass
    `candle_interval` (e.g. "5 minutes", "1 hour") when `candles` are
    1-minute bars and need aggregating first.
    """
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    ohlc = extract_ohlc(candles)
    return calculate_williams_alligator(
        ohlc["high"], ohlc["low"], jaw_length, jaw_offset, teeth_length, teeth_offset, lips_length, lips_offset
    )
