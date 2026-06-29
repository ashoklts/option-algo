from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_ohlc
from .atr import calculate_true_range
from .wilder_smoothing import calculate_wilder_smoothing


def calculate_dmi(
    high: Sequence[float],
    low: Sequence[float],
    close: Sequence[float],
    di_length: int,
    adx_smoothing: int,
) -> dict[str, np.ndarray]:
    """
    Directional Movement Index, matching TradingView's ta.dmi(diLength, adxSmoothing):
    +DM/-DM and True Range are each Wilder-smoothed (ta.rma) over `di_length`,
    +DI/-DI are derived from those, and ADX is the Wilder smoothing of the
    DX ratio over `adx_smoothing`.

    Bar 0 has no previous bar, so +DM/-DM there are treated as 0 (same
    handle_na=true convention used for True Range bar 0 in atr.py) - this
    only shifts where the very first non-NaN value appears by at most one
    bar and has no effect once enough history has accumulated.
    """
    if di_length < 1 or adx_smoothing < 1:
        raise ValueError("di_length and adx_smoothing must be >= 1")

    high_arr = np.asarray(high, dtype=np.float64)
    low_arr = np.asarray(low, dtype=np.float64)
    close_arr = np.asarray(close, dtype=np.float64)
    n = high_arr.shape[0]

    plus_dm = np.zeros(n, dtype=np.float64)
    minus_dm = np.zeros(n, dtype=np.float64)
    if n > 1:
        up_move = high_arr[1:] - high_arr[:-1]
        down_move = low_arr[:-1] - low_arr[1:]
        plus_dm[1:] = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm[1:] = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr = calculate_true_range(high_arr, low_arr, close_arr)

    smoothed_tr = calculate_wilder_smoothing(tr, di_length)
    smoothed_plus_dm = calculate_wilder_smoothing(plus_dm, di_length)
    smoothed_minus_dm = calculate_wilder_smoothing(minus_dm, di_length)

    with np.errstate(divide="ignore", invalid="ignore"):
        plus_di = 100.0 * smoothed_plus_dm / smoothed_tr
        minus_di = 100.0 * smoothed_minus_dm / smoothed_tr

        di_sum = plus_di + minus_di
        dx = np.where(di_sum == 0, 0.0, 100.0 * np.abs(plus_di - minus_di) / di_sum)
    dx[np.isnan(plus_di) | np.isnan(minus_di)] = np.nan

    adx = calculate_wilder_smoothing(dx, adx_smoothing)

    return {"plus_di": plus_di, "minus_di": minus_di, "adx": adx}


def dmi_from_candles(
    candles: Sequence[Mapping[str, Any]],
    di_length: int,
    adx_smoothing: int,
    candle_interval: str | None = None,
) -> dict[str, np.ndarray]:
    """
    DMI/ADX computed directly off raw OHLCV candles. Pass `candle_interval`
    (e.g. "5 minutes", "1 hour") when `candles` are 1-minute bars and need
    aggregating first; omit it if `candles` are already at the target interval.
    """
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    ohlc = extract_ohlc(candles)
    return calculate_dmi(ohlc["high"], ohlc["low"], ohlc["close"], di_length, adx_smoothing)
