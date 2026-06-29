from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_ohlc


def calculate_parabolic_sar(
    high: Sequence[float],
    low: Sequence[float],
    acceleration: float = 0.02,
    maximum: float = 0.2,
) -> np.ndarray:
    """
    Parabolic SAR, Wilder's original recursive algorithm (matches TradingView's
    ta.sar(acceleration, acceleration, maximum) - the UI only exposes one
    "acceleration" field, which is used as both the starting and incrementing
    step, the common simplification since Wilder's own defaults set them equal).

    Starts assuming an uptrend with SAR[0] = low[0], EP = high[0]. Because the
    flip condition just checks whether price crosses the current SAR, a wrong
    initial guess self-corrects within the first bar or two and has no effect
    on the rest of the series - same kind of seed-decay as EMA/Wilder smoothing.
    """
    high_arr = np.asarray(high, dtype=np.float64)
    low_arr = np.asarray(low, dtype=np.float64)
    n = high_arr.shape[0]

    sar = np.full(n, np.nan, dtype=np.float64)
    if n < 2:
        return sar

    is_uptrend = True
    af = acceleration
    ep = high_arr[0]
    sar[0] = low_arr[0]

    for i in range(1, n):
        prev_sar = sar[i - 1]
        calc = prev_sar + af * (ep - prev_sar)

        if is_uptrend:
            floor_low = min(low_arr[i - 1], low_arr[i - 2]) if i >= 2 else low_arr[i - 1]
            candidate = min(calc, floor_low)
            if low_arr[i] < candidate:
                is_uptrend = False
                sar[i] = ep
                ep = low_arr[i]
                af = acceleration
            else:
                sar[i] = candidate
                if high_arr[i] > ep:
                    ep = high_arr[i]
                    af = min(af + acceleration, maximum)
        else:
            ceil_high = max(high_arr[i - 1], high_arr[i - 2]) if i >= 2 else high_arr[i - 1]
            candidate = max(calc, ceil_high)
            if high_arr[i] > candidate:
                is_uptrend = True
                sar[i] = ep
                ep = high_arr[i]
                af = acceleration
            else:
                sar[i] = candidate
                if low_arr[i] < ep:
                    ep = low_arr[i]
                    af = min(af + acceleration, maximum)

    return sar


def parabolic_sar_from_candles(
    candles: Sequence[Mapping[str, Any]],
    acceleration: float = 0.02,
    maximum: float = 0.2,
    candle_interval: str | None = None,
) -> np.ndarray:
    """
    Parabolic SAR computed directly off raw OHLCV candles. Pass `candle_interval`
    (e.g. "5 minutes", "1 hour") when `candles` are 1-minute bars and need
    aggregating first; omit it if `candles` are already at the target interval.
    """
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    ohlc = extract_ohlc(candles)
    return calculate_parabolic_sar(ohlc["high"], ohlc["low"], acceleration, maximum)
