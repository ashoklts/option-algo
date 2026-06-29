from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from ._candle_interval import resample_candles
from ._value_source import extract_value_series


def calculate_ema(values: Sequence[float], period: int) -> np.ndarray:
    """
    Exponential Moving Average, seeded exactly like TradingView's ta.ema:
    the very first bar's EMA *is* the first source value (no SMA seed, no
    NaN warm-up) and every bar after that recurses with alpha = 2/(period+1).

    This matters when cross-checking against TradingView bar-by-bar: an
    SMA-seeded EMA (common in other libraries/platforms) only converges to
    the same values after several periods and will visibly disagree near
    the start of whatever candle history you feed it.

    `values` may itself start with NaN (e.g. an EMA of an already-smoothed
    upstream series that has its own warm-up) - the first non-NaN value is
    treated as bar 0, rather than blindly seeding off raw index 0. Seeding
    off a NaN would poison the whole recursion to NaN forever.
    """
    if period < 1:
        raise ValueError("period must be >= 1")

    prices = np.asarray(values, dtype=np.float64)
    n = prices.shape[0]
    ema = np.full(n, np.nan, dtype=np.float64)

    valid_mask = ~np.isnan(prices)
    if not valid_mask.any():
        return ema
    first_valid = int(np.argmax(valid_mask))

    alpha = 2.0 / (period + 1.0)
    ema[first_valid] = prices[first_valid]
    for i in range(first_valid + 1, n):
        ema[i] = alpha * prices[i] + (1.0 - alpha) * ema[i - 1]
    return ema


def ema_from_candles(
    candles: Sequence[Mapping[str, Any]],
    period: int,
    value_path: str = "Close",
    candle_interval: str | None = None,
) -> np.ndarray:
    """
    EMA computed directly off raw OHLCV candles using the indicator's
    `valuePaths` field. Pass `candle_interval` (e.g. "5 minutes", "1 hour")
    when `candles` are 1-minute bars and need aggregating to that interval
    first; omit it if `candles` are already at the target interval.
    """
    if candle_interval:
        candles = resample_candles(candles, candle_interval)
    prices = extract_value_series(candles, value_path)
    return calculate_ema(prices, period)
