from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

# Maps the "Field" dropdown options exposed on the signal builder UI
# (src/pages/signal/Quantman.tsx fieldOptions) to the OHLCV candle keys
# this backend stores candles under.
_FIELD_TO_CANDLE_KEY: dict[str, str] = {
    "equity": "close",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "openinterest": "oi",
}


def _normalize(value_path: str) -> str:
    return str(value_path or "").strip().lower().replace(" ", "")


def extract_value_series(candles: Sequence[Mapping[str, Any]], value_path: str) -> np.ndarray:
    """Pull a single float64 price series out of OHLCV candle dicts for one `valuePaths` option."""
    key = _normalize(value_path)

    if key == "hlc3":
        return np.array(
            [(float(c["high"]) + float(c["low"]) + float(c["close"])) / 3.0 for c in candles],
            dtype=np.float64,
        )

    candle_key = _FIELD_TO_CANDLE_KEY.get(key)
    if candle_key is None:
        raise ValueError(f"Unsupported valuePaths option: {value_path!r}")

    return np.array([float(c[candle_key]) for c in candles], dtype=np.float64)


def extract_ohlc(candles: Sequence[Mapping[str, Any]]) -> dict[str, np.ndarray]:
    """
    Pull aligned open/high/low/close (+volume/oi when present) float64 arrays
    out of OHLCV candle dicts. Indicators that need more than one price series
    at once (ATR, ADX, SuperTrend, Ichimoku, ...) use this instead of
    `extract_value_series`, which only ever returns one configurable series.
    """
    result = {
        "open": np.array([float(c["open"]) for c in candles], dtype=np.float64),
        "high": np.array([float(c["high"]) for c in candles], dtype=np.float64),
        "low": np.array([float(c["low"]) for c in candles], dtype=np.float64),
        "close": np.array([float(c["close"]) for c in candles], dtype=np.float64),
    }
    if candles and "volume" in candles[0]:
        result["volume"] = np.array([float(c["volume"]) for c in candles], dtype=np.float64)
    if candles and "oi" in candles[0]:
        result["oi"] = np.array([float(c["oi"]) for c in candles], dtype=np.float64)
    return result
