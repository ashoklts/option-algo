from __future__ import annotations

from typing import Any, Mapping, Sequence

import pandas as pd

# Exact option strings from the candleInterval dropdown in
# src/pages/signal/Quantman.tsx (candleIntervalOptions).
_INTRADAY_RULE_BY_LABEL: dict[str, str] = {
    "1 minute": "1min",
    "3 minutes": "3min",
    "5 minutes": "5min",
    "10 minutes": "10min",
    "15 minutes": "15min",
    "30 minutes": "30min",
    "1 hour": "60min",
}

_SESSION_OPEN_TIME = "09:15:00"  # NSE session open - anchors every intraday bucket

_OHLCV_AGG = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum", "oi": "last"}


def resample_candles(candles: Sequence[Mapping[str, Any]], candle_interval: str) -> list[dict[str, Any]]:
    """
    Aggregate 1-minute OHLCV candles (the base granularity this app stores -
    see features/mtm_historical_data.py) into the target `candleInterval`
    ("1 minute" / "3 minutes" / ... / "1 hour" / "1 day" / "1 week" - the
    exact dropdown strings from the signal builder UI).

    Intraday buckets are anchored to the 09:15 NSE session open, not
    midnight - so "10 minutes" buckets as 09:15-09:25, 09:25-09:35, ...,
    matching TradingView/Kite, not 09:10-09:20 from a naive midnight anchor.
    "oi" (open interest) is a point-in-time snapshot, so it takes the last
    value in each bucket rather than being summed like volume.
    """
    if not candles:
        return []

    df = pd.DataFrame(candles)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp").sort_index()

    agg = {key: rule for key, rule in _OHLCV_AGG.items() if key in df.columns}

    normalized = str(candle_interval or "").strip().lower()

    if normalized in ("1 day", "1 week"):
        daily = df.resample("D", label="left", closed="left").agg(agg).dropna(how="all")
        result = daily if normalized == "1 day" else daily.resample("W-MON", label="left", closed="left").agg(agg)
    else:
        rule = _INTRADAY_RULE_BY_LABEL.get(normalized)
        if rule is None:
            raise ValueError(f"Unsupported candleInterval: {candle_interval!r}")
        session_anchor = df.index[0].normalize() + pd.Timedelta(_SESSION_OPEN_TIME)
        result = df.resample(rule, label="left", closed="left", origin=session_anchor).agg(agg)

    result = result.dropna(how="all").reset_index().rename(columns={"index": "timestamp"})
    result["timestamp"] = result["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    return result.to_dict("records")
