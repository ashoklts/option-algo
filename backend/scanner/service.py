from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd

from features.mongo_data import MongoData

DEFAULT_FORMULA = "((70% * 6 Month Volatility) + (20% * 3 Month Performance) + (10% * 1 Year Performance)) / 3 Month Volatility"
LOOKBACK_DAYS = 500
TRADING_PERIODS = {
    "1M": 21,
    "3M": 63,
    "6M": 126,
    "9M": 189,
    "1Y": 252,
}
FORMULA_TOKEN_MAP = {
    "1 Month Performance": "perf_1M",
    "3 Month Performance": "perf_3M",
    "6 Month Performance": "perf_6M",
    "9 Month Performance": "perf_9M",
    "1 Year Performance": "perf_1Y",
    "1 Month Volatility": "vol_1M",
    "3 Month Volatility": "vol_3M",
    "6 Month Volatility": "vol_6M",
    "9 Month Volatility": "vol_9M",
    "1 Year Volatility": "vol_1Y",
}


def _coerce_score_date(raw_value: Any) -> datetime:
    if isinstance(raw_value, datetime):
        return raw_value
    if not raw_value:
        return datetime.now()
    try:
        return datetime.fromisoformat(str(raw_value))
    except Exception:
        return pd.to_datetime(raw_value, errors="coerce").to_pydatetime()


def _normalize_list_payload(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def _normalize_formula(formula: str | None) -> str:
    normalized = str(formula or DEFAULT_FORMULA).strip() or DEFAULT_FORMULA
    normalized = re.sub(r"(\d+(?:\.\d+)?)\s*%", lambda match: str(float(match.group(1)) / 100.0), normalized)
    for label, column in sorted(FORMULA_TOKEN_MAP.items(), key=lambda item: len(item[0]), reverse=True):
        normalized = re.sub(re.escape(label), column, normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"[^A-Za-z0-9_+\-*/(). <>=!&|]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def get_indexes() -> list[dict[str, str]]:
    db = MongoData()._db
    rows = list(
        db["index_stocks"].find(
            {},
            {"_id": 0, "name": 1, "filter_symbol": 1},
        ).sort("name", 1)
    )
    return [
        {
            "label": str(row.get("name") or row.get("filter_symbol") or "").strip(),
            "value": str(row.get("filter_symbol") or "").strip(),
        }
        for row in rows
        if str(row.get("filter_symbol") or "").strip()
    ]


def get_sectors() -> list[dict[str, str]]:
    db = MongoData()._db
    values = db["stocks_list"].distinct("industry")
    sectors = sorted(str(item).strip() for item in values if str(item).strip())
    return [{"label": sector, "value": sector} for sector in sectors]


def _load_meta(index_names: list[str], sectors: list[str]) -> pd.DataFrame:
    db = MongoData()._db
    query: dict[str, Any] = {}
    if index_names:
        query["universe"] = {"$in": index_names}
    projection = {"_id": 0, "symbol": 1, "industry": 1, "universe": 1}
    meta_rows = list(db["stocks_list"].find(query, projection))
    meta = pd.DataFrame(meta_rows)
    if meta.empty:
        return meta

    meta["symbol"] = meta["symbol"].astype(str).str.strip()
    meta["sector"] = meta["industry"].fillna("").astype(str).str.strip()

    def normalize_universe(value: Any) -> str:
        items = value if isinstance(value, list) else [value]
        filtered = [str(item).strip() for item in items if str(item).strip()]
        if index_names:
            filtered = [item for item in filtered if item in index_names]
        return ",".join(sorted(dict.fromkeys(filtered)))

    meta["universe"] = meta["universe"].apply(normalize_universe)
    meta = meta[(meta["symbol"] != "") & (meta["universe"] != "")]
    if sectors:
        meta = meta[meta["sector"].isin(sectors)]
    meta = meta.drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)
    return meta[["symbol", "sector", "universe"]]


def _load_history(symbols: list[str], hist_start: datetime, hist_end: datetime) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    db = MongoData()._db
    query = {
        "h_symbol": {"$in": symbols},
        "ch_timestamp": {
            "$gte": hist_start.strftime("%Y-%m-%d"),
            "$lte": hist_end.strftime("%Y-%m-%d 23:59:59"),
        },
    }
    projection = {"_id": 0, "h_symbol": 1, "ch_timestamp": 1, "ch_closing_price": 1}
    rows = list(db["stock_historical_data"].find(query, projection))
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["h_symbol"] = df["h_symbol"].astype(str).str.strip()
    df["ch_timestamp"] = pd.to_datetime(df["ch_timestamp"], errors="coerce")
    df["ch_closing_price"] = pd.to_numeric(df["ch_closing_price"], errors="coerce")
    df = df.dropna(subset=["h_symbol", "ch_timestamp", "ch_closing_price"])
    df = df.sort_values(["h_symbol", "ch_timestamp"]).reset_index(drop=True)
    return df


def _compute_metrics(history: pd.DataFrame) -> pd.DataFrame:
    df = history.copy()
    df["ret"] = df.groupby("h_symbol")["ch_closing_price"].pct_change()

    for suffix, periods in TRADING_PERIODS.items():
        perf_col = f"perf_{suffix}"
        vol_col = f"vol_{suffix}"
        df[perf_col] = df.groupby("h_symbol")["ch_closing_price"].pct_change(periods=periods) * 100.0
        df[vol_col] = df.groupby("h_symbol")["ret"].transform(
            lambda series: series.rolling(periods, min_periods=5).std() * 100.0
        )

    metric_columns = [column for column in df.columns if column.startswith(("perf_", "vol_"))]
    df[metric_columns] = df[metric_columns].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return df


def _evaluate_scores(df: pd.DataFrame, formula: str) -> pd.Series:
    expression = _normalize_formula(formula)
    try:
        return df.eval(expression, engine="python")
    except Exception as exc:
        raise ValueError(f"Invalid scoring formula: {formula}. Parsed: {expression}") from exc


def _build_portfolio(score_rows: pd.DataFrame, total_capital: float, top_n: int) -> tuple[list[dict[str, Any]], dict[str, float]]:
    top_n = max(1, int(top_n or 1))
    equal_investment = float(total_capital) / float(top_n)
    portfolio_rows: list[dict[str, Any]] = []
    used_capital = 0.0

    for row in score_rows.sort_values("rank").head(top_n).to_dict(orient="records"):
        last_price = float(row.get("last_price") or 0.0)
        qty = int(equal_investment // last_price) if last_price > 0 else 0
        amount = round(qty * last_price, 2)
        used_capital += amount
        portfolio_rows.append(
            {
                **row,
                "qty": qty,
                "amount": amount,
                "Investment": round(equal_investment, 2),
            }
        )

    summary = {
        "total_capital": round(float(total_capital), 2),
        "used_capital": round(used_capital, 2),
        "remaining_capital": round(float(total_capital) - used_capital, 2),
    }
    return portfolio_rows, summary


def generate_stock_scores(payload: dict[str, Any]) -> dict[str, Any]:
    index_names = _normalize_list_payload(payload.get("index_name") or payload.get("index_names"))
    sectors = _normalize_list_payload(payload.get("sectors"))
    min_price = payload.get("min_price")
    max_price = payload.get("max_price")
    top_n = int(payload.get("top_n", 12) or 12)
    total_capital = float(payload.get("total_capital", 1_000_000) or 1_000_000)
    formula = str(payload.get("formula") or DEFAULT_FORMULA)
    score_date = _coerce_score_date(payload.get("score_date"))
    if pd.isna(score_date):
        score_date = datetime.now()

    meta = _load_meta(index_names, sectors)
    if meta.empty:
        return {
            "status": "no_data",
            "message": "No stocks found for the selected filters.",
        }

    hist_start = score_date - timedelta(days=LOOKBACK_DAYS)
    hist_end = score_date - timedelta(days=1)
    history = _load_history(meta["symbol"].tolist(), hist_start, hist_end)
    if history.empty:
        return {
            "status": "no_data",
            "message": "No historical data found for the selected filters.",
        }

    history = _compute_metrics(history)
    history["score"] = _evaluate_scores(history, formula).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    latest = history.groupby("h_symbol", as_index=False).tail(1).copy()
    latest = latest.merge(meta, left_on="h_symbol", right_on="symbol", how="inner")
    latest["last_price"] = latest["ch_closing_price"].round(2)

    if min_price not in (None, "", 0, "0"):
        latest = latest[latest["last_price"] >= float(min_price)]
    if max_price not in (None, "", 0, "0"):
        latest = latest[latest["last_price"] <= float(max_price)]

    if latest.empty:
        return {
            "status": "no_data",
            "message": "No stocks matched the selected price range.",
        }

    latest["rank"] = latest["score"].rank(ascending=False, method="first").astype(int)
    latest = latest.sort_values(["rank", "symbol"]).reset_index(drop=True)

    ordered_columns = [
        "rank",
        "symbol",
        "sector",
        "universe",
        "score",
        "last_price",
        "perf_1M",
        "perf_3M",
        "perf_6M",
        "perf_9M",
        "perf_1Y",
        "vol_1M",
        "vol_3M",
        "vol_6M",
        "vol_9M",
        "vol_1Y",
    ]
    for column in ordered_columns:
        if column not in latest.columns:
            latest[column] = 0.0

    snapshot = latest[ordered_columns].replace([np.inf, -np.inf], np.nan).fillna(0.0).round(6)
    investment_portfolio, summary = _build_portfolio(snapshot, total_capital, top_n)

    return {
        "status": "success",
        "meta": {
            "index_names": index_names,
            "sectors": sectors,
            "min_price": min_price,
            "max_price": max_price,
            "top_n": top_n,
            "total_capital": total_capital,
            "formula_used": formula,
            "score_date": score_date.strftime("%Y-%m-%d"),
            "lookback_start": hist_start.strftime("%Y-%m-%d"),
            "lookback_end": hist_end.strftime("%Y-%m-%d"),
        },
        "stocks_scored": snapshot.to_dict(orient="records"),
        "investment_portfolio": investment_portfolio,
        "summary": summary,
    }
