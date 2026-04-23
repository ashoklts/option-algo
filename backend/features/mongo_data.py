"""
mongo_data.py
─────────────
Single bulk-load from MongoDB — no per-candle queries during backtest.
"""

import logging
from pymongo import MongoClient
from typing import Optional

MONGO_LIVE_DB_CONNECT = False  # True = Atlas cloud DB | False = Local MongoDB

_LIVE_MONGO_URI  = "mongodb+srv://finedgealgo:finedgealgo@cluster0.e66us4f.mongodb.net/"
_LOCAL_MONGO_URI = "mongodb://localhost:27017"

MONGO_URI = _LIVE_MONGO_URI if MONGO_LIVE_DB_CONNECT else _LOCAL_MONGO_URI
DB_NAME   = "stock_data"

print(f"[DB CONFIG] Connected to: {'Atlas Cloud DB' if MONGO_LIVE_DB_CONNECT else 'Local MongoDB'} → {MONGO_URI}")

_log = logging.getLogger("db_activity")


class MongoData:
    def __init__(self, uri: str = MONGO_URI):
        try:
            self._client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            self._db     = self._client[DB_NAME]
            self._chain  = self._db["option_chain"]
            self._hols   = self._db["market_holidays"]
            _log.debug("[DB CONNECT]  uri=%s  db=%s", uri, DB_NAME)
        except Exception as exc:
            _log.error("[DB CONNECT ERROR]  uri=%s  error=%s", uri, exc, exc_info=True)
            raise

    def get_holidays(self) -> set:
        docs = self._hols.find({}, {"date": 1, "_id": 0})
        return {d["date"] for d in docs}

    def get_expiry_rules(self, underlying: str) -> list:
        """
        Load all expiry-day rules for an underlying from expiry_day_config.
        Returns list of (from_date, to_date, weekday) tuples sorted by from_date.
        Called once per backtest run — not per candle.
        """
        docs = self._db["expiry_day_config"].find(
            {"underlying": underlying},
            {"_id": 0, "from_date": 1, "to_date": 1, "weekday": 1},
        ).sort("from_date", 1)
        return [(d["from_date"], d["to_date"], d["weekday"]) for d in docs]

    def get_lot_size(self, date_str: str, underlying: str) -> int:
        """Return lot size for a given underlying on a specific date."""
        doc = self._db["lot_sizes"].find_one({
            "underlying": underlying,
            "from_date":  {"$lte": date_str},
            "to_date":    {"$gte": date_str},
        })
        return int(doc["lot_size"]) if doc else 75  # fallback

    def load_range(self, start_date: str, end_date: str, underlying: str) -> list:
        """
        One bulk query — fetch all candles for the date range.
        Returns list of raw dicts from MongoDB.
        NOTE: Use load_day() per day for large ranges to avoid RAM blowup.
        """
        ts_start = f"{start_date}T00:00:00"
        ts_end   = f"{end_date}T23:59:59"
        cursor = self._chain.find(
            {
                "underlying": underlying,
                "timestamp": {"$gte": ts_start, "$lte": ts_end},
            },
            {"_id": 0, "timestamp": 1, "expiry": 1, "strike": 1,
             "type": 1, "close": 1, "high": 1, "low": 1, "spot_price": 1},
        )
        return list(cursor)

    def load_day(self, date: str, underlying: str) -> list:
        """
        Load candles for a single trading day only.
        Use this in the backtest loop to keep RAM constant regardless of range.
        """
        ts_start = f"{date}T00:00:00"
        ts_end   = f"{date}T23:59:59"
        cursor = self._chain.find(
            {
                "underlying": underlying,
                "timestamp": {"$gte": ts_start, "$lte": ts_end},
            },
            {"_id": 0, "timestamp": 1, "expiry": 1, "strike": 1,
             "type": 1, "close": 1, "high": 1, "low": 1, "spot_price": 1},
        )
        return list(cursor)

    def close(self):
        try:
            self._client.close()
        except Exception as exc:
            _log.error("[DB CLOSE ERROR]  error=%s", exc, exc_info=True)
