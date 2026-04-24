"""
mongo_data.py
─────────────
Single bulk-load from MongoDB — no per-candle queries during backtest.
"""

import logging
import threading
import time
from collections.abc import Mapping
from pymongo import MongoClient
from pymongo import monitoring
from pymongo import ASCENDING, DESCENDING

MONGO_LIVE_DB_CONNECT = True  # True = Atlas cloud DB | False = Local MongoDB

_LIVE_MONGO_URI  = "mongodb://finedgealgo:finedgealgo@13.202.184.58:27017/?authSource=admin" #"mongodb+srv://finedgealgo:finedgealgo@cluster0.e66us4f.mongodb.net/"
_LOCAL_MONGO_URI = "mongodb://localhost:27017"

MONGO_URI = _LIVE_MONGO_URI if MONGO_LIVE_DB_CONNECT else _LOCAL_MONGO_URI
DB_NAME   = "stock_data"
_TARGET   = "atlas" if MONGO_LIVE_DB_CONNECT else "local"
DB_QUERY_STATUS = False

print(f"[DB CONFIG] Connected to: {'Atlas Cloud DB' if MONGO_LIVE_DB_CONNECT else 'Local MongoDB'} → {MONGO_URI}")

_log = logging.getLogger("db_activity")


def _db_query_print_enabled() -> bool:
    return bool(DB_QUERY_STATUS)


def _emit_query_log(tag: str, message: str, *, error: bool = False) -> None:
    if error:
        _log.error("%s %s", tag, message)
    elif _db_query_print_enabled():
        _log.info("%s %s", tag, message)
    else:
        _log.debug("%s %s", tag, message)


def _safe_scalar(value) -> str:
    text = str(value)
    return text.replace(" ", "_")


def _target_from_uri(uri: str) -> str:
    return "atlas" if uri.startswith("mongodb+srv://") else "local"


def _collection_for_command(command_name: str, command: Mapping | None) -> str | None:
    if not command:
        return None
    if command_name in command:
        return command.get(command_name)
    if command_name == "getMore":
        return command.get("collection")
    return None


def _result_count(command_name: str, reply: Mapping | None) -> int | None:
    if not reply:
        return None
    if "cursor" in reply:
        first_batch = ((reply.get("cursor") or {}).get("firstBatch")) or []
        next_batch = ((reply.get("cursor") or {}).get("nextBatch")) or []
        if isinstance(first_batch, list):
            return len(first_batch)
        if isinstance(next_batch, list):
            return len(next_batch)
    if isinstance(reply.get("n"), int):
        return int(reply["n"])
    if isinstance(reply.get("nModified"), int):
        return int(reply["nModified"])
    if isinstance(reply.get("count"), int):
        return int(reply["count"])
    if isinstance(reply.get("nInserted"), int):
        return int(reply["nInserted"])
    if command_name == "distinct" and isinstance(reply.get("values"), list):
        return len(reply["values"])
    return None


def _meta_from_comment(comment) -> dict[str, str]:
    if isinstance(comment, Mapping):
        return {str(k): _safe_scalar(v) for k, v in comment.items() if v is not None}
    if comment is None:
        return {}
    return {"comment": _safe_scalar(comment)}


class _MongoCommandLogger(monitoring.CommandListener):
    def __init__(self):
        self._pending: dict[int, tuple[str, Mapping]] = {}
        self._lock = threading.Lock()

    def started(self, event):
        with self._lock:
            self._pending[event.request_id] = (event.database_name, dict(event.command))
        collection = _collection_for_command(event.command_name, event.command)
        parts = [f"command={event.command_name}"]
        if collection:
            parts.append(f"collection={collection}")
        parts.append(f"db={event.database_name}")
        _log.debug("[DB CMD START] %s", " ".join(parts))

    def succeeded(self, event):
        with self._lock:
            db_name, command = self._pending.pop(event.request_id, (event.database_name, {}))
        collection = _collection_for_command(event.command_name, command)
        duration_ms = round(event.duration_micros / 1000.0, 2)
        meta = _meta_from_comment(command.get("comment"))
        method = meta.pop("method", event.command_name)
        target = meta.pop("target", None)
        db_name = meta.pop("db", db_name)
        if target is None:
            target = _TARGET

        parts = [f"method={method}"]
        if collection:
            parts.append(f"collection={collection}")
        parts.append(f"query_ms={duration_ms}")
        parts.append(f"db={db_name}")
        parts.append(f"target={target}")

        count = _result_count(event.command_name, event.reply)
        if count is not None:
            parts.append(f"count={count}")

        for key, value in meta.items():
            parts.append(f"{key}={value}")

        if event.command_name == "ping":
            _emit_query_log("[DB PING]", " ".join(parts))
            if _db_query_print_enabled():
                print("[DB PING] " + " ".join(parts))
            return

        _emit_query_log("[DB QUERY]", " ".join(parts))
        if _db_query_print_enabled():
            print("[DB QUERY] " + " ".join(parts))

    def failed(self, event):
        with self._lock:
            db_name, command = self._pending.pop(event.request_id, (event.database_name, {}))
        collection = _collection_for_command(event.command_name, command)
        duration_ms = round(event.duration_micros / 1000.0, 2)
        meta = _meta_from_comment(command.get("comment"))
        method = meta.pop("method", event.command_name)
        target = meta.pop("target", _TARGET)
        parts = [f"method={method}"]
        if collection:
            parts.append(f"collection={collection}")
        parts.append(f"query_ms={duration_ms}")
        parts.append(f"db={meta.pop('db', db_name)}")
        parts.append(f"target={target}")
        for key, value in meta.items():
            parts.append(f"{key}={value}")
        parts.append(f"error={_safe_scalar(event.failure)}")
        _emit_query_log("[DB QUERY ERROR]", " ".join(parts), error=True)
        if _db_query_print_enabled():
            print("[DB QUERY ERROR] " + " ".join(parts))


class MongoData:
    _client_cache: dict[str, MongoClient] = {}
    _instance_cache: dict[str, "MongoData"] = {}
    _client_lock = threading.Lock()
    _instance_lock = threading.Lock()
    _command_logger = _MongoCommandLogger()

    def __new__(cls, uri: str = MONGO_URI):
        with cls._instance_lock:
            instance = cls._instance_cache.get(uri)
            if instance is None:
                instance = super().__new__(cls)
                cls._instance_cache[uri] = instance
            return instance

    def __init__(self, uri: str = MONGO_URI):
        if getattr(self, "_initialized", False) and getattr(self, "_uri", None) == uri:
            return
        try:
            t0 = time.perf_counter()
            created_new_client = False
            with self._client_lock:
                cached_client = self._client_cache.get(uri)
                if cached_client is None:
                    cached_client = MongoClient(
                        uri,
                        serverSelectionTimeoutMS=5000,
                        event_listeners=[self._command_logger],
                        appname="option-algo",
                    )
                    self._client_cache[uri] = cached_client
                    created_new_client = True
                self._client = cached_client
            self._db     = self._client[DB_NAME]
            self._chain  = self._db["option_chain"]
            self._hols   = self._db["market_holidays"]
            self._uri    = uri
            self._initialized = True
            ms = round((time.perf_counter() - t0) * 1000, 2)
            self._target = _target_from_uri(uri)
            if created_new_client:
                if _db_query_print_enabled():
                    print(f"[DB CONNECT INIT] db={DB_NAME} connect_init_ms={ms} target={self._target}")
                _log.info("[DB CONNECT INIT] db=%s connect_init_ms=%s target=%s", DB_NAME, ms, self._target)
            else:
                _log.debug("[DB CONNECT REUSE] db=%s acquire_ms=%s target=%s", DB_NAME, ms, self._target)
            _log.debug("[DB CONNECT]  uri=%s  db=%s  target=%s  created_new=%s", uri, DB_NAME, self._target, created_new_client)
        except Exception as exc:
            _log.error("[DB CONNECT ERROR]  uri=%s  error=%s", uri, exc, exc_info=True)
            raise

    def _comment(self, method: str, **extra) -> dict[str, str]:
        payload = {"method": method, "target": self._target, "db": DB_NAME}
        for key, value in extra.items():
            if value is not None:
                payload[key] = value
        return {str(k): _safe_scalar(v) for k, v in payload.items()}

    def ensure_core_indexes(self) -> None:
        try:
            self._db["saved_strategies"].create_index(
                [("name", ASCENDING)],
                unique=True,
                background=True,
                name="uniq_strategy_name",
                comment=self._comment("ensure_index", collection="saved_strategies", index="uniq_strategy_name"),
            )
            self._db["saved_strategies"].create_index(
                [("created_at", DESCENDING)],
                background=True,
                name="strategy_created_at_desc",
                comment=self._comment("ensure_index", collection="saved_strategies", index="strategy_created_at_desc"),
            )
            self._db["saved_portfolios"].create_index(
                [("name", ASCENDING)],
                unique=True,
                background=True,
                name="uniq_portfolio_name",
                comment=self._comment("ensure_index", collection="saved_portfolios", index="uniq_portfolio_name"),
            )
            self._db["saved_portfolios"].create_index(
                [("created_at", DESCENDING)],
                background=True,
                name="portfolio_created_at_desc",
                comment=self._comment("ensure_index", collection="saved_portfolios", index="portfolio_created_at_desc"),
            )
        except Exception as exc:
            _log.warning("[DB INDEX WARN] db=%s target=%s error=%s", DB_NAME, self._target, exc)

    def timed_ping(self, label: str = "mongo") -> float | None:
        t0 = time.perf_counter()
        try:
            self._db.command("ping", comment=self._comment("ping", label=label))
            ms = round((time.perf_counter() - t0) * 1000, 2)
            return ms
        except Exception as exc:
            ms = round((time.perf_counter() - t0) * 1000, 2)
            if _db_query_print_enabled():
                print(f"[DB PING ERROR] label={label} ping_ms={ms} error={exc}")
            return None

    def get_holidays(self) -> set:
        docs = list(self._hols.find(
            {},
            {"date": 1, "_id": 0},
            comment=self._comment("get_holidays"),
        ))
        return {d["date"] for d in docs}

    def get_expiry_rules(self, underlying: str) -> list:
        """
        Load all expiry-day rules for an underlying from expiry_day_config.
        Returns list of (from_date, to_date, weekday) tuples sorted by from_date.
        Called once per backtest run — not per candle.
        """
        docs = list(self._db["expiry_day_config"].find(
            {"underlying": underlying},
            {"_id": 0, "from_date": 1, "to_date": 1, "weekday": 1},
            comment=self._comment("get_expiry_rules", underlying=underlying),
        ).sort("from_date", 1))
        return [(d["from_date"], d["to_date"], d["weekday"]) for d in docs]

    def get_lot_size(self, date_str: str, underlying: str) -> int:
        """Return lot size for a given underlying on a specific date."""
        doc = self._db["lot_sizes"].find_one({
            "underlying": underlying,
            "from_date":  {"$lte": date_str},
            "to_date":    {"$gte": date_str},
        }, comment=self._comment("get_lot_size", underlying=underlying, date=date_str))
        return int(doc["lot_size"]) if doc else 75  # fallback

    def load_range(self, start_date: str, end_date: str, underlying: str) -> list:
        """
        One bulk query — fetch all candles for the date range.
        Returns list of raw dicts from MongoDB.
        NOTE: Use load_day() per day for large ranges to avoid RAM blowup.
        """
        ts_start = f"{start_date}T00:00:00"
        ts_end   = f"{end_date}T23:59:59"
        result = list(self._chain.find(
            {
                "underlying": underlying,
                "timestamp": {"$gte": ts_start, "$lte": ts_end},
            },
            {"_id": 0, "timestamp": 1, "expiry": 1, "strike": 1,
             "type": 1, "close": 1, "high": 1, "low": 1, "spot_price": 1},
            comment=self._comment(
                "load_range",
                underlying=underlying,
                start=start_date,
                end=end_date,
            ),
        ))
        return result

    def load_day(self, date: str, underlying: str) -> list:
        """
        Load candles for a single trading day only.
        Use this in the backtest loop to keep RAM constant regardless of range.
        """
        ts_start = f"{date}T00:00:00"
        ts_end   = f"{date}T23:59:59"
        result = list(self._chain.find(
            {
                "underlying": underlying,
                "timestamp": {"$gte": ts_start, "$lte": ts_end},
            },
            {"_id": 0, "timestamp": 1, "expiry": 1, "strike": 1,
             "type": 1, "close": 1, "high": 1, "low": 1, "spot_price": 1},
            comment=self._comment("load_day", underlying=underlying, date=date),
        ))
        return result

    def close(self):
        # Shared client stays alive for connection pooling; avoid closing it per request.
        return None
