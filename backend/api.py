"""
Local Backtest API
──────────────────
Run:
    uvicorn api:app --reload --port 8001

Endpoints:
    GET  /health                    → health check
    POST /backtest                  → run backtest (blocking, waits for result)
    POST /backtest/file             → run backtest using current_backtest_request.json
    POST /backtest/start            → start backtest in background, returns job_id
    GET  /backtest/status/{job_id}  → poll progress: completed_days / total_days
    GET  /backtest/result/{job_id}  → get final result when status=done
"""

from __future__ import annotations

import hashlib
import json
import logging
import multiprocessing
import os
import re
import threading

log = logging.getLogger(__name__)
import time
import uuid
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from bson import ObjectId
from fastapi import FastAPI, HTTPException, APIRouter, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

from features.backtest_engine import run_backtest
from features.portfolio_worker import strategy_worker
from features.mongo_data     import MongoData
from features.expiry_config  import seed_expiry_config
from features.kite_broker import (
    get_login_url,
    generate_session,
    get_kite_instance,
    save_kite_session,
    get_stored_access_token,
)
from features.kite_ticker import ticker_manager
from features.mock_ticker import mock_ticker_manager
from features.spot_atm_utils import get_kite_expiries, list_kite_option_contracts
from features.execution_socket import (
    broadcast_backtest_simulation_step,
    emit_broker_settings_for_user,
    queue_execute_order_group_start,
    run_backtest_simulation_step,
    socket_router,
    _build_message,
    _extract_broker_configuration_label,
)
from features.live_fast_monitor import live_fast_monitor_supervisor
from features.live_monitor_socket import live_monitor_loop
from features.mock_kite_socket import mock_kite_socket_router
from features.kite_broker_ws import load_credentials_from_db

# ─── Config ───────────────────────────────────────────────────────────────────

REQUEST_JSON_PATH = Path(__file__).parent / "current_backtest_request.json"
SAMPLE_RESULT_PATH = Path(__file__).parent / "sample_backtest_result" / "new_portfolio_result.json"
JOB_STATE_DIR = Path("/tmp/option_algo_backtest_jobs")
CACHE_DIR = Path("/tmp/option_algo_backtest_cache")

JOB_TTL_SECONDS = 3600       # auto-delete completed jobs older than 1 hour
MAX_JOBS        = 10         # max jobs kept in memory at once
DEFAULT_APP_USER_ID = "69dcf52711877c164638d2a7"

# ─── Job store (in-memory) ────────────────────────────────────────────────────
# job_id → { status, completed, total, percent, current_day, result, error, created_at }

_jobs: dict = {}
_jobs_lock = multiprocessing.Lock()
_LIST_CACHE_TTL_SECONDS = 30.0
_list_cache: dict[str, dict] = {}
_list_cache_lock = threading.Lock()


def _resolve_app_user_id(value: str | None = None) -> str:
    normalized_value = str(value or "").strip()
    if normalized_value:
        return normalized_value
    return DEFAULT_APP_USER_ID


def _list_cache_get(key: str):
    now = time.time()
    with _list_cache_lock:
        item = _list_cache.get(key)
        if not item:
            return None
        if now - item.get("ts", 0) > _LIST_CACHE_TTL_SECONDS:
            _list_cache.pop(key, None)
            return None
        return deepcopy(item["value"])


def _list_cache_set(key: str, value) -> None:
    with _list_cache_lock:
        _list_cache[key] = {"ts": time.time(), "value": deepcopy(value)}


def _invalidate_list_cache(*keys: str) -> None:
    with _list_cache_lock:
        if not keys:
            _list_cache.clear()
            return
        for key in keys:
            _list_cache.pop(key, None)


def _request_fingerprint(request: dict) -> str:
    payload = json.dumps(request, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _estimate_total_steps(request: dict) -> int:
    start_date = request.get("start_date")
    end_date = request.get("end_date")
    if not start_date or not end_date:
        return 0
    try:
        db = MongoData()
        holidays = db.get_holidays()
        cur = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        total_days = 0
        while cur <= end_dt:
            if cur.weekday() < 5 and cur.strftime("%Y-%m-%d") not in holidays:
                total_days += 1
            cur += timedelta(days=1)
        db.close()
        return total_days + 1 if total_days > 0 else 0
    except Exception:
        return 0


def _job_state_path(job_id: str) -> Path:
    JOB_STATE_DIR.mkdir(parents=True, exist_ok=True)
    return JOB_STATE_DIR / f"{job_id}.json"


def _cache_path(fingerprint: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{fingerprint}.json"


def _write_job_state(job_id: str, payload: dict) -> None:
    path = _job_state_path(job_id)
    tmp_path = path.with_suffix(".tmp")
    with open(tmp_path, "w") as f:
        json.dump(payload, f)
    os.replace(tmp_path, path)


def _read_job_state(job_id: str) -> dict | None:
    path = _job_state_path(job_id)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def _read_cached_result(fingerprint: str) -> dict | None:
    path = _cache_path(fingerprint)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def _write_cached_result(fingerprint: str, result: dict) -> None:
    path = _cache_path(fingerprint)
    tmp_path = path.with_suffix(".tmp")
    with open(tmp_path, "w") as f:
        json.dump(result, f)
    os.replace(tmp_path, path)


def _cleanup_old_jobs():
    """Remove finished jobs older than JOB_TTL_SECONDS and enforce MAX_JOBS limit."""
    # Sync in-memory "running" jobs from file — child process only writes files,
    # so _jobs in the parent can be stale (still "running" after the child finishes).
    for jid, job in list(_jobs.items()):
        if job["status"] == "running":
            file_state = _read_job_state(jid)
            if file_state and file_state.get("status") != "running":
                _jobs[jid].update(file_state)

    now = time.time()
    expired = [jid for jid, j in _jobs.items()
               if j["status"] != "running"
               and now - j.get("created_at", now) > JOB_TTL_SECONDS]
    for jid in expired:
        state_path = _job_state_path(jid)
        if state_path.exists():
            state_path.unlink()
        del _jobs[jid]

    # if still over limit, remove oldest completed jobs first
    if len(_jobs) >= MAX_JOBS:
        done = sorted(
            [(jid, j) for jid, j in _jobs.items() if j["status"] != "running"],
            key=lambda x: x[1].get("created_at", 0),
        )
        for jid, _ in done[:len(_jobs) - MAX_JOBS + 1]:
            del _jobs[jid]




def _run_job(job_id: str, request: dict):
    try:
        os.nice(15)
    except Exception:
        pass

    state = _read_job_state(job_id) or {}

    def on_progress(completed: int, total: int, day: str):
        state.update({
            "job_id": job_id,
            "status": "running",
            "completed": completed,
            "total": total,
            "percent": round(completed / total * 100, 1) if total else 0,
            "current_day": day,
            "error": None,
            "updated_at": time.time(),
        })
        _write_job_state(job_id, state)

    try:
        result = run_backtest(request, on_progress=on_progress)
        fingerprint = state.get("fingerprint")
        if fingerprint:
            _write_cached_result(fingerprint, result)
        total = state.get("total", 0)
        state.update({
            "job_id": job_id,
            "status": "done",
            "completed": total,
            "percent": 100.0 if total else 0.0,
            "current_day": "Completed",
            "result": result,
            "error": None,
            "updated_at": time.time(),
        })
        _write_job_state(job_id, state)
    except Exception as e:
        state.update({
            "job_id": job_id,
            "status": "error",
            "error": str(e),
            "updated_at": time.time(),
        })
        _write_job_state(job_id, state)


def strategy_worker(args: dict):
    strategy_id_str = str((args or {}).get("strategy_id_str") or "")
    backtest_req = dict((args or {}).get("backtest_req") or {})
    job_id = str((args or {}).get("job_id") or "")

    # Write per-strategy progress to a temp file — avoids Manager IPC complexity
    prog_path = JOB_STATE_DIR / f"{job_id}_{strategy_id_str}.prog" if job_id else None

    def on_progress(completed: int, total: int, day: str):
        if not prog_path:
            return
        try:
            tmp = prog_path.with_suffix(".tmp")
            with open(tmp, "w") as f:
                json.dump({"completed": completed, "total": total, "day": day}, f)
            os.replace(tmp, prog_path)
        except Exception:
            pass

    try:
        result = run_backtest(backtest_req, on_progress=on_progress)
        return {
            "_id": strategy_id_str,
            "item_id": strategy_id_str,
            "status": "completed",
            "error": None,
            "results": result,
        }
    except Exception as exc:
        return {
            "_id": strategy_id_str,
            "item_id": strategy_id_str,
            "status": "error",
            "error": str(exc),
            "results": None,
        }
    finally:
        # Clean up progress file on completion/error
        if prog_path and prog_path.exists():
            try:
                prog_path.unlink()
            except Exception:
                pass


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _calc_leg_pnl(leg: dict) -> dict:
    entry_trade = leg.get("entry_trade") if isinstance(leg.get("entry_trade"), dict) else {}
    exit_trade = leg.get("exit_trade") if isinstance(leg.get("exit_trade"), dict) else {}
    entry_price = _safe_float(entry_trade.get("price"))
    quantity = _safe_int(leg.get("quantity") or entry_trade.get("quantity"))
    lot_size = _safe_int(leg.get("lot_size"), 1)
    effective_quantity = max(0, quantity) * max(1, lot_size)
    is_sell = "sell" in str(leg.get("position") or "").lower()

    if exit_trade:
        mark_price = _safe_float(exit_trade.get("price"))
        pnl_price_source = "exit_trade"
    else:
        mark_price = _safe_float(leg.get("last_saw_price"))
        pnl_price_source = "last_saw_price"

    if entry_price <= 0 or effective_quantity <= 0:
        pnl_value = 0.0
    else:
        pnl_value = ((entry_price - mark_price) if is_sell else (mark_price - entry_price)) * effective_quantity

    leg_payload = dict(leg)
    leg_payload["entry_price"] = entry_price
    leg_payload["mark_price"] = round(mark_price, 2)
    leg_payload["effective_quantity"] = effective_quantity
    leg_payload["pnl_price_source"] = pnl_price_source
    leg_payload["pnl"] = round(pnl_value, 2)
    return leg_payload


def _populate_history_legs(db_instance, records: list) -> list:
    """
    Batch-fetch all algo_trade_positions_history docs for the given trade records
    by querying trade_id. Groups docs per trade and attaches them as legs[].
    Status counts are derived from history docs:
      status=1 → open_legs_count
      status=2 → closed_legs_count
      status=0 → pending_legs_count
    """
    if not records:
        return records

    trade_ids = [str(rec.get("_id") or "") for rec in records if rec.get("_id")]
    if not trade_ids:
        return records

    # Single batch query: all history docs for all trades at once
    history_by_trade: dict[str, list] = {tid: [] for tid in trade_ids}
    try:
        history_col = db_instance["algo_trade_positions_history"]
        for doc in history_col.find({"trade_id": {"$in": trade_ids}}):
            doc["_id"] = str(doc.get("_id") or "")
            tid = str(doc.get("trade_id") or "")
            if tid in history_by_trade:
                history_by_trade[tid].append(doc)
    except Exception:
        pass

    populated = []
    for rec in records:
        trade_id = str(rec.get("_id") or "")
        history_legs = history_by_trade.get(trade_id) or []
        new_rec = dict(rec)
        new_rec["legs"] = history_legs
        new_rec["open_legs_count"] = sum(1 for l in history_legs if _safe_int(l.get("status")) == 1)
        new_rec["closed_legs_count"] = sum(1 for l in history_legs if _safe_int(l.get("status")) == 2)
        new_rec["pending_legs_count"] = sum(1 for l in history_legs if _safe_int(l.get("status")) == 0)
        populated.append(new_rec)
    return populated


def _format_feature_status_timestamp(value) -> str:
    raw_value = str(value or "").strip()
    if not raw_value:
        return ""
    return raw_value.replace("T", " ")


def _format_feature_status_price(value) -> str:
    numeric = _safe_float(value)
    if numeric <= 0:
        return "-"
    return f"₹{numeric:.2f}"


def _describe_feature_status_row(row: dict) -> str:
    if not isinstance(row, dict):
        return ""

    description = str(row.get("trigger_description") or "").strip()
    if description:
        return description

    feature_key = str(row.get("feature") or "").strip()
    if feature_key in {"overall_sl", "overall_target"}:
        label = "Overall SL" if feature_key == "overall_sl" else "Overall Target"
        cycle_number = int(row.get("cycle_number") or 1)
        trigger_value = _format_feature_status_price(row.get("trigger_value"))
        next_value = _format_feature_status_price(row.get("next_trigger_value"))
        reentry_type = str(row.get("reentry_type") or "None")
        reentry_count = int(row.get("reentry_count") or 0)
        reentry_done = int(row.get("reentry_done") or 0)
        return (
            f"{label} active for cycle {cycle_number}. "
            f"Current threshold {trigger_value}. "
            f"Re-entry {reentry_type} used {reentry_done}/{reentry_count}. "
            f"Next cycle threshold {next_value}."
        )
    if feature_key == "pending_entry":
        option = str(row.get("option") or "").strip().upper() or "-"
        position = str(row.get("position") or "").split(".")[-1].strip() or "Position"
        strike = str(row.get("strike") or "").strip() or "-"
        queued_at = _format_feature_status_timestamp(row.get("queued_at"))
        triggered_at = _format_feature_status_timestamp(row.get("triggered_at"))
        status = str(row.get("status") or "").strip().lower()

        if status == "triggered":
            return (
                f"Pending entry triggered for {strike} {option} {position} leg at {triggered_at or '-'}."
            )

        return (
            f"Pending entry active for {strike} {option} {position} leg since {queued_at or '-'}. "
            f"Waiting for next entry cycle."
        )

    if feature_key != "momentum_pending":
        return ""

    status = str(row.get("status") or "").strip().lower()
    option = str(row.get("option") or "").strip().upper() or "-"
    position = str(row.get("position") or "").split(".")[-1].strip() or "Position"
    strike = str(row.get("strike") or "").strip() or "-"
    momentum_type = str(row.get("momentum_type") or "").split(".")[-1].strip() or "Momentum"
    momentum_value = _safe_float(row.get("momentum_value"))
    base_price = _format_feature_status_price(row.get("momentum_base_price"))
    target_price = _format_feature_status_price(row.get("momentum_target_price"))
    queued_at = _format_feature_status_timestamp(row.get("queued_at"))
    armed_at = _format_feature_status_timestamp(row.get("armed_at"))

    if status == "triggered":
        triggered_at = _format_feature_status_timestamp(row.get("triggered_at"))
        return (
            f"Momentum triggered for {strike} {option} {position} leg at {triggered_at or '-'}."
        )

    if _safe_float(row.get("momentum_base_price")) > 0 and _safe_float(row.get("momentum_target_price")) > 0:
        return (
            f"Momentum waiting for {strike} {option} {position} leg. "
            f"{momentum_type} {momentum_value:g} armed at {armed_at or queued_at or '-'} "
            f"with base {base_price} and target {target_price}."
        )

    return (
        f"Momentum queue active for {strike} {option} {position} leg since {queued_at or '-'}. "
        f"Waiting to arm {momentum_type} {momentum_value:g}."
    )


def _build_pending_feature_leg(row: dict) -> dict:
    row_copy = dict(row)
    description = _describe_feature_status_row(row_copy)
    if description:
        row_copy["trigger_description"] = description

    feature_map = {}
    feature_key = str(row_copy.get("feature") or "").strip()
    if feature_key:
        feature_map[feature_key] = row_copy

    return {
        "id": str(row_copy.get("leg_id") or ""),
        "leg_id": str(row_copy.get("leg_id") or ""),
        "status": 0,
        "position": row_copy.get("position"),
        "option": row_copy.get("option"),
        "strike": row_copy.get("strike"),
        "expiry_date": row_copy.get("expiry_date"),
        "token": row_copy.get("token"),
        "symbol": row_copy.get("symbol"),
        "quantity": 0,
        "lot_config_value": int(row_copy.get("lot_config_value") or 1),
        "entry_trade": None,
        "exit_trade": None,
        "last_saw_price": row_copy.get("momentum_base_price"),
        "is_lazy": True,
        "is_pending_feature_leg": True,
        "queued_at": row_copy.get("queued_at"),
        "armed_at": row_copy.get("armed_at"),
        "triggered_at": row_copy.get("triggered_at"),
        "leg_type": row_copy.get("leg_type"),
        "momentum_base_price": row_copy.get("momentum_base_price"),
        "momentum_target_price": row_copy.get("momentum_target_price"),
        "feature_status_rows": [row_copy],
        "feature_status_map": feature_map,
        "active_trigger_descriptions": [description] if description else [],
    }


def _attach_leg_feature_statuses(db_instance, records: list) -> list:
    if not records:
        return records

    trade_ids = [str(rec.get("_id") or "") for rec in records if rec.get("_id")]
    if not trade_ids:
        return records

    feature_rows_by_key: dict[tuple[str, str], list] = {}
    try:
        feature_col = db_instance["algo_leg_feature_status"]
        for doc in feature_col.find(
            {
                "trade_id": {"$in": trade_ids},
                "enabled": True,
            }
        ):
            trade_id = str(doc.get("trade_id") or "")
            leg_id = str(doc.get("leg_id") or "")
            if not trade_id or not leg_id:
                continue
            doc["_id"] = str(doc.get("_id") or "")
            feature_rows_by_key.setdefault((trade_id, leg_id), []).append(doc)
    except Exception:
        return records

    enriched_records = []
    for rec in records:
        trade_id = str(rec.get("_id") or "")
        legs = rec.get("legs") if isinstance(rec.get("legs"), list) else []
        existing_leg_ids = set()
        enriched_legs = []
        for leg in legs:
            if not isinstance(leg, dict):
                enriched_legs.append(leg)
                continue
            leg_id = str(leg.get("_id") or leg.get("leg_id") or leg.get("id") or "")
            if leg_id:
                existing_leg_ids.add(leg_id)
            feature_rows = feature_rows_by_key.get((trade_id, leg_id), [])
            leg_copy = dict(leg)
            leg_copy["feature_status_rows"] = feature_rows
            feature_map = {}
            active_descriptions = []
            for row in feature_rows:
                feature_key = str(row.get("feature") or "").strip()
                if not feature_key:
                    continue
                row_copy = dict(row)
                description = _describe_feature_status_row(row_copy)
                if description:
                    row_copy["trigger_description"] = description
                feature_map[feature_key] = row_copy
                if description:
                    active_descriptions.append(description)
            leg_copy["feature_status_map"] = feature_map
            leg_copy["feature_status_rows"] = list(feature_map.values()) if feature_map else feature_rows
            leg_copy["active_trigger_descriptions"] = active_descriptions
            enriched_legs.append(leg_copy)

        pending_feature_legs = []
        strategy_feature_rows = []
        for (feature_trade_id, feature_leg_id), feature_rows in feature_rows_by_key.items():
            if feature_trade_id != trade_id or not feature_leg_id or feature_leg_id in existing_leg_ids:
                continue
            if feature_leg_id == "__overall__":
                for row in feature_rows:
                    row_copy = dict(row)
                    description = _describe_feature_status_row(row_copy)
                    if description:
                        row_copy["trigger_description"] = description
                    strategy_feature_rows.append(row_copy)
                continue
            for row in feature_rows:
                if str(row.get("feature") or "").strip() not in {"momentum_pending", "pending_entry"}:
                    continue
                if str(row.get("status") or "").strip().lower() != "active":
                    continue
                pending_feature_legs.append(_build_pending_feature_leg(row))

        new_rec = dict(rec)
        new_rec["legs"] = enriched_legs
        new_rec["pending_feature_legs"] = pending_feature_legs
        new_rec["strategy_feature_status_rows"] = strategy_feature_rows
        enriched_records.append(new_rec)
    return enriched_records


def _extract_broker_configuration_label(document: dict, fallback_broker_id: str = "") -> str:
    if not isinstance(document, dict):
        return fallback_broker_id
    for key in (
        "broker_name",
        "display_name",
        "name",
        "title",
        "broker",
        "broker_type",
        "provider",
        "vendor",
    ):
        value = str(document.get(key) or "").strip()
        if value:
            return value
    return str(fallback_broker_id or "").strip()


def _attach_broker_configuration_details(db_instance, records: list) -> list:
    if not records:
        return records

    broker_ids = []
    broker_object_ids = []
    for record in records:
        broker_id = str((record or {}).get("broker") or "").strip()
        if not broker_id:
            continue
        broker_ids.append(broker_id)
        try:
            broker_object_ids.append(ObjectId(broker_id))
        except Exception:
            continue

    if not broker_ids:
        return records

    broker_docs_by_id = {}
    try:
        cursor = db_instance["broker_configuration"].find(
            {"_id": {"$in": broker_object_ids}},
            {
                "_id": 1,
                "broker_name": 1,
                "display_name": 1,
                "name": 1,
                "title": 1,
                "broker": 1,
                "broker_icon": 1,
                "broker_type": 1,
                "provider": 1,
                "vendor": 1,
            },
        )
        for item in cursor:
            if not item:
                continue
            item_id = str(item.get("_id") or "").strip()
            if item_id:
                broker_docs_by_id[item_id] = item
    except Exception:
        return records

    if not broker_docs_by_id:
        return records

    enriched_records = []
    for record in records:
        new_record = dict(record)
        broker_id = str(new_record.get("broker") or "").strip()
        broker_doc = broker_docs_by_id.get(broker_id)
        if broker_doc:
            broker_details = dict(broker_doc)
            broker_details["_id"] = str(broker_doc.get("_id") or broker_id)
            new_record["broker_details"] = broker_details
            new_record["broker_label"] = _extract_broker_configuration_label(broker_doc, broker_id)
        enriched_records.append(new_record)
    return enriched_records


def _enrich_execution_record_with_pnl(record: dict) -> dict:
    legs = record.get("legs") if isinstance(record.get("legs"), list) else []
    enriched_legs = [_calc_leg_pnl(leg) for leg in legs if isinstance(leg, dict)]
    enriched_record = dict(record)
    enriched_record["legs"] = enriched_legs
    return enriched_record


def _run_portfolio_job(job_id: str, request: dict):
    """
    Subprocess worker for portfolio backtest.
    Runs all strategies in parallel using ProcessPoolExecutor.
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed, wait
    import multiprocessing

    try:
        os.nice(10)
    except Exception:
        pass

    state = _read_job_state(job_id) or {}
    portfolio_id = request.get("portfolio")
    start_date   = request.get("start_date")
    end_date     = request.get("end_date")

    try:
        db = MongoData()
        portfolio = db._db["saved_portfolios"].find_one({"_id": ObjectId(portfolio_id)})
        if not portfolio:
            state.update({"job_id": job_id, "status": "error",
                          "error": f"Portfolio {portfolio_id} not found",
                          "updated_at": time.time()})
            _write_job_state(job_id, state)
            db.close()
            return

        strategy_ids = portfolio.get("strategy_ids", [])
        if not strategy_ids:
            state.update({"job_id": job_id, "status": "error",
                          "error": "Portfolio has no strategies",
                          "updated_at": time.time()})
            _write_job_state(job_id, state)
            db.close()
            return

        strategy_docs = list(db._db["saved_strategies"].find(
            {"_id": {"$in": strategy_ids}},
            {"_id": 1, "name": 1, "full_config": 1},
        ))
        db.close()

        strategy_map     = {str(d["_id"]): d for d in strategy_docs}
        total_strategies = len(strategy_ids)
        name_map         = {}

        # Build per-strategy worker args
        worker_args = []
        error_results = []
        for strategy_id_obj in strategy_ids:
            strategy_id_str = str(strategy_id_obj)
            strategy_doc    = strategy_map.get(strategy_id_str)
            strategy_name   = (strategy_doc or {}).get("name") or strategy_id_str
            name_map[strategy_id_str] = strategy_name

            if not strategy_doc:
                error_results.append({
                    "_id":     strategy_id_str,
                    "item_id": strategy_id_str,
                    "status":  "error",
                    "error":   "Strategy not found",
                    "results": None,
                })
                continue

            full_config  = strategy_doc.get("full_config") or {}
            backtest_req = dict(full_config)
            backtest_req["start_date"] = start_date
            backtest_req["end_date"]   = end_date
            if "weekly_old_regime" in request:
                backtest_req["weekly_old_regime"] = request["weekly_old_regime"]

            worker_args.append({
                "strategy_id_str": strategy_id_str,
                "backtest_req":    backtest_req,
                "job_id":          job_id,
            })

        # Initial progress state
        state.update({
            "job_id":         job_id,
            "status":         "running",
            "strategy_count": total_strategies,
            "completed":      0,
            "total":          total_strategies,
            "percent":        0.0,
            "current_day":    f"Running {total_strategies} strategies in parallel…",
            "error":          None,
            "updated_at":     time.time(),
        })
        _write_job_state(job_id, state)

        results_by_id = {}
        for r in error_results:
            results_by_id[r["item_id"]] = r

        # Run in parallel — use min(strategies, cpu_count, 8) workers
        max_workers  = max(1, min(len(worker_args), os.cpu_count() or 4, 8))
        done_count   = len(error_results)

        def _read_prog_files() -> dict:
            """Read all per-strategy progress files for this job."""
            result = {}
            try:
                for p in JOB_STATE_DIR.glob(f"{job_id}_*.prog"):
                    try:
                        with open(p) as f:
                            data = json.load(f)
                        sid = p.stem[len(job_id) + 1:]
                        result[sid] = data
                    except Exception:
                        pass
            except Exception:
                pass
            return result

        if worker_args:
            with ProcessPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(strategy_worker, args): args["strategy_id_str"]
                    for args in worker_args
                }
                while futures:
                    done, not_done = wait(futures, timeout=1.0)

                    # Read per-strategy progress from temp files
                    prog_files = _read_prog_files()
                    total_pct  = done_count * 100.0
                    active_day = f"Completed {done_count}/{total_strategies}"
                    for sid, info in prog_files.items():
                        if info.get("total"):
                            worker_pct = (info["completed"] / info["total"]) * 100.0
                            worker_pct = max(worker_pct, 2.0)
                            total_pct += worker_pct
                            if info.get("day"):
                                active_day = info["day"]

                    overall_pct = round(total_pct / total_strategies, 1) if total_strategies else 0.0
                    state.update({
                        "job_id":      job_id,
                        "status":      "running",
                        "completed":   done_count,
                        "percent":     overall_pct,
                        "current_day": active_day,
                        "error":       None,
                        "updated_at":  time.time(),
                    })
                    _write_job_state(job_id, state)

                    for future in done:
                        result_item = future.result()
                        sid         = result_item["item_id"]
                        results_by_id[sid] = result_item
                        done_count += 1
                        del futures[future]

                        # Write immediately after each strategy completes
                        pct = round(done_count / total_strategies * 100, 1) if total_strategies else 0.0
                        state.update({
                            "completed":   done_count,
                            "percent":     pct,
                            "current_day": f"Completed {done_count}/{total_strategies} strategies",
                            "updated_at":  time.time(),
                        })
                        _write_job_state(job_id, state)

        # Preserve original strategy order
        results = [results_by_id[str(sid)] for sid in strategy_ids if str(sid) in results_by_id]

        final_result = {
            "status":   "completed",
            "progress": 100,
            "results":  results,
        }

        state.update({
            "job_id":      job_id,
            "status":      "done",
            "completed":   total_strategies,
            "total":       total_strategies,
            "percent":     100.0,
            "current_day": "Completed",
            "result":      final_result,
            "error":       None,
            "updated_at":  time.time(),
        })
        _write_job_state(job_id, state)

    except Exception as e:
        import traceback
        state.update({
            "job_id":     job_id,
            "status":     "error",
            "error":      traceback.format_exc(),
            "updated_at": time.time(),
        })
        _write_job_state(job_id, state)


# ─── App ──────────────────────────────────────────────────────────────────────

app    = FastAPI(title="Local Backtest API", version="2.0.0")
router = APIRouter(prefix="/algo")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def _setup_logging():
    from features.app_logger import setup_logging
    setup_logging()
    try:
        MongoData().ensure_core_indexes()
    except Exception:
        log.exception("Failed to ensure MongoDB indexes at startup")


@app.on_event("startup")
async def _redis_prewarm():
    """
    On server startup: if REDIS_MEMORY=True, push all cached pkl5 files to Redis
    in a background thread so the first backtest hits Redis instead of disk.
    Only runs if Redis is reachable and pkl5 cache exists.
    """
    from features.backtest_engine import REDIS_MEMORY, _cache_dir, _pkl5_path, _get_redis, DataIndex
    if not REDIS_MEMORY:
        return
    import threading, pickle, pathlib

    def _warm():
        try:
            r = _get_redis()
        except Exception as e:
            print(f"[prewarm] Redis not available: {e}")
            return

        loaded = 0
        skipped = 0
        base = pathlib.Path.home() / ".backtest_cache"
        for underlying_dir in base.iterdir():
            if not underlying_dir.is_dir():
                continue
            underlying = underlying_dir.name
            for pkl5 in sorted(underlying_dir.glob("*.pkl5")):
                date = pkl5.stem
                key  = f"di:{underlying}:{date}"
                if r.exists(key):
                    skipped += 1
                    continue
                try:
                    with open(pkl5, 'rb') as f:
                        data = pickle.load(f)
                    r.set(key, pickle.dumps(data, protocol=5))
                    loaded += 1
                except Exception:
                    pass

        total = loaded + skipped
        print(f"[prewarm] Redis ready: {total} days ({loaded} loaded, {skipped} already cached)")

    threading.Thread(target=_warm, daemon=True).start()


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.get("/health")
def health():
    return {"status": "ok"}


@router.post("/admin/seed-expiry-config")
def seed_expiry():
    """Re-seed expiry_day_config collection from in-memory EXPIRY_RULES."""
    try:
        db    = MongoData()
        count = seed_expiry_config(db)
        return {"seeded": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/backtest/sample-result")
async def backtest_sample_result():
    """Return sample backtest result JSON for frontend table rendering."""
    if not SAMPLE_RESULT_PATH.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {SAMPLE_RESULT_PATH}")

    try:
        with open(SAMPLE_RESULT_PATH) as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Blocking endpoints (existing behaviour) ───────────────────────────────────

@router.post("/backtest")
async def backtest_from_body(request: dict):
    """Run backtest synchronously — waits until complete then returns result."""
    try:
        return run_backtest(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/backtest/file")
async def backtest_from_file():
    """Run backtest from current_backtest_request.json synchronously."""
    if not REQUEST_JSON_PATH.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {REQUEST_JSON_PATH}")
    with open(REQUEST_JSON_PATH) as f:
        request = json.load(f)
    try:
        return run_backtest(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Background job endpoints (with progress) ──────────────────────────────────

@router.post("/backtest/start")
async def backtest_start(request: dict):
    """
    Start backtest in background. Returns job_id immediately.

    Postman flow:
      1. POST /backtest/start  → { "job_id": "abc123" }
      2. GET  /backtest/status/abc123  →  poll until status=done
      3. GET  /backtest/result/abc123  →  get final result
    """
    fingerprint = _request_fingerprint(request)
    estimated_total = _estimate_total_steps(request)
    cached_result = _read_cached_result(fingerprint)
    if cached_result is not None:
        job_id = str(uuid.uuid4())[:8]
        with _jobs_lock:
            _cleanup_old_jobs()
            _jobs[job_id] = {
                "status": "done",
                "completed": estimated_total,
                "total": estimated_total,
                "percent": 100.0 if estimated_total else 0.0,
                "current_day": "Cached Result",
                "error": None,
                "created_at": time.time(),
                "fingerprint": fingerprint,
            }
        _write_job_state(job_id, {
            "job_id": job_id,
            "status": "done",
            "completed": estimated_total,
            "total": estimated_total,
            "percent": 100.0 if estimated_total else 0.0,
            "current_day": "Cached Result",
            "result": cached_result,
            "error": None,
            "updated_at": time.time(),
        })
        return {"job_id": job_id, "status": "done", "cached": True}
    with _jobs_lock:
        _cleanup_old_jobs()
        for existing_job_id, job in _jobs.items():
            if job["status"] == "running":
                if job.get("fingerprint") == fingerprint:
                    return {
                        "job_id": existing_job_id,
                        "status": "running",
                        "message": "Identical backtest is already running",
                    }
                raise HTTPException(
                    status_code=429,
                    detail={
                        "message": "Another backtest is already running. Wait for it to finish or poll its status.",
                        "job_id": existing_job_id,
                    },
                )
        job_id = str(uuid.uuid4())[:8]
        _jobs[job_id] = {
            "status":      "running",
            "completed":   0,
            "total":       estimated_total,
            "percent":     0.0,
            "current_day": "Queued",
            "error":       None,
            "created_at":  time.time(),
            "fingerprint": fingerprint,
        }
    _write_job_state(job_id, {
        "job_id": job_id,
        "status": "running",
        "completed": 0,
        "total": estimated_total,
        "percent": 0.0,
        "current_day": "Queued",
        "fingerprint": fingerprint,
        "error": None,
        "updated_at": time.time(),
    })
    proc = multiprocessing.Process(target=_run_job, args=(job_id, request), daemon=True)
    proc.start()
    with _jobs_lock:
        _jobs[job_id]["pid"] = proc.pid
    return {"job_id": job_id, "status": "running"}


@router.post("/backtest/start/file")
async def backtest_start_file():
    """Start backtest from current_backtest_request.json in background."""
    if not REQUEST_JSON_PATH.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {REQUEST_JSON_PATH}")
    with open(REQUEST_JSON_PATH) as f:
        request = json.load(f)
    fingerprint = _request_fingerprint(request)
    estimated_total = _estimate_total_steps(request)
    cached_result = _read_cached_result(fingerprint)
    if cached_result is not None:
        job_id = str(uuid.uuid4())[:8]
        with _jobs_lock:
            _cleanup_old_jobs()
            _jobs[job_id] = {
                "status": "done",
                "completed": estimated_total,
                "total": estimated_total,
                "percent": 100.0 if estimated_total else 0.0,
                "current_day": "Cached Result",
                "error": None,
                "created_at": time.time(),
                "fingerprint": fingerprint,
            }
        _write_job_state(job_id, {
            "job_id": job_id,
            "status": "done",
            "completed": estimated_total,
            "total": estimated_total,
            "percent": 100.0 if estimated_total else 0.0,
            "current_day": "Cached Result",
            "result": cached_result,
            "error": None,
            "updated_at": time.time(),
        })
        return {"job_id": job_id, "status": "done", "cached": True}
    with _jobs_lock:
        _cleanup_old_jobs()
        for existing_job_id, job in _jobs.items():
            if job["status"] == "running":
                if job.get("fingerprint") == fingerprint:
                    return {
                        "job_id": existing_job_id,
                        "status": "running",
                        "message": "Identical backtest is already running",
                    }
                raise HTTPException(
                    status_code=429,
                    detail={
                        "message": "Another backtest is already running. Wait for it to finish or poll its status.",
                        "job_id": existing_job_id,
                    },
                )
        job_id = str(uuid.uuid4())[:8]
        _jobs[job_id] = {
            "status":      "running",
            "completed":   0,
            "total":       estimated_total,
            "percent":     0.0,
            "current_day": "Queued",
            "error":       None,
            "created_at":  time.time(),
            "fingerprint": fingerprint,
        }
    _write_job_state(job_id, {
        "job_id": job_id,
        "status": "running",
        "completed": 0,
        "total": estimated_total,
        "percent": 0.0,
        "current_day": "Queued",
        "fingerprint": fingerprint,
        "error": None,
        "updated_at": time.time(),
    })
    proc = multiprocessing.Process(target=_run_job, args=(job_id, request), daemon=True)
    proc.start()
    with _jobs_lock:
        _jobs[job_id]["pid"] = proc.pid
    return {"job_id": job_id, "status": "running"}


@router.get("/backtest/status/{job_id}")
async def backtest_status(job_id: str):
    """
    Poll progress of a running backtest.

    Response:
      { "status": "running", "completed": 45, "total": 250,
        "percent": 18.0, "current_day": "2024-03-15" }

      status values: "running" | "done" | "error"
    """
    job = _read_job_state(job_id) or _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return {
        "job_id":      job_id,
        "status":      job["status"],
        "completed":   job["completed"],
        "total":       job["total"],
        "percent":     job["percent"],
        "current_day": job["current_day"],
        "error":       job["error"],
        "result_ready": job["status"] == "done" and "result" in job,
    }


@router.get("/strategy/check-name")
async def strategy_check_name(name: str):
    """Check if a strategy name already exists."""
    db = MongoData()
    exists = db._db["saved_strategies"].find_one({"name": name}, {"_id": 1})
    return {"exists": exists is not None}


@router.post("/strategy/save")
async def strategy_save(payload: dict):
    """
    Save a strategy to MongoDB.
    payload: { name, start_date, end_date, strategy: {...} }
    Returns 409 if name already exists.
    """
    name = (payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Strategy name is required")

    db = MongoData()
    col = db._db["saved_strategies"]

    if col.find_one({"name": name}, {"_id": 1}):
        raise HTTPException(status_code=409, detail=f"Strategy '{name}' already exists")

    import datetime
    s = payload.get("strategy", {})
    report_data = payload.get("report_data")

    doc = {
        "name":        name,
        "underlying":  s.get("Ticker"),
        "user_id":     _resolve_app_user_id(payload.get("user_id") or s.get("user_id")),
        "created_at":  datetime.datetime.utcnow().isoformat(),
        "full_config": payload,
        "report_data": report_data,
    }
    result = col.insert_one(doc)
    _invalidate_list_cache("strategy_list", "portfolio_list")
    return {"success": True, "id": str(result.inserted_id), "name": name}


@router.get("/strategy/list")
async def strategy_list():
    """List all saved strategy names."""
    cached = _list_cache_get("strategy_list")
    if cached is not None:
        return cached
    db = MongoData()
    docs = list(db._db["saved_strategies"].find({}, {"_id": 1, "name": 1, "created_at": 1}))
    for d in docs:
        d["_id"] = str(d["_id"])
    response = {"strategies": docs}
    _list_cache_set("strategy_list", response)
    return response


@router.get("/portfolio/list")
async def portfolio_list():
    """List all saved portfolios."""
    cached = _list_cache_get("portfolio_list")
    if cached is not None:
        return cached
    db = MongoData()
    docs = list(
        db._db["saved_portfolios"]
        .find({}, {"name": 1, "strategy_ids": 1, "created_at": 1})
        .sort("created_at", -1)
    )
    strategy_ids = []
    for portfolio in docs:
        strategy_ids.extend(portfolio.get("strategy_ids", []))

    strategy_docs = list(
        db._db["saved_strategies"].find(
            {"_id": {"$in": strategy_ids}},
            {"_id": 1, "name": 1},
        )
    ) if strategy_ids else []
    strategy_name_map = {str(item["_id"]): item.get("name") or "" for item in strategy_docs}

    for d in docs:
        d["_id"] = str(d["_id"])
        resolved_ids = [str(item) for item in d.get("strategy_ids", [])]
        d["strategy_ids"] = resolved_ids
        d["strategy_names"] = [
            strategy_name_map[strategy_id]
            for strategy_id in resolved_ids
            if strategy_name_map.get(strategy_id)
        ]
    response = {"portfolios": docs}
    _list_cache_set("portfolio_list", response)
    return response


@router.get("/portfolio/{portfolio_id}")
async def portfolio_get(portfolio_id: str):
    """Fetch a saved portfolio with joined strategy details."""
    db = MongoData()
    try:
        oid = ObjectId(portfolio_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid portfolio_id")

    portfolio = db._db["saved_portfolios"].find_one({"_id": oid})
    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio not found")

    strategy_ids = portfolio.get("strategy_ids", [])
    saved_strategy_meta = portfolio.get("strategies", []) or []
    saved_strategy_meta_map = {}
    for item in saved_strategy_meta:
        if not isinstance(item, dict):
            continue
        sid = str(item.get("id") or item.get("_id") or "")
        if sid:
            saved_strategy_meta_map[sid] = item
    strategy_docs = list(
        db._db["saved_strategies"].find(
            {"_id": {"$in": strategy_ids}},
            {"_id": 1, "name": 1, "underlying": 1, "full_config": 1},
        )
    )
    strategy_map = {str(item["_id"]): item for item in strategy_docs}

    ordered_strategies = []
    for strategy_id in strategy_ids:
        item = strategy_map.get(str(strategy_id))
        if not item:
            continue
        full_config = item.get("full_config") or {}
        strategy_config = full_config.get("strategy") or {}
        saved_meta = saved_strategy_meta_map.get(str(item["_id"]), {})
        ordered_strategies.append({
            "_id": str(item["_id"]),
            "name": item.get("name") or "",
            "underlying": item.get("underlying") or strategy_config.get("Ticker") or "",
            "product": strategy_config.get("Product") or "INTRADAY",
            "checked": bool(saved_meta.get("checked", True)),
            "dte": saved_meta.get("dte") or [0],
            "qty_multiplier": saved_meta.get("qty_multiplier", 1),
            "slippage": saved_meta.get("slippage", 0),
            "weekdays": saved_meta.get("weekdays") or ["M", "T", "W", "Th", "F"],
        })

    portfolio["_id"] = str(portfolio["_id"])
    portfolio["strategy_ids"] = [str(item) for item in strategy_ids]
    portfolio["strategies"] = ordered_strategies
    portfolio["qty_multiplier"] = portfolio.get("qty_multiplier", 1)
    portfolio["is_weekdays"] = bool(portfolio.get("is_weekdays", True))
    return portfolio


@router.post("/portfolio/save")
async def portfolio_save(payload: dict):
    """
    Save a portfolio to MongoDB.
    payload: { name, strategy_ids: [<saved_strategies _id>, ...] }
    """
    import datetime

    name = (payload.get("name") or "").strip()
    portfolio_id = (payload.get("portfolio_id") or "").strip()
    strategy_ids = payload.get("strategy_ids") or []
    strategy_entries = payload.get("strategies") or []
    portfolio_qty_multiplier = payload.get("qty_multiplier")
    is_weekdays = payload.get("is_weekdays")

    if isinstance(strategy_entries, list) and strategy_entries:
        strategy_ids = [item.get("id") for item in strategy_entries if isinstance(item, dict) and item.get("id")]

    if not name:
        raise HTTPException(status_code=400, detail="Portfolio name is required")
    if not isinstance(strategy_ids, list) or not strategy_ids:
        raise HTTPException(status_code=400, detail="At least one strategy must be selected")

    db = MongoData()
    portfolio_col = db._db["saved_portfolios"]
    strategy_col = db._db["saved_strategies"]

    existing_doc = None
    if portfolio_id:
        try:
            existing_doc = portfolio_col.find_one({"_id": ObjectId(portfolio_id)})
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid portfolio_id")

    name_query = {"name": name}
    if existing_doc:
        name_query["_id"] = {"$ne": existing_doc["_id"]}
    if portfolio_col.find_one(name_query, {"_id": 1}):
        raise HTTPException(status_code=409, detail=f"Portfolio '{name}' already exists")

    object_ids = []
    for strategy_id in strategy_ids:
        try:
            object_ids.append(ObjectId(strategy_id))
        except Exception:
            raise HTTPException(status_code=400, detail=f"Invalid strategy_id: {strategy_id}")

    docs = list(
        strategy_col.find(
            {"_id": {"$in": object_ids}},
            {"_id": 1},
        )
    )
    if len(docs) != len(object_ids):
        raise HTTPException(status_code=404, detail="One or more strategies were not found")

    doc_map = {str(item["_id"]): item for item in docs}
    ordered_strategy_ids = []
    for strategy_id, object_id in zip(strategy_ids, object_ids):
        found = doc_map.get(str(object_id))
        if not found:
            raise HTTPException(status_code=404, detail=f"Strategy not found: {strategy_id}")
        ordered_strategy_ids.append(found["_id"])

    ordered_strategy_meta = []
    meta_map = {}
    for item in strategy_entries:
        if isinstance(item, dict) and item.get("id"):
            meta_map[str(item["id"])] = item

    for object_id in ordered_strategy_ids:
        sid = str(object_id)
        meta = meta_map.get(sid, {})
        ordered_strategy_meta.append({
            "id": sid,
            "checked": bool(meta.get("checked", True)),
            "dte": meta.get("dte") or [0],
            "qty_multiplier": meta.get("qty_multiplier", 1),
            "slippage": meta.get("slippage", 0),
            "weekdays": meta.get("weekdays") or ["M", "T", "W", "Th", "F"],
        })

    portfolio_doc = {
        "name": name,
        "strategy_ids": ordered_strategy_ids,
        "strategies": ordered_strategy_meta,
        "qty_multiplier": int(portfolio_qty_multiplier or 1),
        "is_weekdays": bool(True if is_weekdays is None else is_weekdays),
        "updated_at": datetime.datetime.utcnow().isoformat(),
    }

    if existing_doc:
        portfolio_col.update_one({"_id": existing_doc["_id"]}, {"$set": portfolio_doc})
        result_id = existing_doc["_id"]
    else:
        portfolio_doc["created_at"] = datetime.datetime.utcnow().isoformat()
        result = portfolio_col.insert_one(portfolio_doc)
        result_id = result.inserted_id

    _invalidate_list_cache("portfolio_list")
    return {
        "success": True,
        "id": str(result_id),
        "name": name,
        "strategy_ids": [str(item) for item in ordered_strategy_ids],
        "strategies": ordered_strategy_meta,
        "qty_multiplier": portfolio_doc["qty_multiplier"],
        "is_weekdays": portfolio_doc["is_weekdays"],
    }


def _extract_indicator_minutes(node):
    if isinstance(node, list):
        for item in node:
            minutes = _extract_indicator_minutes(item)
            if minutes is not None:
                return minutes
        return None
    if not isinstance(node, dict):
        return None
    value = node.get("Value")
    if isinstance(value, dict) and value.get("IndicatorName") == "IndicatorType.TimeIndicator":
        params = value.get("Parameters") or {}
        try:
            hour = int(params.get("Hour", 0))
            minute = int(params.get("Minute", 0))
            return hour * 60 + minute
        except Exception:
            return None
    if isinstance(value, list):
        nested = _extract_indicator_minutes(value)
        if nested is not None:
            return nested
    children = node.get("children") or node.get("Children")
    if isinstance(children, list):
        return _extract_indicator_minutes(children)
    return None


def _normalize_leg_instrument(option_value, instrument_kind):
    option = str(option_value or "").strip()
    if option.startswith("LegType."):
        return option
    if option in {"CE", "PE", "FUT"}:
        return f"LegType.{option}"
    instrument = str(instrument_kind or "").strip()
    if instrument.startswith("LegType."):
        return instrument
    if instrument in {"CE", "PE", "FUT"}:
        return f"LegType.{instrument}"
    return "LegType.CE"


def _normalize_weekdays_map(values):
    normalized = {
        "monday": False,
        "tuesday": False,
        "wednesday": False,
        "thursday": False,
        "friday": False,
        "saturday": False,
        "sunday": False,
    }
    mapping = {
        "m": "monday",
        "monday": "monday",
        "t": "tuesday",
        "tuesday": "tuesday",
        "w": "wednesday",
        "wednesday": "wednesday",
        "th": "thursday",
        "thu": "thursday",
        "thursday": "thursday",
        "f": "friday",
        "friday": "friday",
        "sat": "saturday",
        "saturday": "saturday",
        "sun": "sunday",
        "sunday": "sunday",
    }
    for value in values if isinstance(values, list) else []:
        key = mapping.get(str(value or "").strip().lower())
        if key:
            normalized[key] = True
    return normalized


def _default_leg_execution_config():
    return {
        "ProductType": "ProductType.NRML",
        "ExitOrder": {
            "Type": "OrderType.Limit",
            "Value": {
                "Buffer": {
                    "Type": "BufferType.Points",
                    "Value": {"TriggerBuffer": 0, "LimitBuffer": 3},
                },
                "Modification": {
                    "ModificationFrequency": 5,
                    "ContinuousMonitoring": "True",
                    "MarketOrderAfter": 1,
                },
            },
        },
        "EntryOrder": {
            "Type": "OrderType.Limit",
            "Value": {
                "Buffer": {
                    "Type": "BufferType.Points",
                    "Value": {"TriggerBuffer": 0, "LimitBuffer": 3},
                },
                "Modification": {"MarketOrderAfter": 40},
            },
        },
        "ReferenceForTgtSL": "PriceReferenceType.Trigger",
        "EntryDelay": 0,
    }


def _build_execution_cache(strategy_detail: dict, strategy_state: dict):
    detail = strategy_detail if isinstance(strategy_detail, dict) else {}
    full_config = detail.get("full_config") if isinstance(detail.get("full_config"), dict) else {}
    strategy = full_config.get("strategy") if isinstance(full_config.get("strategy"), dict) else {}
    legs = strategy.get("ListOfLegConfigs") if isinstance(strategy.get("ListOfLegConfigs"), list) else []
    ticker = detail.get("underlying") or strategy.get("Ticker") or strategy_state.get("ticker") or "NIFTY"

    lot_config = []
    expiries = []
    instruments = []
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        lot = leg.get("LotConfig") if isinstance(leg.get("LotConfig"), dict) else {}
        contract = leg.get("ContractType") if isinstance(leg.get("ContractType"), dict) else {}
        lot_config.append({
            "type": lot.get("Type") or "LotType.Quantity",
            "value": int(lot.get("Value", 1) or 1),
        })
        expiries.append(contract.get("Expiry") or "ExpiryType.Weekly")
        instruments.append(_normalize_leg_instrument(contract.get("Option"), contract.get("InstrumentKind")))

    return {
        "execution_version": "v2",
        "entry_time": _extract_indicator_minutes(strategy.get("EntryIndicators")),
        "exit_time": _extract_indicator_minutes(strategy.get("ExitIndicators")),
        "num_original_legs": len(legs),
        "lot_config": lot_config,
        "expiries": expiries,
        "instruments": instruments,
        "ticker": ticker,
        "strategy_type": strategy.get("StrategyType") or "StrategyType.IntradaySameDay",
        "reentry_restriction": strategy.get("ReentryTimeRestriction"),
    }


def _build_strategy_execution_config(strategy_detail: dict, strategy_state: dict, activation_mode: str):
    detail = strategy_detail if isinstance(strategy_detail, dict) else {}
    full_config = detail.get("full_config") if isinstance(detail.get("full_config"), dict) else {}
    strategy = full_config.get("strategy") if isinstance(full_config.get("strategy"), dict) else {}
    legs = strategy.get("ListOfLegConfigs") if isinstance(strategy.get("ListOfLegConfigs"), list) else []

    execution_config_base = detail.get("execution_config_base") if isinstance(detail.get("execution_config_base"), dict) else {}
    if not execution_config_base:
        execution_config_base = {
            "Multiplier": int(strategy_state.get("qty_multiplier") or 1),
            "LikeBacktester": activation_mode != "live",
            "MarginAutoSquareOff": True,
            "TimeDelta": 0,
        }
    else:
        execution_config_base = dict(execution_config_base)
        execution_config_base.setdefault("Multiplier", int(strategy_state.get("qty_multiplier") or 1))
        execution_config_base.setdefault("LikeBacktester", activation_mode != "live")
        execution_config_base.setdefault("MarginAutoSquareOff", True)
        execution_config_base.setdefault("TimeDelta", 0)

    execution_config_extra = detail.get("execution_config_extra") if isinstance(detail.get("execution_config_extra"), dict) else {}
    if not execution_config_extra or not isinstance(execution_config_extra.get("ListOfLegExecutionConfig"), list):
        execution_config_extra = {
            "ListOfLegExecutionConfig": [_default_leg_execution_config() for _ in legs]
        }
    else:
        execution_config_extra = dict(execution_config_extra)

    return {
        "execution_config_base": execution_config_base,
        "execution_config_extra": execution_config_extra,
        "is_weekdays": bool(strategy_state.get("is_weekdays", True)),
        "dte": strategy_state.get("dte") if isinstance(strategy_state.get("dte"), list) else [],
        "weekdays": _normalize_weekdays_map(strategy_state.get("weekdays") if isinstance(strategy_state.get("weekdays"), list) else []),
        "view_config": detail.get("view_config") if isinstance(detail.get("view_config"), dict) else {"advanced_exec_config_modal": True},
    }


def _normalize_execution_settings_payload(source_detail: dict, payload: dict, activation_mode: str):
    detail = _clone_json_value(source_detail) if isinstance(source_detail, dict) else {}
    incoming = payload if isinstance(payload, dict) else {}

    if isinstance(incoming.get("execution_config_base"), dict):
        detail["execution_config_base"] = _clone_json_value(incoming.get("execution_config_base"))
    if isinstance(incoming.get("execution_config_extra"), dict):
        detail["execution_config_extra"] = _clone_json_value(incoming.get("execution_config_extra"))
    if isinstance(incoming.get("view_config"), dict):
        detail["view_config"] = _clone_json_value(incoming.get("view_config"))

    normalized = _build_strategy_execution_config(
        detail,
        {
            "qty_multiplier": ((incoming.get("execution_config_base") or {}).get("Multiplier") if isinstance(incoming.get("execution_config_base"), dict) else 1) or 1,
            "is_weekdays": incoming.get("is_weekdays", True),
            "dte": incoming.get("dte") if isinstance(incoming.get("dte"), list) else [],
            "weekdays": list((incoming.get("weekdays") or {}).keys()) if isinstance(incoming.get("weekdays"), dict) else [],
        },
        activation_mode,
    )

    if isinstance(incoming.get("weekdays"), dict):
        normalized["weekdays"] = {
            "friday": bool(incoming["weekdays"].get("friday")),
            "monday": bool(incoming["weekdays"].get("monday")),
            "saturday": bool(incoming["weekdays"].get("saturday")),
            "sunday": bool(incoming["weekdays"].get("sunday")),
            "thursday": bool(incoming["weekdays"].get("thursday")),
            "tuesday": bool(incoming["weekdays"].get("tuesday")),
            "wednesday": bool(incoming["weekdays"].get("wednesday")),
        }
    normalized["is_weekdays"] = bool(incoming.get("is_weekdays", normalized.get("is_weekdays", True)))
    normalized["dte"] = incoming.get("dte") if isinstance(incoming.get("dte"), list) else normalized.get("dte", [])
    normalized["view_config"] = incoming.get("view_config") if isinstance(incoming.get("view_config"), dict) else normalized.get("view_config", {"advanced_exec_config_modal": True})
    return normalized


def _clone_json_value(value):
    return deepcopy(value)


def _normalize_optional_config(config):
    if not isinstance(config, dict):
        return None
    normalized = _clone_json_value(config)
    config_type = str(normalized.get("Type") or "").strip()
    if not config_type or config_type == "None":
        return None
    return normalized


def _normalize_reentry_value(config):
    if not isinstance(config, dict):
        return None
    reentry_type = str(config.get("Type") or "").strip()
    if not reentry_type or reentry_type == "None":
        return None

    raw_value = config.get("Value")
    normalized_value = raw_value
    if isinstance(raw_value, dict):
        if "NextLegRef" in raw_value:
            normalized_value = raw_value.get("NextLegRef")
        elif "ReentryCount" in raw_value:
            normalized_value = raw_value.get("ReentryCount")
        elif len(raw_value) == 1:
            normalized_value = next(iter(raw_value.values()))

    return {
        "Type": reentry_type,
        "Value": normalized_value,
    }


def _normalize_option_kind(instrument_kind: str):
    value = str(instrument_kind or "").upper()
    if "PE" in value:
        return "PE"
    return "CE"


def _normalize_contract_strike(value):
    if isinstance(value, (int, float)):
        return value
    raw_value = str(value or "").strip()
    if not raw_value:
        return 0
    if raw_value == "StrikeType.ATM":
        return 0
    numeric_match = re.fullmatch(r"-?\d+(?:\.\d+)?", raw_value)
    if numeric_match:
        parsed = float(raw_value)
        return int(parsed) if parsed.is_integer() else parsed
    return raw_value


def _build_algo_leg_config_entry(leg_config: dict):
    leg = leg_config if isinstance(leg_config, dict) else {}
    stop_loss = _normalize_optional_config(leg.get("LegStopLoss"))
    target = _normalize_optional_config(leg.get("LegTarget"))
    trail = _normalize_optional_config(leg.get("LegTrailSL"))
    momentum = _normalize_optional_config(leg.get("LegMomentum"))
    stop_reentry = _normalize_reentry_value(leg.get("LegReentrySL"))
    target_reentry = _normalize_reentry_value(leg.get("LegReentryTP"))

    if stop_loss and stop_reentry:
        stop_loss["Reentry"] = stop_reentry
    if stop_loss and trail:
        stop_loss["Trail"] = trail
    if target and target_reentry:
        target["Reentry"] = target_reentry

    return {
        "PositionType": leg.get("PositionType") or "PositionType.Sell",
        "ContractType": {
            "Option": _normalize_option_kind(leg.get("InstrumentKind")),
            "Expiry": leg.get("ExpiryKind") or "ExpiryType.Weekly",
            "InstrumentKind": "OPT",
            "StrikeParameter": _normalize_contract_strike(leg.get("StrikeParameter")),
            "EntryKind": leg.get("EntryType") or "EntryType.EntryByStrikeType",
        },
        "LotConfig": _clone_json_value(leg.get("LotConfig")) if isinstance(leg.get("LotConfig"), dict) else {
            "Type": "LotType.Quantity",
            "Value": 1,
        },
        "LegMomentum": momentum,
        "LegTarget": target,
        "LegStopLoss": stop_loss,
    }


def _build_algo_execution_leg_entry(leg_execution_config: dict):
    config = leg_execution_config if isinstance(leg_execution_config, dict) else {}
    entry_order = config.get("EntryOrder") if isinstance(config.get("EntryOrder"), dict) else {}
    exit_order = config.get("ExitOrder") if isinstance(config.get("ExitOrder"), dict) else {}

    entry_order_config = _clone_json_value(entry_order.get("Config")) if isinstance(entry_order.get("Config"), dict) else _clone_json_value(entry_order)
    exit_order_config = _clone_json_value(exit_order.get("Config")) if isinstance(exit_order.get("Config"), dict) else _clone_json_value(exit_order)
    if not entry_order_config:
        entry_order_config = {"Type": "OrderType.Market"}
    if not exit_order_config:
        exit_order_config = {"Type": "OrderType.Market"}

    return {
        "Product": config.get("Product") or config.get("ProductType") or "ProductType.NRML",
        "Reference": config.get("Reference") or config.get("ReferenceForTgtSL") or "PriceReferenceType.Trigger",
        "EntryOrder": {
            "Config": entry_order_config,
            "Delay": int(config.get("EntryDelay") or entry_order.get("Delay") or 0),
        },
        "ExitOrder": {
            "Config": exit_order_config,
            "Delay": int(config.get("ExitDelay") or exit_order.get("Delay") or 0),
        },
    }


def _build_algo_trade_config(strategy_detail: dict, strategy_state: dict, activation_mode: str):
    detail = strategy_detail if isinstance(strategy_detail, dict) else {}
    full_config = detail.get("full_config") if isinstance(detail.get("full_config"), dict) else {}
    strategy = full_config.get("strategy") if isinstance(full_config.get("strategy"), dict) else {}
    if not strategy:
        return None

    parent_legs = strategy.get("ListOfLegConfigs") if isinstance(strategy.get("ListOfLegConfigs"), list) else []
    idle_legs = strategy.get("IdleLegConfigs") if isinstance(strategy.get("IdleLegConfigs"), dict) else {}
    execution_config = _build_strategy_execution_config(detail, strategy_state, activation_mode)
    execution_base = execution_config.get("execution_config_base") if isinstance(execution_config.get("execution_config_base"), dict) else {}
    execution_extra = execution_config.get("execution_config_extra") if isinstance(execution_config.get("execution_config_extra"), dict) else {}
    execution_leg_configs = execution_extra.get("ListOfLegExecutionConfig") if isinstance(execution_extra.get("ListOfLegExecutionConfig"), list) else []

    keyed_leg_configs = {}
    keyed_execution_legs = {}
    for index, leg in enumerate(parent_legs, start=1):
        leg_key = f"og_leg_{index}"
        keyed_leg_configs[leg_key] = _build_algo_leg_config_entry(leg)
        keyed_execution_legs[leg_key] = _build_algo_execution_leg_entry(
            execution_leg_configs[index - 1] if index - 1 < len(execution_leg_configs) else {}
        )

    normalized_idle_legs = {}
    for idle_key, idle_leg in idle_legs.items():
        normalized_idle_legs[str(idle_key)] = _build_algo_leg_config_entry(idle_leg)

    return {
        "ExecutionConfig": {
            "LikeBacktester": bool(execution_base.get("LikeBacktester", activation_mode != "live")),
            "MarginAutoSquareOff": bool(execution_base.get("MarginAutoSquareOff", True)),
            "LotMultiplier": int(execution_base.get("Multiplier") or strategy_state.get("qty_multiplier") or 1),
            "LegsConfig": keyed_execution_legs,
        },
        "Ticker": strategy.get("Ticker") or detail.get("underlying") or strategy_state.get("ticker") or "NIFTY",
        "TakeUnderlyingFromCash": str(strategy.get("TakeUnderlyingFromCashOrNot") or "True").lower() == "true",
        "TrailSLtoBreakeven": _normalize_optional_config(strategy.get("TrailSLtoBreakeven")),
        "SquareOffAllLegs": str(strategy.get("SquareOffAllLegs") or "False").lower() == "true",
        "LegConfigs": keyed_leg_configs,
        "IdleLegConfigs": normalized_idle_legs,
        "OverallSL": _normalize_optional_config(strategy.get("OverallSL")),
        "OverallTgt": _normalize_optional_config(strategy.get("OverallTgt")),
        "LockAndTrail": _normalize_optional_config(strategy.get("LockAndTrail")),
        "OverallTrailSL": _normalize_optional_config(strategy.get("OverallTrailSL")),
        "OverallReentrySL": _normalize_optional_config(strategy.get("OverallReentrySL")),
        "OverallReentryTgt": _normalize_optional_config(strategy.get("OverallReentryTgt")),
        "OverallMomentum": _normalize_optional_config(strategy.get("OverallMomentum")),
    }


@router.post("/portfolio/prepare-activation")
async def portfolio_prepare_activation(payload: dict):
    portfolio_id = str(payload.get("portfolio_id") or "").strip()
    activation_mode = str(payload.get("activation_mode") or "").strip() or "algo-backtest"
    strategies = payload.get("strategies") or []

    if not portfolio_id:
        raise HTTPException(status_code=400, detail="portfolio_id is required")
    if not isinstance(strategies, list) or not strategies:
        raise HTTPException(status_code=400, detail="At least one strategy is required")

    db = MongoData()
    try:
        portfolio_oid = ObjectId(portfolio_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid portfolio_id")

    portfolio_doc = db._db["saved_portfolios"].find_one({"_id": portfolio_oid}, {"_id": 1, "name": 1})
    if not portfolio_doc:
        raise HTTPException(status_code=404, detail="Portfolio not found")

    executed_col = db._db["executed_strategies"]
    now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    split_executions = {}
    docs_to_insert = []
    prepared_rows = []

    for index, item in enumerate(strategies):
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail=f"Strategy at index {index} must be an object")

        source_strategy_id = str(item.get("strategy_id") or "").strip()
        if not source_strategy_id:
            raise HTTPException(status_code=400, detail=f"Strategy at index {index} is missing strategy_id")

        strategy_detail = item.get("strategy_detail") if isinstance(item.get("strategy_detail"), dict) else {}

        # Fetch full_config from saved_strategies and embed it so that
        # /portfolio/activate can write algo_trades.strategy correctly.
        if not strategy_detail.get("full_config"):
            try:
                _saved_doc = db._db["saved_strategies"].find_one(
                    {"_id": ObjectId(source_strategy_id)},
                    {"full_config": 1},
                )
                if _saved_doc and isinstance(_saved_doc.get("full_config"), dict):
                    strategy_detail = dict(strategy_detail)
                    strategy_detail["full_config"] = _saved_doc["full_config"]
            except Exception:
                pass  # invalid ObjectId or not found — proceed without

        broker = str(
            item.get("broker")
            or item.get("broker_type")
            or strategy_detail.get("broker")
            or strategy_detail.get("broker_type")
            or ""
        ).strip() or None
        user_id = _resolve_app_user_id(item.get("user_id") or strategy_detail.get("user_id"))
        execution_number = executed_col.count_documents({
            "portfolio_id": portfolio_id,
            "source_strategy_id": source_strategy_id,
        }) + 1
        assigned_strategy_id = str(ObjectId())
        execution_cache = _build_execution_cache(strategy_detail, item)
        strategy_execution_config = _build_strategy_execution_config(strategy_detail, item, activation_mode)
        ticker = execution_cache.get("ticker") or str(item.get("ticker") or "NIFTY")
        underlying_max_lots = item.get("underlying_max_lots") if isinstance(item.get("underlying_max_lots"), dict) else {ticker: 0}

        docs_to_insert.append({
            "_id": ObjectId(assigned_strategy_id),
            "assigned_strategy_id": assigned_strategy_id,
            "source_strategy_id": source_strategy_id,
            "strategy_id": source_strategy_id,
            "strategy_name": item.get("name") or strategy_detail.get("name") or "",
            "portfolio_id": portfolio_id,
            "portfolio_name": portfolio_doc.get("name") or "",
            "activation_mode": activation_mode,
            "broker": broker,
            "user_id": user_id,
            "number_of_executions": execution_number,
            "execution_cache": execution_cache,
            "multiplier": int(item.get("qty_multiplier") or 1),
            "underlying_max_lots": underlying_max_lots,
            "strategy_detail_snapshot": strategy_detail,  # now contains full_config
            "created_at": now_ts,
            "updated_at": now_ts,
        })

        split_executions.setdefault(str(broker or ""), {})[source_strategy_id] = {
            "number_of_executions": execution_number,
            "assigned_strategy_id": assigned_strategy_id,
        }
        prepared_rows.append({
            "source_strategy_id": source_strategy_id,
            "assigned_strategy_id": assigned_strategy_id,
            "number_of_executions": execution_number,
            "broker": broker,
            "user_id": user_id,
            "ticker": ticker,
        })

    if docs_to_insert:
        executed_col.insert_many(docs_to_insert, ordered=True)

    return {
        "success": True,
        "portfolio_id": portfolio_id,
        "activation_mode": activation_mode,
        "split_executions": split_executions,
        "executed_strategies": prepared_rows,
    }


@router.post("/portfolio/activate")
async def portfolio_activate(payload: dict):
    """
    Persist initial execution records into algo_trades.

    Request body:
      {
        "portfolio_id": "<portfolio_id>",
        "activation_mode": "algo-backtest|forward-test|live",
        "trades": [<live execution record>, ...]
      }
    """
    portfolio_id = str(payload.get("portfolio_id") or "").strip()
    activation_mode = str(payload.get("activation_mode") or "").strip() or "algo-backtest"
    requested_current_datetime = str(payload.get("current_datetime") or "").strip()
    trades = payload.get("trades") or []

    if not portfolio_id:
        raise HTTPException(status_code=400, detail="portfolio_id is required")
    if not isinstance(trades, list) or not trades:
        raise HTTPException(status_code=400, detail="At least one trade record is required")

    db = MongoData()
    try:
        portfolio_oid = ObjectId(portfolio_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid portfolio_id")

    portfolio_doc = db._db["saved_portfolios"].find_one({"_id": portfolio_oid}, {"_id": 1, "name": 1})
    if not portfolio_doc:
        raise HTTPException(status_code=404, detail="Portfolio not found")

    executed_col = db._db["executed_strategies"]
    collection_name = "algo_trades"
    algo_trades_col = db._db[collection_name]
    resolved_now = datetime.utcnow()
    if activation_mode == "algo-backtest" and requested_current_datetime:
        normalized_value = requested_current_datetime.replace("T", " ")
        parsed_now = None
        for pattern in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed_now = datetime.strptime(normalized_value, pattern)
                break
            except ValueError:
                continue
        if parsed_now is not None:
            resolved_now = parsed_now
    now_ts = resolved_now.strftime("%Y-%m-%d %H:%M:%S.%f")
    raw_group_name = str((((trades[0] or {}).get("portfolio") or {}).get("group_name") or "")).strip() if trades else ""
    base_portfolio_group_name = str(portfolio_doc.get("name") or raw_group_name or "Portfolio Activation").strip() or "Portfolio Activation"
    base_portfolio_group_name = re.sub(r" \(\d+\)$", "", base_portfolio_group_name).strip() or "Portfolio Activation"
    matching_group_names = []
    group_name_regex = "^" + re.escape(base_portfolio_group_name) + r"(?: \(\d+\))?$"
    for existing in algo_trades_col.find({"portfolio.portfolio": portfolio_id, "portfolio.group_name": {"$regex": group_name_regex}}, {"portfolio.group_name": 1}):
        existing_name = str(((existing.get("portfolio") or {}).get("group_name") or "")).strip()
        if existing_name:
            matching_group_names.append(existing_name)

    if base_portfolio_group_name not in matching_group_names:
        resolved_portfolio_group_name = base_portfolio_group_name
    else:
        highest_group_index = 0
        for existing_name in matching_group_names:
            if existing_name == base_portfolio_group_name:
                continue
            suffix_match = re.search(r" \((\d+)\)$", existing_name)
            if suffix_match:
                highest_group_index = max(highest_group_index, int(suffix_match.group(1)))
        resolved_portfolio_group_name = f"{base_portfolio_group_name} ({highest_group_index + 1})"

    docs_to_insert = []

    for index, item in enumerate(trades):
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail=f"Trade at index {index} must be an object")

        trade_id = str(item.get("_id") or "").strip()
        strategy_id = str(item.get("strategy_id") or "").strip()
        if not trade_id:
            raise HTTPException(status_code=400, detail=f"Trade at index {index} is missing _id")
        if not strategy_id:
            raise HTTPException(status_code=400, detail=f"Trade at index {index} is missing strategy_id")

        doc = dict(item)
        prepared_execution = executed_col.find_one(
            {"assigned_strategy_id": strategy_id},
            {"strategy_detail_snapshot": 1, "multiplier": 1, "source_strategy_id": 1, "broker": 1, "user_id": 1},
        )
        prepared_strategy_detail = (
            prepared_execution.get("strategy_detail_snapshot")
            if isinstance(prepared_execution, dict) and isinstance(prepared_execution.get("strategy_detail_snapshot"), dict)
            else {}
        )
        prepared_state = {
            "qty_multiplier": (
                doc.get("multiplier")
                or doc.get("qty_multiplier")
                or (prepared_execution.get("multiplier") if isinstance(prepared_execution, dict) else 1)
                or 1
            ),
            "ticker": doc.get("ticker") or doc.get("underlying") or "NIFTY",
        }
        full_config_snapshot = (
            prepared_strategy_detail.get("full_config")
            if isinstance(prepared_strategy_detail.get("full_config"), dict)
            else {}
        )
        imported_strategy = None

        # Primary source: use the incoming strategy_id to fetch the exact
        # saved_strategies.full_config.strategy requested by the activation payload.
        if strategy_id:
            try:
                _saved = db._db["saved_strategies"].find_one(
                    {"_id": ObjectId(strategy_id)},
                    {"full_config": 1},
                )
                _fc = (_saved or {}).get("full_config") if isinstance((_saved or {}).get("full_config"), dict) else {}
                _strat = _fc.get("strategy") if isinstance(_fc.get("strategy"), dict) else None
                if _strat:
                    imported_strategy = _clone_json_value(_strat)
            except Exception:
                pass

        # Fallback to the prepared execution snapshot when the saved strategy
        # lookup is unavailable for older activation records.
        if not imported_strategy:
            imported_strategy = (
                _clone_json_value(full_config_snapshot.get("strategy"))
                if isinstance(full_config_snapshot.get("strategy"), dict)
                else None
            )

        # Final fallback: fetch via source_strategy_id from the prepared record.
        if not imported_strategy:
            _source_sid = str(
                (prepared_execution or {}).get("source_strategy_id")
                or item.get("source_strategy_id")
                or ""
            ).strip()
            if _source_sid:
                try:
                    _saved = db._db["saved_strategies"].find_one(
                        {"_id": ObjectId(_source_sid)},
                        {"full_config": 1},
                    )
                    _fc = (_saved or {}).get("full_config") if isinstance((_saved or {}).get("full_config"), dict) else {}
                    _strat = _fc.get("strategy") if isinstance(_fc.get("strategy"), dict) else None
                    if _strat:
                        imported_strategy = _clone_json_value(_strat)
                except Exception:
                    pass

        doc["_id"] = trade_id
        doc["strategy_id"] = strategy_id
        doc["activation_mode"] = activation_mode
        doc["active_on_server"] = bool(doc.get("active_on_server", True))
        doc["trade_status"] = int(doc.get("trade_status", 1) or 1)
        doc.pop("config", None)
        if imported_strategy:
            doc["strategy"] = imported_strategy
        elif not isinstance(doc.get("strategy"), dict):
            doc["strategy"] = {"Ticker": prepared_state.get("ticker") or "NIFTY"}
        doc["legs"] = doc.get("legs") if isinstance(doc.get("legs"), list) else []
        default_status = "StrategyStatus.Import" if activation_mode == "algo-backtest" else "StrategyStatus.Live_Running"
        doc["status"] = doc.get("status") or default_status
        prepared_broker = (
            str((prepared_execution or {}).get("broker") or prepared_strategy_detail.get("broker") or "").strip()
            or None
        )
        prepared_user_id = (
            str((prepared_execution or {}).get("user_id") or prepared_strategy_detail.get("user_id") or "").strip()
            or _resolve_app_user_id()
        )
        normalized_execution_settings = _build_strategy_execution_config(prepared_strategy_detail, prepared_state, activation_mode)
        doc["broker"] = str(doc.get("broker") or prepared_broker or "").strip() or None
        doc["user_id"] = _resolve_app_user_id(doc.get("user_id") or prepared_user_id)
        doc["source_strategy_id"] = str(
            (prepared_execution or {}).get("source_strategy_id")
            or item.get("source_strategy_id")
            or ""
        ).strip() or None
        doc["execution_config_base"] = normalized_execution_settings.get("execution_config_base") if isinstance(normalized_execution_settings.get("execution_config_base"), dict) else {}
        doc["execution_config_extra"] = normalized_execution_settings.get("execution_config_extra") if isinstance(normalized_execution_settings.get("execution_config_extra"), dict) else {}
        doc["view_config"] = normalized_execution_settings.get("view_config") if isinstance(normalized_execution_settings.get("view_config"), dict) else {"advanced_exec_config_modal": True}
        doc.pop("broker_type", None)
        doc["portfolio"] = doc.get("portfolio") if isinstance(doc.get("portfolio"), dict) else {}
        doc["portfolio"]["portfolio"] = portfolio_id
        doc["portfolio"]["group_name"] = resolved_portfolio_group_name
        if activation_mode == "algo-backtest" and requested_current_datetime:
            doc["creation_ts"] = now_ts
            doc["last_activation_ts"] = now_ts
        else:
            doc.setdefault("creation_ts", now_ts)
            doc.setdefault("last_activation_ts", now_ts)
        docs_to_insert.append(doc)

    existing_ids = {
        str(item["_id"])
        for item in algo_trades_col.find(
            {"_id": {"$in": [doc["_id"] for doc in docs_to_insert]}},
            {"_id": 1},
        )
    }
    if existing_ids:
        raise HTTPException(
            status_code=409,
            detail="Trade records already exist for ids: " + ", ".join(sorted(existing_ids)),
        )

    algo_trades_col.insert_many(docs_to_insert, ordered=True)
    resolved_group_id = str((((docs_to_insert[0] or {}).get("portfolio") or {}).get("group_id") or "")).strip() if docs_to_insert else ""

    return {
        "success": True,
        "portfolio_id": portfolio_id,
        "group_id": resolved_group_id,
        "activation_mode": activation_mode,
        "collection_name": collection_name,
        "inserted_count": len(docs_to_insert),
        "records": docs_to_insert,
    }


@router.get("/portfolio/start/{group_id}")
async def portfolio_start_group(group_id: str, activation_mode: str = "algo-backtest"):
    normalized_group_id = str(group_id or "").strip()
    normalized_mode = str(activation_mode or "").strip() or "algo-backtest"
    if normalized_mode == "forward-test":
        normalized_mode = "fast-forward"

    if not normalized_group_id:
        raise HTTPException(status_code=400, detail="group_id is required")

    db = MongoData()
    algo_trades_col = db._db["algo_trades"]
    records = list(
        algo_trades_col.find(
            {
                "portfolio.group_id": normalized_group_id,
                "activation_mode": normalized_mode,
            }
        ).sort("creation_ts", 1)
    )
    if not records:
        raise HTTPException(status_code=404, detail="No activated strategies found for this group_id")

    normalized_records = []
    for item in records:
        record = dict(item)
        record["_id"] = str(record.get("_id") or "")
        normalized_records.append(record)

    queue_execute_order_group_start(normalized_group_id, normalized_records)

    return {
        "success": True,
        "group_id": normalized_group_id,
        "activation_mode": normalized_mode,
        "count": len(normalized_records),
        "records": normalized_records,
    }


@router.post("/portfolio/execution-settings/update")
async def portfolio_execution_settings_update(payload: dict):
    portfolio_id = str(payload.get("portfolio_id") or "").strip()
    source_strategy_id = str(payload.get("source_strategy_id") or "").strip()
    activation_mode = str(payload.get("activation_mode") or "").strip() or "live"
    execution_settings = payload.get("execution_settings") if isinstance(payload.get("execution_settings"), dict) else {}

    if not portfolio_id:
        raise HTTPException(status_code=400, detail="portfolio_id is required")
    if not source_strategy_id:
        raise HTTPException(status_code=400, detail="source_strategy_id is required")
    if not execution_settings:
        raise HTTPException(status_code=400, detail="execution_settings is required")

    db = MongoData()
    try:
        strategy_oid = ObjectId(source_strategy_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid source_strategy_id")

    saved_strategy = db._db["saved_strategies"].find_one({"_id": strategy_oid}) or {}
    if not saved_strategy:
        raise HTTPException(status_code=404, detail="Saved strategy not found")

    normalized_settings = _normalize_execution_settings_payload(saved_strategy, execution_settings, activation_mode)
    now_iso = datetime.utcnow().isoformat()
    now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

    saved_update = db._db["saved_strategies"].update_one(
        {"_id": strategy_oid},
        {"$set": {
            "execution_config_base": normalized_settings.get("execution_config_base") or {},
            "execution_config_extra": normalized_settings.get("execution_config_extra") or {},
            "view_config": normalized_settings.get("view_config") or {"advanced_exec_config_modal": True},
            "updated_at": now_iso,
        }}
    )

    executed_col = db._db["executed_strategies"]
    latest_execution = executed_col.find_one(
        {"portfolio_id": portfolio_id, "source_strategy_id": source_strategy_id},
        sort=[("created_at", -1)],
    ) or {}

    executed_update_count = 0
    if latest_execution:
        executed_result = executed_col.update_one(
            {"_id": latest_execution["_id"]},
            {"$set": {
                "strategy_detail_snapshot.execution_config_base": normalized_settings.get("execution_config_base") or {},
                "strategy_detail_snapshot.execution_config_extra": normalized_settings.get("execution_config_extra") or {},
                "strategy_detail_snapshot.view_config": normalized_settings.get("view_config") or {"advanced_exec_config_modal": True},
                "updated_at": now_ts,
            }}
        )
        executed_update_count = int(executed_result.modified_count or 0)

    algo_query = {
        "portfolio.portfolio": portfolio_id,
        "activation_mode": activation_mode,
        "source_strategy_id": source_strategy_id,
    }
    algo_result = db._db["algo_trades"].update_many(
        algo_query,
        {"$set": {
            "execution_config_base": normalized_settings.get("execution_config_base") or {},
            "execution_config_extra": normalized_settings.get("execution_config_extra") or {},
            "view_config": normalized_settings.get("view_config") or {"advanced_exec_config_modal": True},
            "updated_at": now_ts,
        }}
    )

    return {
        "success": True,
        "portfolio_id": portfolio_id,
        "source_strategy_id": source_strategy_id,
        "activation_mode": activation_mode,
        "saved_strategy_updated": int(saved_update.modified_count or 0),
        "executed_strategy_updated": executed_update_count,
        "algo_trades_updated": int(algo_result.modified_count or 0),
        "execution_settings": normalized_settings,
    }


@router.get("/trades/list")
async def list_algo_trades(date: str = "", activation_mode: str = "algo-backtest", trade_status: Optional[int] = None):
    """
    List algo trade execution records by activation mode and creation date.

    Query params:
      - date: YYYY-MM-DD
      - activation_mode: algo-backtest|forward-test|live
      - trade_status: numeric trade status filter
    """
    db = MongoData()
    algo_trades_col = db._db["algo_trades"]
    normalized_mode = str(activation_mode or "").strip() or "algo-backtest"
    normalized_date = str(date or "").strip()

    query = {"activation_mode": normalized_mode}
    if normalized_date:
        query["creation_ts"] = {"$regex": f"^{re.escape(normalized_date)}"}
    if trade_status is not None:
        query["trade_status"] = trade_status

    raw_records = []
    cursor = algo_trades_col.find(query).sort("creation_ts", -1)
    for item in cursor:
        raw_records.append({
            "_id": str(item.get("_id") or ""),
            "strategy_id": str(item.get("strategy_id") or ""),
            "name": item.get("name") or "",
            "status": item.get("status") or "",
            "trade_status": item.get("trade_status"),
            "active_on_server": bool(item.get("active_on_server")),
            "activation_mode": item.get("activation_mode") or normalized_mode,
            "broker": item.get("broker") or "",
            "user_id": item.get("user_id") or "",
            "ticker": item.get("ticker") or "",
            "creation_ts": item.get("creation_ts") or "",
            "entry_time": item.get("entry_time") or "",
            "exit_time": item.get("exit_time") or "",
            "portfolio": item.get("portfolio") if isinstance(item.get("portfolio"), dict) else {},
        })

    # Populate string leg IDs with full algo_trade_positions_history docs (single batch query)
    populated_records = _populate_history_legs(db._db, raw_records)
    populated_records = _attach_leg_feature_statuses(db._db, populated_records)
    populated_records = _attach_broker_configuration_details(db._db, populated_records)
    records = [_enrich_execution_record_with_pnl(rec) for rec in populated_records]

    return {
        "success": True,
        "date": normalized_date,
        "activation_mode": normalized_mode,
        "count": len(records),
        "records": records,
    }


@router.get("/executions")
async def list_algo_executions(environment: str = "algo-backtest", is_signal: bool = False, date: str = "", trade_status: Optional[int] = None):
    """
    List execution records using an environment-based query shape.

    Query params:
      - environment: algo-backtest|forward-test|live
      - is_signal: reserved for parity with the upstream API
      - date: YYYY-MM-DD (required when environment=algo-backtest)
      - trade_status: numeric trade status filter
    """
    normalized_environment = str(environment or "").strip() or "algo-backtest"
    normalized_date = str(date or "").strip()

    if normalized_environment == "algo-backtest" and not normalized_date:
        raise HTTPException(status_code=400, detail="date is required when environment=algo-backtest")

    return await list_algo_trades(
        date=normalized_date,
        activation_mode=normalized_environment,
        trade_status=trade_status,
    )


@router.post("/portfolio/backtest/start")
async def portfolio_backtest_start(request: dict):
    """
    Start a portfolio backtest in background.
    Runs backtest for every strategy in the portfolio sequentially.

    Request body:
      { "portfolio": "<portfolio_id>", "start_date": "YYYY-MM-DD",
        "end_date": "YYYY-MM-DD", "weekly_old_regime": true, "source": "WEB" }

    Returns: { "job_id": "...", "status": "running", "strategy_count": N }

    Then poll:  GET /algo/backtest/status/{job_id}
    Then fetch: GET /algo/backtest/result/{job_id}
      → { "status": "completed", "progress": 100,
          "results": [ { "_id": "...", "item_id": "<strategy_id>",
                         "status": "completed", "results": {...} }, ... ] }
    """
    portfolio_id = (request.get("portfolio") or "").strip()
    start_date   = request.get("start_date")
    end_date     = request.get("end_date")

    if not portfolio_id:
        raise HTTPException(status_code=400, detail="portfolio field is required")
    if not start_date or not end_date:
        raise HTTPException(status_code=400, detail="start_date and end_date are required")

    try:
        oid = ObjectId(portfolio_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid portfolio id")

    db        = MongoData()
    portfolio = db._db["saved_portfolios"].find_one({"_id": oid}, {"strategy_ids": 1})
    db.close()

    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio not found")

    strategy_ids     = portfolio.get("strategy_ids", [])
    total_strategies = len(strategy_ids)

    if not strategy_ids:
        raise HTTPException(status_code=400, detail="Portfolio has no strategies")

    fingerprint      = _request_fingerprint(request)
    # Estimate trading days × number of strategies for progress denominator
    estimated_days   = _estimate_total_steps({"start_date": start_date, "end_date": end_date})
    estimated_total  = total_strategies * max(estimated_days, 1)

    with _jobs_lock:
        _cleanup_old_jobs()
        for existing_job_id, job in _jobs.items():
            if job["status"] == "running":
                if job.get("fingerprint") == fingerprint:
                    return {
                        "job_id":         existing_job_id,
                        "status":         "running",
                        "message":        "Identical portfolio backtest is already running",
                        "strategy_count": total_strategies,
                    }
                raise HTTPException(
                    status_code=429,
                    detail={
                        "message": "Another backtest is already running. Wait for it to finish.",
                        "job_id":  existing_job_id,
                    },
                )

        job_id = str(uuid.uuid4())[:8]
        _jobs[job_id] = {
            "status":         "running",
            "completed":      0,
            "total":          estimated_total,
            "percent":        0.0,
            "current_day":    "Queued",
            "error":          None,
            "created_at":     time.time(),
            "fingerprint":    fingerprint,
            "strategy_count": total_strategies,
            "strategy_index": 0,
        }

    _write_job_state(job_id, {
        "job_id":         job_id,
        "status":         "running",
        "completed":      0,
        "total":          estimated_total,
        "percent":        0.0,
        "current_day":    "Queued",
        "fingerprint":    fingerprint,
        "error":          None,
        "strategy_count": total_strategies,
        "strategy_index": 0,
        "updated_at":     time.time(),
    })

    proc = multiprocessing.Process(
        target=_run_portfolio_job, args=(job_id, request), daemon=False
    )
    proc.start()
    with _jobs_lock:
        _jobs[job_id]["pid"] = proc.pid

    return {"job_id": job_id, "status": "running", "strategy_count": total_strategies}


@router.get("/strategy/{strategy_id}")
async def strategy_get(strategy_id: str):
    """Fetch a saved strategy by its MongoDB _id."""
    db = MongoData()
    try:
        doc = db._db["saved_strategies"].find_one({"_id": ObjectId(strategy_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid strategy_id")
    if not doc:
        raise HTTPException(status_code=404, detail="Strategy not found")
    doc["_id"] = str(doc["_id"])
    return doc


@router.put("/strategy/{strategy_id}")
async def strategy_update(strategy_id: str, payload: dict):
    """Update an existing strategy's full_config by its MongoDB _id."""
    import datetime
    db = MongoData()
    try:
        oid = ObjectId(strategy_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid strategy_id")
    s = payload.get("strategy", {})
    report_data = payload.get("report_data")
    result = db._db["saved_strategies"].update_one(
        {"_id": oid},
        {"$set": {
            "full_config":  payload,
            "report_data":  report_data,
            "underlying":   s.get("Ticker"),
            "updated_at":   datetime.datetime.utcnow().isoformat(),
        }}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Strategy not found")
    _invalidate_list_cache("strategy_list", "portfolio_list")
    return {"success": True, "id": strategy_id}


@router.get("/backtest/result/{job_id}")
async def backtest_result(job_id: str):
    """
    Get final result once status=done.
    Returns 400 if still running, 500 if errored.
    """
    job = _read_job_state(job_id) or _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    if job["status"] == "running":
        raise HTTPException(status_code=400, detail="Backtest still running. Check /backtest/status/{job_id}")
    if job["status"] == "error":
        raise HTTPException(status_code=500, detail=job["error"])
    return job["result"]


# ─── Notification history ──────────────────────────────────────────────────────

@router.get("/notifications")
async def get_notifications(
    trade_id: str = "",
    strategy_id: str = "",
    trade_date: str = "",
    event_type: str = "",
    limit: int = 200,
):
    """
    Fetch algo_trade_notification records.

    Query params (all optional):
      - trade_id      : filter by trade _id
      - strategy_id   : filter by strategy_id
      - trade_date    : YYYY-MM-DD
      - event_type    : entry_taken | sl_hit | target_hit | trail_sl_changed | ...
      - limit         : max records (default 200)
    """
    db = MongoData()
    col = db._db["algo_trade_notification"]

    query: dict = {}
    if trade_id:
        query["trade_id"] = trade_id.strip()
    if strategy_id:
        query["strategy_id"] = strategy_id.strip()
    if trade_date:
        query["trade_date"] = trade_date.strip()
    if event_type:
        query["event_type"] = event_type.strip()

    safe_limit = max(1, min(int(limit), 1000))
    docs = list(
        col.find(query, {"_id": 0})
           .sort("timestamp", 1)
           .limit(safe_limit)
    )
    return {
        "success": True,
        "count": len(docs),
        "notifications": docs,
    }


@router.get("/broker-configurations")
async def list_broker_configurations(broker_type: str = ""):
    normalized_broker_type = str(broker_type or "").strip()
    query = {}
    if normalized_broker_type:
        query["broker_type"] = normalized_broker_type

    db = MongoData()
    try:
        cursor = db._db["broker_configuration"].find(
            query,
            {
                "_id": 1,
                "name": 1,
                "broker_name": 1,
                "display_name": 1,
                "title": 1,
                "broker": 1,
                "broker_type": 1,
                "broker_icon": 1,
                "provider": 1,
                "vendor": 1,
                "login_time": 1,
                "user_id": 1,
                "access_token": 1,
            },
        )
        records = []
        for item in cursor:
            broker_id = str(item.get("_id") or "").strip()
            if not broker_id:
                continue
            broker_doc = dict(item)
            broker_doc["_id"] = broker_id
            has_token = bool(str(item.get("access_token") or "").strip())
            records.append({
                "_id": broker_id,
                "name": _extract_broker_configuration_label(broker_doc, broker_id),
                "broker_type": str(item.get("broker_type") or "").strip(),
                "broker_icon": str(item.get("broker_icon") or "").strip(),
                "user_id": str(item.get("user_id") or "").strip(),
                "login_time": str(item.get("login_time") or "").strip(),
                "is_logged_in": has_token,
            })
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load broker configurations: {exc}") from exc
    finally:
        try:
            db.close()
        except Exception:
            pass

    return {
        "success": True,
        "count": len(records),
        "records": records,
    }


@router.get("/broker-orders")
async def list_broker_orders(
    trade_id:     str = "",
    broker_doc_id:str = "",
    status:       str = "",
    order_side:   str = "",
    limit:        int = 200,
):
    """
    Fetch broker orders from the broker_orders collection.

    Query params (all optional):
      trade_id      – filter by algo_trade _id
      broker_doc_id – filter by broker configuration _id
      status        – OPEN | COMPLETE | REJECTED | CANCELLED | FAILED
      order_side    – entry | exit
      limit         – max records (default 200)
    """
    query: dict = {}
    if trade_id.strip():
        query["trade_id"] = trade_id.strip()
    if broker_doc_id.strip():
        query["broker_doc_id"] = broker_doc_id.strip()
    if status.strip():
        query["status"] = status.strip().upper()
    if order_side.strip():
        query["order_side"] = order_side.strip().lower()

    db = MongoData()
    try:
        cursor = (
            db._db["broker_orders"]
            .find(query)
            .sort("placed_at", -1)
            .limit(max(1, min(int(limit), 1000)))
        )
        records = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            records.append(doc)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load broker orders: {exc}") from exc
    finally:
        try:
            db.close()
        except Exception:
            pass

    return {"success": True, "count": len(records), "records": records}


@router.post("/broker-stoploss-settings/save")
async def save_broker_stoploss_settings(payload: dict):
    def _as_int(value, field_name: str) -> int:
        if value is None or str(value).strip() == "":
            raise HTTPException(status_code=400, detail=f"{field_name} is required")
        try:
            return int(value)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{field_name} must be an integer") from exc

    def _as_nullable_int(value, field_name: str):
        if value is None or str(value).strip() == "":
            return None
        try:
            return int(value)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{field_name} must be an integer or null") from exc

    def _normalize_optional_block(value, field_name: str):
        if value is None:
            return None
        if not isinstance(value, dict):
            raise HTTPException(status_code=400, detail=f"{field_name} must be an object or null")
        if "InstrumentMove" not in value or "StopLossMove" not in value:
            raise HTTPException(
                status_code=400,
                detail=f"{field_name} must include InstrumentMove and StopLossMove",
            )
        return {
            "InstrumentMove": _as_int(value.get("InstrumentMove"), f"{field_name}.InstrumentMove"),
            "StopLossMove": _as_int(value.get("StopLossMove"), f"{field_name}.StopLossMove"),
        }

    document = {
        "broker_type": "Broker.Backtest",
        "creation_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": 1,
        "activation_mode": str(payload.get("activation_mode") or "algo-backtest").strip() or "algo-backtest",
        "user_id": _resolve_app_user_id(payload.get("user_id")),
        "broker": str(payload.get("broker") or "").strip() or None,
        "StopLoss": _as_nullable_int(payload.get("StopLoss"), "StopLoss"),
        "Target": _as_nullable_int(payload.get("Target"), "Target"),
        "OverallTrailSL": _normalize_optional_block(payload.get("OverallTrailSL"), "OverallTrailSL"),
        "LockAndTrail": _normalize_optional_block(payload.get("LockAndTrail"), "LockAndTrail"),
    }

    db = MongoData()
    try:
        collection = db._db["algo_borker_stoploss_settings"]
        update_query = None
        if document["user_id"] and document["broker"] and document["activation_mode"]:
            update_query = {
                "user_id": document["user_id"],
                "broker": document["broker"],
                "activation_mode": document["activation_mode"],
            }

        state_reset: dict = {}          # fields to clear when config changes
        state_reset_reason: list = []   # human-readable list of what was reset

        if update_query:
            # Fetch existing doc — need config fields to detect changes
            existing_doc = collection.find_one(update_query, {
                "_id": 1, "creation_ts": 1,
                "StopLoss": 1, "Target": 1,
                "LockAndTrail": 1, "OverallTrailSL": 1,
            })
            updated_document = dict(document)
            updated_document["updated_ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if existing_doc and existing_doc.get("creation_ts"):
                updated_document["creation_ts"] = existing_doc["creation_ts"]

            # ── Detect config changes and reset affected state ──────────────
            # Any change to LockAndTrail, OverallTrailSL, StopLoss or Target
            # invalidates the persisted lock/trail state. Clear it so the very
            # next tick starts fresh (signatures also cleared so tick processor
            # doesn't need to wait for a date change).

            old_lat   = (existing_doc or {}).get("LockAndTrail")
            old_trail = (existing_doc or {}).get("OverallTrailSL")
            old_sl    = (existing_doc or {}).get("StopLoss")
            old_tgt   = (existing_doc or {}).get("Target")

            new_lat   = document["LockAndTrail"]
            new_trail = document["OverallTrailSL"]
            new_sl    = document["StopLoss"]
            new_tgt   = document["Target"]

            # LockAndTrail state reset
            lock_config_changed = (
                old_lat   != new_lat   or
                old_trail != new_trail or
                old_sl    != new_sl    or
                old_tgt   != new_tgt
            )
            if lock_config_changed and existing_doc:
                state_reset.update({
                    "lock_settings_sig":   "",      # tick processor resets on next tick
                    "lock_activated":      False,
                    "current_lock_floor":  0.0,
                    "lock_peak_mtm":       0.0,
                    "lock_activated_at":   None,
                    "lock_activation_mtm": 0.0,
                })
                if old_lat != new_lat:
                    state_reset_reason.append("LockAndTrail changed")
                if old_trail != new_trail:
                    state_reset_reason.append("OverallTrailSL changed")
                if old_sl != new_sl:
                    state_reset_reason.append("StopLoss changed")
                if old_tgt != new_tgt:
                    state_reset_reason.append("Target changed")

            # OverallTrailSL standalone (Case A) state reset
            # Only applies when LockAndTrail is null
            sl_trail_changed = (old_trail != new_trail or old_sl != new_sl)
            if sl_trail_changed and not new_lat and existing_doc:
                state_reset.update({
                    "sl_settings_sig": "",          # tick processor resets on next tick
                    "sl_peak_mtm":     0.0,
                    "effective_sl":    new_sl,
                })
                if "OverallTrailSL changed" not in state_reset_reason:
                    state_reset_reason.append("OverallTrailSL (standalone) changed")

            updated_document.update(state_reset)

            result = collection.update_one(
                update_query,
                {"$set": updated_document},
                upsert=True,
            )
            inserted_id = result.upserted_id or (existing_doc and existing_doc.get("_id"))
            operation = "created" if result.upserted_id else "updated"
            document = updated_document
        else:
            result = collection.insert_one(document)
            inserted_id = result.inserted_id
            operation = "created"
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to save broker stoploss settings: {exc}") from exc
    finally:
        try:
            db.close()
        except Exception:
            pass

    # ── Emit ALL broker settings for this user after save ────────────────────
    await emit_broker_settings_for_user(
        document.get("user_id") or "",
        document.get("activation_mode") or "",
    )

    return {
        "success": True,
        "id": str(inserted_id) if inserted_id is not None else "",
        "operation": operation,
        "state_reset": state_reset_reason if state_reset_reason else None,
        "settings": {
            "broker_type":    document["broker_type"],
            "creation_ts":    document["creation_ts"],
            "status":         document["status"],
            "activation_mode": document["activation_mode"],
            "user_id":        document["user_id"],
            "broker":         document["broker"],
            "StopLoss":       document["StopLoss"],
            "Target":         document["Target"],
            "OverallTrailSL": document["OverallTrailSL"],
            "LockAndTrail":   document["LockAndTrail"],
        },
        # Current runtime state after save (reflects reset if triggered)
        "lock_state": {
            "lock_activated":      document.get("lock_activated", False),
            "current_lock_floor":  document.get("current_lock_floor", 0.0),
            "lock_peak_mtm":       document.get("lock_peak_mtm", 0.0),
            "lock_activated_at":   document.get("lock_activated_at"),
            "lock_activation_mtm": document.get("lock_activation_mtm", 0.0),
            "effective_sl":        document.get("effective_sl"),
            "sl_peak_mtm":         document.get("sl_peak_mtm", 0.0),
        },
    }


@router.get("/get_broker_stoploss_settings/{user_id}/{broker}/{activation_mode}")
async def get_broker_stoploss_settings(user_id: str, broker: str, activation_mode: str):
    normalized_user_id = str(user_id or "").strip()
    normalized_broker = str(broker or "").strip()
    normalized_activation_mode = str(activation_mode or "").strip() or "algo-backtest"

    if not normalized_user_id:
        raise HTTPException(status_code=400, detail="user_id is required")
    if not normalized_broker:
        raise HTTPException(status_code=400, detail="broker is required")

    db = MongoData()
    try:
        document = db._db["algo_borker_stoploss_settings"].find_one(
            {
                "user_id": normalized_user_id,
                "broker": normalized_broker,
                "activation_mode": normalized_activation_mode,
            },
            {"_id": 0},
        )
        broker_name = ""
        broker_details: dict = {}
        try:
            broker_doc = db._db["broker_configuration"].find_one(
                {"_id": ObjectId(normalized_broker)},
                {"_id": 0, "broker_name": 1, "display_name": 1, "name": 1, "title": 1, "broker": 1, "broker_icon": 1},
            )
            if broker_doc:
                broker_details = {k: str(v or "") for k, v in broker_doc.items()}
                for key in ("broker_name", "display_name", "name", "title", "broker"):
                    val = str(broker_doc.get(key) or "").strip()
                    if val:
                        broker_name = val
                        break
        except Exception:
            pass
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load broker stoploss settings: {exc}") from exc
    finally:
        try:
            db.close()
        except Exception:
            pass

    return {
        "success": True,
        "found": document is not None,
        "settings": document,
        "broker_name": broker_name,
        "broker_details": broker_details,
    }


@router.get("/algo-backtest-simulator")
async def algo_backtest_simulator(
    listen_timestamp: str = Query(..., description="Backtest listen timestamp in YYYY-MM-DDTHH:MM:SS"),
    autoload: bool = Query(True, description="Frontend autoload status for reference"),
    activation_mode: str = Query("algo-backtest", description="Activation mode: algo-backtest, fast-forward, or live"),
):
    normalized_timestamp = str(listen_timestamp or "").strip()
    if not normalized_timestamp:
        raise HTTPException(status_code=400, detail="listen_timestamp is required")
    if len(normalized_timestamp) < 19:
        raise HTTPException(status_code=400, detail="listen_timestamp must be in YYYY-MM-DDTHH:MM:SS format")
    normalized_mode = str(activation_mode or "algo-backtest").strip() or "algo-backtest"
    if normalized_mode not in {"algo-backtest", "fast-forward", "live"}:
        raise HTTPException(status_code=400, detail=f"activation_mode must be algo-backtest, fast-forward, or live")

    db = MongoData()
    try:
        if normalized_mode == "live":
            from features.execution_socket import (
                _append_momentum_pending_to_contracts,
                _build_active_contracts_from_records,
                _extract_running_positions,
                _load_running_trade_records,
            )
            from features.kite_event import broker_live_tick
            from features.live_tick_dispatcher import _run_entries_for_mode

            live_now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            trade_date = live_now[:10]
            listen_hhmm = live_now[11:16]
            _start_monitor_services(trade_date=trade_date)

            _run_entries_for_mode(
                db,
                trade_date,
                "live",
                listen_hhmm,
                live_now,
            )
            live_records = _load_running_trade_records(db, trade_date, activation_mode="live")
            broker_result = broker_live_tick(
                db,
                trade_date,
                live_now,
                dict(ticker_manager.ltp_map),
                activation_mode="live",
                running_trades=live_records,
            )
            live_records = _load_running_trade_records(db, trade_date, activation_mode="live")
            active_contracts = _build_active_contracts_from_records(
                live_records,
                db=db,
                trade_date=trade_date,
                market_cache=None,
                activation_mode="live",
            )
            _append_momentum_pending_to_contracts(active_contracts, db, live_records)
            live_ltp = _build_live_ltp_payload(active_contracts, live_now)
            position_snapshot = _extract_running_positions(
                db,
                trade_date,
                listen_hhmm,
                include_position_snapshots=True,
                running_trades=None,
                market_cache=None,
                activation_mode="live",
            )
            result = {
                "listen_timestamp": live_now,
                "listen_time": live_now[11:19],
                "trade_date": trade_date,
                "records": live_records,
                "entry_snapshots": [],
                "entries_executed": [],
                "actions_taken": list(broker_result.get("actions_taken") or []),
                "ltp": live_ltp,
                "open_positions": list(position_snapshot.get("open_positions") or []),
                "active_leg_tokens": list(position_snapshot.get("active_leg_tokens") or []),
                "count": len(live_records),
                "open_positions_count": len(position_snapshot.get("open_positions") or []),
            }
        else:
            result = run_backtest_simulation_step(
                db,
                normalized_timestamp,
                activation_mode=normalized_mode,
            )
        delivered = await broadcast_backtest_simulation_step(db, result)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"algo-backtest-simulator failed: {exc}") from exc
    finally:
        try:
            db.close()
        except Exception:
            pass

    # Return a slim response — LTP and order details are pushed via sockets.
    # The frontend receives full data through:
    #   update channel  → ltp_update  (LTP for active + momentum-pending legs)
    #   update channel  → update      (open positions snapshot)
    #   execute-orders  → execute_order (records + entries_executed order fills)
    #   executions      → countdown_update (listening state + entry snapshots)
    return {
        "success": True,
        "autoload": bool(autoload),
        "listen_timestamp": result.get("listen_timestamp") or normalized_timestamp,
        "listen_time": result.get("listen_time") or "",
        "trade_date": result.get("trade_date") or "",
        "actions_taken": result.get("actions_taken") or [],
        "entries_count": len(result.get("entries_executed") or []),
        "open_positions_count": result.get("open_positions_count") or 0,
        "active_tokens_count": len(result.get("active_leg_tokens") or []),
        "socket_broadcast": delivered,
    }


app.include_router(router)
app.include_router(socket_router)
app.include_router(mock_kite_socket_router)


# ─── Kite Broker Endpoints ────────────────────────────────────────────────────

# Temporary in-memory store: session_id → broker_doc_id
# Cleared after use (one-time use per login)
_kite_pending: dict = {}


@app.get("/broker/kite/login-url")
async def kite_login_url():
    url = get_login_url()
    return {"login_url": url}


@app.get("/live/kite-callback", response_class=HTMLResponse)
async def kite_live_callback(request: Request):
    """
    Kite console redirect URL: http://localhost:8000/live/kite-callback
    Handles: ?status=success&request_token=xxx&action=login&type=login
    """
    status        = request.query_params.get("status", "").strip()
    request_token = request.query_params.get("request_token", "").strip()
    state         = request.query_params.get("state", "").strip()
    broker_doc_id = _kite_pending.pop(state, "") if state else ""

    if status != "success" or not request_token:
        return HTMLResponse(content=_kite_popup_html(
            success=False,
            message=f"Login failed or no token received (status={status})",
        ))

    try:
        session = generate_session(request_token)
    except Exception as e:
        return HTMLResponse(content=_kite_popup_html(
            success=False,
            message=f"Session error: {e}",
        ))

    if broker_doc_id:
        try:
            _local_db = MongoData()
            save_kite_session(_local_db._db, broker_doc_id, session)
            _local_db.close()
        except Exception:
            pass
    else:
        try:
            _save_market_kite_session(session)
        except Exception:
            pass

    return HTMLResponse(content=_kite_popup_html(
        success=True,
        message="Login successful",
        access_token=session.get("access_token", ""),
        user_id=session.get("user_id", ""),
        user_name=session.get("user_name", ""),
        broker_doc_id=broker_doc_id,
    ))


@app.get("/broker/kite/login")
async def kite_login(broker_doc_id: str = ""):
    """
    Hit this URL directly → auto redirect to Zerodha login page.
    After login, Zerodha redirects back → access_token auto generated & saved.

    Usage:
      http://localhost:8000/broker/kite/login
      http://localhost:8000/broker/kite/login?broker_doc_id=<mongo_id>
    """
    import secrets
    session_id = secrets.token_hex(16)
    _kite_pending[session_id] = broker_doc_id

    login_url = get_login_url()
    redirect_to = f"{login_url}&state={session_id}"
    return RedirectResponse(url=redirect_to)



@app.get("/broker/kite/redirect", response_class=HTMLResponse)
async def kite_redirect(request: Request):
    """
    Zerodha redirects here after login with ?request_token=xxx
    Auto-generates access_token, saves to MongoDB, closes popup,
    and sends result back to parent window via postMessage.
    """
    request_token = request.query_params.get("request_token", "").strip()
    error_msg     = request.query_params.get("error", "").strip()

    # Recover broker_doc_id from pending session (set during /broker/kite/login)
    state         = request.query_params.get("state", "").strip()
    broker_doc_id = _kite_pending.pop(state, "") or request.query_params.get("broker_doc_id", "").strip()

    if error_msg or not request_token:
        return HTMLResponse(content=_kite_popup_html(
            success=False,
            message=error_msg or "No request_token received",
        ))

    try:
        session = generate_session(request_token)
    except Exception as e:
        return HTMLResponse(content=_kite_popup_html(
            success=False,
            message=f"Session error: {e}",
        ))

    if broker_doc_id:
        try:
            _local_db = MongoData()
            save_kite_session(_local_db._db, broker_doc_id, session)
            _local_db.close()
        except Exception:
            pass
    else:
        try:
            _save_market_kite_session(session)
        except Exception:
            pass

    return HTMLResponse(content=_kite_popup_html(
        success=True,
        message="Login successful",
        access_token=session.get("access_token", ""),
        user_id=session.get("user_id", ""),
        user_name=session.get("user_name", ""),
        broker_doc_id=broker_doc_id,
    ))


def _kite_popup_html(
    success: bool,
    message: str,
    access_token: str = "",
    user_id: str = "",
    user_name: str = "",
    broker_doc_id: str = "",
) -> str:
    payload = {
        "type":          "KITE_LOGIN",
        "success":       success,
        "message":       message,
        "access_token":  access_token,
        "user_id":       user_id,
        "user_name":     user_name,
        "broker_doc_id": broker_doc_id,
    }
    import json as _json
    payload_js = _json.dumps(payload)
    status_color = "#22c55e" if success else "#ef4444"
    status_icon  = "✓" if success else "✗"
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Kite Login</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      display: flex; align-items: center; justify-content: center;
      min-height: 100vh; margin: 0;
      background: #0f172a; color: #f1f5f9;
    }}
    .card {{
      text-align: center; padding: 2rem;
      background: #1e293b; border-radius: 12px;
      border: 1px solid #334155;
    }}
    .icon {{ font-size: 3rem; color: {status_color}; }}
    h2 {{ margin: 0.5rem 0; }}
    p {{ color: #94a3b8; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">{status_icon}</div>
    <h2>{"Login Successful" if success else "Login Failed"}</h2>
    <p>{message}</p>
    <p style="font-size:0.8rem">You can close this window after checking the URL.</p>
  </div>
  <script>
    const payload = {payload_js};
    if (window.opener) {{
      window.opener.postMessage(payload, "*");
    }}
  </script>
</body>
</html>"""


@app.get("/broker/kite/access-token/{broker_doc_id}")
async def kite_get_access_token(broker_doc_id: str):
    token = get_stored_access_token(db._db, broker_doc_id)
    if not token:
        raise HTTPException(status_code=404, detail="No access token found")
    return {"access_token": token}


# ─── FlatTrade postback (order status push) ──────────────────────────────────

@app.post("/broker/flattrade/postback")
async def flattrade_postback(request: Request):
    """
    FlatTrade Order Notification postback endpoint.

    Configure in FlatTrade Developer Portal:
      Order Notification URL → https://your-server.com/broker/flattrade/postback

    FlatTrade POSTs order updates here when order status changes (fill / reject / cancel).
    Payload format (NorenApi / FlatTrade):
      Content-Type: application/x-www-form-urlencoded
      Body: jData={"t":"om","norenordno":"...","status":"COMPLETE","avgprc":"100.25",
                   "fillshares":"75","rejreason":"","uid":"...","exch":"NFO","tsym":"..."}

    Status mapping:
      COMPLETE        → order filled
      REJECTED        → broker rejected
      CANCELLED       → user / system cancelled
      OPEN            → order acknowledged (ignore — no DB change)
      TRIGGER_PENDING → SL trigger waiting (ignore)
    """
    from features.live_order_manager import process_broker_order_update

    # ── Parse payload — FlatTrade sends form-encoded jData=<json> ─────────────
    data: dict = {}
    try:
        body_bytes = await request.body()
        body_str   = body_bytes.decode("utf-8", errors="replace")

        if body_str.startswith("jData="):
            import urllib.parse
            parsed = urllib.parse.parse_qs(body_str)
            jdata_str = (parsed.get("jData") or ["{}"])[0]
            data = json.loads(jdata_str)
        else:
            # Try plain JSON body
            data = json.loads(body_str) if body_str.strip() else {}
    except Exception as exc:
        log.warning("[FLATTRADE POSTBACK] parse error: %s", exc)
        return {"stat": "Ok"}   # always 200 so FlatTrade doesn't retry

    order_id   = str(data.get("norenordno") or "").strip()
    status_raw = str(data.get("status")     or "").upper()
    fill_price = float(data.get("avgprc")   or data.get("flprc") or 0)
    fill_qty   = int(data.get("fillshares") or 0)
    rej_reason = str(data.get("rejreason")  or "").lower()

    log.info(
        "[FLATTRADE POSTBACK] order_id=%s status=%s fill_price=%s fill_qty=%s",
        order_id, status_raw, fill_price, fill_qty,
    )

    if not order_id:
        return {"stat": "Ok"}

    # Map FlatTrade status → our internal status
    _status_map = {
        "COMPLETE":        "COMPLETE",
        "REJECTED":        "REJECTED",
        "CANCELLED":       "CANCELLED",
        "OPEN":            "OPEN",
        "TRIGGER_PENDING": "OPEN",
    }
    status = _status_map.get(status_raw, status_raw)

    # Only act on terminal / fill statuses
    if status not in ("COMPLETE", "REJECTED", "CANCELLED"):
        return {"stat": "Ok"}

    local_db = MongoData()
    try:
        process_broker_order_update(
            local_db,
            order_id  = order_id,
            status    = status,
            fill_price= fill_price,
            fill_qty  = fill_qty,
            rejection_reason = rej_reason,
            source    = "postback",
        )
    except Exception as exc:
        log.error("[FLATTRADE POSTBACK] processing error order_id=%s: %s", order_id, exc)
    finally:
        try:
            local_db.close()
        except Exception:
            pass

    return {"stat": "Ok"}


# ─── FlatTrade broker login ───────────────────────────────────────────────────

_flattrade_pending: dict = {}


@app.get("/broker/flattrade/login")
async def flattrade_login(broker_doc_id: str = ""):
    """
    Redirect to FlatTrade login page.

    Usage:
      http://localhost:8000/broker/flattrade/login
      http://localhost:8000/broker/flattrade/login?broker_doc_id=<mongo_id>
    """
    import secrets
    from urllib.parse import quote
    from features.flattrade_broker import get_login_url as ft_login_url
    session_id = secrets.token_hex(16)
    state = session_id
    if broker_doc_id:
        state = f"{session_id}:{quote(broker_doc_id, safe='')}"
    _flattrade_pending[state] = broker_doc_id
    login_url = ft_login_url(state=state)
    log.info("FlatTrade login started broker_doc_id=%s state=%s", broker_doc_id or "-", state)
    response = RedirectResponse(url=login_url)
    if broker_doc_id:
        response.set_cookie(
            key="flattrade_broker_doc_id",
            value=broker_doc_id,
            max_age=600,
            httponly=True,
            samesite="lax",
        )
    return response


@app.get("/broker/flattrade/redirect", response_class=HTMLResponse)
async def flattrade_redirect(request: Request):
    """
    FlatTrade redirects here after login with ?code=<request_code>&state=<session_id>.
    Exchanges the code for a jKey session token and saves it to broker_configuration.
    """
    from features.flattrade_broker import _session_token, _session_user_id
    from features.flattrade_broker import generate_session as ft_generate_session
    from features.flattrade_broker import save_flattrade_session

    request_code  = request.query_params.get("code", "").strip()
    error_msg     = request.query_params.get("error", "").strip()
    state         = request.query_params.get("state", "").strip()
    broker_doc_id = _flattrade_pending.pop(state, "") or request.query_params.get("broker_doc_id", "").strip()
    if not broker_doc_id and ":" in state:
        from urllib.parse import unquote
        broker_doc_id = unquote(state.rsplit(":", 1)[-1]).strip()
    if not broker_doc_id:
        broker_doc_id = request.cookies.get("flattrade_broker_doc_id", "").strip()

    if error_msg or not request_code:
        log.error(
            "FlatTrade redirect failed before token exchange state=%s broker_doc_id=%s error=%s has_code=%s",
            state or "-",
            broker_doc_id or "-",
            error_msg or "-",
            bool(request_code),
        )
        response = HTMLResponse(content=_broker_popup_html(
            broker="FlatTrade",
            success=False,
            message=error_msg or "No request code received",
        ))
        response.delete_cookie("flattrade_broker_doc_id")
        return response

    try:
        session = ft_generate_session(request_code)
    except Exception as exc:
        log.exception(
            "FlatTrade token exchange failed state=%s broker_doc_id=%s",
            state or "-",
            broker_doc_id or "-",
        )
        response = HTMLResponse(content=_broker_popup_html(
            broker="FlatTrade",
            success=False,
            message=f"Session error: {exc}",
        ))
        response.delete_cookie("flattrade_broker_doc_id")
        return response

    if broker_doc_id:
        try:
            _local_db = MongoData()
            save_flattrade_session(_local_db._db, broker_doc_id, session)
            _local_db.close()
        except Exception as exc:
            log.exception("FlatTrade session DB save failed broker_doc_id=%s", broker_doc_id)
            response = HTMLResponse(content=_broker_popup_html(
                broker="FlatTrade",
                success=False,
                message=f"Token generated but DB save failed: {exc}",
            ))
            response.delete_cookie("flattrade_broker_doc_id")
            return response
    else:
        log.warning(
            "FlatTrade login succeeded but broker_doc_id was empty; token not saved state=%s query=%s",
            state or "-",
            dict(request.query_params),
        )

    response = HTMLResponse(content=_broker_popup_html(
        broker="FlatTrade",
        success=True,
        message="Login successful",
        access_token=_session_token(session),
        user_id=_session_user_id(session),
        user_name=_session_user_id(session),
        broker_doc_id=broker_doc_id,
    ))
    response.delete_cookie("flattrade_broker_doc_id")
    return response


def _broker_popup_html(
    broker: str,
    success: bool,
    message: str,
    access_token: str = "",
    user_id: str = "",
    user_name: str = "",
    broker_doc_id: str = "",
) -> str:
    import json as _json
    payload = {
        "type":          f"{broker.upper()}_LOGIN",
        "success":       success,
        "message":       message,
        "access_token":  access_token,
        "user_id":       user_id,
        "user_name":     user_name,
        "broker_doc_id": broker_doc_id,
    }
    payload_js   = _json.dumps(payload)
    status_color = "#22c55e" if success else "#ef4444"
    status_icon  = "✓" if success else "✗"
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{broker} Login</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      display: flex; align-items: center; justify-content: center;
      min-height: 100vh; margin: 0;
      background: #0f172a; color: #f1f5f9;
    }}
    .card {{
      text-align: center; padding: 2rem;
      background: #1e293b; border-radius: 12px;
      border: 1px solid #334155;
    }}
    .icon {{ font-size: 3rem; color: {status_color}; }}
    h2 {{ margin: 0.5rem 0; }}
    p {{ color: #94a3b8; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">{status_icon}</div>
    <h2>{"Login Successful" if success else "Login Failed"}</h2>
    <p>{message}</p>
    <p style="font-size:0.8rem">This window will close automatically...</p>
  </div>
  <script>
    const payload = {payload_js};
    if (window.opener) {{
      window.opener.postMessage(payload, "*");
    }}
    setTimeout(() => window.close(), 1500);
  </script>
</body>
</html>"""


# ─── Live Market Data (KiteTicker) ───────────────────────────────────────────

_LIVE_CONTROL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Live Trade Control</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: #0f172a; color: #f1f5f9;
      min-height: 100vh; display: flex;
      align-items: center; justify-content: center;
    }
    .card {
      background: #1e293b; border: 1px solid #334155;
      border-radius: 16px; padding: 2.5rem 3rem;
      width: 420px; text-align: center;
    }
    .title {
      font-size: 1.25rem; font-weight: 600; color: #94a3b8;
      margin-bottom: 2rem; letter-spacing: 0.05em; text-transform: uppercase;
    }
    .status-row {
      display: flex; align-items: center; justify-content: center;
      gap: 0.6rem; margin-bottom: 2rem;
    }
    .dot {
      width: 10px; height: 10px; border-radius: 50%;
      background: #475569; transition: background 0.3s;
    }
    .dot.running    { background: #22c55e; box-shadow: 0 0 8px #22c55e; animation: pulse 1.5s infinite; }
    .dot.stopped    { background: #ef4444; }
    .dot.connecting { background: #f59e0b; animation: pulse 0.8s infinite; }
    .dot.error      { background: #ef4444; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
    .status-text { font-size: 1rem; font-weight: 500; color: #cbd5e1; text-transform: capitalize; }
    .btn {
      width: 100%; padding: 1rem; border: none; border-radius: 10px;
      font-size: 1.1rem; font-weight: 600; cursor: pointer;
      transition: opacity 0.2s, transform 0.1s; letter-spacing: 0.03em;
    }
    .btn:active { transform: scale(0.98); }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }
    .btn-start { background: #22c55e; color: #fff; }
    .btn-start:hover:not(:disabled) { opacity: 0.9; }
    .btn-stop  { background: #ef4444; color: #fff; }
    .btn-stop:hover:not(:disabled)  { opacity: 0.9; }
    .stats {
      display: grid; grid-template-columns: 1fr 1fr;
      gap: 0.75rem; margin-top: 1.75rem;
    }
    .stat-box {
      background: #0f172a; border: 1px solid #1e293b;
      border-radius: 8px; padding: 0.75rem;
    }
    .stat-label { font-size: 0.7rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.3rem; }
    .stat-value { font-size: 1.1rem; font-weight: 700; color: #e2e8f0; }
    .spot-section { margin-top: 1.5rem; text-align: left; }
    .spot-title { font-size: 0.7rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem; }
    .spot-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; }
    .spot-item {
      background: #0f172a; border: 1px solid #1e293b;
      border-radius: 8px; padding: 0.5rem 0.75rem;
      display: flex; justify-content: space-between; align-items: center;
    }
    .spot-name  { font-size: 0.75rem; color: #94a3b8; font-weight: 600; }
    .spot-price { font-size: 0.85rem; color: #22c55e; font-weight: 700; }
    .spot-price.na { color: #475569; }
    .error-msg {
      margin-top: 1rem; font-size: 0.8rem; color: #f87171;
      background: #1a0a0a; border-radius: 6px; padding: 0.5rem 0.75rem; display: none;
    }
    .started-at { margin-top: 1rem; font-size: 0.72rem; color: #475569; }
  </style>
</head>
<body>
<div class="card">
  <div class="title">Live Trade Control</div>
  <div class="status-row">
    <div class="dot" id="dot"></div>
    <span class="status-text" id="statusText">Loading...</span>
  </div>
  <button class="btn" id="actionBtn" disabled onclick="handleAction()">...</button>
  <div class="stats">
    <div class="stat-box">
      <div class="stat-label">Ticks Received</div>
      <div class="stat-value" id="tickCount">—</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">LTP Tokens</div>
      <div class="stat-value" id="ltpCount">—</div>
    </div>
  </div>
  <div class="spot-section">
    <div class="spot-title">Spot Prices</div>
    <div class="spot-grid">
      <div class="spot-item"><span class="spot-name">NIFTY</span><span class="spot-price na" id="spot-NIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">BANKNIFTY</span><span class="spot-price na" id="spot-BANKNIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">FINNIFTY</span><span class="spot-price na" id="spot-FINNIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">SENSEX</span><span class="spot-price na" id="spot-SENSEX">—</span></div>
    </div>
  </div>
  <div class="error-msg" id="errorMsg"></div>
  <div class="started-at" id="startedAt"></div>
</div>
<script>
  const API = '';

  async function fetchStatus() {
    try {
      const res  = await fetch(API + '/live/status');
      const data = await res.json();
      renderStatus(data);
    } catch(e) {
      renderStatus({ status: 'error', error: 'Cannot reach server' });
    }
  }

  function renderStatus(data) {
    const status = data.status || 'stopped';
    document.getElementById('dot').className       = 'dot ' + status;
    document.getElementById('statusText').textContent = status;

    const btn = document.getElementById('actionBtn');
    btn.disabled = false;
    if (status === 'running') {
      btn.textContent = 'Stop Live Trading';
      btn.className   = 'btn btn-stop';
    } else if (status === 'connecting') {
      btn.textContent = 'Connecting...';
      btn.className   = 'btn btn-start';
      btn.disabled    = true;
    } else {
      btn.textContent = 'Start Live Trading';
      btn.className   = 'btn btn-start';
    }

    document.getElementById('tickCount').textContent =
      data.tick_count !== undefined ? data.tick_count.toLocaleString() : '—';
    document.getElementById('ltpCount').textContent =
      data.ltp_count !== undefined ? data.ltp_count.toLocaleString() : '—';

    const spotMap = data.spot_map || {};
    ['NIFTY','BANKNIFTY','FINNIFTY','SENSEX'].forEach(sym => {
      const el = document.getElementById('spot-' + sym);
      const v  = spotMap[sym];
      if (!el) return;
      if (v) {
        el.textContent = '\\u20B9' + Number(v).toLocaleString('en-IN', { minimumFractionDigits: 2 });
        el.className = 'spot-price';
      } else {
        el.textContent = '—';
        el.className = 'spot-price na';
      }
    });

    const errEl = document.getElementById('errorMsg');
    if (data.error) { errEl.textContent = data.error; errEl.style.display = 'block'; }
    else            { errEl.style.display = 'none'; }

    const startEl = document.getElementById('startedAt');
    startEl.textContent = data.started_at
      ? 'Started: ' + data.started_at.replace('T',' ').slice(0,19)
      : '';
  }

  async function handleAction() {
    const btn    = document.getElementById('actionBtn');
    const status = document.getElementById('statusText').textContent;
    btn.disabled    = true;
    btn.textContent = 'Please wait...';
    try {
      const url = status === 'running' ? '/live/stop' : '/live/start';
      await fetch(API + url + '?ui=1');
    } catch(e) { console.error(e); }
    setTimeout(fetchStatus, 800);
    setTimeout(fetchStatus, 2000);
    setTimeout(fetchStatus, 4000);
  }

  fetchStatus();
  setInterval(fetchStatus, 3000);
</script>
</body>
</html>"""


def _start_ticker_bg():
    """Run in background thread — loads tokens from DB and starts KiteTicker."""
    _db = MongoData()
    try:
        print(
            f'[MONITOR TICKER START] '
            f'current_status={ticker_manager.status} '
            f'tick_count={int(ticker_manager.tick_count or 0)}'
        )
        if ticker_manager.status == "running":
            ticker_manager.restart(_db._db)
        else:
            ticker_manager.start(_db._db)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).error("ticker start error: %s", exc)
    finally:
        try:
            _db.close()
        except Exception:
            pass


def _build_monitor_control_html() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Live + Fast-Forward Monitor</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background:
        radial-gradient(circle at top, rgba(34, 197, 94, 0.14), transparent 34%),
        linear-gradient(160deg, #07111f 0%, #0f172a 55%, #111827 100%);
      color: #e5eefb;
      font-family: "Segoe UI", Tahoma, sans-serif;
    }}
    .card {{
      width: min(560px, calc(100vw - 32px));
      background: rgba(10, 19, 34, 0.94);
      border: 1px solid rgba(148, 163, 184, 0.18);
      border-radius: 24px;
      padding: 28px;
      box-shadow: 0 22px 70px rgba(0, 0, 0, 0.35);
    }}
    .eyebrow {{
      color: #7dd3fc;
      font-size: 12px;
      letter-spacing: 0.18em;
      text-transform: uppercase;
      margin-bottom: 10px;
    }}
    .title {{
      font-size: 30px;
      font-weight: 700;
      margin-bottom: 10px;
    }}
    .subtitle {{
      color: #94a3b8;
      font-size: 14px;
      line-height: 1.6;
      margin-bottom: 20px;
    }}
    .hero {{
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 16px;
      align-items: center;
      background: rgba(15, 23, 42, 0.95);
      border: 1px solid rgba(125, 211, 252, 0.12);
      border-radius: 18px;
      padding: 18px 20px;
      margin-bottom: 18px;
    }}
    .status-row {{
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 18px;
      font-weight: 600;
    }}
    .dot {{
      width: 12px;
      height: 12px;
      border-radius: 999px;
      background: #64748b;
      box-shadow: 0 0 0 transparent;
    }}
    .dot.running {{ background: #22c55e; box-shadow: 0 0 12px rgba(34, 197, 94, 0.8); }}
    .dot.connecting {{ background: #f59e0b; box-shadow: 0 0 12px rgba(245, 158, 11, 0.8); }}
    .dot.stopped {{ background: #ef4444; box-shadow: 0 0 12px rgba(239, 68, 68, 0.45); }}
    .clock-box {{
      text-align: right;
      font-variant-numeric: tabular-nums;
    }}
    .clock-label {{
      color: #64748b;
      font-size: 11px;
      letter-spacing: 0.14em;
      text-transform: uppercase;
    }}
    .clock-value {{
      margin-top: 6px;
      font-size: 18px;
      font-weight: 700;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }}
    .stat {{
      background: rgba(15, 23, 42, 0.85);
      border: 1px solid rgba(148, 163, 184, 0.12);
      border-radius: 16px;
      padding: 14px 16px;
    }}
    .stat-label {{
      font-size: 11px;
      color: #64748b;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      margin-bottom: 8px;
    }}
    .stat-value {{
      font-size: 19px;
      font-weight: 700;
      line-height: 1.35;
      word-break: break-word;
    }}
    .actions {{
      display: flex;
      gap: 12px;
      margin-bottom: 18px;
    }}
    .btn {{
      flex: 1;
      border: none;
      border-radius: 14px;
      padding: 14px 16px;
      font-size: 15px;
      font-weight: 700;
      cursor: pointer;
      transition: transform 0.12s ease, opacity 0.2s ease;
    }}
    .btn:active {{ transform: scale(0.985); }}
    .btn-primary {{ background: linear-gradient(135deg, #22c55e, #16a34a); color: #04110a; }}
    .btn-danger {{ background: linear-gradient(135deg, #f97316, #ef4444); color: #fff7ed; }}
    .btn-secondary {{ background: #1e293b; color: #cbd5e1; border: 1px solid rgba(148, 163, 184, 0.18); }}
    .btn:disabled {{ opacity: 0.55; cursor: not-allowed; }}
    .panel {{
      background: rgba(15, 23, 42, 0.88);
      border: 1px solid rgba(148, 163, 184, 0.12);
      border-radius: 18px;
      padding: 16px;
    }}
    .panel-title {{
      color: #cbd5e1;
      font-size: 13px;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      margin-bottom: 10px;
    }}
    .strategies {{
      display: flex;
      flex-direction: column;
      gap: 10px;
      max-height: 220px;
      overflow: auto;
    }}
    .strategy-item {{
      border-radius: 12px;
      padding: 12px 14px;
      background: rgba(8, 15, 28, 0.9);
      border: 1px solid rgba(148, 163, 184, 0.1);
    }}
    .strategy-name {{
      font-size: 14px;
      font-weight: 700;
      margin-bottom: 4px;
    }}
    .strategy-meta {{
      color: #94a3b8;
      font-size: 12px;
      line-height: 1.5;
    }}
    .empty {{
      color: #94a3b8;
      font-size: 13px;
      line-height: 1.6;
      padding: 10px 4px 2px;
    }}
  </style>
</head>
<body>
  <div class="card">
    <div class="eyebrow">Auto Monitor</div>
    <div class="title">Live + Fast-Forward Monitor</div>
    <div class="subtitle">
      Single control page for both <b>live</b> and <b>fast-forward</b>. The backend supervisor starts automatically,
      refreshes active strategies every second, and keeps the live execution path highest priority.
    </div>

    <div class="hero">
      <div class="status-row">
        <span class="dot stopped" id="statusDot"></span>
        <span id="statusText">Loading...</span>
      </div>
      <div class="clock-box">
        <div class="clock-label">Server Time</div>
        <div class="clock-value" id="serverTime">--</div>
      </div>
    </div>

    <div class="grid">
      <div class="stat">
        <div class="stat-label">Trade Date</div>
        <div class="stat-value" id="tradeDateValue">--</div>
      </div>
      <div class="stat">
        <div class="stat-label">Live Count</div>
        <div class="stat-value" id="liveCountValue">0</div>
      </div>
      <div class="stat">
        <div class="stat-label">Started At</div>
        <div class="stat-value" id="startedAtValue">--</div>
      </div>
      <div class="stat">
        <div class="stat-label">Fast-Forward Count</div>
        <div class="stat-value" id="ffCountValue">0</div>
      </div>
      <div class="stat">
        <div class="stat-label">Last Tick</div>
        <div class="stat-value" id="lastTickValue">--</div>
      </div>
      <div class="stat">
        <div class="stat-label">Ticker Ticks</div>
        <div class="stat-value" id="tickCountValue">0</div>
      </div>
    </div>

    <div class="actions">
      <button class="btn btn-primary" id="toggleBtn" onclick="toggleMonitor()" disabled>Loading...</button>
      <button class="btn btn-secondary" onclick="refreshStatus()">Refresh</button>
    </div>

    <div class="panel">
      <div class="panel-title">Live Strategies</div>
      <div class="strategies" id="strategiesBox">
        <div class="empty">Checking active live strategies...</div>
      </div>
    </div>

    <div class="panel" style="margin-top: 14px;">
      <div class="panel-title">Fast-Forward Strategies</div>
      <div class="strategies" id="ffStrategiesBox">
        <div class="empty">Checking active fast-forward strategies...</div>
      </div>
    </div>
  </div>

  <script>
    function formatDateTime(value) {{
      if (!value) return '--';
      return String(value).replace('T', ' ').slice(0, 19);
    }}

    function escapeHtml(value) {{
      return String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }}

    async function startMonitorSilently() {{
      try {{
        await fetch('/monitor/start');
      }} catch (err) {{
        console.error(err);
      }}
    }}

    async function refreshStatus() {{
      try {{
        const res = await fetch('/monitor/status');
        const data = await res.json();
        renderStatus(data);
      }} catch (err) {{
        console.error(err);
      }}
    }}

    function renderStatus(data) {{
      const running = !!data.running;
      const status = data.monitor_status || (running ? 'running' : 'stopped');
      const button = document.getElementById('toggleBtn');
      const statusDot = document.getElementById('statusDot');
      const statusText = document.getElementById('statusText');
      const serverTime = document.getElementById('serverTime');
      const tradeDate = document.getElementById('tradeDateValue');
      const startedAt = document.getElementById('startedAtValue');
      const lastTick = document.getElementById('lastTickValue');
      const liveCountValue = document.getElementById('liveCountValue');
      const ffCountValue = document.getElementById('ffCountValue');
      const tickCountValue = document.getElementById('tickCountValue');
      const strategiesBox = document.getElementById('strategiesBox');
      const ffStrategiesBox = document.getElementById('ffStrategiesBox');

      statusDot.className = 'dot ' + (running ? 'running' : 'stopped');
      statusText.textContent = running ? 'Listening' : 'Stopped';
      serverTime.textContent = formatDateTime(data.server_time);
      tradeDate.textContent = data.trade_date || '--';
      startedAt.textContent = formatDateTime(data.started_at);
      lastTick.textContent = formatDateTime(data.last_tick_at);
      liveCountValue.textContent = String(((data.counts || {}).live) || 0);
      ffCountValue.textContent = String(((data.counts || {})['fast-forward']) || 0);
      tickCountValue.textContent = String(data.tick_count || 0);

      button.disabled = false;
      button.textContent = running ? 'Stop Listening' : 'Start Listening';
      button.className = 'btn ' + (running ? 'btn-danger' : 'btn-primary');
      button.dataset.running = running ? '1' : '0';

      const recordsByMode = data.records_by_mode || {{}};
      const liveRecords = Array.isArray(recordsByMode.live) ? recordsByMode.live : [];
      const ffRecords = Array.isArray(recordsByMode['fast-forward']) ? recordsByMode['fast-forward'] : [];

      function renderRecords(records, emptyText) {{
        if (!records.length) {{
          return '<div class="empty">' + emptyText + '</div>';
        }}
        return records.map(function(record) {{
          return (
            '<div class="strategy-item">' +
              '<div class="strategy-name">' + escapeHtml(record.name || '-') + '</div>' +
              '<div class="strategy-meta">' +
                'Group: ' + escapeHtml(record.group_name || '-') + '<br>' +
                'Ticker: ' + escapeHtml(record.ticker || '-') + '<br>' +
                'Mode: ' + escapeHtml(record.activation_mode || '-') + '<br>' +
                'Entry: ' + escapeHtml(record.entry_time || '-') + ' | Exit: ' + escapeHtml(record.exit_time || '-') + '<br>' +
                'Open Legs: ' + escapeHtml(record.open_legs || 0) + '/' + escapeHtml(record.total_legs || 0) +
              '</div>' +
            '</div>'
          );
        }}).join('');
      }}

      strategiesBox.innerHTML = renderRecords(
        liveRecords,
        'No active live strategies right now. Supervisor still keeps checking every second.'
      );
      ffStrategiesBox.innerHTML = renderRecords(
        ffRecords,
        'No active fast-forward strategies right now. Supervisor still keeps checking every second.'
      );
    }}

    async function toggleMonitor() {{
      const button = document.getElementById('toggleBtn');
      const running = button.dataset.running === '1';
      button.disabled = true;
      button.textContent = 'Please wait...';
      try {{
        const path = running ? '/monitor/stop' : '/monitor/start';
        await fetch(path);
      }} catch (err) {{
        console.error(err);
      }}
      setTimeout(refreshStatus, 400);
      setTimeout(refreshStatus, 1200);
    }}

    startMonitorSilently().then(function() {{
      refreshStatus();
      setInterval(refreshStatus, 1000);
    }});
  </script>
</body>
</html>"""


def _start_monitor_services(trade_date: str = '') -> dict:
    import threading

    normalized_trade_date = str(trade_date or '').strip() or datetime.now().strftime('%Y-%m-%d')
    print(
        f'[MONITOR START REQUEST] '
        f'trade_date={normalized_trade_date} '
        f'ticker_status={ticker_manager.status} '
        f'tick_count={int(ticker_manager.tick_count or 0)}'
    )
    if ticker_manager.status not in ('running', 'connecting'):
        threading.Thread(target=_start_ticker_bg, daemon=True).start()
    live_fast_monitor_supervisor.start(trade_date=normalized_trade_date)
    return {
        'ok': True,
        'message': 'Global monitor started',
        'trade_date': live_fast_monitor_supervisor.trade_date,
    }


def _build_monitor_status_payload() -> dict:
    supervisor_status = live_fast_monitor_supervisor.get_status()
    ticker_status = ticker_manager.get_status()
    return {
        'server_time': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        'running': bool(supervisor_status.get('running')),
        'monitor_status': 'running' if bool(supervisor_status.get('running')) else 'stopped',
        'trade_date': str(supervisor_status.get('trade_date') or datetime.now().strftime('%Y-%m-%d')),
        'started_at': str(supervisor_status.get('started_at') or ''),
        'last_tick_at': str(supervisor_status.get('last_tick_at') or ''),
        'last_refresh_at': str(supervisor_status.get('last_refresh_at') or ''),
        'counts': supervisor_status.get('counts') or {},
        'records_by_mode': supervisor_status.get('records_by_mode') or {},
        'ticker_status': str(ticker_status.get('status') or ''),
        'tick_count': ticker_status.get('tick_count'),
        'ltp_count': ticker_status.get('ltp_count'),
        'spot_map': ticker_status.get('spot_map') or {},
        'ticker_error': str(ticker_status.get('error') or ''),
    }


def _build_live_ltp_payload(active_contracts: list[dict], now_ts: str) -> list[dict]:
    payload: list[dict] = []
    for contract in (active_contracts or []):
        token = str(contract.get("token") or "").strip()
        option_type = str(contract.get("option") or "").strip()
        if option_type == "SPOT":
            underlying = str(contract.get("underlying") or "").strip().upper()
            spot_price = float(ticker_manager.get_spot(underlying) or 0.0)
            if spot_price <= 0:
                continue
            payload.append({
                "token": token,
                "timestamp": now_ts,
                "ltp": spot_price,
                "bb_qty": 0,
                "bb_price": 0.0,
                "ba_qty": 0,
                "ba_price": 0.0,
                "vol_in_day": 0,
                "underlying": underlying,
                "option_type": "SPOT",
            })
            continue

        live_ltp = float(ticker_manager.get_ltp(token) or 0.0)
        if live_ltp <= 0:
            continue
        payload.append({
            "token": token,
            "timestamp": now_ts,
            "ltp": live_ltp,
            "bb_qty": 0,
            "bb_price": 0.0,
            "ba_qty": 0,
            "ba_price": 0.0,
            "vol_in_day": 0,
            "expiry": str(contract.get("expiry_date") or ""),
            "strike": contract.get("strike"),
            "option_type": option_type,
        })
    return payload


def _save_market_kite_session(session: dict) -> None:
    update_fields = {
        "access_token": session.get("access_token"),
        "login_time": datetime.now().isoformat(),
        "user_id": session.get("user_id"),
        "user_name": session.get("user_name"),
    }
    local_db = MongoData()
    try:
        local_db._db["kite_market_config"].update_one(
            {"enabled": True},
            {"$set": update_fields},
            upsert=True,
        )
    finally:
        local_db.close()


def _clear_market_kite_session() -> None:
    local_db = MongoData()
    try:
        local_db._db["kite_market_config"].update_one(
            {"enabled": True},
            {"$set": {"access_token": "", "login_time": datetime.now().isoformat()}},
            upsert=True,
        )
    finally:
        local_db.close()


def _get_kite_market_session_status() -> tuple[bool, str]:
    local_db = MongoData()
    try:
        cfg = local_db._db["kite_market_config"].find_one(
            {"enabled": True},
            {"access_token": 1, "api_key": 1, "login_time": 1},
        ) or {}
        api_key = str(cfg.get("api_key") or "").strip()
        access_token = str(cfg.get("access_token") or "").strip()
        login_time = str(cfg.get("login_time") or "").strip()
        if not api_key:
            return False, (
                "Kite market config missing api_key"
                + (f" (login_time: {login_time})" if login_time else "")
            )
        if not access_token:
            return False, "Access token not found"
    finally:
        local_db.close()

    try:
        kite = get_kite_instance(access_token)
        kite.profile()
        return True, "Access token valid"
    except Exception as exc:
        try:
            _clear_market_kite_session()
        except Exception:
            pass
        return False, f"Access token invalid or expired: {exc}"


def _has_ready_kite_market_session() -> bool:
    is_ready, _ = _get_kite_market_session_status()
    return is_ready


def _build_monitor_kite_login_page(trade_date: str = '', reason: str = '') -> str:
    normalized_trade_date = str(trade_date or '').strip()
    retry_url = "/monitor/start"
    if normalized_trade_date:
        retry_url += f"?trade_date={normalized_trade_date}"
    reason_text = str(reason or "No Kite session found").strip()
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Kite Login Required</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background:
        radial-gradient(circle at top, rgba(59, 130, 246, 0.16), transparent 34%),
        linear-gradient(155deg, #07111f 0%, #0f172a 58%, #111827 100%);
      color: #e2e8f0;
      font-family: "Segoe UI", Tahoma, sans-serif;
    }}
    .card {{
      width: min(520px, calc(100vw - 32px));
      background: rgba(9, 17, 31, 0.95);
      border: 1px solid rgba(148, 163, 184, 0.16);
      border-radius: 28px;
      padding: 34px 28px;
      box-shadow: 0 28px 80px rgba(0, 0, 0, 0.38);
      text-align: center;
    }}
    .badge {{
      display: inline-block;
      padding: 9px 16px;
      border-radius: 999px;
      background: rgba(15, 23, 42, 0.92);
      border: 1px solid rgba(148, 163, 184, 0.14);
      color: #7dd3fc;
      letter-spacing: 0.14em;
      font-size: 12px;
      text-transform: uppercase;
    }}
    h1 {{
      margin: 18px 0 12px;
      font-size: 32px;
      line-height: 1.15;
      color: #f8fafc;
    }}
    p {{
      margin: 0 auto;
      max-width: 420px;
      color: #94a3b8;
      line-height: 1.7;
      font-size: 15px;
    }}
    .actions {{
      margin-top: 28px;
      display: flex;
      justify-content: center;
      gap: 12px;
      flex-wrap: wrap;
    }}
    .btn {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 220px;
      padding: 16px 22px;
      border-radius: 18px;
      text-decoration: none;
      border: none;
      cursor: pointer;
      font-size: 17px;
      font-weight: 700;
    }}
    .btn.primary {{
      background: linear-gradient(135deg, #38bdf8, #2563eb);
      color: #eff6ff;
      box-shadow: 0 16px 32px rgba(37, 99, 235, 0.24);
    }}
    .btn.secondary {{
      background: #1e293b;
      color: #cbd5e1;
      border: 1px solid rgba(148, 163, 184, 0.18);
    }}
    .hint {{
      margin-top: 18px;
      color: #7dd3fc;
      font-size: 13px;
    }}
  </style>
</head>
<body>
  <div class="card">
    <div class="badge">Kite Required</div>
    <h1>Connect Kite API First</h1>
    <p>
      Monitor start needs a valid Kite access token. Login popup will open, save the access token,
      and then this page will automatically start the server listener.
    </p>
    <p style="margin-top:14px;color:#7dd3fc;font-size:13px;">Reason: {reason_text}</p>
    <div class="actions">
      <button class="btn primary" onclick="openKiteLogin()">Connect Kite API</button>
      <a class="btn secondary" href="/monitor/stop">Open Stop Page</a>
    </div>
    <div class="hint" id="hintText">Waiting for Kite login...</div>
  </div>

  <script>
    let kitePopup = null;

    function openKiteLogin() {{
      kitePopup = window.open('/broker/kite/login', 'kiteLogin', 'width=540,height=720');
      if (!kitePopup) {{
        document.getElementById('hintText').textContent = 'Popup blocked. Please allow popups and click again.';
        return;
      }}
      document.getElementById('hintText').textContent = 'Kite login popup opened. Complete login to continue.';
    }}

    window.addEventListener('message', function(event) {{
      const data = event.data || {{}};
      if (data.type !== 'KITE_LOGIN') return;
      if (!data.success) {{
        document.getElementById('hintText').textContent = data.message || 'Kite login failed.';
        return;
      }}
      document.getElementById('hintText').textContent = 'Kite login successful. Starting monitor...';
      window.location.href = {json.dumps(retry_url)};
    }});

    setTimeout(openKiteLogin, 250);
  </script>
</body>
</html>"""


def _build_monitor_action_page(*, running: bool, trade_date: str = '') -> str:
    title = 'Monitor Running' if running else 'Monitor Stopped'
    status_text = 'Listening is active' if running else 'Listening is stopped'
    button_label = 'Stop Listening' if running else 'Start Listening'
    button_href = '/monitor/stop' if running else '/monitor/start'
    button_class = 'danger' if running else 'success'
    trade_date_text = str(trade_date or '').strip() or datetime.now().strftime('%Y-%m-%d')
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title}</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background:
        radial-gradient(circle at top, rgba(56, 189, 248, 0.18), transparent 32%),
        linear-gradient(155deg, #06101d 0%, #0f172a 58%, #111827 100%);
      color: #e2e8f0;
      font-family: "Segoe UI", Tahoma, sans-serif;
    }}
    .shell {{
      width: min(540px, calc(100vw - 32px));
      padding: 18px;
    }}
    .card {{
      background: rgba(9, 17, 31, 0.95);
      border: 1px solid rgba(148, 163, 184, 0.16);
      border-radius: 28px;
      padding: 34px 28px;
      box-shadow: 0 28px 80px rgba(0, 0, 0, 0.38);
      text-align: center;
    }}
    .pill {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      padding: 9px 16px;
      border-radius: 999px;
      background: rgba(15, 23, 42, 0.95);
      border: 1px solid rgba(148, 163, 184, 0.14);
      font-size: 13px;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      color: #cbd5e1;
    }}
    .dot {{
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: {('#22c55e' if running else '#ef4444')};
      box-shadow: 0 0 14px {('rgba(34, 197, 94, 0.85)' if running else 'rgba(239, 68, 68, 0.7)')};
    }}
    h1 {{
      margin: 20px 0 12px;
      font-size: 34px;
      line-height: 1.15;
      color: #f8fafc;
    }}
    p {{
      margin: 0 auto;
      max-width: 420px;
      font-size: 15px;
      line-height: 1.7;
      color: #94a3b8;
    }}
    .meta {{
      margin-top: 18px;
      font-size: 13px;
      color: #7dd3fc;
      letter-spacing: 0.06em;
      font-variant-numeric: tabular-nums;
    }}
    .actions {{
      margin-top: 28px;
      display: flex;
      justify-content: center;
    }}
    .btn {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 240px;
      padding: 16px 24px;
      border-radius: 18px;
      text-decoration: none;
      font-size: 18px;
      font-weight: 700;
      transition: transform 0.12s ease, opacity 0.2s ease;
    }}
    .btn:active {{ transform: scale(0.985); }}
    .btn.success {{
      background: linear-gradient(135deg, #22c55e, #16a34a);
      color: #04110a;
      box-shadow: 0 16px 32px rgba(22, 163, 74, 0.28);
    }}
    .btn.danger {{
      background: linear-gradient(135deg, #fb7185, #ef4444);
      color: #fff7ed;
      box-shadow: 0 16px 32px rgba(239, 68, 68, 0.24);
    }}
    .link-row {{
      margin-top: 18px;
      font-size: 14px;
    }}
    .link-row a {{
      color: #7dd3fc;
      text-decoration: none;
    }}
  </style>
</head>
<body>
  <div class="shell">
    <div class="card">
      <div class="pill"><span class="dot"></span>{status_text}</div>
      <h1>{title}</h1>
      <p>
        Single monitor service for live and fast-forward is currently
        {'running and checking active strategies every second.' if running else 'stopped. Click below to start listening again.'}
      </p>
      <div class="meta">Trade Date: {trade_date_text}</div>
      <div class="actions">
        <a class="btn {button_class}" href="{button_href}">{button_label}</a>
      </div>
      <div class="link-row"><a href="/monitor">Open Full Monitor</a></div>
    </div>
  </div>
</body>
</html>"""


@app.get("/live/start")
async def live_start(trade_date: str = Query(default=''), ui: str = Query(default='')):
    """Start KiteTicker + live monitor background loop."""
    import threading
    if ticker_manager.status not in ("running", "connecting"):
        threading.Thread(target=_start_ticker_bg, daemon=True).start()
    live_monitor_loop.start(trade_date=trade_date)
    if ui:
        return HTMLResponse(content=_LIVE_CONTROL_HTML)
    return {"ok": True, "message": "Live monitor started", "trade_date": live_monitor_loop.trade_date}


@app.get("/live/stop")
async def live_stop(ui: str = Query(default='')):
    """Stop KiteTicker + live monitor background loop."""
    ticker_manager.stop()
    live_monitor_loop.stop()
    if ui:
        return HTMLResponse(content=_LIVE_CONTROL_HTML)
    return {"ok": True, "message": "Live monitor stopped"}


@app.get("/live/status")
async def live_status():
    """JSON status for polling."""
    return ticker_manager.get_status()


@app.get("/monitor", response_class=HTMLResponse)
async def monitor_page(trade_date: str = Query(default='')):
    _start_monitor_services(trade_date=trade_date)
    return HTMLResponse(content=_build_monitor_control_html())


@app.get("/monitor/start")
async def monitor_start(trade_date: str = Query(default='')):
    is_ready, reason = _get_kite_market_session_status()
    if not is_ready:
        return HTMLResponse(content=_build_monitor_kite_login_page(trade_date=trade_date, reason=reason))
    payload = _start_monitor_services(trade_date=trade_date)
    return HTMLResponse(
        content=_build_monitor_action_page(
            running=True,
            trade_date=str(payload.get('trade_date') or trade_date or ''),
        )
    )


@app.get("/monitor/stop")
async def monitor_stop():
    trade_date = live_fast_monitor_supervisor.trade_date
    ticker_manager.stop()
    live_fast_monitor_supervisor.stop()
    live_monitor_loop.stop()
    return HTMLResponse(
        content=_build_monitor_action_page(
            running=False,
            trade_date=trade_date,
        )
    )


@app.get("/monitor/status")
async def monitor_status():
    return _build_monitor_status_payload()


@app.get("/live/ltp/{token}")
async def live_ltp(token: str):
    ltp = ticker_manager.get_ltp(token)
    if ltp is None:
        raise HTTPException(status_code=404, detail=f"No LTP for token {token}")
    return {"token": token, "ltp": ltp}


# ─── Mock Ticker ──────────────────────────────────────────────────────────────

_MOCK_CONTROL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Mock Ticker Control</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: #0f172a; color: #f1f5f9;
      min-height: 100vh; display: flex;
      align-items: center; justify-content: center;
    }
    .card {
      background: #1e293b; border: 1px solid #334155;
      border-radius: 16px; padding: 2.5rem 3rem;
      width: 460px; text-align: center;
    }
    .title {
      font-size: 1.25rem; font-weight: 600; color: #a78bfa;
      margin-bottom: 0.5rem; letter-spacing: 0.05em; text-transform: uppercase;
    }
    .subtitle {
      font-size: 0.75rem; color: #475569;
      margin-bottom: 2rem;
    }
    .status-row {
      display: flex; align-items: center; justify-content: center;
      gap: 0.6rem; margin-bottom: 1.5rem;
    }
    .dot {
      width: 10px; height: 10px; border-radius: 50%;
      background: #475569; transition: background 0.3s;
    }
    .dot.running    { background: #a78bfa; box-shadow: 0 0 8px #a78bfa; animation: pulse 1.5s infinite; }
    .dot.connecting { background: #f59e0b; animation: pulse 0.8s infinite; }
    .dot.stopped    { background: #ef4444; }
    .dot.error      { background: #ef4444; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
    .status-text { font-size: 1rem; font-weight: 500; color: #cbd5e1; text-transform: capitalize; }
    .mock-time-badge {
      font-size: 0.78rem; color: #a78bfa; margin-bottom: 1.25rem;
      font-variant-numeric: tabular-nums; min-height: 1.2em;
    }
    /* Time picker row — only shown when stopped */
    .time-row {
      display: flex; gap: 0.5rem; margin-bottom: 1.25rem;
    }
    .time-input {
      flex: 1; padding: 0.65rem 0.75rem;
      background: #0f172a; border: 1px solid #334155;
      border-radius: 8px; color: #e2e8f0; font-size: 0.875rem; outline: none;
      color-scheme: dark;
    }
    .time-input:focus { border-color: #7c3aed; }
    .btn {
      width: 100%; padding: 1rem; border: none; border-radius: 10px;
      font-size: 1.1rem; font-weight: 600; cursor: pointer;
      transition: opacity 0.2s, transform 0.1s; letter-spacing: 0.03em;
    }
    .btn:active { transform: scale(0.98); }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }
    .btn-start { background: #7c3aed; color: #fff; }
    .btn-start:hover:not(:disabled) { opacity: 0.9; }
    .btn-stop  { background: #ef4444; color: #fff; }
    .btn-stop:hover:not(:disabled)  { opacity: 0.9; }
    .stats {
      display: grid; grid-template-columns: 1fr 1fr 1fr;
      gap: 0.75rem; margin-top: 1.75rem;
    }
    .stat-box {
      background: #0f172a; border: 1px solid #1e293b;
      border-radius: 8px; padding: 0.75rem;
    }
    .stat-label { font-size: 0.65rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.3rem; }
    .stat-value { font-size: 1rem; font-weight: 700; color: #e2e8f0; }
    .spot-section { margin-top: 1.5rem; text-align: left; }
    .spot-title { font-size: 0.7rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem; }
    .spot-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; }
    .spot-item { background: #0f172a; border: 1px solid #1e293b; border-radius: 8px; padding: 0.5rem 0.75rem; display: flex; justify-content: space-between; align-items: center; }
    .spot-name  { font-size: 0.75rem; color: #94a3b8; font-weight: 600; }
    .spot-price { font-size: 0.85rem; color: #a78bfa; font-weight: 700; }
    .spot-price.na { color: #475569; }
    .error-msg { margin-top: 1rem; font-size: 0.8rem; color: #f87171; background: #1a0a0a; border-radius: 6px; padding: 0.5rem 0.75rem; display: none; }
    .started-at { margin-top: 1rem; font-size: 0.72rem; color: #475569; }
  </style>
</head>
<body>
<div class="card">
  <div class="title">Mock Ticker</div>
  <div class="subtitle">Simulates Kite WebSocket using historical DB data</div>

  <div class="status-row">
    <div class="dot" id="dot"></div>
    <span class="status-text" id="statusText">Loading...</span>
  </div>

  <div class="mock-time-badge" id="mockTimeBadge"></div>

  <!-- Time picker — hidden when running -->
  <div class="time-row" id="timeRow">
    <input class="time-input" type="datetime-local" id="mockTimeInput" step="60" />
  </div>

  <button class="btn btn-start" id="actionBtn" disabled onclick="handleAction()">...</button>

  <div class="stats">
    <div class="stat-box">
      <div class="stat-label">Ticks</div>
      <div class="stat-value" id="tickCount">—</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">LTP Tokens</div>
      <div class="stat-value" id="ltpCount">—</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">Subscribed</div>
      <div class="stat-value" id="subCount">—</div>
    </div>
  </div>

  <div class="spot-section">
    <div class="spot-title">Mock Spot Prices</div>
    <div class="spot-grid">
      <div class="spot-item"><span class="spot-name">NIFTY</span><span class="spot-price na" id="spot-NIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">BANKNIFTY</span><span class="spot-price na" id="spot-BANKNIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">FINNIFTY</span><span class="spot-price na" id="spot-FINNIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">SENSEX</span><span class="spot-price na" id="spot-SENSEX">—</span></div>
    </div>
  </div>

  <div class="error-msg" id="errorMsg"></div>
  <div class="started-at" id="startedAt"></div>
</div>

<script>
  const API = '';   // same origin

  async function fetchStatus() {
    try {
      const res  = await fetch(API + '/mock/status');
      const data = await res.json();
      renderStatus(data);
    } catch(e) {
      renderStatus({ status: 'error', error: 'Cannot reach server' });
    }
  }

  function renderStatus(data) {
    const status = data.status || 'stopped';
    document.getElementById('dot').className       = 'dot ' + status;
    document.getElementById('statusText').textContent = status;

    const btn      = document.getElementById('actionBtn');
    const timeRow  = document.getElementById('timeRow');
    const badgeEl  = document.getElementById('mockTimeBadge');

    btn.disabled = false;

    if (status === 'running' || status === 'connecting') {
      btn.textContent = 'Stop Mock Server';
      btn.className   = 'btn btn-stop';
      if (status === 'connecting') btn.disabled = true;
      timeRow.style.display = 'none';
      badgeEl.textContent   = data.mock_time
        ? '\\u25B6 Simulating: ' + data.mock_time.replace('T', ' ')
        : '';
    } else {
      btn.textContent       = 'Start Listening';
      btn.className         = 'btn btn-start';
      timeRow.style.display = 'flex';
      const inputEl = document.getElementById('mockTimeInput');
      if (inputEl && data.mock_time) {
        inputEl.value = data.mock_time.slice(0, 16);
      }
      badgeEl.textContent   = data.mock_time
        ? 'Last stopped at: ' + data.mock_time.replace('T', ' ')
        : 'Set simulation start time above';
    }

    document.getElementById('tickCount').textContent =
      data.tick_count !== undefined ? data.tick_count.toLocaleString() : '—';
    document.getElementById('ltpCount').textContent =
      data.ltp_count !== undefined ? data.ltp_count.toLocaleString() : '—';
    document.getElementById('subCount').textContent =
      data.subscribed_tokens !== undefined ? data.subscribed_tokens.toLocaleString() : '—';

    const spotMap = data.spot_map || {};
    ['NIFTY','BANKNIFTY','FINNIFTY','SENSEX'].forEach(sym => {
      const el  = document.getElementById('spot-' + sym);
      const val = spotMap[sym];
      if (!el) return;
      if (val) {
        el.textContent = '\\u20B9' + Number(val).toLocaleString('en-IN', { minimumFractionDigits: 2 });
        el.className = 'spot-price';
      } else {
        el.textContent = '—';
        el.className = 'spot-price na';
      }
    });

    const errEl = document.getElementById('errorMsg');
    if (data.error) { errEl.textContent = data.error; errEl.style.display = 'block'; }
    else            { errEl.style.display = 'none'; }

    const startEl = document.getElementById('startedAt');
    startEl.textContent = data.started_at
      ? 'Started: ' + data.started_at.replace('T',' ').slice(0,19)
      : '';
  }

  async function handleAction() {
    const btn    = document.getElementById('actionBtn');
    const status = document.getElementById('statusText').textContent;
    btn.disabled    = true;
    btn.textContent = 'Please wait...';

    try {
      if (status === 'running') {
        await fetch(API + '/mock/stop');
      } else {
        const raw = document.getElementById('mockTimeInput').value;
        if (!raw) {
          await fetch(API + '/mock/start');
        } else {
          const timeStr = raw.length === 16 ? raw + ':00' : raw;
          await fetch(API + '/mock/start?time=' + encodeURIComponent(timeStr));
        }
      }
    } catch(e) { console.error(e); }

    setTimeout(fetchStatus, 600);
    setTimeout(fetchStatus, 1800);
    setTimeout(fetchStatus, 4000);
  }

  fetchStatus();
  setInterval(fetchStatus, 2000);
</script>
</body>
</html>"""


def _start_mock_bg(time_str: str) -> None:
    """Run in a daemon thread — sets mock time then starts MockTicker."""
    result = mock_ticker_manager.set_mock_time(time_str)
    if not result.get("ok"):
        import logging
        logging.getLogger(__name__).error("mock set_mock_time failed: %s", result)
        return
    _db = MongoData()
    try:
        mock_ticker_manager.start(_db)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).error("mock start error: %s", exc)
    finally:
        try:
            _db.close()
        except Exception:
            pass


def _sync_active_option_tokens(instrument: str) -> dict:
    normalized_instrument = str(instrument or "").strip().upper()
    if not normalized_instrument:
        raise HTTPException(status_code=400, detail="Instrument is required")

    today_str = datetime.now().strftime("%Y-%m-%d")
    db = MongoData()
    try:
        credentials_loaded = load_credentials_from_db(db)
        active_tokens_col = db._db["active_option_tokens"]
        active_tokens_col.create_index(
            [("instrument", 1), ("expiry", 1), ("strike", 1), ("option_type", 1)],
            name="idx_active_option_contract",
        )

        expiries = get_kite_expiries(normalized_instrument, today_str, force_refresh=True)
        if not expiries:
            return {
                "instrument": normalized_instrument,
                "expiries": [],
                "contracts_processed": 0,
                "created": 0,
                "updated": 0,
                "message": "No active option contracts found",
                "credentials_loaded": credentials_loaded,
                "hint": (
                    "Check kite_market_config access_token/login if this instrument should have live contracts"
                ),
            }

        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        created_count = 0
        updated_count = 0
        contracts_processed = 0

        for expiry_index, expiry in enumerate(expiries):
            contracts = list_kite_option_contracts(
                normalized_instrument,
                expiry,
                force_refresh=(expiry_index == 0),
            )
            for contract in contracts:
                contracts_processed += 1
                query = {
                    "instrument": normalized_instrument,
                    "expiry": str(contract.get("expiry") or "").strip()[:10],
                    "strike": contract.get("strike"),
                    "option_type": str(contract.get("option_type") or "").strip().upper(),
                }
                update_payload = {
                    "instrument": normalized_instrument,
                    "expiry": query["expiry"],
                    "strike": query["strike"],
                    "option_type": query["option_type"],
                    "token": str(contract.get("token") or "").strip(),
                    "tokens": str(contract.get("tokens") or contract.get("token") or "").strip(),
                    "symbol": str(contract.get("symbol") or "").strip(),
                    "exchange": str(contract.get("exchange") or "").strip(),
                    "updated_at": now_ts,
                }
                result = active_tokens_col.update_one(
                    query,
                    {
                        "$set": update_payload,
                        "$setOnInsert": {"created_at": now_ts},
                    },
                    upsert=True,
                )
                if result.upserted_id is not None:
                    created_count += 1
                elif result.matched_count:
                    updated_count += 1

        return {
            "instrument": normalized_instrument,
            "expiries": expiries,
            "contracts_processed": contracts_processed,
            "created": created_count,
            "updated": updated_count,
            "credentials_loaded": credentials_loaded,
            "message": "active_option_tokens sync completed",
        }
    finally:
        db.close()


@app.get("/mock/start")
async def mock_start(time: str = Query(default="")):
    """
    Start mock ticker.
    Pass ?time=2025-11-03T09:15:00 to set the simulation start time.
    If time is omitted, the last saved mock time is resumed.
    Returns HTML control page.
    """
    if mock_ticker_manager.status not in ("running", "connecting"):
        resume_time = (time or mock_ticker_manager.mock_current_time or "").strip()
        if resume_time:
            import threading
            threading.Thread(target=_start_mock_bg, args=(resume_time,), daemon=True).start()
    live_monitor_loop.start()
    return HTMLResponse(content=_MOCK_CONTROL_HTML)


@app.get("/get_active_tokens/{instrument}")
async def get_active_tokens(instrument: str):
    return _sync_active_option_tokens(instrument)


@app.get("/mock/stop")
async def mock_stop():
    """Stop mock ticker. Returns HTML control page."""
    mock_ticker_manager.stop()
    live_monitor_loop.stop()
    return HTMLResponse(content=_MOCK_CONTROL_HTML)


@app.get("/mock/status")
async def mock_status():
    """JSON status — polled by the control page every 2 s."""
    return mock_ticker_manager.get_status()


@app.get("/mock/ltp/{token}")
async def mock_ltp(token: str):
    ltp = mock_ticker_manager.get_ltp(token)
    if ltp is None:
        raise HTTPException(status_code=404, detail=f"No mock LTP for token {token}")
    return {"token": token, "ltp": ltp}
