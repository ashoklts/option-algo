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

import hashlib
import json
import multiprocessing
import os
import re
import time
import uuid
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path

from bson import ObjectId
from fastapi import FastAPI, HTTPException, APIRouter, Query
from fastapi.middleware.cors import CORSMiddleware
from pymongo import UpdateOne

from features.backtest_engine import run_backtest
from features.portfolio_worker import strategy_worker
from features.mongo_data     import MongoData
from features.expiry_config  import seed_expiry_config
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


def _resolve_app_user_id(value: str | None = None) -> str:
    normalized_value = str(value or "").strip()
    if normalized_value:
        return normalized_value
    return DEFAULT_APP_USER_ID


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
                if str(row.get("feature") or "").strip() != "momentum_pending":
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


def _sync_active_option_tokens(body: dict):
    """
    Fetch all CE/PE contracts for the requested instrument from Kite instruments
    and store them in `active_option_tokens`.

    Body:
      {
        "instrument": "NIFTY" | "BANKNIFTY",
        "expiry": "YYYY-MM-DD"   # optional; if omitted imports all expiries for next 3 months
      }
    """
    from features.kite_broker_ws import is_configured, validate_access_token

    instrument = str((body or {}).get("instrument") or "").strip().upper()
    requested_expiry = str((body or {}).get("expiry") or "").strip()[:10]
    if instrument not in {"NIFTY", "BANKNIFTY"}:
        raise HTTPException(status_code=400, detail="instrument must be NIFTY or BANKNIFTY")

    if not is_configured():
        raise HTTPException(
            status_code=400,
            detail="Kite credentials not configured. Call POST /kite/config first.",
        )

    if not validate_access_token():
        raise HTTPException(
            status_code=401,
            detail="Stored Kite access_token is invalid or expired. Refresh it via POST /kite/config.",
        )

    today_ist = (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d")
    expiries = get_kite_expiries(instrument, today_ist)
    if not expiries:
        raise HTTPException(
            status_code=404,
            detail=f"No Kite option expiries found for {instrument}.",
        )

    if requested_expiry and requested_expiry not in expiries:
        raise HTTPException(
            status_code=404,
            detail=f"Requested expiry {requested_expiry} is not available for {instrument}.",
        )

    month_cutoff = (datetime.strptime(today_ist, "%Y-%m-%d") + timedelta(days=92)).strftime("%Y-%m-%d")
    target_expiries = [requested_expiry] if requested_expiry else [
        expiry_value for expiry_value in expiries
        if today_ist <= expiry_value <= month_cutoff
    ]
    if not target_expiries:
        raise HTTPException(
            status_code=404,
            detail=f"No option expiries found within the next 3 months for {instrument}.",
        )

    now_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    operations: list[UpdateOne] = []
    active_tokens_by_expiry: dict[str, list[str]] = {}
    all_contracts: list[dict] = []
    for target_expiry in target_expiries:
        contracts = list_kite_option_contracts(instrument, target_expiry)
        if not contracts:
            continue
        active_tokens_by_expiry[target_expiry] = []
        for contract in contracts:
            token_value = str(contract.get("token") or "").strip()
            option_type = str(contract.get("option_type") or "").strip().upper()
            if not token_value or option_type not in {"CE", "PE"}:
                continue
            active_tokens_by_expiry[target_expiry].append(token_value)
            all_contracts.append(contract)
            operations.append(
                UpdateOne(
                    {
                        "instrument": instrument,
                        "expiry": target_expiry,
                        "strike": contract.get("strike"),
                        "option_type": option_type,
                    },
                    {
                        "$set": {
                            "instrument": instrument,
                            "expiry": target_expiry,
                            "strike": contract.get("strike"),
                            "option_type": option_type,
                            "token": token_value,
                            "tokens": token_value,
                            "symbol": contract.get("symbol") or "",
                            "exchange": contract.get("exchange") or "NFO",
                            "updated_at": now_ts,
                        },
                        "$setOnInsert": {
                            "created_at": now_ts,
                        },
                    },
                    upsert=True,
                )
            )

    if not operations:
        raise HTTPException(
            status_code=500,
            detail=f"No valid option tokens resolved for {instrument} in the selected expiry range.",
        )

    db = MongoData()
    try:
        collection = db._db["active_option_tokens"]
        collection.create_index(
            [("instrument", 1), ("expiry", 1), ("strike", 1), ("option_type", 1)],
            unique=True,
        )
        bulk_result = collection.bulk_write(operations, ordered=False)
        deleted_stale_count = 0
        for target_expiry, active_tokens in active_tokens_by_expiry.items():
            delete_result = collection.delete_many({
                "instrument": instrument,
                "expiry": target_expiry,
                "token": {"$nin": active_tokens},
            })
            deleted_stale_count += int(delete_result.deleted_count or 0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

    return {
        "success": True,
        "instrument": instrument,
        "expiry": requested_expiry or "",
        "imported_expiries": target_expiries,
        "available_expiries": expiries,
        "stored_count": len(all_contracts),
        "upserted_count": bulk_result.upserted_count,
        "modified_count": bulk_result.modified_count,
        "deleted_stale_count": deleted_stale_count,
        "collection_name": "active_option_tokens",
        "sample": all_contracts[:5],
    }


@router.get("/option-tokens/sync")
def sync_active_option_tokens_get(
    instrument: str = Query(..., description="NIFTY or BANKNIFTY"),
    expiry: str = Query("", description="Optional expiry in YYYY-MM-DD"),
):
    return _sync_active_option_tokens({
        "instrument": instrument,
        "expiry": expiry,
    })


@router.post("/option-tokens/sync")
def sync_active_option_tokens_post(body: dict):
    return _sync_active_option_tokens(body)


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
    return {"success": True, "id": str(result.inserted_id), "name": name}


@router.get("/strategy/list")
async def strategy_list():
    """List all saved strategy names."""
    db = MongoData()
    docs = list(db._db["saved_strategies"].find({}, {"_id": 1, "name": 1, "created_at": 1}))
    for d in docs:
        d["_id"] = str(d["_id"])
    return {"strategies": docs}


@router.get("/portfolio/list")
async def portfolio_list():
    """List all saved portfolios."""
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
    return {"portfolios": docs}


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
        doc["broker"] = str(doc.get("broker") or prepared_broker or "").strip() or None
        doc["user_id"] = _resolve_app_user_id(doc.get("user_id") or prepared_user_id)
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


@router.get("/trades/list")
async def list_algo_trades(date: str = "", activation_mode: str = "algo-backtest", trade_status: int | None = None):
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
async def list_algo_executions(environment: str = "algo-backtest", is_signal: bool = False, date: str = "", trade_status: int | None = None):
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
            },
        )
        records = []
        for item in cursor:
            broker_id = str(item.get("_id") or "").strip()
            if not broker_id:
                continue
            broker_doc = dict(item)
            broker_doc["_id"] = broker_id
            records.append({
                "_id": broker_id,
                "name": _extract_broker_configuration_label(broker_doc, broker_id),
                "broker_type": str(item.get("broker_type") or "").strip(),
                "broker_icon": str(item.get("broker_icon") or "").strip(),
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
    }


@router.get("/algo-backtest-simulator")
async def algo_backtest_simulator(
    listen_timestamp: str = Query(..., description="Backtest listen timestamp in YYYY-MM-DDTHH:MM:SS"),
    autoload: bool = Query(True, description="Frontend autoload status for reference"),
):
    normalized_timestamp = str(listen_timestamp or "").strip()
    if not normalized_timestamp:
        raise HTTPException(status_code=400, detail="listen_timestamp is required")
    if len(normalized_timestamp) < 19:
        raise HTTPException(status_code=400, detail="listen_timestamp must be in YYYY-MM-DDTHH:MM:SS format")

    db = MongoData()
    try:
        result = run_backtest_simulation_step(
            db,
            normalized_timestamp,
            activation_mode="algo-backtest",
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


# ─── Kite market-data config ──────────────────────────────────────────────────
# One common Kite account is used for all users' live LTP data.
# access_token expires daily — update it each morning via this endpoint.
#
#   POST /kite/config
#   Body: {"api_key": "...", "access_token": "..."}
#
#   GET  /kite/config
#   Returns current config status (no secrets returned).

kite_router = APIRouter(prefix='/kite')


@kite_router.post('/config')
async def set_kite_config(body: dict):
    """
    Set the common Kite credentials used for all users' live LTP.
    Call this once daily after generating today's access_token.
    Saves to MongoDB (kite_market_config) and hot-reloads the ticker.
    Also attaches the live monitor service as a Kite tick listener
    so background monitoring starts immediately.
    """
    from features.kite_broker_ws import (
        save_credentials_to_db,
        set_common_credentials,
    )
    api_key = str((body or {}).get('api_key') or '').strip()
    access_token = str((body or {}).get('access_token') or '').strip()
    if not api_key or not access_token:
        raise HTTPException(status_code=400, detail='api_key and access_token are required')
    db = MongoData()
    try:
        save_credentials_to_db(db, api_key, access_token)
        set_common_credentials(api_key, access_token)
    finally:
        db.close()
    # Attach live monitor listener now that credentials are ready
    from features.live_monitor_service import attach_kite_listener
    attach_kite_listener()
    # Subscribe index tokens for entry monitor (spot price before entry time)
    from features.live_entry_monitor import attach_kite_listener as _lem_attach
    _lem_attach()
    return {'success': True, 'message': 'Kite credentials updated — live monitor active'}


@kite_router.get('/config')
async def get_kite_config():
    """Return current Kite config + live monitor status."""
    from features.kite_broker_ws import get_common_api_key, is_configured
    from features.live_monitor_service import get_service
    svc = get_service()
    return {
        'configured': is_configured(),
        'api_key_set': bool(get_common_api_key()),
        'live_monitor_running': svc._running,
        'tick_queue_size': svc._queue.qsize(),
    }


app.include_router(kite_router)


# ─── Live monitor control endpoints ───────────────────────────────────────────
#
#   GET /live/start          → validate token; if expired redirect to Kite login
#   GET /live/kite-callback  → Kite OAuth redirect lands here, generates token
#   GET /live/stop           → stop position checking
#   GET /live/status         → current state
#
# Open these in a browser — no body or auth needed.

from fastapi import Request
from fastapi.responses import HTMLResponse, RedirectResponse

live_router = APIRouter(prefix='/live')


@live_router.get('/start')
async def live_start(request: Request):
    """
    Hit this URL in the browser to start live monitoring.

    Flow:
      1. Check if stored access_token is still valid (Kite profile call).
      2. Valid  → attach listener → show success page.
      3. Expired/missing → redirect browser to Kite login page automatically.
         After login Kite redirects to /live/kite-callback which generates a
         fresh token and starts monitoring.
    """
    from features.kite_broker_ws import (
        get_login_url,
        is_configured,
        validate_access_token,
    )
    from features.live_monitor_service import get_service

    # ── Step 1: validate existing token ───────────────────────────────────
    if is_configured() and validate_access_token():
        import asyncio as _asyncio
        from features.live_entry_monitor import get_monitor

        # Re-attach SL/TP position monitor
        svc = get_service()
        loop = _asyncio.get_event_loop()
        if not svc._running:
            svc.start(loop)
        else:
            svc.attach_kite_listener()

        # Restart entry monitor fresh from current time
        mon = get_monitor()
        mon.start(loop)

        return HTMLResponse(_html_page(
            title='Live Monitor Started',
            color='#22c55e',
            icon='✅',
            heading='Monitoring Active',
            body='Kite token is valid. All positions are being monitored.<br>'
                 'SL / TP / Trail / Overall checks running on every tick.<br>'
                 'Entry monitor active — checking every second.',
            show_stop=True,
        ))

    # ── Step 2: token missing or expired → redirect to Kite login ─────────
    login_url = get_login_url()
    return RedirectResponse(url=login_url)


@live_router.get('/kite-callback')
async def kite_callback(request: Request, request_token: str = '', status: str = ''):
    """
    Kite OAuth callback — Zerodha redirects here after login.
    URL pattern: /live/kite-callback?request_token=xxx&status=success

    Generates access_token, saves to MongoDB, starts monitoring.
    """
    from features.kite_broker_ws import (
        generate_access_token,
        save_access_token_to_db,
        set_common_credentials,
        get_common_api_key,
    )
    from features.live_monitor_service import get_service

    if status != 'success' or not request_token:
        return HTMLResponse(_html_page(
            title='Login Failed',
            color='#ef4444',
            icon='❌',
            heading='Kite Login Failed',
            body=f'status={status or "unknown"}<br>request_token missing or invalid.<br>'
                 'Please try again.',
            show_stop=False,
        ))

    # ── Generate access_token from request_token ───────────────────────────
    try:
        access_token = generate_access_token(request_token)
    except Exception as exc:
        return HTMLResponse(_html_page(
            title='Token Error',
            color='#ef4444',
            icon='❌',
            heading='Token Generation Failed',
            body=f'Error: {exc}',
            show_stop=False,
        ))

    # ── Save to MongoDB + activate in memory ──────────────────────────────
    db = MongoData()
    try:
        save_access_token_to_db(db, access_token)
    finally:
        db.close()

    set_common_credentials(get_common_api_key(), access_token)

    # ── Start monitoring ──────────────────────────────────────────────────
    import asyncio as _asyncio
    from features.live_entry_monitor import get_monitor

    svc = get_service()
    loop = _asyncio.get_event_loop()
    if not svc._running:
        svc.start(loop)
    else:
        svc.attach_kite_listener()

    mon = get_monitor()
    mon.start(loop)

    return HTMLResponse(_html_page(
        title='Live Monitor Started',
        color='#22c55e',
        icon='✅',
        heading='Login Successful — Monitoring Active',
        body='Access token generated and saved.<br>'
             'All positions are now being monitored on every Kite tick.<br>'
             'SL / TP / Trail / Overall / Broker settings — all active.<br>'
             'Entry monitor active — checking every second.',
        show_stop=True,
    ))


@live_router.get('/stop')
async def live_stop():
    """Stop position checking and entry monitoring. Kite socket stays connected."""
    from features.live_monitor_service import get_service
    from features.live_entry_monitor import get_monitor

    # Stop SL/TP position monitor
    svc = get_service()
    svc.stop()

    # Stop entry time monitor (clears all state)
    mon = get_monitor()
    mon.stop()

    # NOTE: Kite socket is intentionally NOT stopped here.
    # Stopping and recreating the socket on every live monitor restart
    # causes [KITE WS SUBSCRIBE QUEUED] / socket-not-connected failures.
    # The socket is shared infrastructure — it stays alive until full server
    # shutdown (@app.on_event('shutdown') calls stop_all()).

    return HTMLResponse(_html_page(
        title='Live Monitor Stopped',
        color='#f59e0b',
        icon='⏸',
        heading='Monitoring Paused',
        body='Position checking stopped.<br>'
             'Entry monitoring stopped.<br>'
             'Kite WebSocket remains connected.<br>'
             'Hit <a href="/live/start">/live/start</a> to resume.',
        show_stop=False,
    ))


@live_router.get('/status')
async def live_status():
    """JSON status of live monitor and Kite connection."""
    from features.kite_broker_ws import get_common_api_key, is_configured
    from features.live_monitor_service import get_service

    svc = get_service()
    configured = is_configured()
    return {
        'kite_configured': configured,
        'kite_api_key': get_common_api_key(),
        'monitor_loop_running': svc._running,
        'tick_queue_size': svc._queue.qsize(),
        'status': 'active' if (svc._running and configured) else 'paused',
        'message': (
            'Monitoring active — SL/TP/trail/overall checking on every tick'
            if svc._running and configured
            else 'Monitoring paused — hit /live/start to begin'
        ),
    }


def _html_page(
    title: str,
    color: str,
    icon: str,
    heading: str,
    body: str,
    show_stop: bool,
) -> str:
    stop_btn = (
        '<br><br><a href="/live/stop" style="'
        'background:#ef4444;color:#fff;padding:10px 24px;'
        'border-radius:8px;text-decoration:none;font-size:15px;">'
        '⏸ Stop Monitoring</a>'
        if show_stop else ''
    )
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <style>
    body {{font-family:system-ui,sans-serif;display:flex;align-items:center;
           justify-content:center;min-height:100vh;margin:0;background:#0f172a;color:#e2e8f0}}
    .card {{background:#1e293b;border-radius:16px;padding:40px 48px;
            text-align:center;max-width:480px;border:2px solid {color}}}
    .icon {{font-size:56px;margin-bottom:12px}}
    h1 {{color:{color};font-size:22px;margin:0 0 16px}}
    p {{color:#94a3b8;font-size:15px;line-height:1.6;margin:0}}
    a {{color:{color}}}
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">{icon}</div>
    <h1>{heading}</h1>
    <p>{body}{stop_btn}</p>
  </div>
</body>
</html>"""


app.include_router(live_router)


# ─── Startup / Shutdown ───────────────────────────────────────────────────────

@app.on_event('startup')
async def _on_startup():
    """
    1. Load Kite credentials from MongoDB.
    2. Start the persistent live monitor background service.
       (Continues running regardless of frontend connections.)
    """
    import asyncio as _asyncio

    # ── Load Kite credentials ──────────────────────────────────────────────
    try:
        from features.kite_broker_ws import load_credentials_from_db
        db = MongoData()
        loaded = load_credentials_from_db(db)
        db.close()
        if loaded:
            print('[STARTUP] Kite common credentials loaded from DB')
        else:
            print('[STARTUP] No Kite credentials in DB — set via POST /kite/config')
    except Exception as exc:
        print(f'[STARTUP] Kite credentials load error: {exc}')

    # Live monitor service and live entry monitor are NOT started automatically.
    # Hit GET /live/start to start them manually.


@app.on_event('shutdown')
async def _on_shutdown():
    """Stop live monitor services and Kite connection cleanly."""
    try:
        from features.live_monitor_service import stop as _lm_stop
        _lm_stop()
        print('[SHUTDOWN] Live monitor service stopped')
    except Exception as exc:
        print(f'[SHUTDOWN] Live monitor stop error: {exc}')
    try:
        from features.live_entry_monitor import stop as _lem_stop
        _lem_stop()
        print('[SHUTDOWN] Live entry monitor stopped')
    except Exception as exc:
        print(f'[SHUTDOWN] Live entry monitor stop error: {exc}')
    try:
        from features.kite_broker_ws import stop_all
        stop_all()
        print('[SHUTDOWN] Kite WebSocket stopped')
    except Exception as exc:
        print(f'[SHUTDOWN] Kite stop error: {exc}')
