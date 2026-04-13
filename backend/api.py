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

from features.backtest_engine import run_backtest
from features.portfolio_worker import strategy_worker
from features.mongo_data     import MongoData
from features.expiry_config  import seed_expiry_config
from features.execution_socket import (
    build_backtest_simulation_socket_messages,
    broadcast_backtest_simulation_step,
    queue_execute_order_group_start,
    run_backtest_simulation_step,
    socket_router,
)

# ─── Config ───────────────────────────────────────────────────────────────────

REQUEST_JSON_PATH = Path(__file__).parent / "current_backtest_request.json"
SAMPLE_RESULT_PATH = Path(__file__).parent / "sample_backtest_result" / "new_portfolio_result.json"
JOB_STATE_DIR = Path("/tmp/option_algo_backtest_jobs")
CACHE_DIR = Path("/tmp/option_algo_backtest_cache")

JOB_TTL_SECONDS = 3600       # auto-delete completed jobs older than 1 hour
MAX_JOBS        = 10         # max jobs kept in memory at once

# ─── Job store (in-memory) ────────────────────────────────────────────────────
# job_id → { status, completed, total, percent, current_day, result, error, created_at }

_jobs: dict = {}
_jobs_lock = multiprocessing.Lock()


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

        broker_type = str(item.get("broker_type") or strategy_detail.get("broker_type") or ("Broker.Backtest" if activation_mode == "algo-backtest" else "Broker.Dummy")).strip()
        broker = item.get("broker") or strategy_detail.get("broker")
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
            "broker_type": broker_type,
            "number_of_executions": execution_number,
            "execution_cache": execution_cache,
            "multiplier": int(item.get("qty_multiplier") or 1),
            "underlying_max_lots": underlying_max_lots,
            "strategy_detail_snapshot": strategy_detail,  # now contains full_config
            "created_at": now_ts,
            "updated_at": now_ts,
        })

        split_executions.setdefault(broker_type, {})[source_strategy_id] = {
            "number_of_executions": execution_number,
            "assigned_strategy_id": assigned_strategy_id,
        }
        prepared_rows.append({
            "source_strategy_id": source_strategy_id,
            "assigned_strategy_id": assigned_strategy_id,
            "number_of_executions": execution_number,
            "broker_type": broker_type,
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
            {"strategy_detail_snapshot": 1, "multiplier": 1, "source_strategy_id": 1},
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
        if activation_mode == "algo-backtest":
            doc["broker_type"] = "Broker.Backtest"
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
            "broker_type": item.get("broker_type") or "",
            "ticker": item.get("ticker") or "",
            "creation_ts": item.get("creation_ts") or "",
            "entry_time": item.get("entry_time") or "",
            "exit_time": item.get("exit_time") or "",
            "portfolio": item.get("portfolio") if isinstance(item.get("portfolio"), dict) else {},
        })

    # Populate string leg IDs with full algo_trade_positions_history docs (single batch query)
    populated_records = _populate_history_legs(db._db, raw_records)
    populated_records = _attach_leg_feature_statuses(db._db, populated_records)
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
        # Build socket messages exactly once — broadcast reuses them to avoid
        # a second _populate_legs_from_history DB call.
        socket_messages = build_backtest_simulation_socket_messages(db, result)
        delivered = await broadcast_backtest_simulation_step(
            db, result, prebuilt_messages=socket_messages
        )
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
