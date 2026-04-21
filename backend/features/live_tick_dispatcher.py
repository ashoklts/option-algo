"""
live_tick_dispatcher.py
───────────────────────
Ultra-thin broker tick dispatcher for live + fast-forward modes.

Goals
─────
1. Keep the broker WebSocket on_ticks callback extremely light.
2. Give live order execution its own dedicated worker thread.
3. Prevent fast-forward processing from blocking live execution.
4. Run minute-entry work outside the socket callback.

Design
──────
- One spot writer worker (coalesced).
- One live worker   (preserve tick order, no coalescing).
- One fast-forward worker (coalesced to newest pending tick).

Live is never blocked by fast-forward because each mode has its own worker.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from threading import Condition, Lock, Thread
import logging

from features.mongo_data import MongoData
from features.runtime_mode_registry import runtime_mode_registry

logger = logging.getLogger(__name__)

SPOT_TOKEN_BY_UNDERLYING = {
    "NIFTY": "256265",
    "BANKNIFTY": "260105",
    "FINNIFTY": "257801",
    "SENSEX": "265",
    "MIDCPNIFTY": "288009",
}


@dataclass
class TickTask:
    trade_date: str
    now_ts: str
    now_minute: str
    listen_time: str
    broker_ltp_map: dict[str, float]
    spot_ticks_received: list[tuple[str, float, str]]


class _WorkerQueue:
    def __init__(self, *, coalesce_pending: bool = False) -> None:
        self._items: deque[TickTask] = deque()
        self._cond = Condition()
        self._coalesce_pending = coalesce_pending

    def put(self, item: TickTask) -> None:
        with self._cond:
            if self._coalesce_pending and self._items:
                self._items.clear()
            self._items.append(item)
            self._cond.notify()

    def get(self) -> TickTask:
        with self._cond:
            while not self._items:
                self._cond.wait()
            return self._items.popleft()


class _SpotWriterQueue:
    def __init__(self) -> None:
        self._items: deque[list[tuple[str, float, str]]] = deque()
        self._cond = Condition()

    def put(self, item: list[tuple[str, float, str]]) -> None:
        if not item:
            return
        with self._cond:
            # Keep only the newest pending spot batch; old pending writes are stale.
            self._items.clear()
            self._items.append(item)
            self._cond.notify()

    def get(self) -> list[tuple[str, float, str]]:
        with self._cond:
            while not self._items:
                self._cond.wait()
            return self._items.popleft()


def _persist_spot_ticks(db: MongoData, spot_ticks_received: list[tuple[str, float, str]], source: str) -> None:
    if not spot_ticks_received:
        return
    try:
        spot_col = db._db["option_chain_index_spot"]
        for underlying, spot_price, ts in spot_ticks_received:
            spot_col.update_one(
                {"underlying": underlying, "timestamp": ts},
                {"$set": {
                    "underlying": underlying,
                    "spot_price": float(spot_price),
                    "timestamp": ts,
                    "token": SPOT_TOKEN_BY_UNDERLYING.get(str(underlying or "").upper(), ""),
                    "source": source,
                }},
                upsert=True,
            )
    except Exception as exc:
        logger.error("spot write error source=%s: %s", source, exc)


def _run_entries_for_mode(
    db: MongoData,
    trade_date: str,
    activation_mode: str,
    listen_time: str,
    now_ts: str,
) -> None:
    from features.execution_socket import (
        _execute_backtest_entries,
        _load_running_trade_records,
        _sync_entered_legs_to_history,
        _validate_trade_leg_storage,
        build_entry_spot_snapshots,
    )

    records = _load_running_trade_records(
        db, trade_date, activation_mode=activation_mode,
    )
    if not records:
        return

    if activation_mode not in {"live", "fast-forward"}:
        build_entry_spot_snapshots(db, records, listen_time, now_ts)
    entries_executed = _execute_backtest_entries(
        db, records, listen_time, now_ts,
    )
    if not entries_executed:
        return

    synced_ids: dict[str, dict] = {}
    for entry in entries_executed:
        if not entry.get("entered"):
            continue
        trade_id = str(entry.get("trade_id") or "").strip()
        if trade_id:
            synced_ids[trade_id] = {"_id": trade_id}
    if not synced_ids:
        return

    _sync_entered_legs_to_history(db, list(synced_ids.values()))
    for trade_id in synced_ids:
        _validate_trade_leg_storage(db, trade_id)


class _LiveTickDispatcher:
    def __init__(self) -> None:
        self._started = False
        self._start_lock = Lock()
        self._spot_queue = _SpotWriterQueue()
        self._live_queue = _WorkerQueue(coalesce_pending=False)
        self._fast_forward_queue = _WorkerQueue(coalesce_pending=True)

    def ensure_started(self) -> None:
        if self._started:
            return
        with self._start_lock:
            if self._started:
                return
            Thread(target=self._spot_writer_loop, daemon=True, name="spot_tick_writer").start()
            Thread(
                target=self._mode_worker_loop,
                args=("live", self._live_queue, "kite_live"),
                daemon=True,
                name="live_tick_worker",
            ).start()
            Thread(
                target=self._mode_worker_loop,
                args=("fast-forward", self._fast_forward_queue, "kite_live"),
                daemon=True,
                name="fast_forward_tick_worker",
            ).start()
            self._started = True

    def dispatch_tick(
        self,
        *,
        trade_date: str,
        now_ts: str,
        now_minute: str,
        listen_time: str,
        broker_ltp_map: dict[str, float],
        spot_ticks_received: list[tuple[str, float, str]],
    ) -> None:
        self.ensure_started()
        task = TickTask(
            trade_date=trade_date,
            now_ts=now_ts,
            now_minute=now_minute,
            listen_time=listen_time,
            broker_ltp_map=dict(broker_ltp_map or {}),
            spot_ticks_received=list(spot_ticks_received or []),
        )
        if task.spot_ticks_received:
            self._spot_queue.put(task.spot_ticks_received)
        if runtime_mode_registry.has_active_mode("live"):
            self._live_queue.put(task)
        if runtime_mode_registry.has_active_mode("fast-forward"):
            self._fast_forward_queue.put(task)

    def _spot_writer_loop(self) -> None:
        while True:
            spot_ticks = self._spot_queue.get()
            db = MongoData()
            try:
                _persist_spot_ticks(db, spot_ticks, "kite_live")
            finally:
                db.close()

    def _mode_worker_loop(
        self,
        activation_mode: str,
        queue_obj: _WorkerQueue,
        spot_source: str,
    ) -> None:
        from features.kite_event import broker_live_tick

        while True:
            task = queue_obj.get()
            db = MongoData()
            try:
                broker_live_tick(
                    db,
                    task.trade_date,
                    task.now_ts,
                    task.broker_ltp_map,
                    activation_mode=activation_mode,
                )

                if task.spot_ticks_received:
                    _persist_spot_ticks(db, task.spot_ticks_received, spot_source)
                _run_entries_for_mode(
                    db,
                    task.trade_date,
                    activation_mode,
                    task.listen_time,
                    task.now_ts,
                )
            except Exception as exc:
                logger.error(
                    "mode tick worker error mode=%s ts=%s: %s",
                    activation_mode,
                    task.now_ts,
                    exc,
                )
            finally:
                db.close()


live_tick_dispatcher = _LiveTickDispatcher()
