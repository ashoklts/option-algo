"""
api_server.py
-------------
APIRouter for the Mini Strangle backtesting engine.
Included in main.py with prefix="/engine".

Endpoints:
  POST /engine/mini-strangle/start           → starts engine, returns SSE stream
  POST /engine/mini-strangle/stop/{id}       → stops a running session
  GET  /engine/mini-strangle/sessions        → list active session IDs
  GET  /engine/health                        → health check

SSE Event Types emitted on the stream:
  started              → engine initialised
  positions_opened     → CE + PE sells placed, adjustment levels included
  monitor              → per-tick update (spot, ATM, PnL, risk status …)
  adjustment_triggered → spot hit upper or lower adjustment level
  otm_adjustment_triggered → sold strike moved within OTM shift distance of ATM
  positions_closed     → sells closed before re-entry
  reentry_scheduled    → re-entry delay queued
  event_reentry_scheduled → SL/target/TSL continuation queued
  hedge_opened         → hedge BUY positions placed
  hedge_closed         → hedge positions closed
  stoploss_hit         → stop-loss exit
  target_hit           → profit target exit
  trailing_sl_hit      → trailing stop-loss exit
  stopped              → engine has finished
  error                → unrecoverable error
"""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from pymongo import MongoClient

from .models import MiniStrangleRequest
from .monitor_service import get_simulator_monitor_service
from .monitor_ui import build_monitor_toggle_page
from .strategy_monitor_bridge import (
    reentry_status as simulator_bridge_reentry_status,
    start as simulator_bridge_start,
    status as simulator_bridge_status,
    stop as simulator_bridge_stop,
)
from .strategy_engine import StrategyEngine
from .streaming_controller import StreamingController
from .zerodha_broker import ZerodhaBroker

_broker = ZerodhaBroker()
_mongo_client = MongoClient("mongodb://localhost:27017/")
_stock_db = _mongo_client["stock_data"]
_holiday_collection = _stock_db["market_holidays"]
_option_chain_collection = _stock_db["option_chain"]
_paper_trade_portfolio_col = _stock_db["paper_trade_portfolio"]
_paper_trade_strategy_col = _stock_db["paper_trade_strategy"]
_lot_sizes_col = _stock_db["lot_sizes"]
IST = timezone(timedelta(hours=5, minutes=30))
_DEFAULT_PAPER_TRADE_PORTFOLIOS = [
    "Running Trades", "Exited Trades", "Archived Trades",
    "Aditional Position Strategy", "Nifty Weekly Expiry BB Stra",
    "Nifty Expiry Day BB Stra", "Banknifty Expiry Day BB Stra",
    "Nifty Monthly Stra", "Week On Nct Mnth", "Small Strangle",
]

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/simulator")

# Active engine sessions  {session_id → StrategyEngine}
_sessions: Dict[str, StrategyEngine] = {}


class PTPortfolioIn(BaseModel):
    name: str


class PTPositionIn(BaseModel):
    type: str
    option_type: str
    strike: float
    expiry: str
    entry_price: float
    entry_time: Optional[str] = None
    lots: Optional[int] = 1
    lot_size: Optional[int] = 75
    quantity: Optional[float] = None
    exited: Optional[bool] = False
    exit_price: Optional[float] = None
    exit_time: Optional[str] = None
    pnl: Optional[float] = None
    pnl_pct: Optional[float] = None


class PTStrategyIn(BaseModel):
    portfolio_name: str
    strategy_name: str
    instrument: Optional[str] = "nifty"
    spot_price: Optional[float] = None
    config: Optional[Dict[str, Any]] = None
    positions: Optional[List[PTPositionIn]] = []


def _str_id(doc: dict | None) -> dict | None:
    if doc and "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc


def _ensure_default_paper_trade_portfolios() -> None:
    for portfolio_name in _DEFAULT_PAPER_TRADE_PORTFOLIOS:
        if not _paper_trade_portfolio_col.find_one({"name": portfolio_name}, {"_id": 1}):
            _paper_trade_portfolio_col.insert_one({
                "name": portfolio_name,
                "created_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
            })


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/mini-strangle/start", summary="Start a Mini Strangle backtesting session")
async def start_mini_strangle(request: MiniStrangleRequest) -> StreamingResponse:
    """
    Starts the engine and streams results back as Server-Sent Events.

    Each SSE message is a JSON object:
    ```json
    {"event": "<event_type>", "ts": "<ISO timestamp>", "data": { … }}
    ```

    The session ID is returned in the `X-Session-ID` response header.
    Use it to call `/engine/mini-strangle/stop/{session_id}` to halt early.
    """
    session_id = str(uuid.uuid4())
    stream = StreamingController(position_start_time=request.position_start_time)
    engine = StrategyEngine(request, stream)

    _sessions[session_id] = engine

    asyncio.create_task(_run_session(session_id, engine))

    logger.info(
        f"Session {session_id} started | "
        f"{request.backtest_start_date} → {request.backtest_end_date}"
    )

    return StreamingResponse(
        stream.stream(),
        media_type="text/event-stream",
        headers={
            "X-Session-ID": session_id,
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.post(
    "/mini-strangle/stop/{session_id}",
    summary="Stop an active Mini Strangle session",
)
async def stop_mini_strangle(session_id: str) -> dict:
    engine = _sessions.get(session_id)
    if not engine:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")
    engine.stop()
    _sessions.pop(session_id, None)
    logger.info(f"Session {session_id} stopped via API")
    return {"status": "stopped", "session_id": session_id}


@router.get("/mini-strangle/sessions", summary="List all active session IDs")
async def list_sessions() -> dict:
    return {"active_sessions": list(_sessions.keys()), "count": len(_sessions)}


@router.get("/monitor/start")
async def start_monitor(
    strategy_id: str = Query(default=""),
    portfolio_name: str = Query(default=""),
) -> HTMLResponse:
    try:
        if getattr(_broker, "kite", None) is None:
            return HTMLResponse(content=build_monitor_toggle_page(
                running=False,
                title="Simulator Monitor",
                status_text="Zerodha market session is not ready.",
                detail_text="Configure Zerodha first, then open this start page again.",
                start_href="./start",
                stop_href="./stop",
                status_href="./status",
            ))
        payload = await simulator_bridge_start(
            _broker.kite,
            _paper_trade_strategy_col,
            _stock_db,
        )
        detail_parts = []
        if strategy_id:
            detail_parts.append(f"strategy_id={strategy_id}")
        if portfolio_name:
            detail_parts.append(f"portfolio_name={portfolio_name}")
        if payload.get("subscribed_tokens") is not None:
            detail_parts.append(f"subscribed_tokens={payload.get('subscribed_tokens')}")
        return HTMLResponse(content=build_monitor_toggle_page(
            running=True,
            title="Simulator Monitor",
            status_text=str(payload.get("message") or payload.get("status") or "Monitor started"),
            detail_text=" | ".join(detail_parts) or "Monitor is running. Click Stop to stop the background monitor.",
            start_href="./start",
            stop_href="./stop",
            status_href="./status",
        ))
    except Exception as exc:
        return HTMLResponse(content=build_monitor_toggle_page(
            running=False,
            title="Simulator Monitor",
            status_text="Failed to start monitor.",
            detail_text=str(exc),
            start_href="./start",
            stop_href="./stop",
            status_href="./status",
        ), status_code=500)


@router.post("/monitor/start")
async def start_monitor_post(
    strategy_id: str = Query(default=""),
    portfolio_name: str = Query(default=""),
) -> dict:
    try:
        if getattr(_broker, "kite", None) is None:
            return {
                "status": "error",
                "message": "Simulator market session not ready. Configure Zerodha first.",
            }
        payload = await simulator_bridge_start(
            _broker.kite,
            _paper_trade_strategy_col,
            _stock_db,
        )
        if strategy_id or portfolio_name:
            payload["requested_strategy_id"] = strategy_id
            payload["requested_portfolio_name"] = portfolio_name
        return payload
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/monitor/stop")
async def stop_monitor() -> HTMLResponse:
    try:
        payload = await simulator_bridge_stop(
            getattr(_broker, "kite", None),
            _paper_trade_strategy_col,
            _stock_db,
        )
        return HTMLResponse(content=build_monitor_toggle_page(
            running=False,
            title="Simulator Monitor",
            status_text=str(payload.get("message") or payload.get("status") or "Monitor stopped"),
            detail_text="Monitor is stopped. Click Start to start it again.",
            start_href="./start",
            stop_href="./stop",
            status_href="./status",
        ))
    except Exception as exc:
        return HTMLResponse(content=build_monitor_toggle_page(
            running=False,
            title="Simulator Monitor",
            status_text="Failed to stop monitor.",
            detail_text=str(exc),
            start_href="./start",
            stop_href="./stop",
            status_href="./status",
        ), status_code=500)


@router.post("/monitor/stop")
async def stop_monitor_post() -> dict:
    try:
        return await simulator_bridge_stop(
            getattr(_broker, "kite", None),
            _paper_trade_strategy_col,
            _stock_db,
        )
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/monitor/status")
async def monitor_status() -> dict:
    return await simulator_bridge_status(
        getattr(_broker, "kite", None),
        _paper_trade_strategy_col,
        _stock_db,
    )


@router.get("/monitor/reentry-status")
async def monitor_reentry_status() -> dict:
    return await simulator_bridge_reentry_status(
        getattr(_broker, "kite", None),
        _paper_trade_strategy_col,
        _stock_db,
    )


@router.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@router.get("/zerodha/status")
async def zerodha_status() -> dict:
    connected, profile = _broker.is_connected()
    return {
        "connected": connected,
        "has_config": _broker.has_config(),
        "user_name": profile.get("user_name") if profile else None,
        "user_id": profile.get("user_id") if profile else None,
    }


@router.get("/get-market-holidays")
async def get_market_holidays() -> dict:
    try:
        dates = [
            doc["date"]
            for doc in _holiday_collection.find({}, {"_id": 0, "date": 1})
            if "date" in doc
        ]
        return {"status": "success", "holidays": sorted(dates)}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/get-option-chain")
async def get_option_chain(timestamp: str = Query(...)) -> dict:
    try:
        data = list(_option_chain_collection.find({"timestamp": timestamp}, {"_id": 0}))
        return {
            "status": "success",
            "timestamp": timestamp,
            "count": len(data),
            "data": data,
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/lot-size")
async def get_lot_size(instrument: str = "nifty") -> dict:
    today = datetime.now(IST).strftime("%Y-%m-%d")
    symbol = str(instrument or "nifty").upper()
    doc = _lot_sizes_col.find_one(
        {
            "instrument": symbol,
            "effective_from": {"$lte": today},
            "$or": [
                {"effective_to": None},
                {"effective_to": {"$exists": False}},
                {"effective_to": {"$gte": today}},
            ],
        },
        sort=[("effective_from", -1)],
    )
    if doc:
        return {"instrument": symbol, "lot_size": int(doc["lot_size"])}
    defaults = {"NIFTY": 75, "BANKNIFTY": 15, "FINNIFTY": 40, "MIDCPNIFTY": 120, "SENSEX": 10}
    return {"instrument": symbol, "lot_size": defaults.get(symbol, 75)}


@router.get("/paper-trade/portfolios")
async def pt_list_portfolios() -> dict:
    try:
        _ensure_default_paper_trade_portfolios()
        docs = list(_paper_trade_portfolio_col.find({}, {"_id": 1, "name": 1}))
        for doc in docs:
            doc["_id"] = str(doc["_id"])
        return {"status": "success", "portfolios": docs}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.post("/paper-trade/portfolios")
async def pt_create_portfolio(body: PTPortfolioIn) -> dict:
    try:
        existing = _paper_trade_portfolio_col.find_one({"name": body.name}, {"_id": 1})
        if existing:
            return {"status": "success", "id": str(existing["_id"]), "created": False}
        result = _paper_trade_portfolio_col.insert_one({
            "name": body.name,
            "created_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
        })
        return {"status": "success", "id": str(result.inserted_id), "created": True}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/paper-trade/strategies")
async def pt_list_strategies(portfolio_name: Optional[str] = None) -> dict:
    try:
        filt = {}
        if portfolio_name:
            filt["portfolio_name"] = portfolio_name
        docs = list(_paper_trade_strategy_col.find(filt).sort("saved_at", -1))
        result = []
        for doc in docs:
            doc["_id"] = str(doc["_id"])
            positions = doc.pop("positions", [])
            doc["position_count"] = len(positions)
            doc["all_exited"] = all(pos.get("exited", False) for pos in positions) if positions else False
            realized = 0.0
            open_positions = []
            for pos in positions:
                qty = pos.get("quantity") or ((pos.get("lots") or 1) * (pos.get("lot_size") or 1))
                is_sell = str(pos.get("type", "")).lower() == "sell"
                if pos.get("exited"):
                    if pos.get("pnl") is not None:
                        realized += pos["pnl"]
                    elif pos.get("exit_price") is not None and pos.get("entry_price") is not None:
                        if is_sell:
                            realized += (pos["entry_price"] - pos["exit_price"]) * qty
                        else:
                            realized += (pos["exit_price"] - pos["entry_price"]) * qty
                else:
                    open_positions.append({
                        "type": pos.get("type", ""),
                        "option_type": pos.get("option_type", ""),
                        "strike": pos.get("strike", 0),
                        "expiry": pos.get("expiry", ""),
                        "entry_price": pos.get("entry_price", 0),
                        "quantity": qty,
                    })
            doc["realized_pnl"] = round(realized, 2)
            doc["open_positions"] = open_positions
            result.append(doc)
        return {"status": "success", "strategies": result}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/paper-trade/strategies/{strategy_id}")
async def pt_get_strategy(strategy_id: str) -> dict:
    try:
        doc = _paper_trade_strategy_col.find_one({"_id": ObjectId(strategy_id)})
        if not doc:
            return {"status": "error", "message": "Not found"}
        return {"status": "success", "strategy": _str_id(doc)}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.put("/paper-trade/strategies/{strategy_id}")
async def pt_update_strategy(strategy_id: str, body: PTStrategyIn) -> dict:
    try:
        portfolio = _paper_trade_portfolio_col.find_one({"name": body.portfolio_name}, {"_id": 1})
        if not portfolio:
            result = _paper_trade_portfolio_col.insert_one({
                "name": body.portfolio_name,
                "created_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
            })
            portfolio_id = result.inserted_id
        else:
            portfolio_id = portfolio["_id"]

        positions = []
        for position in (body.positions or []):
            pos = position.dict()
            if pos.get("quantity") is None:
                pos["quantity"] = (pos.get("lots") or 1) * (pos.get("lot_size") or 1)
            positions.append(pos)

        update = {
            "portfolio_id": str(portfolio_id),
            "portfolio_name": body.portfolio_name,
            "strategy_name": body.strategy_name,
            "instrument": body.instrument or "nifty",
            "spot_price": body.spot_price,
            "config": body.config or {},
            "positions": positions,
            "updated_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
        }

        result = _paper_trade_strategy_col.update_one(
            {"_id": ObjectId(strategy_id)},
            {"$set": update},
        )
        if result.matched_count == 0:
            return {"status": "error", "message": "Strategy not found"}
        return {"status": "success", "id": strategy_id}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.post("/paper-trade/strategies")
async def pt_save_strategy(body: PTStrategyIn) -> dict:
    try:
        portfolio = _paper_trade_portfolio_col.find_one({"name": body.portfolio_name}, {"_id": 1})
        if not portfolio:
            result = _paper_trade_portfolio_col.insert_one({
                "name": body.portfolio_name,
                "created_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
            })
            portfolio_id = result.inserted_id
        else:
            portfolio_id = portfolio["_id"]

        positions = []
        for position in (body.positions or []):
            pos = position.dict()
            if pos.get("quantity") is None:
                pos["quantity"] = (pos.get("lots") or 1) * (pos.get("lot_size") or 1)
            positions.append(pos)

        now_iso = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        initial_pos_history = [
            {
                "action": "INITIAL_SAVE",
                "time": now_iso,
                "strike": pos.get("strike"),
                "option_type": pos.get("option_type") or pos.get("type"),
                "expiry": str(pos.get("expiry", ""))[:10],
                "entry_price": pos.get("entry_price"),
                "lots": pos.get("lots"),
                "lot_size": pos.get("lot_size"),
            }
            for pos in positions
            if not pos.get("exited")
        ]

        doc = {
            "portfolio_id": str(portfolio_id),
            "portfolio_name": body.portfolio_name,
            "strategy_name": body.strategy_name,
            "instrument": body.instrument or "nifty",
            "spot_price": body.spot_price,
            "config": body.config or {},
            "positions": positions,
            "saved_at": now_iso,
            "position_history": initial_pos_history,
        }
        result = _paper_trade_strategy_col.insert_one(doc)
        return {"status": "success", "id": str(result.inserted_id)}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _run_session(session_id: str, engine: StrategyEngine) -> None:
    """Wrapper that removes the session from the registry when the engine finishes."""
    try:
        await engine.run()
    finally:
        _sessions.pop(session_id, None)
        logger.info(f"Session {session_id} removed from registry")
