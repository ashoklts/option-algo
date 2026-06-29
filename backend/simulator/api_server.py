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
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from pymongo import MongoClient

from features import auth as app_auth
from features.mongo_data import MongoData
from features.simulator_risk_monitor import simulator_risk_monitor
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
_option_chain_collection = _stock_db["option_chain_historical_data"]
_simulator_portfolio_col = _stock_db["simulator_portfolio"]
_simulator_strategy_col = _stock_db["simulator_strategy"]
_tv_chart_state_col = _stock_db["tv_chart_state"]
_tv_alerts_col = _stock_db["tv_alerts"]
_lot_sizes_col = _stock_db["lot_sizes"]
IST = timezone(timedelta(hours=5, minutes=30))
_DEFAULT_PAPER_TRADE_SPOT_BROKER_ID = "69e18416c3d234dc8c90e6ca"
_DEFAULT_PAPER_TRADE_PORTFOLIOS = [
    "Running Trades",  "Week On Nct Mnth"
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


class PTPositionRiskIn(BaseModel):
    index: int
    sl_mode: str = "percent"
    sl_value: float = 0.0
    tp_mode: str = "percent"
    tp_value: float = 0.0


class PTStrategyAlertConfigIn(BaseModel):
    positions: List[PTPositionRiskIn] = []
    trading_mode: str = "auto"
    stoploss: Dict[str, Any] = {}
    target: Dict[str, Any] = {}
    trailing_stop: Dict[str, Any] = {}
    hedge_strike_type: Dict[str, Any] = {}
    hedge_time_control: Dict[str, Any] = {}


class PTStrategySlMarkerIn(BaseModel):
    sl_upper: Optional[float] = None
    sl_lower: Optional[float] = None


class PTStrategyIn(BaseModel):
    portfolio_name: str
    strategy_name: str
    instrument: Optional[str] = "nifty"
    spot_price: Optional[float] = None
    config: Optional[Dict[str, Any]] = None
    positions: Optional[List[PTPositionIn]] = []
    # "backtest" for strategies saved from the historical-data builder
    # (PaperTradeBacktest.tsx), "live" for everything saved from the
    # live-broker/positions views. Only honoured on create (pt_save_strategy)
    # — pt_update_strategy deliberately leaves it out of its $set so an
    # update can never flip a strategy's mode back to the model default.
    mode: Optional[str] = "live"


class SimulatorChartStateIn(BaseModel):
    page_id: str = "simulator_chart_workspace"
    symbol: str = "nifty_50"
    layout: Any = None
    resolution: Optional[str] = None


class SimulatorAlertIn(BaseModel):
    page_id: str = "simulator_chart_workspace"
    symbol: str = "nifty_50"
    alert: Dict[str, Any]


def _str_id(doc: Optional[dict]) -> Optional[dict]:
    if doc and "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc


def _get_required_user_id(current_user: dict) -> str:
    user_id = str(current_user.get("_id") or "").strip()
    if not user_id:
        raise HTTPException(status_code=401, detail="Authenticated user is required")
    return user_id


def _ensure_default_simulator_portfolios() -> None:
    for portfolio_name in _DEFAULT_PAPER_TRADE_PORTFOLIOS:
        if not _simulator_portfolio_col.find_one({"name": portfolio_name}, {"_id": 1}):
            _simulator_portfolio_col.insert_one({
                "name": portfolio_name,
                "created_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
            })


def _serialize_instrument_spot_token(doc: dict) -> dict:
    return {
        "_id": str(doc.get("_id") or "").strip(),
        "broker_id": str(doc.get("broker_id") or "").strip(),
        "instrument": str(doc.get("instrument") or "").strip().upper(),
        "code": str(doc.get("code") or "").strip().upper(),
        "token": str(doc.get("token") or "").strip(),
    }


def _get_instrument_spot_token_docs(broker_id: str = "") -> list[dict]:
    resolved_broker_id = str(broker_id or _DEFAULT_PAPER_TRADE_SPOT_BROKER_ID).strip()
    query = {"broker_id": resolved_broker_id} if resolved_broker_id else {}
    docs = list(
        _stock_db["instrument_spot_token"].find(
            query,
            {"broker_id": 1, "instrument": 1, "code": 1, "token": 1},
        ).sort("instrument", 1)
    )
    return [_serialize_instrument_spot_token(doc) for doc in docs]


def _get_simulator_default_quote_tokens(broker_id: str = "") -> list[str]:
    return [
        str(item.get("token") or "").strip()
        for item in _get_instrument_spot_token_docs(broker_id)
        if str(item.get("token") or "").strip()
    ]


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
            _simulator_strategy_col,
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
            _simulator_strategy_col,
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
            _simulator_strategy_col,
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
            _simulator_strategy_col,
            _stock_db,
        )
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/monitor/status")
async def monitor_status() -> dict:
    return await simulator_bridge_status(
        getattr(_broker, "kite", None),
        _simulator_strategy_col,
        _stock_db,
    )


@router.get("/monitor/reentry-status")
async def monitor_reentry_status() -> dict:
    return await simulator_bridge_reentry_status(
        getattr(_broker, "kite", None),
        _simulator_strategy_col,
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


@router.get("/chart-state")
async def get_chart_state(
    page_id: str = Query(default="simulator_chart_workspace"),
    symbol: str = Query(default="nifty_50"),
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    """Layout + resolution only — each alert is its own document in
    tv_alerts (see GET /alerts), not nested in this doc anymore."""
    try:
        user_id = _get_required_user_id(current_user)
        doc = _tv_chart_state_col.find_one(
            {"user_id": user_id, "page_id": page_id, "symbol": symbol},
            {"_id": 0, "layout": 1, "resolution": 1, "updated_at": 1, "page_id": 1, "symbol": 1},
        )
        return {
            "status": "success",
            "state": doc
            or {
                "page_id": page_id,
                "symbol": symbol,
                "layout": None,
                "resolution": None,
                "updated_at": None,
            },
        }
    except HTTPException:
        raise
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.put("/chart-state")
async def save_chart_state(
    body: SimulatorChartStateIn,
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    try:
        user_id = _get_required_user_id(current_user)
        normalized_page_id = str(body.page_id or "simulator_chart_workspace").strip() or "simulator_chart_workspace"
        normalized_symbol = str(body.symbol or "nifty_50").strip() or "nifty_50"
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        _tv_chart_state_col.update_one(
            {"user_id": user_id, "page_id": normalized_page_id, "symbol": normalized_symbol},
            {
                "$set": {
                    "user_id": user_id,
                    "page_id": normalized_page_id,
                    "symbol": normalized_symbol,
                    "layout": body.layout,
                    "resolution": body.resolution,
                    "updated_at": now_str,
                }
            },
            upsert=True,
        )
        return {"status": "success", "updated_at": now_str}
    except HTTPException:
        raise
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/alerts")
async def list_chart_alerts(
    page_id: str = Query(default="simulator_chart_workspace"),
    symbol: str = Query(default="nifty_50"),
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    """Every alert is its own tv_alerts document — one new record per
    alert created, never a shared array field rewritten on every save."""
    try:
        user_id = _get_required_user_id(current_user)
        docs = list(_tv_alerts_col.find({"user_id": user_id, "page_id": page_id, "symbol": symbol}, {"_id": 0}))
        return {"status": "success", "alerts": docs}
    except HTTPException:
        raise
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.put("/alerts/{alert_id}")
async def save_chart_alert(
    alert_id: str,
    body: SimulatorAlertIn,
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    """Upserts exactly one alert document, scoped to this user — creating
    a new alert and editing an existing one both land here, keyed on the
    alert's own client-generated id (tv_alerts_id_uq)."""
    try:
        user_id = _get_required_user_id(current_user)
        normalized_page_id = str(body.page_id or "simulator_chart_workspace").strip() or "simulator_chart_workspace"
        normalized_symbol = str(body.symbol or "nifty_50").strip() or "nifty_50"
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        doc = dict(body.alert)
        doc["id"] = alert_id
        doc["user_id"] = user_id
        doc["page_id"] = normalized_page_id
        doc["symbol"] = normalized_symbol
        doc["updated_at"] = now_str
        _tv_alerts_col.update_one(
            {"id": alert_id, "user_id": user_id},
            {"$set": doc},
            upsert=True,
        )
        return {"status": "success", "updated_at": now_str}
    except HTTPException:
        raise
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.delete("/alerts/{alert_id}")
async def delete_chart_alert(
    alert_id: str,
    current_user: dict = Depends(app_auth.require_current_user),
) -> dict:
    try:
        user_id = _get_required_user_id(current_user)
        _tv_alerts_col.delete_one({"id": alert_id, "user_id": user_id})
        return {"status": "success"}
    except HTTPException:
        raise
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/paper-trade/portfolios")
async def pt_list_portfolios(current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        _ensure_default_simulator_portfolios()
        docs = list(_simulator_portfolio_col.find({}, {"_id": 1, "name": 1}))
        for doc in docs:
            doc["_id"] = str(doc["_id"])
        return {"status": "success", "portfolios": docs}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.post("/paper-trade/portfolios")
async def pt_create_portfolio(body: PTPortfolioIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        existing = _simulator_portfolio_col.find_one({"name": body.name}, {"_id": 1})
        if existing:
            return {"status": "success", "id": str(existing["_id"]), "created": False}
        result = _simulator_portfolio_col.insert_one({
            "name": body.name,
            "created_at": datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S"),
        })
        return {"status": "success", "id": str(result.inserted_id), "created": True}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/paper-trade/strategies")
async def pt_list_strategies(
    portfolio_id: Optional[str] = None,
    portfolio_name: Optional[str] = None,
    current_user: dict = Depends(app_auth.get_current_user),
) -> dict:
    try:
        filt = {}
        normalized_portfolio_id = str(portfolio_id or "").strip()
        normalized_portfolio_name = str(portfolio_name or "").strip()
        if normalized_portfolio_id:
            filt["portfolio_id"] = normalized_portfolio_id
        elif normalized_portfolio_name:
            filt["portfolio_name"] = normalized_portfolio_name
        docs = list(_simulator_strategy_col.find(filt).sort("saved_at", -1))
        result = []
        for doc in docs:
            doc["_id"] = str(doc["_id"])
            positions = doc.get("positions", [])
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
async def pt_get_strategy(strategy_id: str, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        doc = _simulator_strategy_col.find_one({"_id": ObjectId(strategy_id)})
        if not doc:
            return {"status": "error", "message": "Not found"}
        # This module is mounted at the "/algo" prefix (api.py:
        # app.include_router(simulator_router, prefix="/algo")) and
        # PaperTradeNew.tsx's API_BASE *is* "http://localhost:8000/algo"
        # (VITE_API_BASE_URL) — so the frontend's "get saved strategy" call
        # actually lands here, not on api.py's own same-named
        # /simulator/paper-trade/strategies/{id} route. That route's handler
        # (simulator_pt_get_strategy) resolves each position's token and
        # fetches current_ltp via _enrich_pt_strategy_positions; this one
        # used to just return the raw doc, so token/current_ltp were always
        # absent and the frontend fell back to entry_price for "ltp" every
        # time — same bug, just on the endpoint actually being hit. Lazy
        # import (not at module level) since api.py imports *this* module at
        # startup — a top-level import here would be circular.
        from api import _enrich_pt_strategy_positions  # type: ignore
        return {"status": "success", "strategy": _str_id(_enrich_pt_strategy_positions(doc))}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.put("/paper-trade/strategies/{strategy_id}/alert-config")
async def pt_save_strategy_alert_config(strategy_id: str, body: PTStrategyAlertConfigIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Persists Stoploss/Target (per-leg + basket) directly onto this saved
    strategy's own doc. A saved/virtual strategy has no broker_id/leg_id, so
    this can't reuse simulator_triggers/simulator_portfolio_triggers
    (those are keyed by real broker position identity) — same shape of data,
    different home. Position identity here is array index into
    `positions[]`; reordering/adding/removing positions after saving an
    alert invalidates that position's saved risk — accepted, no drift-check,
    same trade-off already made for simpler features earlier this session.
    features/simulator_risk_monitor.py reads these same fields back to
    check/fire (paper exit only — no real broker order, see its
    `paper_leg`/`paper_basket` scope).
    """
    try:
        doc = _simulator_strategy_col.find_one({"_id": ObjectId(strategy_id)})
        if not doc:
            return {"status": "error", "message": "Not found"}

        positions = list(doc.get("positions") or [])
        risk_by_index = {r.index: r for r in body.positions}
        for i, pos in enumerate(positions):
            risk = risk_by_index.get(i)
            if risk:
                pos["sl_mode"] = risk.sl_mode
                pos["sl_value"] = risk.sl_value
                pos["tp_mode"] = risk.tp_mode
                pos["tp_value"] = risk.tp_value

        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        _simulator_strategy_col.update_one(
            {"_id": ObjectId(strategy_id)},
            {"$set": {
                "positions": positions,
                "alert_trading_mode": body.trading_mode,
                "alert_stoploss": body.stoploss,
                "alert_target": body.target,
                "alert_trailing_stop": body.trailing_stop,
                "alert_hedge_strike_type": body.hedge_strike_type,
                "alert_hedge_time_control": body.hedge_time_control,
                "alert_peak_mtm": 0.0,
                "alert_status": "active",
                "alert_updated_at": now_str,
            }},
        )
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.post("/paper-trade/strategies/{strategy_id}/manual-check")
async def pt_manual_check_strategy(strategy_id: str, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    "Manual Trigger" button on the trade page — runs the SAME per-leg SL/TP,
    basket adjustment band, and basket MTM SL/Target checks the background
    risk monitor runs every tick, but once, on demand, for this one strategy.
    A hit fires for real (real exit/adjustment + the real Telegram alert via
    notify_user/notify_admin already inside those fire functions) — this is
    for verifying those alerts actually work, not a dry-run preview.
    """
    try:
        result = await simulator_risk_monitor.manual_check_paper_strategy(MongoData(), strategy_id)
        return result
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.put("/paper-trade/strategies/{strategy_id}/sl-marker")
async def pt_save_strategy_sl_marker(strategy_id: str, body: PTStrategySlMarkerIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Persists the payoff chart's upper/lower stoploss marker directly onto
    this saved strategy's own doc. The live-broker counterpart
    (savePortfolioTrigger -> POST /simulator/paper-trade/portfolio-triggers,
    api.py) is keyed by (broker_id, underlying) — a saved/virtual strategy
    has neither, so that POST's required broker_id field was silently
    arriving empty/missing and 422'ing before this endpoint existed (the
    save looked like it worked client-side since fetch() doesn't throw on a
    non-2xx, but nothing ever reached Mongo). No legs_snapshot/drift-check
    needed here either — same reasoning as pt_save_strategy_alert_config
    above: this doc owns its own positions outright.
    sl_marker_status is a separate field from alert_status (the MTM
    Stoploss/Target feature above) even though both now live on the same
    doc — same independence the live feature keeps between
    simulator_portfolio_triggers' own `status` and `alert_status` fields,
    since dragging this chart marker and saving the Position Configuration
    panel are two independent user actions.
    """
    try:
        now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
        result = _simulator_strategy_col.update_one(
            {"_id": ObjectId(strategy_id)},
            {"$set": {
                "sl_upper": body.sl_upper,
                "sl_lower": body.sl_lower,
                "sl_marker_status": "active",
                "sl_marker_updated_at": now_str,
            }},
        )
        if result.matched_count == 0:
            return {"status": "error", "message": "Not found"}
        return {"status": "success"}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/paper-trade/spot-tokens")
async def pt_spot_tokens(broker_id: str = Query(default=""), current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        resolved_broker_id = str(broker_id or _DEFAULT_PAPER_TRADE_SPOT_BROKER_ID).strip()
        return {
            "status": "success",
            "broker_id": resolved_broker_id,
            "items": _get_instrument_spot_token_docs(resolved_broker_id),
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.put("/paper-trade/strategies/{strategy_id}")
async def pt_update_strategy(strategy_id: str, body: PTStrategyIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        portfolio = _simulator_portfolio_col.find_one({"name": body.portfolio_name}, {"_id": 1})
        if not portfolio:
            result = _simulator_portfolio_col.insert_one({
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

        result = _simulator_strategy_col.update_one(
            {"_id": ObjectId(strategy_id)},
            {"$set": update},
        )
        if result.matched_count == 0:
            return {"status": "error", "message": "Strategy not found"}
        return {"status": "success", "id": strategy_id}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.post("/paper-trade/strategies")
async def pt_save_strategy(body: PTStrategyIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    try:
        portfolio = _simulator_portfolio_col.find_one({"name": body.portfolio_name}, {"_id": 1})
        if not portfolio:
            result = _simulator_portfolio_col.insert_one({
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
            "mode": body.mode or "live",
        }
        result = _simulator_strategy_col.insert_one(doc)
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
