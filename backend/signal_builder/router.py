from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from features.alert_checker import (
    is_indicator_alert_monitor_running,
    start_indicator_alert_monitor,
    stop_indicator_alert_monitor,
)
from simulator.monitor_ui import build_monitor_toggle_page

from .service import (
    delete_signal_indicator,
    delete_signal_strategy,
    get_signal_indicators,
    get_signal_schemas_catalog,
    get_signal_strategies,
    get_tv_indicator_conditions,
    get_tv_tool_conditions,
    save_signal_indicator,
    save_signal_strategy,
    seed_indicator_schemas,
)

router = APIRouter(prefix="/signal", tags=["signal"])


class SaveSignalIndicatorRequest(BaseModel):
    label: str
    type: str
    name: str
    chartType: str
    schemaStatus: str
    config: dict[str, Any] = {}


class StrategyConditionRow(BaseModel):
    indicatorId: str
    indicatorLabel: str
    operator: str
    compareType: str
    value: float | None = None
    compareIndicatorId: str | None = None
    compareIndicatorLabel: str | None = None
    logicalOperator: str | None = None


class SaveSignalStrategyRequest(BaseModel):
    name: str
    underlying: str
    strategyType: str
    quantity: int
    entryConditions: list[StrategyConditionRow] = []
    exitConditions: list[StrategyConditionRow] = []
    overallStopLoss: dict[str, Any] = {}
    overallTarget: dict[str, Any] = {}


@router.get("/indicator-catalog")
async def signal_indicator_catalog() -> list[dict[str, Any]]:
    """Return the full indicator catalog from MongoDB (seeded from execute-order-stopped.json)."""
    try:
        return get_signal_schemas_catalog()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/seed-indicator-schemas")
async def signal_seed_indicator_schemas() -> dict[str, Any]:
    """Parse execute-order-stopped.json and upsert all indicators into MongoDB."""
    try:
        return seed_indicator_schemas()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/indicators")
async def signal_list_indicators() -> list[dict[str, Any]]:
    try:
        return get_signal_indicators()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/indicators")
async def signal_save_indicator(body: SaveSignalIndicatorRequest) -> dict[str, Any]:
    try:
        return save_signal_indicator(body.dict())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.delete("/indicators/{indicator_id}")
async def signal_delete_indicator(indicator_id: str) -> dict[str, Any]:
    try:
        return delete_signal_indicator(indicator_id)
    except ValueError as exc:
        status_code = 404 if "not found" in str(exc).lower() else 400
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/tv-tool-conditions")
async def signal_tv_tool_conditions() -> list[dict[str, Any]]:
    """Return the TradingView line-tool -> alert-condition mapping from MongoDB."""
    try:
        return get_tv_tool_conditions()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/tv-indicator-conditions")
async def signal_tv_indicator_conditions() -> list[dict[str, Any]]:
    """Return the TradingView built-in indicator -> alert-condition mapping from MongoDB."""
    try:
        return get_tv_indicator_conditions()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/strategies")
async def signal_list_strategies() -> list[dict[str, Any]]:
    try:
        return get_signal_strategies()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/strategies")
async def signal_save_strategy(body: SaveSignalStrategyRequest) -> dict[str, Any]:
    try:
        return save_signal_strategy(body.dict())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.delete("/strategies/{strategy_id}")
async def signal_delete_strategy(strategy_id: str) -> dict[str, Any]:
    try:
        return delete_signal_strategy(strategy_id)
    except ValueError as exc:
        status_code = 404 if "not found" in str(exc).lower() else 400
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# Manual on/off control for the indicator-condition (Supertrend/MACD/MA
# Cross/RSI/Stochastic) alert checker — NOT auto-started at boot, unlike the
# price/trendline alert loop (see api.py). Same browser-clickable toggle
# page + JSON status pattern simulator/api_server.py's
# /monitor/{start,stop,status} already uses for the Simulator Monitor.
_MONITOR_TITLE = "Indicator Alert Monitor"


@router.get("/indicator-alert-monitor/start")
async def indicator_alert_monitor_start_page() -> HTMLResponse:
    try:
        result = start_indicator_alert_monitor()
        return HTMLResponse(content=build_monitor_toggle_page(
            running=True,
            title=_MONITOR_TITLE,
            status_text=str(result.get("message")),
            detail_text="Checking Supertrend/MACD/MA Cross/RSI/Stochastic alerts on their own bar-close schedule.",
            start_href="./start",
            stop_href="./stop",
            status_href="./status",
        ))
    except Exception as exc:
        return HTMLResponse(content=build_monitor_toggle_page(
            running=False,
            title=_MONITOR_TITLE,
            status_text="Failed to start monitor.",
            detail_text=str(exc),
            start_href="./start",
            stop_href="./stop",
            status_href="./status",
        ), status_code=500)


@router.get("/indicator-alert-monitor/stop")
async def indicator_alert_monitor_stop_page() -> HTMLResponse:
    try:
        result = await stop_indicator_alert_monitor()
        return HTMLResponse(content=build_monitor_toggle_page(
            running=False,
            title=_MONITOR_TITLE,
            status_text=str(result.get("message")),
            detail_text="Indicator-condition alerts are not being checked. Click Start to resume.",
            start_href="./start",
            stop_href="./stop",
            status_href="./status",
        ))
    except Exception as exc:
        return HTMLResponse(content=build_monitor_toggle_page(
            running=is_indicator_alert_monitor_running(),
            title=_MONITOR_TITLE,
            status_text="Failed to stop monitor.",
            detail_text=str(exc),
            start_href="./start",
            stop_href="./stop",
            status_href="./status",
        ), status_code=500)


@router.post("/indicator-alert-monitor/start")
async def indicator_alert_monitor_start_post() -> dict[str, Any]:
    try:
        return start_indicator_alert_monitor()
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.post("/indicator-alert-monitor/stop")
async def indicator_alert_monitor_stop_post() -> dict[str, Any]:
    try:
        return await stop_indicator_alert_monitor()
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


@router.get("/indicator-alert-monitor/status")
async def indicator_alert_monitor_status() -> dict[str, Any]:
    return {"status": "success", "running": is_indicator_alert_monitor_running()}
