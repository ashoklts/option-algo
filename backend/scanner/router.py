from typing import Any, List, Optional, Union

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .service import (
    exit_goldbees_strategy,
    generate_combained_stock_scores,
    generate_stock_scores,
    get_combained_portfolio_detail,
    get_live_combained_portfolio,
    get_live_portfolio,
    get_combained_portfolio_detail,
    get_indexes,
    get_portfolio_detail,
    get_portfolio_summary,
    get_sectors,
    invest_goldbees_strategy,
    rebalance_combained_portfolio,
    rebalance_portfolio,
    save_portfolio,
    backfill_stocks_list_kite_tokens,
    get_previous_universe_stocks,
    remove_universe_field_from_stocks_list,
    kite_sync_universe_stock_list,
    sync_universe_stock_list,
    update_portfolio_investments,
)

router = APIRouter(prefix="/scanner", tags=["scanner"])


class EodScoringRequest(BaseModel):
    index_name: Optional[Union[List[str], str]] = None
    index_names: Optional[Union[List[str], str]] = None
    sectors: Optional[Union[List[str], str]] = None
    min_price: Optional[Union[float, int, str]] = None
    max_price: Optional[Union[float, int, str]] = None
    top_n: int = 12
    total_capital: float = 1_000_000
    score_date: Optional[str] = None
    formula: Optional[str] = None
    index_name_1: Optional[Union[List[str], str]] = None
    index_names_1: Optional[Union[List[str], str]] = None
    sectors_1: Optional[Union[List[str], str]] = None
    min_price_1: Optional[Union[float, int, str]] = None
    max_price_1: Optional[Union[float, int, str]] = None
    top_n_1: Optional[int] = None
    total_capital_1: Optional[float] = None
    score_date_1: Optional[str] = None
    formula_1: Optional[str] = None


class UpdatePortfolioRequest(BaseModel):
    portfolio_strategy_id: str
    invest_stock_data: list[dict[str, Any]]


class SavePortfolioRequest(BaseModel):
    portfolio_settings: dict[str, Any]
    invest_stock_data: list[dict[str, Any]]


@router.get("/indexes")
async def scanner_indexes() -> dict[str, Any]:
    return {"status": "success", "items": get_indexes()}


@router.get("/sectors")
async def scanner_sectors() -> dict[str, Any]:
    return {"status": "success", "items": get_sectors()}


@router.get("/sync_universe_stocks")
async def scanner_sync_universe_stocks() -> dict[str, Any]:
    try:
        return sync_universe_stock_list()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/kite_sync_universe_stocks")
async def scanner_kite_sync_universe_stocks() -> dict[str, Any]:
    try:
        return kite_sync_universe_stock_list()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/backfill_stocks_kite_tokens")
async def scanner_backfill_stocks_kite_tokens() -> dict[str, Any]:
    try:
        return backfill_stocks_list_kite_tokens()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/remove_universe_field")
async def scanner_remove_universe_field() -> dict[str, Any]:
    try:
        return remove_universe_field_from_stocks_list()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/previous_universe_stocks/{filter_symbol}")
async def scanner_previous_universe_stocks(filter_symbol: str) -> dict[str, Any]:
    try:
        return get_previous_universe_stocks(filter_symbol)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/eod_scoring")
async def scanner_eod_scoring(body: EodScoringRequest) -> dict[str, Any]:
    try:
        result = generate_stock_scores(body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result


@router.post("/eod_scoring_combained")
async def scanner_eod_scoring_combained(body: EodScoringRequest) -> dict[str, Any]:
    try:
        result = generate_combained_stock_scores(body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result


@router.post("/update_portfolio")
async def scanner_update_portfolio(body: UpdatePortfolioRequest) -> dict[str, Any]:
    try:
        return update_portfolio_investments(body.portfolio_strategy_id, body.invest_stock_data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/save_portfolio")
async def scanner_save_portfolio(body: SavePortfolioRequest) -> dict[str, Any]:
    try:
        return save_portfolio(body.portfolio_settings, body.invest_stock_data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/portfolio_summary")
async def scanner_portfolio_summary() -> list[dict[str, Any]]:
    try:
        return get_portfolio_summary()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/detailPortfolio/{strategy_id}")
async def scanner_portfolio_detail(strategy_id: str) -> dict[str, Any]:
    try:
        return get_portfolio_detail(strategy_id)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/combainedDetailPortfolio/{strategy_id}")
async def scanner_combained_portfolio_detail(strategy_id: str) -> dict[str, Any]:
    try:
        return get_combained_portfolio_detail(strategy_id)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/live_prices/strategy/{strategy_id}")
async def scanner_live_portfolio(strategy_id: str) -> dict[str, Any]:
    try:
        return get_live_portfolio(strategy_id)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/live_prices/combained_strategy/{strategy_id}")
async def scanner_live_combained_portfolio(strategy_id: str) -> dict[str, Any]:
    try:
        return get_live_combained_portfolio(strategy_id)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/prtfolio/rebalance/{strategy_id}")
async def scanner_rebalance_portfolio(strategy_id: str) -> dict[str, Any]:
    try:
        return rebalance_portfolio(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/prtfolio/combained_rebalance/{strategy_id}")
async def scanner_rebalance_combained_portfolio(strategy_id: str) -> dict[str, Any]:
    try:
        return rebalance_combained_portfolio(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/prtfolio/invest_goldbees/{strategy_id}")
async def scanner_invest_goldbees(strategy_id: str) -> dict[str, Any]:
    try:
        return invest_goldbees_strategy(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/prtfolio/exit_goldbees/{strategy_id}")
async def scanner_exit_goldbees(strategy_id: str) -> dict[str, Any]:
    try:
        return exit_goldbees_strategy(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
