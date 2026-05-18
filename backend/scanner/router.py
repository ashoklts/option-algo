from typing import Any, List, Optional, Union

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .service import generate_stock_scores, get_indexes, get_sectors

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


@router.get("/indexes")
async def scanner_indexes() -> dict[str, Any]:
    return {"status": "success", "items": get_indexes()}


@router.get("/sectors")
async def scanner_sectors() -> dict[str, Any]:
    return {"status": "success", "items": get_sectors()}


@router.post("/eod_scoring")
async def scanner_eod_scoring(body: EodScoringRequest) -> dict[str, Any]:
    try:
        result = generate_stock_scores(body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result
