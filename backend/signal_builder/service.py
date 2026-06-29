from __future__ import annotations

from datetime import datetime
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId

from features.mongo_data import MongoData

SIGNAL_INDICATOR_COLLECTION = "signal_indicators"
SIGNAL_STRATEGY_COLLECTION = "signal_strategies"
INDICATOR_SCHEMA_COLLECTION = "indicator_schemas"
TV_TOOL_CONDITION_COLLECTION = "signal_tv_tool_condition"
TV_INDICATOR_CONDITION_COLLECTION = "tv_indicator_alert_conditions"

# TradingView gates which alert conditions are offered based on the
# drawing tool's own category, not the individual tool — channel tools
# (Parallel Channel / Disjoint Angle / Flat Top-Bottom) all share the same
# 4 "enter/exit/inside/outside channel" conditions, while every other line
# tool here shares the same 5 crossing/comparison conditions. `tool_key`
# is TradingView's internal LineTool name, kept only as a human-readable
# join key — the row's own Mongo `_id` is what this app uses as the id.
TV_TOOL_CONDITIONS: list[dict[str, Any]] = [
    *(
        {
            "tool_key": tool_key,
            "tool_name": tool_name,
            "tool_group": "single_line",
            "conditions": [
                {"key": "cross", "label": "Crossing"},
                {"key": "cross_up", "label": "Crossing Up"},
                {"key": "cross_down", "label": "Crossing Down"},
                {"key": "greater", "label": "Greater Than"},
                {"key": "less", "label": "Less Than"},
            ],
        }
        for tool_key, tool_name in [
            ("LineToolTrendLine", "trendline"),
            ("LineToolRay", "ray"),
            ("LineToolInfoLine", "info line"),
            ("LineToolExtended", "extended line"),
            ("LineToolTrendAngle", "trend angle"),
            ("LineToolHorzLine", "horizontal line"),
            ("LineToolHorzRay", "horizontal ray"),
            ("LineToolVertLine", "vertical line"),
        ]
    ),
    *(
        {
            "tool_key": tool_key,
            "tool_name": tool_name,
            "tool_group": "channel",
            "conditions": [
                {"key": "enter_channel", "label": "Entering Channel"},
                {"key": "exit_channel", "label": "Exiting Channel"},
                {"key": "inside_channel", "label": "Inside Channel"},
                {"key": "outside_channel", "label": "Outside Channel"},
            ],
        }
        for tool_key, tool_name in [
            ("LineToolParallelChannel", "parallel channel"),
            ("LineToolFlatBottom", "flat top/bottom"),
            ("LineToolDisjointAngle", "disjoint channel"),
        ]
    ),
]


def _as_object_id(value: str) -> ObjectId:
    try:
        return ObjectId(value)
    except (InvalidId, TypeError) as exc:
        raise ValueError(f"Invalid indicator id: {value}") from exc


def _serialize(doc: dict[str, Any]) -> dict[str, Any]:
    doc["_id"] = str(doc["_id"])
    created_at = doc.get("created_at")
    if isinstance(created_at, datetime):
        doc["created_at"] = created_at.isoformat()
    return doc


def seed_indicator_schemas() -> dict[str, Any]:
    """Parse execute-order-stopped.json and upsert every indicator schema into MongoDB."""
    from dataclasses import asdict
    from .indicator_catalog import build_indicator_catalog

    # Force rebuild so we pick up the latest file
    build_indicator_catalog.cache_clear()
    catalog = build_indicator_catalog()

    db = MongoData()._db
    col = db[INDICATOR_SCHEMA_COLLECTION]
    seeded_at = datetime.utcnow()

    upserted = 0
    for defn in catalog:
        doc = {
            "schema_key": defn.schema_key,
            "label": defn.label,
            "status": defn.status,
            "access_tier": defn.access_tier,
            "tier_reason": defn.tier_reason,
            "fields": [asdict(f) for f in defn.fields],
            "seeded_at": seeded_at,
        }
        col.update_one(
            {"schema_key": defn.schema_key},
            {"$set": doc},
            upsert=True,
        )
        upserted += 1

    return {"status": "success", "upserted": upserted}


def get_indicator_schemas() -> list[dict[str, Any]]:
    """Return all indicator schemas from MongoDB; fall back to built-in catalog if empty."""
    db = MongoData()._db
    col = db[INDICATOR_SCHEMA_COLLECTION]
    items = list(col.find({}, {"_id": 0}).sort("label", 1))
    if items:
        return items

    # Fallback: build from file if DB is empty
    from dataclasses import asdict
    from .indicator_catalog import build_indicator_catalog
    catalog = build_indicator_catalog()
    return [
        {
            "schema_key": d.schema_key,
            "label": d.label,
            "status": d.status,
            "access_tier": d.access_tier,
            "tier_reason": d.tier_reason,
            "fields": [asdict(f) for f in d.fields],
        }
        for d in catalog
    ]


get_signal_schemas_catalog = get_indicator_schemas


def seed_tv_tool_conditions() -> dict[str, Any]:
    """Upsert the TradingView line-tool -> alert-condition mapping into MongoDB."""
    db = MongoData()._db
    col = db[TV_TOOL_CONDITION_COLLECTION]
    seeded_at = datetime.utcnow()

    upserted = 0
    for defn in TV_TOOL_CONDITIONS:
        doc = {**defn, "seeded_at": seeded_at}
        col.update_one(
            {"tool_key": defn["tool_key"]},
            {"$set": doc},
            upsert=True,
        )
        upserted += 1

    return {"status": "success", "upserted": upserted}


def get_tv_tool_conditions() -> list[dict[str, Any]]:
    """Return every tool -> alert-condition mapping row from MongoDB."""
    db = MongoData()._db
    items = list(db[TV_TOOL_CONDITION_COLLECTION].find({}).sort("tool_name", 1))
    return [_serialize(item) for item in items]


def get_tv_indicator_conditions() -> list[dict[str, Any]]:
    """Return every TradingView built-in indicator -> alert-condition row from MongoDB."""
    db = MongoData()._db
    items = list(db[TV_INDICATOR_CONDITION_COLLECTION].find({}, {"_id": 0}).sort("indicator", 1))
    return items


def save_signal_indicator(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict) or not payload:
        raise ValueError("payload is required.")
    if not str(payload.get("label") or "").strip():
        raise ValueError("label is required.")

    db = MongoData()._db
    doc = dict(payload)
    doc["created_at"] = datetime.utcnow()
    result = db[SIGNAL_INDICATOR_COLLECTION].insert_one(doc)
    return {"status": "success", "_id": str(result.inserted_id)}


def get_signal_indicators() -> list[dict[str, Any]]:
    db = MongoData()._db
    items = list(db[SIGNAL_INDICATOR_COLLECTION].find({}).sort("created_at", -1))
    return [_serialize(item) for item in items]


def delete_signal_indicator(indicator_id: str) -> dict[str, Any]:
    object_id = _as_object_id(indicator_id)
    db = MongoData()._db
    result = db[SIGNAL_INDICATOR_COLLECTION].delete_one({"_id": object_id})
    if result.deleted_count == 0:
        raise ValueError(f"Indicator not found: {indicator_id}")
    return {"status": "success", "deleted_id": indicator_id}


def save_signal_strategy(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict) or not payload:
        raise ValueError("payload is required.")
    if not str(payload.get("name") or "").strip():
        raise ValueError("name is required.")

    db = MongoData()._db
    doc = dict(payload)
    doc["created_at"] = datetime.utcnow()
    result = db[SIGNAL_STRATEGY_COLLECTION].insert_one(doc)
    return {"status": "success", "_id": str(result.inserted_id)}


def get_signal_strategies() -> list[dict[str, Any]]:
    db = MongoData()._db
    items = list(db[SIGNAL_STRATEGY_COLLECTION].find({}).sort("created_at", -1))
    return [_serialize(item) for item in items]


def delete_signal_strategy(strategy_id: str) -> dict[str, Any]:
    object_id = _as_object_id(strategy_id)
    db = MongoData()._db
    result = db[SIGNAL_STRATEGY_COLLECTION].delete_one({"_id": object_id})
    if result.deleted_count == 0:
        raise ValueError(f"Strategy not found: {strategy_id}")
    return {"status": "success", "deleted_id": strategy_id}
