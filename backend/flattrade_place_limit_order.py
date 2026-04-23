"""
Manual FlatTrade LIMIT order test.

Dry run:
  python3 flattrade_place_limit_order.py \
    --broker_doc_id 69e8f91d0e59052e21916992 \
    --symbol "NIFTY..." --side BUY --qty 75 --price 1.00

Place real order:
  python3 flattrade_place_limit_order.py \
    --broker_doc_id 69e8f91d0e59052e21916992 \
    --symbol "NIFTY..." --side BUY --qty 75 --price 1.00 --place
"""

from __future__ import annotations

import argparse
import sys

from bson import ObjectId

from features.flattrade_broker import get_flattrade_instance
from features.mongo_data import MongoData


def _load_broker_doc(broker_doc_id: str) -> dict:
    db = MongoData()
    try:
        doc = db._db["broker_configuration"].find_one({"_id": ObjectId(broker_doc_id)})
    finally:
        db.close()
    if not doc:
        raise SystemExit(f"Broker document not found: {broker_doc_id}")
    return doc


def main() -> int:
    parser = argparse.ArgumentParser(description="Place a manual FlatTrade LIMIT option order")
    parser.add_argument("--broker_doc_id", required=True, help="Mongo broker_configuration _id")
    parser.add_argument("--symbol", required=True, help="FlatTrade trading symbol / tsym exactly as broker expects")
    parser.add_argument("--exchange", default="NFO", help="NFO or BFO. Default: NFO")
    parser.add_argument("--side", choices=["BUY", "SELL"], required=True)
    parser.add_argument("--qty", type=int, required=True)
    parser.add_argument("--price", type=float, required=True)
    parser.add_argument("--product", choices=["NRML", "MIS"], default="NRML")
    parser.add_argument("--validity", default="DAY", help="FlatTrade ret value, e.g. DAY or EOS")
    parser.add_argument("--place", action="store_true", help="Actually place the order. Omit for dry-run.")
    args = parser.parse_args()

    broker_doc = _load_broker_doc(args.broker_doc_id)
    user_id = str(broker_doc.get("user_id") or "").strip()
    access_token = str(broker_doc.get("access_token") or "").strip()

    if not user_id or not access_token:
        raise SystemExit("FlatTrade broker is not connected: missing user_id/access_token in DB")

    order = {
        "tradingsymbol": args.symbol,
        "exchange": args.exchange,
        "transaction_type": args.side,
        "quantity": args.qty,
        "order_type": "LIMIT",
        "product": args.product,
        "price": args.price,
        "validity": args.validity,
    }

    print("FlatTrade LIMIT order test")
    print("Broker doc:", args.broker_doc_id)
    print("User id:", user_id)
    print("Order:", order)

    if not args.place:
        print("DRY RUN ONLY. Add --place to send this order to FlatTrade.")
        return 0

    broker = get_flattrade_instance(user_id, access_token)
    if not broker:
        raise SystemExit("Unable to create FlatTrade broker instance")

    order_id = broker.place_order(**order)
    print("ORDER_PLACED:", order_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
