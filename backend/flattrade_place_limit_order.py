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

from features.flattrade_broker import _is_flattrade_doc, get_flattrade_instance
from features.mongo_data import MongoData


def _has_token(doc: dict) -> bool:
    return bool(str(
        doc.get("access_token")
        or doc.get("jKey")
        or doc.get("jkey")
        or doc.get("token")
        or ""
    ).strip())


def _load_broker_doc(broker_doc_id: str = "") -> dict:
    db = MongoData()
    try:
        collection = db._db["broker_configuration"]
        if broker_doc_id:
            doc = collection.find_one({"_id": ObjectId(broker_doc_id)})
        else:
            docs = list(collection.find({}))
            connected_flattrade_docs = [
                doc for doc in docs
                if _is_flattrade_doc(doc) and _has_token(doc)
            ]
            if len(connected_flattrade_docs) == 1:
                doc = connected_flattrade_docs[0]
            elif len(connected_flattrade_docs) > 1:
                ids = ", ".join(str(doc.get("_id")) for doc in connected_flattrade_docs)
                raise SystemExit(
                    "Multiple connected FlatTrade brokers found. "
                    f"Please pass --broker_doc_id. Available ids: {ids}"
                )
            else:
                doc = next((doc for doc in docs if _is_flattrade_doc(doc)), None)
    finally:
        db.close()
    if not doc:
        raise SystemExit(
            f"Broker document not found: {broker_doc_id}"
            if broker_doc_id
            else "Connected FlatTrade broker not found. Login FlatTrade first or pass --broker_doc_id."
        )
    return doc


def main() -> int:
    parser = argparse.ArgumentParser(description="Place a manual FlatTrade LIMIT option order")
    parser.add_argument("--broker_doc_id", default="", help="Mongo broker_configuration _id. Optional if one FlatTrade broker is connected.")
    parser.add_argument("--symbol", required=True, help="FlatTrade trading symbol / tsym exactly as broker expects")
    parser.add_argument("--exchange", default="NFO", help="NFO or BFO. Default: NFO")
    parser.add_argument("--side", choices=["BUY", "SELL"], required=True)
    parser.add_argument("--qty", type=int, required=True)
    parser.add_argument("--price", type=float, required=True)
    parser.add_argument("--product", choices=["NRML", "MIS"], default="NRML")
    parser.add_argument("--validity", default="DAY", help="FlatTrade ret value, e.g. DAY or EOS")
    parser.add_argument("--user-id", default="", help="Optional FlatTrade uid override, e.g. FT056897")
    parser.add_argument("--jkey", default="", help="Optional FlatTrade jKey override. Prefer DB token when possible.")
    parser.add_argument("--place", action="store_true", help="Actually place the order. Omit for dry-run.")
    args = parser.parse_args()

    broker_doc = _load_broker_doc(args.broker_doc_id)
    user_id = str(args.user_id or broker_doc.get("user_id") or broker_doc.get("clientid") or broker_doc.get("uid") or "").strip()
    access_token = str(
        args.jkey
        or broker_doc.get("access_token")
        or broker_doc.get("jKey")
        or broker_doc.get("jkey")
        or broker_doc.get("token")
        or ""
    ).strip()

    if not user_id or not access_token:
        visible_keys = sorted(k for k in broker_doc.keys() if k != "access_token")
        raise SystemExit(
            "FlatTrade broker is not connected: missing user_id/access_token in DB\n"
            f"Broker doc name: {broker_doc.get('name') or broker_doc.get('broker_name') or '-'}\n"
            f"Available keys: {visible_keys}\n"
            "Tip: verify the id from /algo/broker-configurations, or pass --user-id and --jkey for a one-off test."
        )

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
    print("Broker doc:", broker_doc.get("_id"))
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
