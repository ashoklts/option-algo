"""
update_india_vix_token.py
─────────────────────────
Backfill `token` field in MongoDB collection: stock_data.india_vix

Run:
    python3 update_india_vix_token.py
"""

from features.mongo_data import MongoData

COLLECTION_NAME = "india_vix"
TOKEN_VALUE = "NSE_00"


def main() -> None:
    db = MongoData()
    collection = db._db[COLLECTION_NAME]

    result = collection.update_many({}, {"$set": {"token": TOKEN_VALUE}})
    print(
        f"Matched {result.matched_count} records and updated "
        f"{result.modified_count} records in {COLLECTION_NAME}."
    )

    sample_doc = collection.find_one({"token": TOKEN_VALUE}, {"token": 1})
    print(f"Verification token value: {(sample_doc or {}).get('token')}")

    db.close()


if __name__ == "__main__":
    main()
