"""
append_india_vix_to_option_chain_index_spot.py
──────────────────────────────────────────────
Append all documents from `india_vix` into `option_chain_index_spot`.

Run:
    python3 append_india_vix_to_option_chain_index_spot.py
"""

from features.mongo_data import MongoData

SOURCE_COLLECTION = "india_vix"
TARGET_COLLECTION = "option_chain_index_spot"
BATCH_SIZE = 5000


def _iter_docs(collection):
    for doc in collection.find({}):
        doc.pop("_id", None)
        yield doc


def main() -> None:
    db = MongoData()
    source = db._db[SOURCE_COLLECTION]
    target = db._db[TARGET_COLLECTION]

    inserted_total = 0
    batch = []

    for doc in _iter_docs(source):
        batch.append(doc)
        if len(batch) >= BATCH_SIZE:
            result = target.insert_many(batch)
            inserted_total += len(result.inserted_ids)
            batch = []

    if batch:
        result = target.insert_many(batch)
        inserted_total += len(result.inserted_ids)

    print(
        f"Appended {inserted_total} documents from {SOURCE_COLLECTION} "
        f"to {TARGET_COLLECTION}."
    )
    print(f"{TARGET_COLLECTION} total count: {target.count_documents({})}")

    db.close()


if __name__ == "__main__":
    main()
