"""
insert_lot_sizes.py
───────────────────
Insert historical lot size data into MongoDB.
Collection: stock_data.lot_sizes

Run once:
    python3 insert_lot_sizes.py
"""

from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME   = "stock_data"

LOT_SIZE_DATA = [
    # ── NIFTY ──────────────────────────────────────────────────────────────
    # 75 till 22 Jul'21
    {"underlying": "NIFTY", "from_date": "2000-01-01", "to_date": "2021-07-22", "lot_size": 75},
    # 50 till 25 Apr'24
    {"underlying": "NIFTY", "from_date": "2021-07-23", "to_date": "2024-04-25", "lot_size": 50},
    # 25 till 26 Dec'25
    {"underlying": "NIFTY", "from_date": "2024-04-26", "to_date": "2025-12-26", "lot_size": 25},
    # 75 till 30 Dec'25
    {"underlying": "NIFTY", "from_date": "2025-12-27", "to_date": "2025-12-30", "lot_size": 75},
    # 65 after that
    {"underlying": "NIFTY", "from_date": "2025-12-31", "to_date": "9999-12-31", "lot_size": 65},

    # ── BANKNIFTY ──────────────────────────────────────────────────────────
    # 25 till 20 Jul'23
    {"underlying": "BANKNIFTY", "from_date": "2000-01-01", "to_date": "2023-07-20", "lot_size": 25},
    # 15 from 21 Jul'23
    {"underlying": "BANKNIFTY", "from_date": "2023-07-21", "to_date": "9999-12-31", "lot_size": 15},

    # ── FINNIFTY ───────────────────────────────────────────────────────────
    # 40 till 23 Jul'24
    {"underlying": "FINNIFTY", "from_date": "2000-01-01", "to_date": "2024-07-23", "lot_size": 40},
    # 25 till 28 Jan'25
    {"underlying": "FINNIFTY", "from_date": "2024-07-24", "to_date": "2025-01-28", "lot_size": 25},
    # 65 till 30 Dec'25
    {"underlying": "FINNIFTY", "from_date": "2025-01-29", "to_date": "2025-12-30", "lot_size": 65},
    # 60 after that
    {"underlying": "FINNIFTY", "from_date": "2025-12-31", "to_date": "9999-12-31", "lot_size": 60},

    # ── MIDCPNIFTY ─────────────────────────────────────────────────────────
    # 75 till 22 Jul'24
    {"underlying": "MIDCPNIFTY", "from_date": "2000-01-01", "to_date": "2024-07-22", "lot_size": 75},
    # 50 till 27 Jan'25
    {"underlying": "MIDCPNIFTY", "from_date": "2024-07-23", "to_date": "2025-01-27", "lot_size": 50},
    # 120 till 26 Jun'25
    {"underlying": "MIDCPNIFTY", "from_date": "2025-01-28", "to_date": "2025-06-26", "lot_size": 120},
    # 140 till 30 Dec'25
    {"underlying": "MIDCPNIFTY", "from_date": "2025-06-27", "to_date": "2025-12-30", "lot_size": 140},
    # 120 after that
    {"underlying": "MIDCPNIFTY", "from_date": "2025-12-31", "to_date": "9999-12-31", "lot_size": 120},

    # ── SENSEX ─────────────────────────────────────────────────────────────
    # 10 till 3 Jan'25
    {"underlying": "SENSEX", "from_date": "2000-01-01", "to_date": "2025-01-03", "lot_size": 10},
    # 20 after that
    {"underlying": "SENSEX", "from_date": "2025-01-04", "to_date": "9999-12-31", "lot_size": 20},

    # ── BANKEX ─────────────────────────────────────────────────────────────
    # 15 till 27 Jan'25
    {"underlying": "BANKEX", "from_date": "2000-01-01", "to_date": "2025-01-27", "lot_size": 15},
    # 30 after that
    {"underlying": "BANKEX", "from_date": "2025-01-28", "to_date": "9999-12-31", "lot_size": 30},
]


def main():
    client = MongoClient(MONGO_URI)
    col    = client[DB_NAME]["lot_sizes"]

    # Drop and re-insert for clean state
    col.drop()
    result = col.insert_many(LOT_SIZE_DATA)
    print(f"Inserted {len(result.inserted_ids)} lot size records.")

    # Create index for fast lookup
    col.create_index([("underlying", 1), ("from_date", 1), ("to_date", 1)])
    print("Index created on (underlying, from_date, to_date).")

    # Quick verify
    test_cases = [
        ("NIFTY",      "2025-10-01"),   # expect 25
        ("NIFTY",      "2025-12-28"),   # expect 75
        ("NIFTY",      "2026-01-01"),   # expect 65
        ("BANKNIFTY",  "2023-06-01"),   # expect 25
        ("BANKNIFTY",  "2023-08-01"),   # expect 15
        ("FINNIFTY",   "2025-06-01"),   # expect 65
        ("MIDCPNIFTY", "2025-04-01"),   # expect 120
        ("SENSEX",     "2025-01-01"),   # expect 10
        ("BANKEX",     "2025-03-01"),   # expect 30
    ]
    print("\nVerification:")
    for underlying, date_str in test_cases:
        doc = col.find_one({
            "underlying": underlying,
            "from_date":  {"$lte": date_str},
            "to_date":    {"$gte": date_str},
        })
        lot_size = doc["lot_size"] if doc else "NOT FOUND"
        print(f"  {underlying:12s} {date_str} → lot_size: {lot_size}")

    client.close()


if __name__ == "__main__":
    main()
