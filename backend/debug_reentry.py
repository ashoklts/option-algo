"""
Debug re-entry — minimal direct MongoDB queries, no bulk load.
"""
from features.mongo_data import MongoData

DAY    = "2025-10-03"
EXPIRY = "2025-10-07"
OTYPE  = "PE"

db = MongoData()

# 1. Spot at exit time
print("=== Spot at 09:54 ===")
doc = db._chain.find_one(
    {"timestamp": f"{DAY}T09:54:00", "underlying": "NIFTY"},
    {"spot_price": 1, "_id": 0}
)
print(doc)

spot = doc["spot_price"] if doc else None
step = 50
atm  = round(spot / step) * step if spot else None
print(f"New ATM = {atm}")

# 2. Entry candle at new strike
print(f"\n=== {atm} PE candle at 09:54 ===")
doc2 = db._chain.find_one(
    {"timestamp": f"{DAY}T09:54:00", "underlying": "NIFTY",
     "expiry": EXPIRY, "strike": atm, "type": OTYPE},
    {"close": 1, "_id": 0}
)
print(doc2)

# 3. Candle at 09:55
print(f"\n=== {atm} PE candle at 09:55 ===")
doc3 = db._chain.find_one(
    {"timestamp": f"{DAY}T09:55:00", "underlying": "NIFTY",
     "expiry": EXPIRY, "strike": atm, "type": OTYPE},
    {"close": 1, "_id": 0}
)
print(doc3)

# 4. Count all PE candles for new strike on this day
print(f"\n=== Total {atm} PE candles on {DAY} ===")
count = db._chain.count_documents(
    {"timestamp": {"$gte": f"{DAY}T00:00:00", "$lte": f"{DAY}T23:59:59"},
     "underlying": "NIFTY", "expiry": EXPIRY, "strike": atm, "type": OTYPE}
)
print(f"count = {count}")

# 5. Re-entry type in request
print("\n=== Re-entry config (PE leg) ===")
import json, pathlib
req = json.loads((pathlib.Path(__file__).parent / "current_backtest_request.json").read_text())
pe_leg = next(l for l in req["strategy"]["ListOfLegConfigs"] if "PE" in l["InstrumentKind"])
print("LegReentryTP:", pe_leg["LegReentryTP"])
print("LegReentrySL:", pe_leg["LegReentrySL"])

db.close()
