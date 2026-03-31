import sys
import time
import json
import cProfile
sys.path.append("/home/ashok-innoppl/Documents/option-algo/backend")
from features.backtest_engine import run_backtest

def main():
    with open("/home/ashok-innoppl/Documents/option-algo/backend/current_backtest_request.json") as f:
        req = json.load(f)

    # Let's run for full Oct 2024 to mimic 1 month (approx 22 days)
    req["start_date"] = "2024-10-01"
    req["end_date"] = "2024-10-31"

    t0 = time.time()
    res = run_backtest(req)
    t1 = time.time()
    print(f"Total time: {t1 - t0:.2f} seconds")

if __name__ == "__main__":
    main()
