import sys
import time
sys.path.append("/home/ashok-innoppl/Documents/option-algo/backend")
import concurrent.futures
from features.mongo_data import MongoData
from features.backtest_engine import DataIndex

trading_days = ['2024-10-01', '2024-10-03', '2024-10-04', '2024-10-07', '2024-10-08', '2024-10-09', '2024-10-10', '2024-10-11', '2024-10-14', '2024-10-15', '2024-10-16', '2024-10-17']

def fetch_and_index_db(day, underlying):
    from features.mongo_data import MongoData
    db = MongoData()
    r = db.load_day(day, underlying)
    idx = DataIndex(r) if r else None
    db.close()
    return idx

def test_thread():
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futures = {day: ex.submit(fetch_and_index_db, day, "NIFTY") for day in trading_days}
        results = {day: f.result() for day, f in futures.items()}
    return time.time() - t0

def test_process():
    t0 = time.time()
    with concurrent.futures.ProcessPoolExecutor(max_workers=5) as ex:
        futures = {day: ex.submit(fetch_and_index_db, day, "NIFTY") for day in trading_days}
        results = {day: f.result() for day, f in futures.items()}
    return time.time() - t0

if __name__ == "__main__":
    t_thread = test_thread()
    print(f"ThreadPool: {t_thread:.2f}s")
    t_process = test_process()
    print(f"ProcessPool: {t_process:.2f}s")
