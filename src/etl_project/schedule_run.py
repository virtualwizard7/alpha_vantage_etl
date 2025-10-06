from __future__ import annotations
import argparse
import time
import schedule
from .main import run_etl

def parse_args():
    ap = argparse.ArgumentParser(description="Schedule ETL daily with 'schedule' library")
    ap.add_argument("--time", default="09:00", help="HH:MM local time to run daily (default: 09:00)")
    return ap.parse_args()

def job():
    print("[scheduler] Running ETL job...")
    try:
        res = run_etl()
        print("[scheduler] Done:", res)
    except Exception as e:
        print("[scheduler] Error:", e)

if __name__ == "__main__":
    args = parse_args()
    schedule.every().day.at(args.time).do(job)
    print(f"[scheduler] Scheduled daily ETL at {args.time}. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(1)