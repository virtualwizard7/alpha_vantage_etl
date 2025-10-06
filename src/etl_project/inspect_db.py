from __future__ import annotations
import argparse
import os
import sqlite3
from .load import DB_PATH

def parse_args():
    ap = argparse.ArgumentParser(description="Inspect records from stock_daily_data table")
    ap.add_argument("--limit", type=int, default=10)
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    if not os.path.exists(DB_PATH):
        print("DB not found:", DB_PATH)
        raise SystemExit(1)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT symbol, date, open_price, close_price, daily_change_percentage FROM stock_daily_data ORDER BY date DESC LIMIT ?", (args.limit,))
        rows = cur.fetchall()
        for r in rows:
            print(r)