from __future__ import annotations
import argparse
import json
import os
from typing import List, Dict
from .extract import fetch_and_save_raw, SYMBOLS_DEFAULT
from .transform import json_to_dataframe
from .load import upsert_dataframe

def run_etl(symbols: List[str] | None = None) -> Dict[str, int]:
    if symbols is None:
        symbols = SYMBOLS_DEFAULT
    saved = fetch_and_save_raw(symbols=symbols)
    total_inserted = 0
    for sym, path in saved.items():
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        df = json_to_dataframe(raw)
        inserted = upsert_dataframe(df)
        total_inserted += inserted
        print(f"{sym}: rows -> {len(df)}, inserted -> {inserted}")
    return {"inserted_total": total_inserted}

def parse_args():
    ap = argparse.ArgumentParser(description="Alpha Vantage Daily ETL")
    ap.add_argument("--symbols", nargs="*", help="Symbols to fetch (default: AAPL GOOG MSFT)")
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    run_etl(symbols=args.symbols if args.symbols else None)