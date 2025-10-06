from __future__ import annotations
import os
import time
import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any, List
import requests
from dotenv import load_dotenv

SYMBOLS_DEFAULT = ["AAPL", "GOOG", "MSFT"]
API_URL = "https://www.alphavantage.co/query"
RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "raw_data")

def _today_str_tbilisi() -> str:
    # Asia/Tbilisi per assignment context
    tz = ZoneInfo("Asia/Tbilisi")
    return datetime.now(tz).strftime("%Y-%m-%d")

def fetch_and_save_raw(symbols: List[str] | None = None, api_key: str | None = None) -> Dict[str, str]:
    """Fetch TIME_SERIES_DAILY for each symbol and save raw JSON to raw_data/{SYMBOL}_YYYY-MM-DD.json.
    Returns a map of symbol -> saved filepath.
    """
    load_dotenv()
    if symbols is None:
        symbols = SYMBOLS_DEFAULT
    if api_key is None:
        api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        raise RuntimeError("ALPHA_VANTAGE_API_KEY is missing. Set it in .env or environment.")

    os.makedirs(RAW_DIR, exist_ok=True)
    saved = {}
    run_date = _today_str_tbilisi()

    for i, sym in enumerate(symbols):
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": sym,
            "outputsize": "compact",
            "datatype": "json",
            "apikey": api_key,
        }
        resp = requests.get(API_URL, params=params, timeout=30)
        # Handle rate limits: if we hit a note field, sleep and retry once
        data = resp.json()
        if "Note" in data or "Information" in data:
            time.sleep(60)
            resp = requests.get(API_URL, params=params, timeout=30)
            data = resp.json()

        # Basic sanity check
        if "Time Series (Daily)" not in data:
            # Save anyway for debugging
            filepath = os.path.join(RAW_DIR, f"{sym}_{run_date}_ERROR.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            raise RuntimeError(f"Unexpected response for {sym}. Saved {filepath}")

        filepath = os.path.join(RAW_DIR, f"{sym}_{run_date}.json")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        saved[sym] = filepath

        # friendly spacing for free tier
        if i < len(symbols) - 1:
            time.sleep(15)

    return saved