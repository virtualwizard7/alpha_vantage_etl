from __future__ import annotations
from typing import Dict, Any
import json
import pandas as pd

def json_to_dataframe(raw_json: Dict[str, Any]) -> pd.DataFrame:
    """Convert Alpha Vantage TIME_SERIES_DAILY JSON into a normalized DataFrame with:
    date, open, high, low, close, volume, daily_change_percentage
    """
    meta = raw_json.get("Meta Data", {})
    symbol = meta.get("2. Symbol") or meta.get("Symbol") or "UNKNOWN"

    ts = raw_json.get("Time Series (Daily)", {})
    records = []
    for date_str, ohlc in ts.items():
        try:
            open_p = float(ohlc.get("1. open"))
            high_p = float(ohlc.get("2. high"))
            low_p = float(ohlc.get("3. low"))
            close_p = float(ohlc.get("4. close"))
            volume = int(ohlc.get("5. volume"))
        except (TypeError, ValueError):
            # skip bad rows
            continue
        change_pct = ((close_p - open_p) / open_p) * 100 if open_p != 0 else 0.0
        records.append({
            "symbol": symbol,
            "date": date_str,
            "open": open_p,
            "high": high_p,
            "low": low_p,
            "close": close_p,
            "volume": volume,
            "daily_change_percentage": change_pct,
        })
    df = pd.DataFrame.from_records(records)
    # sort by date ascending
    if not df.empty:
        df = df.sort_values("date").reset_index(drop=True)
    return df