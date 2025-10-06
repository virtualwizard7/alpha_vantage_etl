from __future__ import annotations
import os
import sqlite3
from datetime import datetime, timezone
from typing import Iterable
import pandas as pd
from .validation import StockDailyRecord

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "stock_data.db")

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS stock_daily_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    open_price REAL,
    high_price REAL,
    low_price REAL,
    close_price REAL,
    volume INTEGER,
    daily_change_percentage REAL,
    extraction_timestamp TEXT NOT NULL,
    UNIQUE(symbol, date)
);
"""

def ensure_table():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(CREATE_SQL)
        conn.commit()

def upsert_dataframe(df: pd.DataFrame) -> int:
    """Insert DataFrame rows into the DB. Enforces uniqueness on (symbol, date).
    Returns number of rows inserted (skips duplicates).
    """
    ensure_table()
    inserted = 0
    now_ts = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        for _, row in df.iterrows():
            record = StockDailyRecord(
                symbol=row["symbol"],
                date=row["date"],
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(row["volume"]),
                daily_change_percentage=float(row["daily_change_percentage"]),
            )
            try:
                conn.execute(
                    """INSERT INTO stock_daily_data
                    (symbol, date, open_price, high_price, low_price, close_price, volume, daily_change_percentage, extraction_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.symbol,
                        record.date,
                        record.open,
                        record.high,
                        record.low,
                        record.close,
                        record.volume,
                        record.daily_change_percentage,
                        now_ts,
                    )
                )
                inserted += 1
            except sqlite3.IntegrityError:
                # Duplicate (symbol, date) -> skip
                pass
        conn.commit()
    return inserted