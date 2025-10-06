# Batch 9 Data Engineering Intern Project — ETL Pipeline with Alpha Vantage

This repository implements a complete ETL pipeline in Python that fetches **daily stock data** for **AAPL**, **GOOG**, and **MSFT** from Alpha Vantage, transforms the data, and loads it into a SQLite database.

## Features
- **Extract**: Calls `TIME_SERIES_DAILY` and stores **raw JSON** into `raw_data/` as `SYMBOL_YYYY-MM-DD.json`
- **Transform**: Converts JSON to a clean pandas DataFrame with columns: `date, open, high, low, close, volume` and `daily_change_percentage`
- **Load**: Upserts into SQLite table `stock_daily_data` with uniqueness on `(symbol, date)`
- **Validation**: Pydantic model validates input rows before insert
- **Scheduling (optional)**: Daily run with the `schedule` library (or set up cron / Task Scheduler)

## Quickstart

### 1) Clone & set up
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Configure your API key
Create a `.env` file in the repo root:
```
ALPHA_VANTAGE_API_KEY=YOUR_KEY_HERE
```

Alternatively, set the environment variable:
```bash
export ALPHA_VANTAGE_API_KEY=YOUR_KEY_HERE
```

### 3) Run the ETL (all symbols)
```bash
python -m etl_project.main
```
To run for a specific set of symbols:
```bash
python -m etl_project.main --symbols AAPL GOOG MSFT
```

### 4) Run the scheduler (optional)
Runs once every day at 09:00 local time by default.
```bash
python -m etl_project.schedule_run --time 09:00
```

### 5) Inspect the database
The SQLite database is at `data/stock_data.db`.
```bash
python -m etl_project.inspect_db --limit 10
```

## Table Schema
`stock_daily_data`:
- `id` INTEGER PRIMARY KEY AUTOINCREMENT
- `symbol` TEXT NOT NULL
- `date` TEXT NOT NULL (ISO YYYY-MM-DD)
- `open_price` REAL
- `high_price` REAL
- `low_price` REAL
- `close_price` REAL
- `volume` INTEGER
- `daily_change_percentage` REAL
- `extraction_timestamp` TEXT NOT NULL (ISO 8601 with timezone)
- **UNIQUE(symbol, date)** to prevent duplicates

## Scheduling Examples

### Using cron (Linux/macOS)
Edit cron with `crontab -e`, then add (runs at 09:00 every day):
```
0 9 * * * /path/to/venv/bin/python -m etl_project.main >> /path/to/log.txt 2>&1
```

### Using Windows Task Scheduler
Create a basic task to run:
```
C:\path\to\venv\Scripts\python.exe -m etl_project.main
```
Set the trigger to daily at your desired time.

## Tests
Run unit tests:
```bash
pip install pytest
pytest
```

## Notes
- API free tier is rate-limited; if you hit limits, the script will back off and retry.
- Raw JSON files are stored per run day in `raw_data/` and can be reprocessed without another API call.