from etl_project.transform import json_to_dataframe

def test_transform_sample():
    # Minimal sample structure following Alpha Vantage shape
    raw = {
        "Meta Data": {"2. Symbol": "AAPL"},
        "Time Series (Daily)": {
            "2025-10-01": {
                "1. open": "100.0",
                "2. high": "110.0",
                "3. low": "95.0",
                "4. close": "105.0",
                "5. volume": "123456"
            },
            "2025-10-02": {
                "1. open": "105.0",
                "2. high": "112.0",
                "3. low": "101.0",
                "4. close": "102.0",
                "5. volume": "654321"
            }
        }
    }
    df = json_to_dataframe(raw)
    assert list(df.columns) == ["symbol","date","open","high","low","close","volume","daily_change_percentage"]
    assert df.iloc[0]["symbol"] == "AAPL"
    assert df.iloc[0]["date"] == "2025-10-01"
    #  (105-100)/100*100 = 5, (102-105)/105*100 ≈ -2.857...
    assert round(df.iloc[0]["daily_change_percentage"], 2) == 5.00
    assert round(df.iloc[1]["daily_change_percentage"], 2) == -2.86