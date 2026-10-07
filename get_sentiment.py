import pandas as pd
import requests
import time
import pandas_market_calendars as mcal
from datetime import datetime, timedelta

API_KEY = 'S5kCxs5BX9z5qo149csn'
DATASET_URL = "https://data.nasdaq.com/api/v3/datatables/NDAQ/RTAT.json"

# Load NYSE calendar
nyse = mcal.get_calendar('NYSE')
schedule = nyse.valid_days(start_date="2000-01-01", end_date=datetime.today().strftime("%Y-%m-%d"))
schedule = schedule.tz_localize(None)

def get_valid_trading_day(date_str):
    try:
        date_obj = datetime.strptime(str(date_str), "%Y-%m-%d")
    except ValueError:
        return None
    while date_obj not in schedule:
        date_obj -= timedelta(days=1)
    return date_obj.strftime("%Y-%m-%d")

# Load and clean input
input_df = pd.read_csv("input.csv", header=None, names=["ticker", "date"])
print(f"✅ Loaded {len(input_df)} rows from input.csv")

input_df["date"] = pd.to_datetime(input_df["date"], errors="coerce")
input_df = input_df.dropna(subset=["date"])
input_df["date"] = input_df["date"].dt.strftime("%Y-%m-%d")

results = []

for _, row in input_df.iterrows():
    ticker = row["ticker"]
    original_date = row["date"]
    date = get_valid_trading_day(original_date)

    if date is None:
        print(f"⚠️ Skipping {ticker} — invalid date: {original_date}")
        results.append({"ticker": ticker, "original_date": original_date, "used_date": None})
        continue

    print(f"📊 Requesting {ticker} on {original_date} (using {date})")

    params = {
        "ticker": ticker,
        "date": date,
        "api_key": API_KEY
    }

    try:
        response = requests.get(DATASET_URL, params=params)
        response.raise_for_status()
        data = response.json()

        datatable = data.get("datatable", {})
        records = datatable.get("data", [])
        columns = [col["name"] for col in datatable.get("columns", [])]

        if records:
            row_data = pd.DataFrame(records, columns=columns).iloc[0].to_dict()
            row_data.update({"ticker": ticker, "original_date": original_date, "used_date": date})
            results.append(row_data)
        else:
            print(f"⚠️ No data for {ticker} on {date}")
            results.append({
                "ticker": ticker,
                "original_date": original_date,
                "used_date": date,
                "sentiment": None,
                "activity": None,
                "volume": None,
                "retail_volume": None,
                "message_volume": None,
                "rank": None,
                "buzz_score": None,
                "net_flow": None
            })

    except Exception as e:
        print(f"❌ Error on {ticker} {date}: {e}")
        results.append({
            "ticker": ticker,
            "original_date": original_date,
            "used_date": date,
            "sentiment": None,
            "activity": None,
            "volume": None,
            "retail_volume": None,
            "message_volume": None,
            "rank": None,
            "buzz_score": None,
            "net_flow": None
        })

    time.sleep(0.2)

# Save to CSV
output_df = pd.DataFrame(results)
output_df.to_csv("sentiment_output.csv", index=False)
print("✅ All rows saved to sentiment_output.csv")
