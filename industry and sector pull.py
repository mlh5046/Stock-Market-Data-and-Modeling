import os
import time
import traceback
import sys

try:
    import requests
    import pandas as pd
except ImportError as e:
    print("ERROR: Missing required library. Please run: pip install requests pandas")
    print(f"Details: {e}")
    input("Press Enter to exit...")
    sys.exit(1)

from datetime import datetime

# =========================
# CONFIG
# =========================
FMP_API_KEY = "bCNaHtlk6fJfMAglG5ENlprN3Dj0L9mG"

BASE_DIR = r"C:\Users\mlh50\OneDrive\Stocks\Stock Final Files"
INPUT_FILE = os.path.join(BASE_DIR, "industry and sector input file 6-24-26.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "industry and sector output file 6-24-26.csv")
LOG_FILE = os.path.join(BASE_DIR, "fmp_last6_run.log")

N_DAYS = 7
N_FORWARD = 2
TIMESERIES = 350
SLEEP_BETWEEN_CALLS = 0.05
MAX_RETRIES = 6

DATE_COL = "buy_date"
INDUSTRY_COL = "industry"
SECTOR_COL = "sector"

SESSION = requests.Session()
HIST_CACHE = {}

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except:
        pass


def safe_ticker(x):
    if x is None or pd.isna(x):
        return ""
    s = str(x).strip().upper()
    if s in ("", "NAN", "NONE"):
        return ""
    return s


def parse_date(x):
    if pd.isna(x):
        return None

    if isinstance(x, (pd.Timestamp, datetime)):
        return pd.to_datetime(x).date()

    s = str(x).strip()

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass

    try:
        return pd.to_datetime(s).date()
    except:
        return None


def fmp_get_json(url, params):

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            r = SESSION.get(url, params=params, timeout=30)

            if r.status_code == 429:
                time.sleep(0.5 * attempt)
                continue

            r.raise_for_status()

            return r.json()

        except Exception as e:

            if attempt == MAX_RETRIES:
                return {"_error": str(e)}

            time.sleep(0.3 * attempt)

    return {"_error": "unknown"}


def fetch_fmp_daily_history(ticker):

    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}"
    params = {"timeseries": TIMESERIES, "apikey": FMP_API_KEY}

    js = fmp_get_json(url, params)

    if js.get("_error"):
        raise RuntimeError(js["_error"])

    hist = js.get("historical")

    if not hist:
        return pd.DataFrame(columns=["date", "close", "volume"])

    df = pd.DataFrame(hist)

    out = pd.DataFrame({
        "date": pd.to_datetime(df["date"], errors="coerce").dt.date,
        "close": pd.to_numeric(df.get("close"), errors="coerce"),
        "volume": pd.to_numeric(df.get("volume"), errors="coerce")
    })

    out = (
        out
        .dropna(subset=["date"])
        .sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )

    return out


def get_hist_cached(ticker):

    if ticker == "":
        return pd.DataFrame(columns=["date", "close", "volume"])

    if ticker in HIST_CACHE:
        return HIST_CACHE[ticker]

    df = fetch_fmp_daily_history(ticker)

    HIST_CACHE[ticker] = df

    time.sleep(SLEEP_BETWEEN_CALLS)

    return df


def get_anchor_trading_day(df_hist, buy_date):

    df = df_hist[df_hist["date"] <= buy_date].sort_values("date")

    if df.empty:
        return None

    return df["date"].iloc[-1]


def get_last_n_days(df_hist, buy_date, n):

    anchor = get_anchor_trading_day(df_hist, buy_date)

    if anchor is None:
        return []

    df = df_hist[df_hist["date"] <= anchor].sort_values("date")

    tail = df.tail(n)

    return list(zip(
        tail["date"].tolist(),
        tail["close"].tolist(),
        tail["volume"].tolist()
    ))


def get_next_n_days(df_hist, buy_date, n):

    df = df_hist[df_hist["date"] > buy_date].sort_values("date")

    if df.empty:
        return []

    head = df.head(n)

    return list(zip(
        head["date"].tolist(),
        head["close"].tolist(),
        head["volume"].tolist()
    ))


def write_lastn_fields(out, prefix, series, n):

    series = list(series)[-n:]

    while len(series) < n:
        series = [(None, None, None)] + series

    for i, (d, c, v) in enumerate(series, start=1):

        out[f"{prefix}_d{i}_date"] = d.isoformat() if d else None
        out[f"{prefix}_d{i}_close"] = None if pd.isna(c) else float(c)
        out[f"{prefix}_d{i}_vol"] = None if pd.isna(v) else float(v)


def write_forward_fields(out, prefix, series, n):

    series = list(series)[:n]

    while len(series) < n:
        series.append((None, None, None))

    for i, (d, c, v) in enumerate(series, start=1):

        out[f"{prefix}_p{i}_date"] = d.isoformat() if d else None
        out[f"{prefix}_p{i}_close"] = None if pd.isna(c) else float(c)
        out[f"{prefix}_p{i}_vol"] = None if pd.isna(v) else float(v)


def main():

    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)

    with open(LOG_FILE, "w"):
        pass

    log("STARTING RUN")

    df = pd.read_csv(INPUT_FILE)

    cols_lower = {c.lower(): c for c in df.columns}

    date_col = cols_lower[DATE_COL.lower()]
    ind_col = cols_lower[INDUSTRY_COL.lower()]
    sec_col = cols_lower[SECTOR_COL.lower()]

    out_rows = []

    start = time.time()

    for idx, row in df.iterrows():

        try:

            buy_date = parse_date(row[date_col])

            t_ind = safe_ticker(row[ind_col])
            t_sec = safe_ticker(row[sec_col])

            hist_ind = get_hist_cached(t_ind)
            hist_sec = get_hist_cached(t_sec)

            ind_last = get_last_n_days(hist_ind, buy_date, N_DAYS)
            sec_last = get_last_n_days(hist_sec, buy_date, N_DAYS)

            ind_next = get_next_n_days(hist_ind, buy_date, N_FORWARD)
            sec_next = get_next_n_days(hist_sec, buy_date, N_FORWARD)

            out = dict(row)

            write_lastn_fields(out, "industry", ind_last, N_DAYS)
            write_lastn_fields(out, "sector", sec_last, N_DAYS)

            write_forward_fields(out, "industry", ind_next, N_FORWARD)
            write_forward_fields(out, "sector", sec_next, N_FORWARD)

            out_rows.append(out)

            if (idx + 1) % 250 == 0:
                log(f"Processed {idx+1} rows | elapsed {time.time()-start:.1f}s")

        except Exception as e:

            log(f"Row {idx} ERROR {e}")

            out = dict(row)

            write_lastn_fields(out, "industry", [], N_DAYS)
            write_lastn_fields(out, "sector", [], N_DAYS)

            write_forward_fields(out, "industry", [], N_FORWARD)
            write_forward_fields(out, "sector", [], N_FORWARD)

            out_rows.append(out)

    out_df = pd.DataFrame(out_rows)

    out_df.to_csv(OUTPUT_FILE, index=False)

    log("DONE")
    log(f"Rows written: {len(out_df)}")
    log(f"Unique tickers fetched: {len(HIST_CACHE)}")


if __name__ == "__main__":

    try:
        main()

    except Exception as e:

        log("FATAL ERROR")
        log(str(e))
        log(traceback.format_exc())

        input("Press Enter to exit...")

        sys.exit(1)