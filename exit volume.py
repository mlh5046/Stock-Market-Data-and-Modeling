#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, time, argparse
from datetime import datetime, timedelta

import requests
import pandas as pd
from dateutil import parser as dateparser

# -------------------- CONFIG --------------------
BASE_PATH   = r"C:\Users\mlh50\OneDrive\Stocks"
INPUT_PATH  = os.path.join(BASE_PATH, "additional exit data.csv")  # columns: ticker + (date OR buy date -1)
OUTPUT_PATH = os.path.join(BASE_PATH, "exit_volume_only.csv")
LOG_PATH    = os.path.join(BASE_PATH, "exit_volume_only.log")

# FMP key: override by setting environment variable FMP_API_KEY if desired
FMP_API_KEY = os.environ.get("FMP_API_KEY", "bCNaHtlk6fJfMAglG5ENlprN3Dj0L9mG")

# Fetch window buffer so we can always look back 2 trading days
LEAD_BUFFER_DAYS = 10

REQUEST_TIMEOUT  = 30
MAX_RETRIES      = 4
BACKOFF_BASE     = 1.6
# -------------------------------------------------

def log(line: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{ts}] {line}"
    print(msg, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass

def robust_parse_date(val):
    """Parse flexible date formats; return None if invalid."""
    if pd.isna(val): 
        return None
    if isinstance(val, datetime):
        return val.date()
    if hasattr(val, "date"):
        try:
            return val.date()
        except Exception:
            pass
    s = str(val).strip()
    # If already YYYY-MM-DD
    if len(s) >= 10 and s[4:5] == "-" and s[7:8] == "-":
        try:
            d = pd.to_datetime(s[:10], errors="coerce")
            return None if pd.isna(d) else d.date()
        except Exception:
            pass
    # Generic parse
    try:
        return dateparser.parse(s).date()
    except Exception:
        return None

def get_fmp_history(symbol: str, date_from, date_to) -> pd.DataFrame:
    """
    Fetch FULL daily bars for [date_from, date_to] inclusive.
    IMPORTANT: We do NOT use serietype=line (that omits volume).
    Returns DataFrame with 'date','volume' sorted ascending.
    """
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"
    params = {
        "from": date_from.strftime("%Y-%m-%d"),
        "to":   date_to.strftime("%Y-%m-%d"),
        "apikey": FMP_API_KEY,
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                hist = data.get("historical") or []
                if not hist:
                    return pd.DataFrame(columns=["date","volume"])
                df = pd.DataFrame(hist)
                # Normalize fields
                df["date"] = pd.to_datetime(df["date"]).dt.date
                if "volume" not in df.columns:
                    df["volume"] = pd.NA
                else:
                    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
                return df[["date","volume"]].sort_values("date").reset_index(drop=True)
            elif resp.status_code in (429, 500, 502, 503, 504):
                sleep_s = BACKOFF_BASE ** attempt
                log(f"{symbol}: HTTP {resp.status_code} (attempt {attempt}/{MAX_RETRIES}). Backing off {sleep_s:.1f}s")
                time.sleep(sleep_s)
            else:
                log(f"{symbol}: HTTP {resp.status_code} -> {resp.text[:200]}")
                break
        except Exception as e:
            sleep_s = BACKOFF_BASE ** attempt
            log(f"{symbol}: Request error {e} (attempt {attempt}/{MAX_RETRIES}). Backing off {sleep_s:.1f}s")
            time.sleep(sleep_s)
    return pd.DataFrame(columns=["date","volume"])

def main():
    # Fresh log
    try:
        if os.path.exists(LOG_PATH):
            os.remove(LOG_PATH)
    except Exception:
        pass

    log("=== exit_volume_only_core (KEEP ALL ROWS, ORDER PRESERVED) ===")

    if not os.path.exists(BASE_PATH):
        log(f"ERROR: Base path not found: {BASE_PATH}")
        return 2
    if not os.path.exists(INPUT_PATH):
        log(f"ERROR: Input file not found: {INPUT_PATH}")
        return 2

    parser = argparse.ArgumentParser(description="Fill vol_0, vol_-1, vol_-2 for each (ticker, date)")
    parser.add_argument("--limit", type=int, default=None, help="Process only first N rows (quick test)")
    args = parser.parse_args()

    # Read input (do NOT drop any rows; we keep blanks)
    try:
        df = pd.read_csv(INPUT_PATH)
    except Exception as e:
        log(f"ERROR reading CSV: {e}")
        return 2

    # Stable ordering index
    df["_row_order_"] = range(len(df))

    # Identify columns (accepts 'date' or 'buy date -1')
    lc = {c.lower(): c for c in df.columns}
    if "ticker" not in lc:
        log(f"ERROR: Missing 'ticker' column. Found: {list(df.columns)}")
        return 2

    date_col = None
    for cand in ("date", "buy date -1"):
        if cand in lc:
            date_col = lc[cand]
            break
    if date_col is None:
        log(f"ERROR: Missing a date column ('date' or 'buy date -1'). Found: {list(df.columns)}")
        return 2

    # Normalize tickers & dates but do NOT drop invalids
    df["ticker_norm"] = df[lc["ticker"]].astype(str).str.strip().str.upper()
    df["date_norm"]   = df[date_col].apply(robust_parse_date)

    # Optional limit (preserve input order)
    if args.limit:
        df = df.sort_values("_row_order_").head(args.limit).copy()
        log(f"Testing first {len(df)} rows (limit).")

    total = len(df)
    valid_dates = df["date_norm"].notna().sum()
    log(f"Loaded {total} rows. Valid dates: {valid_dates}. Invalid/blank dates will remain with blank volume outputs.")

    # Build per-ticker history windows over valid dates to minimize API calls
    per_ticker_dates = (
        df[df["date_norm"].notna()]
        .groupby("ticker_norm")["date_norm"]
        .agg(["min","max"])
        .reset_index()
    )
    # Cache for fetched histories
    hist_cache = {}

    def fetch_hist(sym, dmin, dmax):
        if sym in hist_cache:
            return hist_cache[sym]
        if pd.isna(dmin) or pd.isna(dmax):
            hist_cache[sym] = pd.DataFrame(columns=["date","volume"])
            return hist_cache[sym]
        date_from = dmin - timedelta(days=LEAD_BUFFER_DAYS)
        date_to   = dmax
        log(f"{sym}: fetching history {date_from}..{date_to}")
        hist_cache[sym] = get_fmp_history(sym, date_from, date_to)
        return hist_cache[sym]

    # Quick lookup for per-ticker date windows
    dwin = {r["ticker_norm"]: (r["min"], r["max"]) for _, r in per_ticker_dates.iterrows()}

    # Process in original order, row-by-row (preserves exact input order)
    out_rows = []
    for _, r in df.sort_values("_row_order_").iterrows():
        sym = r["ticker_norm"]
        d   = r["date_norm"]

        # If date invalid -> keep row with blanks
        if pd.isna(d):
            out_rows.append({
                "ticker": sym,
                "date": str(r[date_col]) if not pd.isna(r[date_col]) else "",
                "vol_0": pd.NA, "vol_-1": pd.NA, "vol_-2": pd.NA
            })
            continue

        # Ensure we have history for this ticker
        dmin, dmax = dwin.get(sym, (None, None))
        hist = fetch_hist(sym, dmin, dmax)
        if hist.empty:
            out_rows.append({
                "ticker": sym, "date": d.isoformat(),
                "vol_0": pd.NA, "vol_-1": pd.NA, "vol_-2": pd.NA
            })
            continue

        # Find the trading day on/before d (roll back)
        idx = None
        try:
            mask = hist["date"] <= d
            if mask.any():
                idx = mask[mask].index[-1]
        except Exception:
            idx = None

        if idx is None:
            out_rows.append({
                "ticker": sym, "date": d.isoformat(),
                "vol_0": pd.NA, "vol_-1": pd.NA, "vol_-2": pd.NA
            })
        else:
            vol_0  = hist.at[idx, "volume"]
            vol_m1 = hist.at[idx-1, "volume"] if idx-1 >= 0 else pd.NA
            vol_m2 = hist.at[idx-2, "volume"] if idx-2 >= 0 else pd.NA
            out_rows.append({
                "ticker": sym, "date": d.isoformat(),
                "vol_0": vol_0, "vol_-1": vol_m1, "vol_-2": vol_m2
            })

    out = pd.DataFrame(out_rows, columns=["ticker","date","vol_0","vol_-1","vol_-2"])
    out.to_csv(OUTPUT_PATH, index=False)
    log(f"Saved: {OUTPUT_PATH}")
    log("=== Done ===")
    return 0

if __name__ == "__main__":
    rc = 1
    try:
        rc = main()
    except Exception as e:
        import traceback
        print("FATAL:", e)
        print(traceback.format_exc())
        rc = 2
    finally:
        # Pause on double-click so the window doesn't vanish
        try:
            if sys.stdin is None or not sys.stdin.isatty():
                input("Press Enter to close...")
        except Exception:
            time.sleep(5)
    sys.exit(rc)
