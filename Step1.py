"""
SCRIPT #1 (NO INPUT FILE): Build earnings master table from FMP
Period: 2026-01-01 -> today
Output: earnings_master_2026-01-01_to_<today>.csv

What it does (one row per earnings event):
- Pull all earnings events in range (FMP earning_calendar)
- STRICT DE-DUPE: one row per (symbol, earnings_calendar_date)
- Add EPS actual/estimate
- Add Sector + Industry (FMP stable/profile)
- Add BMO/AMC:
    * Use calendar 'time' when present
    * Otherwise fallback to stable/earnings per symbol
- Pull DAILY OHLCV candles (FMP stable/historical-price-eod/full)
  and compute ONLY TRADING-DAY-BASED fields:
    * P0/P1/P2 (date/open/low/close; include volume for all 3)
    * buy_date + buy_open_price:
        - BMO => p0_open
        - AMC => p1_open
    * Previous 6 trading days (d1..d6) close+volume RELATIVE TO buy_date
- Adds audit columns:
    * earnings_trading_day (same as p0_date)
    * event_day (BMO=>p0_date, AMC=>p1_date) for sanity checks
"""

import os
import json
import time
import math
import hashlib
import random
import requests
import pandas as pd
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
FMP_API_KEY = "bCNaHtlk6fJfMAglG5ENlprN3Dj0L9mG"

START_DATE = "2026-01-01"             # inclusive
END_DATE   = date.today().isoformat()  # inclusive (today)

OUT_CSV = f"earnings_master_{START_DATE}_to_{END_DATE}.csv"
CACHE_DIR = "./cache_fmp"

# Concurrency (tune if you hit rate limits)
MAX_WORKERS_PROFILE = 12
MAX_WORKERS_DAILY   = 10
MAX_WORKERS_EARN    = 10

# Candle window buffers (for P2 and missing trading days)
DAILY_BUFFER_BEFORE_DAYS = 60
DAILY_BUFFER_AFTER_DAYS  = 15

# Checkpoints
WRITE_CHECKPOINTS = True
CHECKPOINT_EVERY_N_SYMBOLS = 150

# =========================
# FMP endpoints
# =========================
EARN_CAL_URL = "https://financialmodelingprep.com/api/v3/earning_calendar"
PROFILE_URL  = "https://financialmodelingprep.com/stable/profile"
DAILY_URL    = "https://financialmodelingprep.com/stable/historical-price-eod/full"
EARN_SYM_URL = "https://financialmodelingprep.com/stable/earnings"


# =========================
# Helpers: caching + http
# =========================
def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def cache_path(prefix: str, key: str) -> str:
    safe = sha1(key)
    return os.path.join(CACHE_DIR, prefix, f"{safe}.json")

def load_cache(prefix: str, key: str):
    fp = cache_path(prefix, key)
    if os.path.exists(fp):
        with open(fp, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def save_cache(prefix: str, key: str, payload):
    fp = cache_path(prefix, key)
    ensure_dir(os.path.dirname(fp))
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(payload, f)

def requests_get_json(url: str, params: dict, cache_prefix: str = None, cache_key: str = None,
                      timeout: int = 30, max_retries: int = 7):
    """
    Cached GET -> JSON with retry/backoff on 429/5xx.
    """
    if cache_prefix and cache_key:
        cached = load_cache(cache_prefix, cache_key)
        if cached is not None:
            return cached

    s = requests.Session()
    last_err = None
    for attempt in range(max_retries):
        try:
            r = s.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                data = r.json()
                if cache_prefix and cache_key:
                    save_cache(cache_prefix, cache_key, data)
                return data

            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep((2 ** attempt) + random.random())
                continue

            r.raise_for_status()

        except Exception as e:
            last_err = e
            time.sleep((2 ** attempt) + random.random())

    raise RuntimeError(f"GET failed after retries: {url} params={params} err={last_err}")

def daterange_month_chunks(start_iso: str, end_iso: str):
    """
    Yield (from,to) month-sized chunks inclusive.
    """
    start = datetime.fromisoformat(start_iso).date()
    end   = datetime.fromisoformat(end_iso).date()

    cur = start.replace(day=1)
    while cur <= end:
        nxt_month = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        chunk_start = max(start, cur)
        chunk_end   = min(end, nxt_month - timedelta(days=1))
        yield chunk_start.isoformat(), chunk_end.isoformat()
        cur = nxt_month

def normalize_bmo_amc(val):
    """
    Convert many possible strings to 'BMO' / 'AMC' / ''.
    """
    if val is None:
        return ""
    s = str(val).strip().lower()
    if not s:
        return ""
    if "amc" in s or "after" in s or "close" in s:
        return "AMC"
    if "bmo" in s or "before" in s or "open" in s:
        return "BMO"
    return ""


# =========================
# Step 1: Earnings calendar
# =========================
def fetch_earnings_calendar_all(start_iso: str, end_iso: str) -> pd.DataFrame:
    frames = []
    for frm, to in daterange_month_chunks(start_iso, end_iso):
        params = {"from": frm, "to": to, "apikey": FMP_API_KEY}
        key = f"{EARN_CAL_URL}|{frm}|{to}"
        data = requests_get_json(EARN_CAL_URL, params, cache_prefix="earn_cal", cache_key=key)

        if isinstance(data, list) and data:
            frames.append(pd.DataFrame(data))

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    return df


def strict_dedupe_earnings(df: pd.DataFrame) -> pd.DataFrame:
    """
    STRICT: One row per (symbol, earnings_calendar_date).
    Keep best row by:
      1) eps_actual present
      2) time present (helps BMO/AMC)
      3) otherwise first
    """
    # Standardize columns we might use for ranking
    if "eps_actual" not in df.columns:
        df["eps_actual"] = None
    if "time" not in df.columns:
        df["time"] = None

    df["_has_eps"] = df["eps_actual"].notna().astype(int)
    df["_has_time"] = df["time"].notna().astype(int)

    # Sort so best row is first per key
    df = df.sort_values(
        ["symbol", "earnings_calendar_date", "_has_eps", "_has_time"],
        ascending=[True, True, False, False]
    )

    before = len(df)
    df = df.drop_duplicates(subset=["symbol", "earnings_calendar_date"], keep="first").copy()
    after = len(df)

    df = df.drop(columns=["_has_eps", "_has_time"], errors="ignore")
    print(f"De-dupe: {before:,} -> {after:,} rows (removed {before-after:,} duplicate calendar rows)")
    return df


# =========================
# Profiles + earnings timing
# =========================
def fetch_profile(symbol: str) -> dict:
    params = {"symbol": symbol, "apikey": FMP_API_KEY}
    key = f"{PROFILE_URL}|{symbol}"
    data = requests_get_json(PROFILE_URL, params, cache_prefix="profile", cache_key=key)

    if isinstance(data, list) and data:
        return data[0]
    return {}

def fetch_earnings_reports(symbol: str) -> list:
    params = {"symbol": symbol, "apikey": FMP_API_KEY}
    key = f"{EARN_SYM_URL}|{symbol}"
    data = requests_get_json(EARN_SYM_URL, params, cache_prefix="earn_sym", cache_key=key)
    if isinstance(data, list):
        return data
    return []


# =========================
# Daily candles + trading-day logic
# =========================
def fetch_daily(symbol: str) -> pd.DataFrame:
    params = {"symbol": symbol, "apikey": FMP_API_KEY}
    key = f"{DAILY_URL}|{symbol}"
    data = requests_get_json(DAILY_URL, params, cache_prefix="daily", cache_key=key)

    if not isinstance(data, list):
        return pd.DataFrame()

    df = pd.DataFrame(data)
    if df.empty or "date" not in df.columns:
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values("date").reset_index(drop=True)
    return df

def build_trading_index(daily_df: pd.DataFrame):
    dates = daily_df["date"].tolist()
    idx_by_date = {d: i for i, d in enumerate(dates)}
    return dates, idx_by_date

def find_first_trading_idx_on_or_after(dates, target_date: date):
    # binary search first date >= target_date
    lo, hi = 0, len(dates) - 1
    pos = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if dates[mid] >= target_date:
            pos = mid
            hi = mid - 1
        else:
            lo = mid + 1
    return pos

def prev_trading_days(dates, idx, n):
    out = []
    for k in range(1, n + 1):
        j = idx - k
        if j < 0:
            break
        out.append((dates[j], j))
    return out

def next_trading_idx(idx, n=1):
    if idx is None:
        return None
    return idx + n

def ohlcv_at(daily: pd.DataFrame, idx: int):
    if idx is None or idx < 0 or idx >= len(daily):
        return {"date": None, "open": math.nan, "low": math.nan, "close": math.nan, "volume": math.nan}
    row = daily.iloc[idx]
    return {
        "date": row["date"],
        "open": float(row.get("open", math.nan)),
        "low": float(row.get("low", math.nan)),
        "close": float(row.get("close", math.nan)),
        "volume": float(row.get("volume", math.nan)),
    }


# =========================
# Main
# =========================
def main():
    ensure_dir(CACHE_DIR)
    print(f"Building earnings master from {START_DATE} to {END_DATE}")

    # ---- Pull calendar
    print("Step 1/4: Pull earnings calendar...")
    cal = fetch_earnings_calendar_all(START_DATE, END_DATE)
    if cal.empty:
        print("No earnings events returned. Exiting.")
        return

    # Standardize column names
    cal = cal.rename(columns={
        "date": "earnings_calendar_date",
        "symbol": "symbol",
        "eps": "eps_actual",
        "epsEstimated": "eps_estimated",
        "epsestimated": "eps_estimated",
        "epsActual": "eps_actual",
        "time": "time",
    })

    # Ensure required columns exist
    for c in ["symbol", "earnings_calendar_date", "eps_actual", "eps_estimated", "time"]:
        if c not in cal.columns:
            cal[c] = None

    cal["symbol"] = cal["symbol"].astype(str)
    cal["earnings_calendar_date"] = pd.to_datetime(cal["earnings_calendar_date"]).dt.date

    # Strict de-dupe
    cal = strict_dedupe_earnings(cal)

    symbols = sorted(set(cal["symbol"].dropna()))
    print(f"Earnings events: {len(cal):,} | Unique symbols: {len(symbols):,}")

    # ---- Profiles
    print("Step 2/4: Fetch profiles (sector/industry)...")
    profile_map = {}

    def profile_worker(sym):
        p = fetch_profile(sym)
        sector = p.get("sector") or p.get("Sector") or ""
        industry = p.get("industry") or p.get("Industry") or ""
        return sym, sector, industry

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PROFILE) as ex:
        futs = {ex.submit(profile_worker, s): s for s in symbols}
        for i, fut in enumerate(as_completed(futs), 1):
            sym, sector, industry = fut.result()
            profile_map[sym] = {"sector": sector, "industry": industry}
            if i % 500 == 0:
                print(f"  profiles: {i:,}/{len(symbols):,}")

    cal["sector"] = cal["symbol"].map(lambda s: profile_map.get(s, {}).get("sector", ""))
    cal["industry"] = cal["symbol"].map(lambda s: profile_map.get(s, {}).get("industry", ""))

    # ---- BMO/AMC timing
    print("Step 3/4: Build BMO/AMC timing map...")
    # 1) try calendar "time" field
    cal["bmo_amc_cal"] = cal["time"].apply(normalize_bmo_amc)
    calendar_has_any_timing = (cal["bmo_amc_cal"] != "").any()

    timing_map = {}  # symbol -> {date: 'BMO'/'AMC'}
    if calendar_has_any_timing:
        for sym, g in cal[["symbol", "earnings_calendar_date", "bmo_amc_cal"]].groupby("symbol"):
            m = {}
            for d, t in zip(g["earnings_calendar_date"], g["bmo_amc_cal"]):
                if t:
                    m[d] = t
            timing_map[sym] = m

    # 2) fallback: per-symbol stable/earnings if needed
    # We ONLY fetch per symbol if we still have blanks after calendar timing.
    need_fallback_syms = sorted(set(
        cal.loc[cal["bmo_amc_cal"] == "", "symbol"].tolist()
    ))

    if need_fallback_syms:
        print(f"  timing fallback needed for {len(need_fallback_syms):,} symbols (calendar timing missing)")
        def earn_worker(sym):
            rows = fetch_earnings_reports(sym)
            m = {}
            for r in rows:
                d_raw = r.get("date") or r.get("Date")
                if not d_raw:
                    continue
                try:
                    d = pd.to_datetime(d_raw).date()
                except Exception:
                    continue
                t_raw = r.get("time") or r.get("Time") or r.get("timing") or r.get("Timing") or r.get("releaseTime") or ""
                t = normalize_bmo_amc(t_raw)
                if t:
                    m[d] = t
            return sym, m

        with ThreadPoolExecutor(max_workers=MAX_WORKERS_EARN) as ex:
            futs = {ex.submit(earn_worker, s): s for s in need_fallback_syms}
            for i, fut in enumerate(as_completed(futs), 1):
                sym, m = fut.result()
                if sym not in timing_map:
                    timing_map[sym] = {}
                timing_map[sym].update(m)
                if i % 500 == 0:
                    print(f"  timing fallback: {i:,}/{len(need_fallback_syms):,}")

    def resolve_bmo_amc(sym, d, cal_val):
        if cal_val:
            return cal_val
        return timing_map.get(sym, {}).get(d, "")

    cal["bmo_amc"] = [
        resolve_bmo_amc(sym, d, calv)
        for sym, d, calv in zip(cal["symbol"], cal["earnings_calendar_date"], cal["bmo_amc_cal"])
    ]
    cal = cal.drop(columns=["bmo_amc_cal"], errors="ignore")

    # NOTE: If still blank, we keep it blank (UNKNOWN). You can decide later how to treat UNKNOWN.
    # We will NOT silently treat blank as BMO here; we’ll store buy fields as NaN if UNKNOWN.

    # ---- Daily features
    print("Step 4/4: Fetch daily candles + build trading-day features...")
    grouped = list(cal.groupby("symbol"))
    total_syms = len(grouped)

    start_buf = (datetime.fromisoformat(START_DATE).date() - timedelta(days=DAILY_BUFFER_BEFORE_DAYS))
    end_buf   = (datetime.fromisoformat(END_DATE).date() + timedelta(days=DAILY_BUFFER_AFTER_DAYS))

    def daily_worker(sym, gdf):
        daily = fetch_daily(sym)
        if daily.empty:
            return sym, []

        daily = daily[(daily["date"] >= start_buf) & (daily["date"] <= end_buf)].copy()
        if daily.empty:
            return sym, []

        dates, idx_by_date = build_trading_index(daily)

        rows = []
        for _, r in gdf.iterrows():
            ecal = r["earnings_calendar_date"]
            timing = (r.get("bmo_amc") or "").strip().upper()

            # P0 = first trading day on or after earnings calendar date
            p0_idx = idx_by_date.get(ecal)
            if p0_idx is None:
                p0_idx = find_first_trading_idx_on_or_after(dates, ecal)
            if p0_idx is None:
                continue  # cannot map to trading day

            p1_idx = next_trading_idx(p0_idx, 1)
            p2_idx = next_trading_idx(p0_idx, 2)

            p0 = ohlcv_at(daily, p0_idx)
            p1 = ohlcv_at(daily, p1_idx)
            p2 = ohlcv_at(daily, p2_idx)

            earnings_trading_day = p0["date"]  # audit column

            # Buy rule:
            # - BMO => buy at p0_open
            # - AMC => buy at p1_open
            # - UNKNOWN => leave blank (so you can filter later)
            if timing == "BMO":
                buy_date = p0["date"]
                buy_price = p0["open"]
                buy_idx = p0_idx
                event_day = p0["date"]
            elif timing == "AMC":
                buy_date = p1["date"]
                buy_price = p1["open"]
                buy_idx = p1_idx if p1["date"] is not None else None
                event_day = p1["date"]
            else:
                buy_date = None
                buy_price = math.nan
                buy_idx = None
                event_day = None

            # Previous 6 TRADING DAYS relative to buy_idx (exclusive)
            d_fields = {f"d{k}_date": None for k in range(1, 7)}
            d_fields.update({f"d{k}_close": math.nan for k in range(1, 7)})
            d_fields.update({f"d{k}_volume": math.nan for k in range(1, 7)})

            if buy_idx is not None and 0 <= buy_idx < len(dates):
                prev6 = prev_trading_days(dates, buy_idx, 6)  # d1..d6 (most recent first)
                for k, (d, idx) in enumerate(prev6, 1):
                    rr = daily.iloc[idx]
                    d_fields[f"d{k}_date"] = d
                    d_fields[f"d{k}_close"] = float(rr.get("close", math.nan))
                    d_fields[f"d{k}_volume"] = float(rr.get("volume", math.nan))

            row_out = {
                "symbol": sym,

                # earnings
                "earnings_calendar_date": ecal,
                "earnings_trading_day": earnings_trading_day,  # audit
                "bmo_amc": timing,
                "eps_actual": r.get("eps_actual", None),
                "eps_estimated": r.get("eps_estimated", None),
                "sector": r.get("sector", ""),
                "industry": r.get("industry", ""),

                # P0/P1/P2 (daily candles; trading days only)
                "p0_date": p0["date"], "p0_open": p0["open"], "p0_low": p0["low"], "p0_close": p0["close"], "p0_volume": p0["volume"],
                "p1_date": p1["date"], "p1_open": p1["open"], "p1_low": p1["low"], "p1_close": p1["close"], "p1_volume": p1["volume"],
                "p2_date": p2["date"], "p2_open": p2["open"], "p2_low": p2["low"], "p2_close": p2["close"], "p2_volume": p2["volume"],

                # buy anchor (your rule)
                "buy_date": buy_date,
                "buy_open_price": buy_price,
                "event_day": event_day,  # audit (BMO=>P0, AMC=>P1)
            }
            row_out.update(d_fields)
            rows.append(row_out)

        return sym, rows

    all_rows = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_DAILY) as ex:
        futs = {ex.submit(daily_worker, sym, gdf.copy()): sym for sym, gdf in grouped}
        done = 0
        for fut in as_completed(futs):
            sym = futs[fut]
            _, rows = fut.result()
            all_rows.extend(rows)
            done += 1

            if done % 100 == 0 or done == total_syms:
                print(f"  daily done: {done:,}/{total_syms:,} | rows so far: {len(all_rows):,}")

            if WRITE_CHECKPOINTS and done % CHECKPOINT_EVERY_N_SYMBOLS == 0:
                ck = pd.DataFrame(all_rows)
                ck_path = OUT_CSV.replace(".csv", f"_checkpoint_{done}.csv")
                ck.to_csv(ck_path, index=False)
                print(f"  checkpoint wrote: {ck_path}")

    out_df = pd.DataFrame(all_rows)
    if out_df.empty:
        print("No output rows produced (check API access / symbol coverage).")
        return

    out_df = out_df.sort_values(["earnings_calendar_date", "symbol"]).reset_index(drop=True)
    out_df.to_csv(OUT_CSV, index=False)

    print(f"\n✅ Wrote: {OUT_CSV} | rows={len(out_df):,}")

    # Quick sanity stats
    print("\nTiming distribution:")
    print(out_df["bmo_amc"].fillna("").replace("", "UNKNOWN").value_counts().head(10))

    # Verify no duplicates on (symbol, earnings_calendar_date)
    dup_ct = out_df.duplicated(subset=["symbol", "earnings_calendar_date"]).sum()
    print(f"\nDuplicate check (symbol, earnings_calendar_date): {dup_ct} duplicates")


if __name__ == "__main__":
    main()
