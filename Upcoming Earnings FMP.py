import requests
import pandas as pd
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
FMP_API_KEY = "bCNaHtlk6fJfMAglG5ENlprN3Dj0L9mG"

DAYS_AHEAD = 14          # how far forward to pull the earnings calendar
PREV_DAYS = 6            # trading days of price/volume history before the earnings date
US_ONLY = True           # drop foreign listings (symbols with "." like RMS.PA)
MAX_WORKERS = 10         # keep modest to stay under FMP rate limits

OUTPUT_FILE = r"C:\Users\mlh50\OneDrive\Stocks\Stock Final Files\UpcomingEarningsFMP.csv"

BASE_V3 = "https://financialmodelingprep.com/api/v3"
BASE_STABLE = "https://financialmodelingprep.com/stable"

# =========================
# FUNCTIONS
# =========================
def fmp_get(url, params=None):
    params = dict(params or {})
    params["apikey"] = FMP_API_KEY
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}: {r.text[:200]}"
        return r.json(), None
    except Exception as e:
        return None, str(e)


def get_earnings_calendar(from_date, to_date):
    """Returns df with: symbol, earnings_date, timing, eps_estimate, eps_actual,
    revenue_estimate, revenue_actual."""
    params = {"from": from_date, "to": to_date}

    # v3 endpoint includes the bmo/amc "time" field
    data, err = fmp_get(f"{BASE_V3}/earning_calendar", params)

    if isinstance(data, list) and data:
        df = pd.DataFrame(data)
        df["timing"] = (
            df.get("time", pd.Series(index=df.index, dtype=object))
            .astype(str).str.lower()
            .map({"bmo": "BMO", "amc": "AMC"})
            .fillna("Unknown")
        )
        df = df.rename(columns={
            "date": "earnings_date",
            "eps": "eps_actual",
            "epsEstimated": "eps_estimate",
            "revenue": "revenue_actual",
            "revenueEstimated": "revenue_estimate",
        })
        return df, None

    # fallback: stable endpoint (no bmo/amc field)
    data, err2 = fmp_get(f"{BASE_STABLE}/earnings-calendar", params)
    if isinstance(data, list) and data:
        df = pd.DataFrame(data)
        df["timing"] = "Unknown"
        df = df.rename(columns={
            "date": "earnings_date",
            "epsActual": "eps_actual",
            "epsEstimated": "eps_estimate",
            "revenueActual": "revenue_actual",
            "revenueEstimated": "revenue_estimate",
        })
        return df, None

    return None, f"v3: {err} | stable: {err2}"


def get_profiles(symbols):
    """Batch company profiles. Returns dict: symbol -> profile fields."""
    profiles = {}
    chunk_size = 50

    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        data, err = fmp_get(f"{BASE_V3}/profile/{','.join(chunk)}")

        if not isinstance(data, list):
            # fallback: stable profile, one symbol at a time
            for sym in chunk:
                d, _ = fmp_get(f"{BASE_STABLE}/profile", {"symbol": sym})
                if isinstance(d, list) and d:
                    data = d
                    profiles[sym] = _profile_fields(d[0])
            continue

        for p in data:
            profiles[p.get("symbol")] = _profile_fields(p)

    return profiles


def _profile_fields(p):
    return {
        "company_name": p.get("companyName"),
        "sector": p.get("sector"),
        "industry": p.get("industry"),
        "exchange": p.get("exchangeShortName") or p.get("exchange"),
        "market_cap": p.get("mktCap") or p.get("marketCap"),
    }


def get_prev_days(symbol, earnings_date):
    """Last PREV_DAYS trading days strictly before earnings_date.
    Returns flat dict: prev1_* (most recent) ... prevN_*."""
    ed = pd.to_datetime(earnings_date).date()
    from_date = (ed - timedelta(days=30)).strftime("%Y-%m-%d")
    to_date = (ed - timedelta(days=1)).strftime("%Y-%m-%d")

    data, err = fmp_get(
        f"{BASE_V3}/historical-price-full/{symbol}",
        {"from": from_date, "to": to_date},
    )
    bars = data.get("historical") if isinstance(data, dict) else None

    if not bars:
        # fallback: stable endpoint
        data, err2 = fmp_get(
            f"{BASE_STABLE}/historical-price-eod/full",
            {"symbol": symbol, "from": from_date, "to": to_date},
        )
        bars = data if isinstance(data, list) else None
        if not bars:
            return {"price_error": f"{err} | {err2}"}

    bars = sorted(bars, key=lambda b: b["date"], reverse=True)
    bars = [b for b in bars if b["date"] < str(ed)][:PREV_DAYS]

    out = {"price_error": None}
    for n, b in enumerate(bars, start=1):
        out[f"prev{n}_date"] = b.get("date")
        out[f"prev{n}_open"] = b.get("open")
        out[f"prev{n}_high"] = b.get("high")
        out[f"prev{n}_low"] = b.get("low")
        out[f"prev{n}_close"] = b.get("close")
        out[f"prev{n}_volume"] = b.get("volume")

    vols = [b.get("volume") for b in bars if b.get("volume") is not None]
    out["prev_avg_volume"] = sum(vols) / len(vols) if vols else None
    return out


def process_row(idx, row, profiles):
    symbol = row["symbol"]

    base = {
        "symbol": symbol,
        "earnings_date": row["earnings_date"],
        "timing": row["timing"],
        "eps_estimate": row.get("eps_estimate"),
        "eps_actual": row.get("eps_actual"),
        "revenue_estimate": row.get("revenue_estimate"),
        "revenue_actual": row.get("revenue_actual"),
    }

    prof = profiles.get(symbol, {})
    base["company_name"] = prof.get("company_name")
    base["sector"] = prof.get("sector")
    base["industry"] = prof.get("industry")
    base["exchange"] = prof.get("exchange")
    base["market_cap"] = prof.get("market_cap")

    base.update(get_prev_days(symbol, row["earnings_date"]))
    return idx, base


# =========================
# MAIN
# =========================
today = date.today()
from_date = today.strftime("%Y-%m-%d")
to_date = (today + timedelta(days=DAYS_AHEAD)).strftime("%Y-%m-%d")

print(f"Pulling earnings calendar {from_date} to {to_date}...")
cal, err = get_earnings_calendar(from_date, to_date)

if cal is None:
    raise SystemExit(f"Failed to pull earnings calendar: {err}")

cal["symbol"] = cal["symbol"].astype(str).str.upper().str.strip()

if US_ONLY:
    before = len(cal)
    cal = cal[~cal["symbol"].str.contains(r"\.", regex=True)].copy()
    print(f"US_ONLY filter: {before} -> {len(cal)} rows")

cal = cal.drop_duplicates(subset=["symbol", "earnings_date"]).reset_index(drop=True)
print(f"{len(cal)} earnings events found.")

symbols = sorted(cal["symbol"].unique().tolist())
print(f"Fetching profiles for {len(symbols)} symbols...")
profiles = get_profiles(symbols)

results = []
print(f"Pulling {PREV_DAYS}-day price/volume history with {MAX_WORKERS} workers...")

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [
        executor.submit(process_row, idx, row, profiles)
        for idx, row in cal.iterrows()
    ]

    for n, future in enumerate(as_completed(futures), start=1):
        idx, result = future.result()
        results.append((idx, result))

        if n % 100 == 0:
            print(f"Completed {n}/{len(cal)}")

# preserve calendar order (by date, then symbol)
results = [r for _, r in sorted(results, key=lambda x: x[0])]

out = pd.DataFrame(results)
out = out.sort_values(["earnings_date", "timing", "symbol"]).reset_index(drop=True)
out.to_csv(OUTPUT_FILE, index=False)

print("Done.")
print(f"Saved {len(out)} rows to: {OUTPUT_FILE}")
print(out["timing"].value_counts().to_string())
