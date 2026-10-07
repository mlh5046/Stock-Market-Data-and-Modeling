import requests
import pandas as pd
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
FMP_API_KEY = "bCNaHtlk6fJfMAglG5ENlprN3Dj0L9mG"

START_DATE = "2026-03-01"
END_DATE = "2026-03-30"

PREV_DAYS = 6
US_ONLY = True
MAX_WORKERS = 10

OUTPUT_FILE = r"C:\Users\mlh50\OneDrive\Stocks\Stock Final Files\EarningsFMP_2026_03_01_to_2026_03_30.csv"

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
    params = {"from": from_date, "to": to_date}

    data, err = fmp_get(f"{BASE_V3}/earning_calendar", params)

    if isinstance(data, list) and data:
        df = pd.DataFrame(data)

        df["timing"] = (
            df.get("time", pd.Series(index=df.index, dtype=object))
            .astype(str)
            .str.lower()
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


def _profile_fields(p):
    return {
        "company_name": p.get("companyName"),
        "sector": p.get("sector"),
        "industry": p.get("industry"),
        "exchange": p.get("exchangeShortName") or p.get("exchange"),
        "market_cap": p.get("mktCap") or p.get("marketCap"),
    }


def get_profiles(symbols):
    profiles = {}
    chunk_size = 50

    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]

        data, err = fmp_get(f"{BASE_V3}/profile/{','.join(chunk)}")

        if isinstance(data, list):
            for p in data:
                sym = p.get("symbol")
                if sym:
                    profiles[sym.upper()] = _profile_fields(p)
        else:
            for sym in chunk:
                d, _ = fmp_get(f"{BASE_STABLE}/profile", {"symbol": sym})
                if isinstance(d, list) and d:
                    profiles[sym.upper()] = _profile_fields(d[0])

    return profiles


def get_prev_days(symbol, earnings_date):
    ed = pd.to_datetime(earnings_date).date()

    price_from_date = (ed - timedelta(days=30)).strftime("%Y-%m-%d")
    price_to_date = (ed - timedelta(days=1)).strftime("%Y-%m-%d")

    data, err = fmp_get(
        f"{BASE_V3}/historical-price-full/{symbol}",
        {"from": price_from_date, "to": price_to_date},
    )

    bars = data.get("historical") if isinstance(data, dict) else None

    if not bars:
        data, err2 = fmp_get(
            f"{BASE_STABLE}/historical-price-eod/full",
            {"symbol": symbol, "from": price_from_date, "to": price_to_date},
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
from_date = START_DATE
to_date = END_DATE

print(f"Pulling earnings calendar {from_date} to {to_date}...")

cal, err = get_earnings_calendar(from_date, to_date)

if cal is None:
    raise SystemExit(f"Failed to pull earnings calendar: {err}")

cal["symbol"] = cal["symbol"].astype(str).str.upper().str.strip()
cal["earnings_date"] = pd.to_datetime(cal["earnings_date"]).dt.strftime("%Y-%m-%d")

if US_ONLY:
    before = len(cal)
    cal = cal[~cal["symbol"].str.contains(r"\.", regex=True)].copy()
    print(f"US_ONLY filter: {before} -> {len(cal)} rows")

cal = cal.drop_duplicates(subset=["symbol", "earnings_date"]).reset_index(drop=True)

print("Earliest earnings_date in calendar:", cal["earnings_date"].min())
print("Latest earnings_date in calendar:", cal["earnings_date"].max())
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

results = [r for _, r in sorted(results, key=lambda x: x[0])]

out = pd.DataFrame(results)

out = out.sort_values(["earnings_date", "timing", "symbol"]).reset_index(drop=True)

out.to_csv(OUTPUT_FILE, index=False)

print("Done.")
print(f"Saved {len(out)} rows to: {OUTPUT_FILE}")

print("\nTiming counts:")
print(out["timing"].value_counts(dropna=False).to_string())

print("\nMissing price rows:")
print(out["price_error"].notna().sum())