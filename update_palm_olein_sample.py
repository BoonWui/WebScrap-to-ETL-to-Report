# -*- coding: utf-8 -*-
"""
Created on Thu Nov 13 16:27:49 2025

@author: bwlau
"""


# -*- coding: utf-8 -*-

import requests
import json
import re
import pandas as pd
from datetime import datetime, timedelta
import os

# === CONFIG ===
BASE_URL = ""
TRADE_MARKET_CODE = ""   
TRADE_CODE = ""             
ORG_CODE = ""             #
EXCEL_FILE = "futures_positions.xlsx"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://.com/futures/position.html"
}

# === HELPERS ===
def parse_json_or_jsonp(text):
    s = text.strip()
    if re.match(r'^[a-zA-Z0-9_]+\s*\(', s):
        s = s[s.find('(') + 1:s.rfind(')')]
    return json.loads(s)

def _normalize_colname(name):
    """Normalize column name for matching (upper, remove non-alnum)."""
    return re.sub(r'[^0-9A-Z]', '', name.upper())

def find_best_column(cols, preferred_list=None, positive_keywords=None, negative_keywords=None):
    """
    cols: list of actual column names
    preferred_list: list of exact names to try in order (case-insensitive)
    positive_keywords: list of keywords that should appear (substring match)
    negative_keywords: list of keywords that must NOT appear
    returns: matched column name or None
    """
    if preferred_list is None:
        preferred_list = []
    if positive_keywords is None:
        positive_keywords = []
    if negative_keywords is None:
        negative_keywords = []

    # 1) exact match attempts (case-insensitive)
    normalized_map = {_normalize_colname(c): c for c in cols}
    for pref in preferred_list:
        norm = _normalize_colname(pref)
        if norm in normalized_map:
            return normalized_map[norm]

    # 2) substring match for positive keywords and ensure negatives are not present
    for c in cols:
        norm = _normalize_colname(c)
        pos_ok = all(k.upper() in norm for k in positive_keywords) if positive_keywords else False
        neg_ok = all(k.upper() not in norm for k in negative_keywords) if negative_keywords else True
        if pos_ok and neg_ok:
            return c

    # 3) fallback: any column that contains any of the positive keywords (less strict)
    for c in cols:
        norm = _normalize_colname(c)
        for k in positive_keywords:
            if k.upper() in norm:
                # ensure not a negative keyword
                if all(nk.upper() not in norm for nk in negative_keywords):
                    return c

    return None

def compute_final_position(df):
    """
    Detect long/short columns, coerce to numeric, compute Final_Position = long - short.
    Prints which columns used.
    """
    cols = list(df.columns)

    # Common exact names to try first
    long_candidates_exact = ["NET_LONG_POSITION", "NET_LONG", "LONG_POSITION", "LONG_POS", "LONG", "多头持仓", "多头"]
    short_candidates_exact = ["NET_SHORT_POSITION", "NET_SHORT", "SHORT_POSITION", "SHORT_POS", "SHORT", "空头持仓", "空头"]

    # Try exact/near-exact matches
    long_col = find_best_column(cols, preferred_list=long_candidates_exact,
                                positive_keywords=["LONG", "多头", "多"], negative_keywords=["SHORT", "空"])
    short_col = find_best_column(cols, preferred_list=short_candidates_exact,
                                 positive_keywords=["SHORT", "空头", "空"], negative_keywords=["LONG", "多头", "多"])

    # If not found, try other heuristics: e.g., columns with "LONG" or Chinese '多' / '空'
    if not long_col:
        long_col = find_best_column(cols, positive_keywords=["LONG"]) or find_best_column(cols, positive_keywords=["多"])
    if not short_col:
        short_col = find_best_column(cols, positive_keywords=["SHORT"]) or find_best_column(cols, positive_keywords=["空"])

    # As an extra fallback, look for columns with 'POSITION' or '持仓' and assign by context / order (risky)
    if not long_col or not short_col:
        possible_pos = [c for c in cols if "POSITION" in _normalize_colname(c) or "持仓" in c]
        if possible_pos and (not long_col or not short_col):
            # if there's only one such column, we cannot split it — leave None
            if len(possible_pos) >= 2:
                # assume first = long, second = short (best-effort)
                if not long_col:
                    long_col = possible_pos[0]
                if not short_col and len(possible_pos) > 1:
                    short_col = possible_pos[1]

    print(f"Detected long_col = {long_col}, short_col = {short_col}")

    # convert to numeric safely
    def to_numeric_series(s):
        if s is None:
            return None
        # remove commas and whitespace, coerce to numeric
        return pd.to_numeric(s.astype(str).str.replace(',', '').str.strip(), errors='coerce')

    long_series = to_numeric_series(df[long_col]) if long_col in df.columns else None
    short_series = to_numeric_series(df[short_col]) if short_col in df.columns else None

    # compute final; prefer NaN if both sides are missing
    if long_series is None and short_series is None:
        df["Final_Position"] = pd.NA
    else:
        # treat missing side as 0 for arithmetic, but result will be numeric or NaN if both NaN
        long_filled = long_series.fillna(0) if long_series is not None else 0
        short_filled = short_series.fillna(0) if short_series is not None else 0
        df["Final_Position"] = long_filled - short_filled

    # show a sample for verification
    print("Sample Final_Position (first 5 rows):")
    print(df[["Final_Position"]].head(5))
    return df

# === FETCHER ===
def fetch_daily(date_str):
    params = {
        "callback": "",
        "reportName": "DAILYPOSITION",
        "columns": "ALL",
        "filter": f'(TRADE_MARKET_CODE="{TRADE_MARKET_CODE}")(TRADE_CODE="{TRADE_CODE}")(ORG_CODE="{ORG_CODE}")(TRADE_DATE=\'{date_str}\')',
        "sortColumns": "SECURITY_CODE",
        "sortTypes": "1",
        "pageNumber": "1",
        "pageSize": "200",
        "source": "WEB",
        "client": "WEB"
    }
    try:
        r = requests.get(BASE_URL, params=params, headers=headers, timeout=15)
        data = parse_json_or_jsonp(r.text)
        rows = data.get("result", {}).get("data") or data.get("data", {}).get("rows")
        if not rows:
            print(f"  ⚠️ No data for {date_str}")
            return None
        df = pd.DataFrame(rows)
        df["TRADE_DATE"] = date_str
        df = compute_final_position(df)
        print(f"  ✅ Retrieved {len(df)} rows for {date_str}")
        return df
    except Exception as e:
        print(f"  ❌ Error fetching {date_str}: {e}")
        return None

def get_last_trade_date():
    if not os.path.exists(EXCEL_FILE):
        return None
    try:
        old_df = pd.read_excel(EXCEL_FILE, sheet_name="Sheet1")
        if "TRADE_DATE" in old_df.columns:
            last_date = pd.to_datetime(old_df["TRADE_DATE"]).max().date()
            print(f"📅 Last recorded date in file: {last_date}")
            return last_date
    except Exception as e:
        print(f"⚠️ Could not read existing Excel: {e}")
    return None

# === MAIN (single-run) ===
print("=== Update run:", datetime.now().isoformat())

last_date = get_last_trade_date()
start_date = (last_date + timedelta(days=1)) if last_date else datetime(2021, 1, 1).date()
end_date = datetime.now().date()

all_data = []
curr = start_date
while curr <= end_date:
    if curr.weekday() < 5:  # skip weekends
        date_str = curr.strftime("%Y-%m-%d")
        print(f"Fetching {date_str} ...")
        df = fetch_daily(date_str)
        if df is not None:
            all_data.append(df)
    curr += timedelta(days=1)

if all_data:
    new_data = pd.concat(all_data, ignore_index=True)

    # Save/append to Sheet1 (we rewrite full Sheet1 for simplicity)
    if os.path.exists(EXCEL_FILE):
        try:
            existing = pd.read_excel(EXCEL_FILE, sheet_name="Sheet1")
        except:
            existing = pd.DataFrame()
        combined_sheet1 = pd.concat([existing, new_data], ignore_index=True)
    else:
        combined_sheet1 = new_data

    combined_sheet1.to_excel(EXCEL_FILE, sheet_name="Sheet1", index=False)
    print("✅ Sheet1 saved/updated")

    # === UPDATE SHEET2 ===
    try:
        print("🧹 Refreshing Sheet2 from Sheet1 ...")

        # Load Sheet1 freshly from file (contains all updated data)
        sheet1_df = pd.read_excel(EXCEL_FILE, sheet_name="Sheet1")

        # Ensure Final_Position is numeric
        if "Final_Position" in sheet1_df.columns:
            sheet1_df["Final_Position"] = pd.to_numeric(sheet1_df["Final_Position"], errors="coerce")

        # Compute absolute value for comparison, but don't overwrite original
        sheet1_df["_abs_final"] = sheet1_df["Final_Position"].abs()

        # Sort by abs(final_position) descending, then drop duplicates by TRADE_DATE
        sheet2_cleaned = (
            sheet1_df.sort_values("_abs_final", ascending=False)
                     .drop_duplicates(subset=["TRADE_DATE"], keep="first")
                     .drop(columns=["_abs_final"])
                     .reset_index(drop=True)
        )

        # --- Sort by TRADE_DATE (oldest → newest)
        if "TRADE_DATE" in sheet2_cleaned.columns:
            # Try parsing to datetime if not already
            sheet2_cleaned["TRADE_DATE"] = pd.to_datetime(sheet2_cleaned["TRADE_DATE"], errors="coerce")
            sheet2_cleaned = sheet2_cleaned.sort_values("TRADE_DATE", ascending=True).reset_index(drop=True)

        # Write back — always REPLACE Sheet2 completely
        with pd.ExcelWriter(EXCEL_FILE, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            sheet2_cleaned.to_excel(writer, sheet_name="Sheet2", index=False)

        print(f"✅ Sheet2 refreshed: {len(sheet2_cleaned)} rows (max abs Final_Position per date, sorted)")

    except Exception as e:
        print("⚠️ Error updating Sheet2:", e)

# === PLOT CHART FROM SHEET2 ===
try:
    print("📊 Generating chart from Sheet2 ...")

    # Load Sheet2 data
    sheet2_df = pd.read_excel(EXCEL_FILE, sheet_name="Sheet2")

    # Ensure correct types
    sheet2_df["TRADE_DATE"] = pd.to_datetime(sheet2_df["TRADE_DATE"], errors="coerce")
    sheet2_df = sheet2_df.sort_values("TRADE_DATE", ascending=True)

    # Check if necessary columns exist
    if "Final_Position" not in sheet2_df.columns or "SETTLE_PRICE" not in sheet2_df.columns:
        print("⚠️ Missing Final_Position or SETTLE_PRICE column in Sheet2 — skipping chart.")
    else:
        import matplotlib.pyplot as plt

        fig, ax1 = plt.subplots(figsize=(10, 6))

        # Left Y-axis: Final Position
        ax1.plot(sheet2_df["TRADE_DATE"], sheet2_df["Final_Position"],
                 color="tab:blue", label="Final Position", linewidth=1.8)
        ax1.set_xlabel("Trade Date")
        ax1.set_ylabel("Final Position", color="tab:blue")
        ax1.tick_params(axis="y", labelcolor="tab:blue")

        # Right Y-axis: Settle Price
        ax2 = ax1.twinx()
        ax2.plot(sheet2_df["TRADE_DATE"], sheet2_df["SETTLE_PRICE"],
                 color="tab:red", label="Settle Price", linewidth=1.8, linestyle="--")
        ax2.set_ylabel("Settle Price", color="tab:red")
        ax2.tick_params(axis="y", labelcolor="tab:red")

        # Title and formatting
        plt.title("Position vs Palm Olein Settlement Price",
                  fontsize=14, fontweight="bold")
        fig.tight_layout()

        # Save chart as image
        chart_file = "futures_chart.png"
        plt.savefig(chart_file, dpi=150)
        plt.close()

        print(f"✅ Chart saved as {chart_file}")

except Exception as e:
    print("⚠️ Error generating chart:", e)



else:
    print("⚠ No new data found.")

if __name__ == "__main__":
    main()