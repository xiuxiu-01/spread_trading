#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import os
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
import MetaTrader5 as mt5
import requests
from dotenv import load_dotenv

# Load params
load_dotenv()
SYMBOL_MT5 = os.getenv('MT5_SYMBOL', 'XAU')
# OKX Symbol format adjustment usually needed for API (e.g., XAU-USDT-SWAP)
# If env is "XAU/USDT:USDT" (from ccxt style), we might need "XAU-USDT-SWAP" for raw API
SYMBOL_OKX_ENV = os.getenv('OKX_SYMBOL', 'XAU-USDT-SWAP') 

# Simple mapping if needed, user said "xau contract" so likely SWAP
if "SWAP" not in SYMBOL_OKX_ENV and "USDT" in SYMBOL_OKX_ENV:
    # Just a guess, let's try to default to XAU-USDT-SWAP if not clear
    SYMBOL_OKX = "XAU-USDT-SWAP"
else:
    SYMBOL_OKX = SYMBOL_OKX_ENV

print(f"Target Symbols -> MT5: {SYMBOL_MT5}, OKX: {SYMBOL_OKX}")

def fetch_mt5_data(days=30):
    if not mt5.initialize():
        print(f"MT5 Init failed: {mt5.last_error()}")
        return None

    utc_to = datetime.now(timezone.utc)
    utc_from = utc_to - timedelta(days=days)

    # Check if symbol exists
    if not mt5.symbol_select(SYMBOL_MT5, True):
         print(f"MT5 Symbol {SYMBOL_MT5} not found")
         return None

    # Retrieve history
    rates = mt5.copy_rates_range(SYMBOL_MT5, mt5.TIMEFRAME_M1, utc_from, utc_to)
    mt5.shutdown()

    if rates is None or len(rates) == 0:
        return pd.DataFrame()

    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    # MT5 often returns server time. We assume raw unix timestamp is correct or needs offset if mismatched.
    # Usually copy_rates_range returns timestamps that align with UNIX epoch, but timezone depends on broker.
    # For alignment, we treat the 'time' column as the join key.
    df = df[['time', 'close']].rename(columns={'close': 'mt5_close'})
    df.set_index('time', inplace=True)
    return df

def fetch_okx_data(days=30):
    # OKX API limit is 100 bars per request, 1440 * 30 bars needed.
    # We need to loop. API: GET /api/v5/market/history-candles (for older than recent)
    # Or /api/v5/market/candles for recent.

    base_url = "https://www.okx.com"
    end_ts = int(time.time() * 1000)
    start_ts = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)

    all_candles = []

    # Cursor pagination
    cursor = ""
    print("Fetching OKX data... (this might take a moment)")

    while True:
        url = f"{base_url}/api/v5/market/history-candles?instId={SYMBOL_OKX}&bar=1m&limit=100"
        if cursor:
            url += f"&after={cursor}"

        try:
            r = requests.get(url, timeout=10)
            data = r.json()
            if data['code'] != '0':
                print(f"OKX Error: {data}")
                break

            candles = data['data']
            if not candles:
                break

            all_candles.extend(candles)
            cursor = candles[-1][0] # Last timestamp is the cursor for 'after'

            last_ts = int(cursor)
            if last_ts < start_ts:
                break

            # Rate limit sleep
            time.sleep(0.1)

        except Exception as e:
            print(f"Fetch error: {e}")
            break

    if not all_candles:
        return pd.DataFrame()

    # OKX format: [ts, o, h, l, c, vol, ...]
    df = pd.DataFrame(all_candles, columns=['ts', 'o', 'h', 'l', 'c', 'v', 'vccy', 'vc', 'confirm'])
    df['time'] = pd.to_datetime(df['ts'].astype(int), unit='ms')
    df['okx_close'] = df['c'].astype(float)
    df = df[['time', 'okx_close']]
    df.set_index('time', inplace=True)
    return df

def main():
    print("Step 1: Fetching MT5 History...")
    df_mt5 = fetch_mt5_data(30)
    print(f"MT5 Records: {len(df_mt5)}")

    print("Step 2: Fetching OKX History...")
    df_okx = fetch_okx_data(30)
    print(f"OKX Records: {len(df_okx)}")

    if df_mt5.empty or df_okx.empty:
        print("One of the data sources is empty.")
        return

    print("Step 3: Merging Data...")
    # Join on index (time), inner join to see overlapping valid data
    df_merged = df_mt5.join(df_okx, how='inner')

    df_merged['spread'] = df_merged['mt5_close'] - df_merged['okx_close']

    print(f"Merged Records: {len(df_merged)}")

    print("\n--- Last 2 days of data (Sample) ---")
    # Show last 2 * 1440 minutes approx
    cutoff = datetime.now() - timedelta(days=2)
    recent_df = df_merged[df_merged.index >= cutoff]

    if recent_df.empty:
        print("No intersecting data in the last 2 days.")
        print("Showing last 20 records available instead:")
        print(df_merged.tail(20))
    else:
        pd.set_option('display.max_rows', 100) # Show reasonable chunk
        print(recent_df.tail(50)) # Print last 50 minutes as sample

        print("\nSummary Stats (Last 2 Days):")
        print(recent_df.describe())

if __name__ == "__main__":
    main()

