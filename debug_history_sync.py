
import asyncio
import os
import sys
import json
from datetime import datetime, timezone

# Add project root to path
sys.path.append(os.getcwd())

from src.gateway.mt5_gateway import MT5Gateway
from src.gateway.ccxt_gateway import CCXTGateway
from src.config import settings

async def main():
    print("Starting debug history sync...")
    
    # Setup MT5
    # Assuming symbol "XAU" or check tasks
    mt5_symbol = "XAUUSD" 
    
    # Setup OKX
    # Assuming symbol "XAU/USDT:USDT"
    okx_symbol = "XAU/USDT:USDT"
    
    print(f"MT5 Symbol: {mt5_symbol}")
    print(f"OKX Symbol: {okx_symbol}")
    
    gw_mt5 = MT5Gateway(symbol=mt5_symbol)
    gw_okx = CCXTGateway(exchange_name="okx", symbol=okx_symbol)
    
    # Initialize gateways
    print("Initializing MT5...")
    if not await gw_mt5.connect():
        print("Failed to connect to MT5")
        return
    
    # Debug raw MT5
    import MetaTrader5 as mt5
    print(f"MT5 Version: {mt5.version()}")
    print(f"MT5 Account: {mt5.account_info()}")
    print(f"MT5 Symbol Info 'XAU': {mt5.symbol_info('XAU')}")
    print(f"MT5 Last Error: {mt5.last_error()}")
    
    # Try fetching history directly with low level call
    rates = mt5.copy_rates_from_pos("XAU", mt5.TIMEFRAME_M1, 0, 10)
    print(f"Direct copy_rates 'XAU': {len(rates) if rates is not None else 'None'}")
    if rates is None:
         print(f"Error: {mt5.last_error()}")

    print("Initializing OKX...")
    if not await gw_okx.connect():
        print("Failed to connect to OKX")
        return

    try:
        limit = 10
        print(f"Fetching last {limit} bars from MT5...")
        klines_mt5 = await gw_mt5.get_klines(limit=limit)
        
        if not klines_mt5:
             print("No MT5 data!")
             return

        # Mimic Manager Logic
        start_ts = klines_mt5[0]['time']
        end_ts = klines_mt5[-1]['time']
        print(f"MT5 Range: {start_ts} to {end_ts} (Diff: {end_ts - start_ts})")

        print(f"Fetching OKX bars starting from {start_ts} to match MT5...")
        # We need to fetch enough bars to cover the range
        klines_okx = await gw_okx.get_klines(limit=limit, start_time=start_ts)
        
        print(f"\nMT5 Bars ({len(klines_mt5)}):")
        # Only print first/last
        if klines_mt5:
             print(f"  First: {klines_mt5[0]['time']} ({datetime.fromtimestamp(klines_mt5[0]['time'], timezone.utc)})")
             print(f"  Last:  {klines_mt5[-1]['time']} ({datetime.fromtimestamp(klines_mt5[-1]['time'], timezone.utc)})")
            
        print(f"\nOKX Bars ({len(klines_okx)}):")
        if klines_okx:
             print(f"  First: {klines_okx[0]['time']} ({datetime.fromtimestamp(klines_okx[0]['time'], timezone.utc)})")
             print(f"  Last:  {klines_okx[-1]['time']} ({datetime.fromtimestamp(klines_okx[-1]['time'], timezone.utc)})")
            
        # Analysis
        dict_a = {x['time']: x for x in klines_mt5}
        dict_b = {x['time']: x for x in klines_okx}
        common = sorted(list(set(dict_a.keys()) & set(dict_b.keys())))
        
        print(f"\nCommon Timestamps: {len(common)}")
        if common:
            print(f"First common: {common[0]}")
            print(f"Last common: {common[-1]}")
        else:
            print("NO OVERLAP!")
            if klines_mt5 and klines_okx:
                diff = klines_mt5[-1]['time'] - klines_okx[-1]['time']
                print(f"Time difference at latest bars: {diff} seconds")

    finally:
        await gw_mt5.disconnect()
        await gw_okx.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
