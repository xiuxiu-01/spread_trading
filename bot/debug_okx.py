import os
import ccxt
from dotenv import load_dotenv
import json
import traceback

load_dotenv()

def debug_okx():
    print("Starting debug_okx...")
    api_key = os.getenv('OKX_API_KEY')
    secret = os.getenv('OKX_API_SECRET')
    password = os.getenv('OKX_API_PASSPHRASE')

    if not api_key:
        print("Error: credentials not found")
        return

    proxies = {
        'http': os.getenv('HTTP_PROXY'),
        'https': os.getenv('HTTPS_PROXY'),
    }

    opts = {
        'apiKey': api_key,
        'secret': secret,
        'password': password,
    }
    
    if any(proxies.values()):
        opts['proxies'] = proxies

    exchange = ccxt.okx(opts)

    # Use the specific base URL if configured (from gateway code)
    base = os.getenv('OKX_API_BASE')
    if base:
        exchange.urls['api']['public'] = base
        exchange.urls['api']['private'] = base

    symbol = 'XAU/USDT:USDT'

    print(f"--- Fetching Market Info for {symbol} ---")
    try:
        markets = exchange.load_markets()
        market = markets.get(symbol)
        if market:
            print(f"Contract Size: {market.get('contractSize')}")
            print(f"Linear?: {market.get('linear')}")
            print(f"Inverse?: {market.get('inverse')}")
            print(f"Settle: {market.get('settle')}")
            print(f"Min Size (amount): {market['limits']['amount']['min']}")
            print(f"Precision (amount): {market['precision']['amount']}")
            print(f"Taker Fee: {market.get('taker')}")
            print(f"Maker Fee: {market.get('maker')}")
            # Raw info might contain 'ctVal' etc.
            if 'info' in market:
                print(f"Raw info (subset): {json.dumps({k:v for k,v in market['info'].items() if k in ['ctVal', 'ctMult', 'ul', 'lever', 'alias']}, indent=2)}")
        else:
            print(f"Symbol {symbol} not found in markets.")
            # Let's list similar symbols just in case
            print(f"Did you mean? {[s for s in markets.keys() if 'XAU' in s]}")

    except Exception as e:
        print(f"Error fetching market info: {e}")
        traceback.print_exc()

    print(f"\n--- Fetching Account Config ---")
    try:
        # Get Position Mode
        # In okx, this is often under 'get_account_configuration' or specific endpoints
        # CCXT unifies this poorly sometimes, let's try raw request or specific method
        
        # Method 1: config
        try:
            config = exchange.fetch_position_mode(symbol) # CCXT unified
            print(f"Position Mode (CCXT): {config}") 
        except Exception as e:
            print(f"fetch_position_mode failed: {e}")
            # Try raw
            try:
                # GET /api/v5/account/config
                res = exchange.private_get_account_config()
                if 'data' in res and len(res['data']) > 0:
                     print(f"Account Config (Raw): PosMode={res['data'][0].get('posMode')}, AcctLv={res['data'][0].get('acctLv')}")
            except Exception as e2:
                print(f"Raw config fetch failed: {e2}")

        # Get Leverage
        try:
            # Try to fetch leverage for the specific symbol
            lev_info = exchange.fetch_leverage(symbol)
            print(f"Leverage (CCXT): {lev_info}")
        except Exception as e:
            print(f"fetch_leverage failed: {e}")
            # Try raw leverage info
            try:
                res = exchange.private_get_account_leverage_info({'instId': symbol.replace('/', '-').replace(':USDT','')}) # Try various formats if needed
                print(f"Leverage (Raw): {res}")
            except Exception as e2:
                # Try with the exact market ID from CCXT
                try:
                    if market:
                        res = exchange.private_get_account_leverage_info({'instId': market['id']})
                        print(f"Leverage (Raw-ID): {res}")
                except Exception as e3:
                    print(f"Raw leverage fetch failed: {e3}")
            
    except Exception as e:
        print(f"Error account config: {e}")

    print(f"\n--- Fetching Open Positions ---")
    try:
        positions = exchange.fetch_positions([symbol])
        if positions:
            for p in positions:
                print(f"Position: Side={p['side']}, Contracts={p['contracts']}, Amt={p.get('info', {}).get('pos')}")
                print(f"  > Margin: {p.get('initialMargin')}, Leverage: {p.get('leverage')}")
                print(f"  > Entry Price: {p.get('entryPrice')}, Mark Price: {p.get('markPrice')}")
                print(f"  > Margin Mode: {p.get('info', {}).get('mgnMode')}")
                print(f"  > Pos Side: {p.get('info', {}).get('posSide')}")
        else:
            print("No open positions found.")
    except Exception as e:
        print(f"Error fetching positions: {e}")
        traceback.print_exc()

    print(f"\n--- Checking Ticker Price ---")
    try:
         ticker = exchange.fetch_ticker(symbol)
         print(f"Last Price: {ticker['last']}")
    except Exception as e:
         print(f"Ticker error: {e}")

if __name__ == "__main__":
    debug_okx()
