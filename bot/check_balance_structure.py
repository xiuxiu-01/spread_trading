import os
import ccxt
from dotenv import load_dotenv
import json

load_dotenv()

def check_balance():
    api_key = os.getenv('OKX_API_KEY')
    secret = os.getenv('OKX_API_SECRET')
    password = os.getenv('OKX_API_PASSPHRASE')
    
    proxies = {
        'http': os.getenv('HTTP_PROXY'),
        'https': os.getenv('HTTPS_PROXY'),
    }
    
    opts = {
        'apiKey': api_key,
        'secret': secret,
        'password': password,
        'enableRateLimit': True,
    }
    if any(proxies.values()):
        opts['proxies'] = proxies
        
    exchange = ccxt.okx(opts)
    
    base = os.getenv('OKX_API_BASE')
    if base:
        exchange.urls['api']['public'] = base
        exchange.urls['api']['private'] = base

    print("--- Fetching Balance ---")
    try:
        bal = exchange.fetch_balance()
        # Print just the USDT section to see free/used/total vs 'info' structure
        usdt = bal.get('USDT')
        print("USDT Balance Summary:")
        print(json.dumps(usdt, indent=2))
        
        # Sometimes 'info' contains the detailed margin info
        print("\n--- Detailed 'info.details' (first item) ---")
        if bal.get('info') and bal['info'].get('details'):
             print(json.dumps(bal['info']['details'][0], indent=2))
             
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_balance()
