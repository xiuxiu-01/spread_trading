import ccxt
import os
from dotenv import load_dotenv

load_dotenv()

def list_contracts():
    try:
        # Initialize without keys for public data if possible, or use keys if env vars present
        # Using a proxy if configured in other files
        proxies = {
            'http': os.getenv('HTTP_PROXY'),
            'https': os.getenv('HTTPS_PROXY'),
        }
        opts = {}
        if any(proxies.values()):
            opts['proxies'] = proxies
            
        exchange = ccxt.okx(opts)
        
        print("Fetching markets from OKX...")
        # Explicitly ask for SWAP to avoid default SPOT errors if any
        # actually fetch_markets gets all, but let's see why it failed. 
        # The error said "v5/public/instruments?instType=SPOT". 
        # Implicit fetch_markets sometimes defaults or iterates.
        
        # Let's try fetch_markers with params if supported, or just catch specific errors.
        # But actually let's try to just load markets.
        markets = exchange.load_markets()
        
        # Filter for Swaps and Futures
        swaps = [m for m in markets.keys() if markets[m].get('swap') or markets[m].get('future')]
        
        print(f"Total derivative markets found: {len(swaps)}")
        
        # Look for Gold related (XAU, XAU, XAUT)
        gold_related = [s for s in swaps if 'XAU' in s or 'XAU' in s or 'GOLD' in s]
        
        print("\n--- Gold/Metal Related Contracts ---")
        for s in sorted(gold_related):
            info = markets[s]
            m_type = info.get('type')
            contract_size = info.get('contractSize')
            print(f"{s} | Type: {m_type} | Contract Size: {contract_size}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_contracts()
