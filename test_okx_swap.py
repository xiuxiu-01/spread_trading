"""Test OKX swap market connection directly."""
import asyncio
import os
from dotenv import load_dotenv

# Load .env from project root
load_dotenv()

# Fix aiohttp DNS resolver issue on Windows
import aiohttp
async def test_okx_swap():
    try:
        import ccxt.pro as ccxtpro
    except ImportError:
        import ccxt.async_support as ccxtpro
    
    api_key = os.getenv("OKX_API_KEY", "")
    api_secret = os.getenv("OKX_API_SECRET", "")
    passphrase = os.getenv("OKX_API_PASSPHRASE", "")
    
    print(f"API Key: {api_key[:8]}..." if api_key else "No API key")
    
    # Create custom session with ThreadedResolver to avoid aiodns issues
    connector = aiohttp.TCPConnector(resolver=aiohttp.resolver.ThreadedResolver())
    session = aiohttp.ClientSession(connector=connector)
    
    exchange = ccxtpro.okx({
        "apiKey": api_key,
        "secret": api_secret,
        "password": passphrase,
        "enableRateLimit": True,
        "session": session,
        "options": {
            "defaultType": "swap",
            "defaultMarket": "swap",
        }
    })
    
    try:
        print("Loading swap markets...")
        await exchange.load_markets(params={"instType": "SWAP"})
        
        # Check for XAU
        xau_symbols = [s for s in exchange.markets if "XAU" in s]
        print(f"\nXAU symbols found: {xau_symbols}")
        
        # Check target symbol
        target = "XAU/USDT:USDT"
        if target in exchange.markets:
            market = exchange.markets[target]
            print(f"\n{target} details:")
            print(f"  ID: {market.get('id')}")
            print(f"  Type: {market.get('type')}")
            print(f"  Linear: {market.get('linear')}")
            print(f"  Contract: {market.get('contract')}")
            
            # Try to get ticker
            print(f"\nFetching ticker for {target}...")
            ticker = await exchange.fetch_ticker(target)
            print(f"  Bid: {ticker.get('bid')}")
            print(f"  Ask: {ticker.get('ask')}")
            print(f"  Last: {ticker.get('last')}")
        else:
            print(f"\n{target} not found!")
            
    finally:
        await exchange.close()
        await session.close()
        print("\nExchange closed.")

if __name__ == "__main__":
    asyncio.run(test_okx_swap())
