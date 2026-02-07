
import asyncio
from src.config import settings
from src.gateway.mt5_gateway import MT5Gateway
from src.gateway.ccxt_gateway import CCXTGateway

async def check_tickers():
    print("Checking XAU...")
    mt5_xau = MT5Gateway("XAUUSD", settings.mt5.host, settings.mt5.port)
    
    okx_conf = settings.get_exchange("okx")
    okx_xau = CCXTGateway("okx", "XAU/USDT:USDT", okx_conf.api_key, okx_conf.api_secret, okx_conf.password)
    
    await mt5_xau.connect()
    await okx_xau.connect()
    
    print("Fetching Balance...")
    try:
        bal = await okx_xau.get_balance()
        print(f"OKX Balance: {bal}")
    except Exception as e:
        print(f"OKX Balance failed: {e}")

    t1 = await mt5_xau.get_ticker()
    t2 = await okx_xau.get_ticker()
    print(f"XAU MT5: {t1}")
    print(f"XAU OKX: {t2}")
    
    await mt5_xau.disconnect()
    await okx_xau.disconnect()

    print("\nChecking XAG...")
    mt5_xag = MT5Gateway("XAGUSD", settings.mt5.host, settings.mt5.port)
    # Check strict config from main.py
    # symbol_b="XAG/USDT:USDT"
    
    okx_conf = settings.get_exchange("okx")
    okx_xag = CCXTGateway("okx", "XAG/USDT:USDT", okx_conf.api_key, okx_conf.api_secret, okx_conf.password)

    await mt5_xag.connect()
    await okx_xag.connect()

    t3 = await mt5_xag.get_ticker()
    t4 = await okx_xag.get_ticker()
    print(f"XAG MT5: {t3}")
    print(f"XAG OKX: {t4}")

    await mt5_xag.disconnect()
    await okx_xag.disconnect()

if __name__ == "__main__":
    asyncio.run(check_tickers())
