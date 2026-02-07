
import asyncio
from src.config.settings import settings
from src.gateway.mt5_gateway import MT5Gateway
from src.gateway.ccxt_gateway import CCXTGateway
from dotenv import load_dotenv

load_dotenv()

async def check_positions():
    print("--- Checking MT5 Position ---")
    mt5 = MT5Gateway("XAUUSD", settings.mt5.host, settings.mt5.port)
    if await mt5.connect():
        pos = await mt5.get_position()
        print(f"MT5 XAUUSD Position: {pos}")
        await mt5.disconnect()
    else:
        print("MT5 Connect Failed")

    print("\n--- Checking OKX Position ---")
    # Using XAU/USDT:USDT as in main.py
    okx_conf = settings.get_exchange("okx")
    if okx_conf:
        okx = CCXTGateway("okx", "XAU/USDT:USDT", okx_conf.api_key, okx_conf.api_secret, okx_conf.password)
        if await okx.connect():
            try:
                # CCXT gateway's get_position implementation fetches all and filters by symbol
                pos = await okx.get_position()
                print(f"OKX XAU Position: {pos}")
                
                # Debug raw positions
                if okx._exchange:
                    print("Fetching raw positions...")
                    # For swap/futures, we often need to ensure we look at the right type
                    # The Gateway code does: positions = await self._exchange.fetch_positions([self.symbol])
                    raw = await okx._exchange.fetch_positions(["XAU/USDT:USDT"])
                    print(f"Raw OKX Response: {len(raw)} items")
                    for p in raw:
                        print(f" - {p['symbol']}: {p['contracts']} contracts, side={p['side']}")
            except Exception as e:
                print(f"Error fetching OKX pos: {e}")
            
            await okx.disconnect()
    else:
        print("OKX Config missing")

if __name__ == "__main__":
    asyncio.run(check_positions())
