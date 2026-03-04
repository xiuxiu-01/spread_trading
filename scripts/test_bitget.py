import asyncio
import sys
from pathlib import Path

# Ensure repo root is on path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import settings
from src.gateway.ccxt_gateway import CCXTGateway

async def main():
    print("Loaded exchanges:", list(settings.exchanges.keys()))
    cfg = settings.get_exchange('bitget')
    if not cfg:
        print("No Bitget config loaded")
        return
    print("Bitget config:", cfg)

    # Use perpetual swap symbol format so CCXTGateway selects swap market type
    gw = CCXTGateway.create_from_config(cfg, 'BTC/USDT:USDT')
    try:
        ok = await gw.connect()
        print("connect result:", ok)
        if not ok:
            return
        # Try fetching positions (futures) and balances
        try:
            pos = await gw.get_position()
            print("position:", pos)
        except Exception as e:
            print("position error:", e)

        try:
            bal = await gw.get_balance()
            print("balance:", bal)
        except Exception as e:
            print("balance error:", e)
    except Exception as e:
        print("Error during test:", e)
    finally:
        try:
            await gw.disconnect()
        except Exception:
            pass

if __name__ == '__main__':
    asyncio.run(main())
