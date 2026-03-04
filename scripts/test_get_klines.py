import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Ensure repo root is on path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import settings
from src.gateway.ccxt_gateway import CCXTGateway

async def main():
    cfg = settings.get_exchange('bitget')
    if not cfg:
        print('No Bitget config')
        return

    gw = CCXTGateway.create_from_config(cfg, 'XAU/USDT:USDT')
    ok = await gw.connect()
    print('connect:', ok)
    if not ok:
        return

    # test recent 10 minutes
    end_ts = int(datetime.utcnow().timestamp())
    start_ts = end_ts - 60*10
    print('requesting klines start=%s end=%s' % (start_ts, end_ts))
    klines = await gw.get_klines(timeframe='1m', limit=20, start_time=start_ts, end_time=end_ts)
    print('got', len(klines), 'klines')
    if klines:
        print(klines[:2])

    await gw.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
