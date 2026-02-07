import asyncio
import json
import websockets

async def main():
    async with websockets.connect('ws://127.0.0.1:8766/ws', max_size=None) as ws:
        while True:
            msg = json.loads(await ws.recv())
            if msg.get('type') == 'history':
                payload = msg.get('payload', {})
                ema = payload.get('ema', [])
                bad = [i for i, d in enumerate(ema) if d.get('value') is None]
                print('ema bad count', len(bad))
                if bad:
                    print('first bad', ema[bad[0]])
                spread_sell = payload.get('spread_sell', [])
                bad_ss = [i for i, d in enumerate(spread_sell) if d.get('value') is None]
                print('spread_sell bad', len(bad_ss))
                spread_buy = payload.get('spread_buy', [])
                bad_sb = [i for i, d in enumerate(spread_buy) if d.get('value') is None]
                print('spread_buy bad', len(bad_sb))
                return

asyncio.run(main())
