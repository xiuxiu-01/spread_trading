import asyncio
import json
import websockets

async def main():
    async with websockets.connect('ws://127.0.0.1:8766/ws', max_size=None) as ws:
        while True:
            msg = json.loads(await ws.recv())
            t = msg.get('type')
            if t == 'bar':
                bar = msg.get('payload', {})
                print('bar ts', bar.get('ts'))
                print('mt5', bar.get('mt5'))
                print('okx', bar.get('okx'))
                print('spread', bar.get('spread'), 'spread_sell', bar.get('spread_sell'), 'spread_buy', bar.get('spread_buy'), 'ema', bar.get('ema'))
                return

asyncio.run(main())
