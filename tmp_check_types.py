import asyncio
import json
import websockets

async def main():
    async with websockets.connect('ws://127.0.0.1:8766/ws', max_size=None) as ws:
        while True:
            msg = json.loads(await ws.recv())
            if msg.get('type') == 'bar':
                bar = msg.get('payload', {})
                mt5 = bar.get('mt5', {})
                print('bar mt5 types', {k: type(v).__name__ for k, v in mt5.items()})
                print('bar spread type', type(bar.get('spread_sell')).__name__)
                return
asyncio.run(main())
