import asyncio
import json
import websockets

async def main():
    async with websockets.connect('ws://127.0.0.1:8766/ws', max_size=None) as ws:
        while True:
            msg = json.loads(await ws.recv())
            if msg.get('type') == 'history':
                payload = msg.get('payload', {})
                mt5 = payload.get('mt5', [])
                zeros = [i for i,b in enumerate(mt5) if b.get('time') in (0, None)]
                print('zero time count', len(zeros))
                return

asyncio.run(main())
