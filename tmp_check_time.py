import asyncio
import json
import websockets

async def main():
    uri = "ws://127.0.0.1:8766/ws"
    async with websockets.connect(uri, max_size=None) as ws:
        while True:
            msg = json.loads(await ws.recv())
            if msg.get("type") == "history":
                payload = msg.get("payload", {})
                mt5 = payload.get("mt5", [])
                missing = [i for i, bar in enumerate(mt5) if bar.get("time") is None]
                print("missing time count", len(missing))
                if mt5:
                    print("first sample", mt5[0])
                return

asyncio.run(main())
