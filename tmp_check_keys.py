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
                for key in ("mt5", "okx"):
                    miss = [i for i, bar in enumerate(payload.get(key, [])) if not all(k in bar for k in ("open", "high", "low", "close"))]
                    print(key, "missing", len(miss))
                    if miss:
                        print("first missing entry", payload[key][miss[0]])
                return

asyncio.run(main())
