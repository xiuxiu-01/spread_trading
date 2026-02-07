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
                    for idx, bar in enumerate(payload.get(key, [])):
                        for field in ("open", "high", "low", "close"):
                            if bar.get(field) is None:
                                print(f"{key} {field} null at idx {idx}")
                                return
                print("payload ok", len(payload.get("mt5", [])))
                return

asyncio.run(main())
