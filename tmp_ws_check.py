import asyncio
import json

import websockets


def main():
    async def run():
        uri = "ws://127.0.0.1:8766/ws"
        async with websockets.connect(uri, max_size=None) as ws:
            for _ in range(20):
                raw = await ws.recv()
                msg = json.loads(raw)
                print("recv", msg.get("type"))
                if msg.get("type") == "history":
                    payload = msg.get("payload", {})
                    print("history keys", list(payload.keys())[:10])
                    print(
                        "counts",
                        len(payload.get("ts", [])),
                        len(payload.get("mt5", [])),
                        len(payload.get("spread", [])),
                    )
                    print("first ts", payload.get("ts", [])[:3])
                    if payload.get("mt5"):
                        print("sample mt5", payload["mt5"][0])
                    if payload.get("okx"):
                        print("sample okx", payload["okx"][0])
                    if payload.get("spread"):
                        print("sample spread", payload["spread"][0])
                    if payload.get("ema"):
                        print("sample ema", payload["ema"][0])
                    break
    asyncio.run(run())


if __name__ == "__main__":
    main()
