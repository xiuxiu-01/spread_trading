"""Check if tasks were loaded after restart."""
import asyncio
import json
import websockets

async def check():
    async with websockets.connect("ws://localhost:8766/ws") as ws:
        await ws.send(json.dumps({"type": "get_tasks"}))
        r = json.loads(await ws.recv())
        tasks = r.get("payload", [])
        print(f"✅ Found {len(tasks)} persisted tasks after restart:")
        for t in tasks:
            print(f"   - {t['task_id']}: {t['exchange_a']} <-> {t['exchange_b']}")

asyncio.run(check())
