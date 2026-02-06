"""
Test task creation via WebSocket API.
"""
import asyncio
import json
import websockets

async def test_create_task():
    uri = "ws://localhost:8766/ws"
    
    async with websockets.connect(uri) as ws:
        print("✅ Connected to WebSocket server")
        
        # Test 1: Create task with mt5 + okx (should succeed if OKX has valid API key)
        print("\n" + "=" * 60)
        print("Test 1: Create task (MT5 + OKX)")
        print("=" * 60)
        
        payload = {
            "task_id": "test-mt5-okx-xauusd",
            "exchange_a": "mt5",
            "exchange_b": "okx",
            "symbol": "XAUUSD",
            "symbol_a": "XAUUSD",
            "symbol_b": "XAU/USDT:USDT",
            "config": {
                "emaPeriod": 20,
                "firstSpread": 3.0,
                "nextSpread": 1.0,
                "takeProfit": 0.5,
                "maxPos": 3,
                "tradeVolume": 0.01,
                "dryRun": True
            },
            "autoStart": False
        }
        
        await ws.send(json.dumps({"type": "create_task", "payload": payload}))
        
        # Wait for response
        response = await asyncio.wait_for(ws.recv(), timeout=30)
        msg = json.loads(response)
        print(f"\nResponse type: {msg.get('type')}")
        
        if msg.get("type") == "task_created":
            print("✅ Task created successfully!")
            print(f"   Task ID: {msg['payload'].get('task_id')}")
            if msg['payload'].get('verification'):
                print("\n   Verified balances:")
                for ex, v in msg['payload']['verification'].items():
                    balance = v.get('balance', {})
                    print(f"   - {ex}: ${balance.get('USDT_total', balance.get('USDT', '?'))}")
        elif msg.get("type") == "task_creation_failed":
            print("❌ Task creation failed!")
            print(f"   Error: {msg['payload'].get('error')}")
            if msg['payload'].get('verification'):
                print("\n   Details:")
                for ex, v in msg['payload']['verification'].items():
                    status = "✅" if v.get('success') else "❌"
                    print(f"   {status} {ex}: {v.get('error') or 'OK'}")
        else:
            print(f"Unexpected response: {json.dumps(msg, indent=2)}")
        
        # Test 2: Create task with binance (should fail - no API key)
        print("\n" + "=" * 60)
        print("Test 2: Create task (MT5 + Binance - should fail)")
        print("=" * 60)
        
        payload2 = {
            "task_id": "test-mt5-binance-xauusd",
            "exchange_a": "mt5",
            "exchange_b": "binance",
            "symbol": "XAUUSD",
            "symbol_a": "XAUUSD",
            "symbol_b": "XAU/USDT:USDT",
            "config": {
                "dryRun": True
            }
        }
        
        await ws.send(json.dumps({"type": "create_task", "payload": payload2}))
        
        response2 = await asyncio.wait_for(ws.recv(), timeout=30)
        msg2 = json.loads(response2)
        print(f"\nResponse type: {msg2.get('type')}")
        
        if msg2.get("type") == "task_creation_failed":
            print("✅ Correctly rejected (no API key)")
            print(f"   Error: {msg2['payload'].get('error')}")
        elif msg2.get("type") == "task_created":
            print("⚠️ Task created unexpectedly (Binance API key exists?)")
        else:
            print(f"Response: {json.dumps(msg2, indent=2)}")
        
        # Test 3: Get all tasks (should show persisted tasks)
        print("\n" + "=" * 60)
        print("Test 3: Get all tasks")
        print("=" * 60)
        
        await ws.send(json.dumps({"type": "get_tasks"}))
        
        response3 = await asyncio.wait_for(ws.recv(), timeout=10)
        msg3 = json.loads(response3)
        
        if msg3.get("type") == "tasks":
            tasks = msg3.get("payload", [])
            print(f"\n✅ Found {len(tasks)} tasks:")
            for t in tasks:
                print(f"   - {t.get('task_id')}: {t.get('exchange_a')} <-> {t.get('exchange_b')}")
        
        print("\n" + "=" * 60)
        print("All tests completed!")
        print("=" * 60)

if __name__ == "__main__":
    asyncio.run(test_create_task())
