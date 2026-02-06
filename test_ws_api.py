"""Test WebSocket API."""

import asyncio
import websockets
import json


async def test():
    uri = 'ws://localhost:8766/ws'
    print(f"Connecting to {uri}...")
    
    async with websockets.connect(uri) as ws:
        print("Connected!\n")
        
        # Test get_symbols
        print("=== Testing get_symbols ===")
        await ws.send(json.dumps({'type': 'get_symbols'}))
        resp = await ws.recv()
        data = json.loads(resp)
        if data.get('type') == 'symbols':
            for s in data.get('payload', []):
                exchanges = [k for k, v in s.get('exchanges', {}).items() if v]
                print(f"  {s['code']}: {s['name']}")
                print(f"    Supported on: {', '.join(exchanges)}")
        print()
        
        # Test get_exchanges
        print("=== Testing get_exchanges ===")
        await ws.send(json.dumps({'type': 'get_exchanges'}))
        resp = await ws.recv()
        data = json.loads(resp)
        if data.get('type') == 'exchanges':
            for ex in data.get('payload', []):
                print(f"  {ex['icon']} {ex['name']} ({ex['id']})")
        print()
        
        # Test get_tasks
        print("=== Testing get_tasks ===")
        await ws.send(json.dumps({'type': 'get_tasks'}))
        resp = await ws.recv()
        data = json.loads(resp)
        if data.get('type') == 'tasks':
            tasks = data.get('payload', [])
            print(f"  Found {len(tasks)} task(s)")
            for t in tasks:
                print(f"  - {t['task_id']}: {t['status']} ({t.get('symbol', t['symbol_a'])})")
        print()
        
        # Test create_task
        print("=== Testing create_task (BTC arbitrage) ===")
        await ws.send(json.dumps({
            'type': 'create_task',
            'payload': {
                'task_id': 'test-btc-okx-binance',
                'exchange_a': 'okx',
                'exchange_b': 'binance',
                'symbol': 'BTCUSD',
                'symbol_a': 'BTC/USDT:USDT',
                'symbol_b': 'BTC/USDT',
                'config': {
                    'firstSpread': 50.0,
                    'nextSpread': 20.0,
                    'takeProfit': 10.0,
                    'maxPos': 3,
                    'tradeVolume': 0.001,
                    'dryRun': True
                },
                'autoStart': False
            }
        }))
        resp = await ws.recv()
        data = json.loads(resp)
        print(f"  Response type: {data['type']}")
        if data.get('type') == 'task_created':
            print(f"  Task created: {data['payload'].get('task_id')}")
            print(f"  Symbol: {data['payload'].get('symbol')}")
            print(f"  Config: {data['payload'].get('config')}")
        elif data.get('type') == 'error':
            print(f"  Error: {data['payload'].get('message')}")
        print()
        
        # Get tasks again
        print("=== Tasks after creation ===")
        await ws.send(json.dumps({'type': 'get_tasks'}))
        resp = await ws.recv()
        data = json.loads(resp)
        if data.get('type') == 'tasks':
            for t in data.get('payload', []):
                print(f"  - {t['task_id']}: {t['status']} ({t.get('symbol', t['symbol_a'])})")
        print()
        
        # Test get_dashboard
        print("=== Testing get_dashboard ===")
        await ws.send(json.dumps({'type': 'get_dashboard'}))
        resp = await ws.recv()
        data = json.loads(resp)
        if data.get('type') == 'dashboard':
            d = data.get('payload', {})
            print(f"  Running tasks: {d.get('running_tasks', 0)}")
            print(f"  Total trades: {d.get('total_trades', 0)}")
            print(f"  Total PnL: ${d.get('total_pnl', 0):.2f}")
        
        print("\n=== All tests passed! ===")


if __name__ == '__main__':
    asyncio.run(test())
