"""Start a task via WebSocket."""
import asyncio
import json
import websockets

async def start_task():
    uri = "ws://localhost:8766/ws"
    
    try:
        async with websockets.connect(uri, max_size=10*1024*1024) as ws:  # 10MB limit
            # First wait for initial state (params, history, dashboard, tasks)
            print("Waiting for initial state...")
            initial_count = 0
            while initial_count < 5:
                try:
                    response = await asyncio.wait_for(ws.recv(), timeout=10.0)
                    data = json.loads(response)
                    msg_type = data.get('type', 'unknown')
                    print(f"Initial: {msg_type}")
                    initial_count += 1
                    if msg_type == 'tasks':
                        break
                except asyncio.TimeoutError:
                    print("Timeout waiting for initial state")
                    break
            
            # Start task - task_id goes in payload
            msg = {
                "type": "start_task",
                "payload": {
                    "task_id": "test-mt5-okx-xauusd"
                }
            }
            print(f"\nSending: {msg}")
            await ws.send(json.dumps(msg))
            
            # Wait for responses
            for i in range(10):
                try:
                    response = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    data = json.loads(response)
                    print(f"Received: {data.get('type', 'unknown')}")
                except asyncio.TimeoutError:
                    print("No more messages (timeout)")
                    break
                    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(start_task())
