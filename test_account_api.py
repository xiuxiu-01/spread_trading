import asyncio
import websockets
import json

async def test_account():
    uri = "ws://localhost:8766/ws"
    try:
        async with websockets.connect(uri) as websocket:
            print("Connected")
            
            # Send get_account request
            await websocket.send(json.dumps({"type": "get_account"}))
            
            # Wait for response
            while True:
                response = await websocket.recv()
                data = json.loads(response)
                if data["type"] == "account":
                    print("Received account info:")
                    print(json.dumps(data["payload"], indent=2))
                    break
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test_account())
