import sys
import os

# Add the project root to sys.path so we can import 'gateway'
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.append(ROOT)

import time
from gateway.okx_ws import OKXWS

# Create and start the websocket client for the swap contract
# Assuming XAU-USDT-SWAP is the instrument ID you are interested in.
# If you are looking for spot, use 'PAXG-USDT' or similar.
ws = OKXWS(instId="XAU-USDT-SWAP")
ws.start()

print("Staying alive... Press Ctrl+C to stop")
try:
    while True:
        # Check if we have received any data
        if ws.latest:
            # print(f"Latest Ticker: {ws.latest}")
            bids = ws.latest.get('bids')
            asks = ws.latest.get('asks')
            if bids and asks:
                best_bid = bids[0]
                best_ask = asks[0]
                print(f"Time: {ws.latest.get('ts')} | Bid: {best_bid[0]} ({best_bid[1]}) | Ask: {best_ask[0]} ({best_ask[1]})")
        else:
            print("Waiting for data...")
        
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping...")
    ws.stop()
