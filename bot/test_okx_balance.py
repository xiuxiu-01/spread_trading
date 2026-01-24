import os
import sys
# Ensure project root is on sys.path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from gateway.okx_gateway import OKXGateway

def main():
    print("Initializing OKX Gateway...")
    try:
        okx = OKXGateway()
        print(f"Initialized OKXGateway for symbol: {okx.symbol}")
        
        print("\n--- Testing Balance ---")
        balance = okx.get_balance()
        print(f"Balance (Free USDT): {balance}")

        print("\n--- Testing Positions ---")
        positions = okx.get_positions()
        print(f"Positions count: {len(positions)}")
        for p in positions:
            print(p)
            
    except Exception as e:
        print(f"Test failed: {e}")

if __name__ == '__main__':
    main()
