import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).parent))
from old.mt5_client import init_mt5, shutdown_mt5, place_market_order

if __name__ == '__main__':
    side = sys.argv[1] if len(sys.argv) > 1 else 'buy'
    volume = float(sys.argv[2]) if len(sys.argv) > 2 else 0.01
    symbol = sys.argv[3] if len(sys.argv) > 3 else 'XAU'
    try:
        init_mt5()
        print(f"Placing MT5 market order: side={side} volume={volume} symbol={symbol}")
        res = place_market_order(side, volume, symbol=symbol)
        print('Result:', res)
    except Exception as e:
        print('Error placing order:', e)
        sys.exit(1)
    finally:
        shutdown_mt5()
