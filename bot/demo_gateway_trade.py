import os
import sys
# Ensure project root is on sys.path so `gateway` can be imported when running this script directly
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.append(ROOT)
from dotenv import load_dotenv
from gateway.okx_gateway import OKXGateway
from gateway.mt5_gateway import MT5Gateway

load_dotenv()

# Demo: use gateways to place a simulated OKX post-only limit and MT5 market order
# Set SIMULATE=1 to skip real orders and just print payloads.
SIMULATE = os.getenv('SIMULATE', '0') == '1'

def main():
    # OKX post-only limit far from market to avoid execution
    okx = OKXGateway(symbol='XAU/USDT')
    ticker = okx.get_ticker()
    last = ticker.get('last') or 1.0
    price = round(last * 0.5, 4)  # far below to ensure post-only
    amount = 0.001
    order_payload = {
        'side': 'buy',
        'amount': amount,
        'price': price,
        'post_only': True,
    }
    print('[OKX] prepared order', order_payload)
    if not SIMULATE:
        try:
            res = okx.place_limit_order(**order_payload)
            print('[OKX] order result', res)
        except Exception as e:
            print('[OKX] order error', e)

    # MT5 market order (demo account, respect trading hours)
    try:
        mt5 = MT5Gateway(symbol='XAU')
        tick = mt5.get_tick()
        print('[MT5] current tick', tick)
        mt5_payload = {
            'side': 'buy',
            'volume': 0.01,
        }
        print('[MT5] prepared market order', mt5_payload)
        if not SIMULATE:
            res = mt5.place_market_order(**mt5_payload)
            print('[MT5] order result', res)
    except Exception as e:
        print('[MT5] error', e)
    finally:
        try:
            mt5.shutdown()
        except Exception:
            pass

if __name__ == '__main__':
    main()
