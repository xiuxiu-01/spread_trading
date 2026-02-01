import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).parent))
from okx_rest import create_okx, get_orderbook
from mt5_client import init_mt5, get_tick, shutdown_mt5


def test_okx():
    try:
        okx = create_okx()  # public market data only
        ob = get_orderbook(okx, 'XAU/USDT', limit=5)
        best_bid = ob['bids'][0][0] if ob['bids'] else None
        best_ask = ob['asks'][0][0] if ob['asks'] else None
        print('[OKX] best bid/ask:', best_bid, best_ask)
        return best_bid is not None and best_ask is not None
    except Exception as e:
        print('[OKX] error:', e)
        return False


def test_mt5():
    try:
        init_mt5()
        tick = get_tick('XAU')
        print('[MT5] tick:', tick)
        return tick is not None
    except Exception as e:
        print('[MT5] error:', e)
        return False
    finally:
        shutdown_mt5()


if __name__ == '__main__':
    ok_okx = test_okx()
    ok_mt5 = test_mt5()
    print('OKX:', ok_okx, 'MT5:', ok_mt5)
    sys.exit(0 if (ok_okx and ok_mt5) else 1)
