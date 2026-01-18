import os
import sys
import time
from dotenv import load_dotenv

# Ensure project root on path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from gateway.okx_gateway import OKXGateway
from gateway.mt5_gateway import MT5Gateway

load_dotenv()

SYMBOL_OKX = os.getenv('OKX_SYMBOL', 'PAXG/USDT')
SYMBOL_MT5 = os.getenv('MT5_SYMBOL', 'XAU')
RECORD_FILE = os.getenv('OKX_RECORD_FILE', 'ticks.jsonl')

def main():
    okx = OKXGateway(symbol=SYMBOL_OKX)
    okx.start_ws(record_file=RECORD_FILE)

    # Wait for first WS snapshot (up to 10s)
    if not okx.wait_for_ws_snapshot(timeout=10.0):
        print('[Arb] OKX WS snapshot not ready, continuing but values may be None')

    mt5 = None
    try:
        mt5 = MT5Gateway(symbol=SYMBOL_MT5)
    except Exception as e:
        print('[MT5] init error', e)
        return

    print('[Arb] starting WS + MT5 tick streams...')
    try:
        for tick in mt5.stream_ticks():
            mt5_bid = tick.get('bid')
            mt5_ask = tick.get('ask')
            ob = okx.latest_ws_snapshot() or {}
            # print(ob)
            bids = ob.get('bids') or []
            asks = ob.get('asks') or []
            okx_best_bid = bids[0][0] if bids else None
            okx_best_ask = asks[0][0] if asks else None

            # If any value missing, print simple status and continue
            if mt5_bid is None or mt5_ask is None or okx_best_bid is None or okx_best_ask is None:
                print('[Arb] waiting for prices...', {'mt5': tick, 'okx_top': {'bid': okx_best_bid, 'ask': okx_best_ask}})
                time.sleep(0.5)
                continue

            # Compute spreads (OKX vs MT5)
            spread_sell_okx_buy_mt5 = okx_best_bid - mt5_ask
            spread_sell_mt5_buy_okx = mt5_bid - okx_best_ask

            print(f"[Arb] MT5 bid/ask={mt5_bid:.2f}/{mt5_ask:.2f} | OKX bid/ask={okx_best_bid:.4f}/{okx_best_ask:.4f} | spreads: okx->mt5={spread_sell_okx_buy_mt5:.4f} mt5->okx={spread_sell_mt5_buy_okx:.4f}")

            # Here you can add thresholds to trigger simulated orders via gateways
            # For safety, this demo only prints. Hook into OKXGateway.place_limit_order and MT5Gateway.place_market_order as needed.
    except KeyboardInterrupt:
        print('Stopped by user')
    finally:
        try:
            okx.stop_ws()
        except Exception:
            pass
        try:
            mt5.shutdown()
        except Exception:
            pass

if __name__ == '__main__':
    main()
