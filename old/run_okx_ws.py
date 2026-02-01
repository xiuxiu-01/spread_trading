import sys, pathlib, time, os
sys.path.append(str(pathlib.Path(__file__).parent))
from gateway.okx_ws import OKXWS, OKX_INST_MAP
from old.mt5_client import get_tick, init_mt5, shutdown_mt5

if __name__ == '__main__':
    inst = OKX_INST_MAP.get('XAU/USDT', 'XAU-USDT')
    record = os.getenv('OKX_RECORD_FILE', 'ticks.jsonl')
    ws = OKXWS(instId=inst, record_file=record)
    ws.start()
    print('OKX WS started for', inst)
    # optional: also show MT5 tick side-by-side
    try:
        init_mt5()
    except Exception as e:
        print('MT5 init failed:', e)
    try:
        while True:
            snap = ws.latest
            mt5_tick = get_tick('XAU')
            if snap:
                best_bid = snap['bids'][0][0] if snap.get('bids') else None
                best_ask = snap['asks'][0][0] if snap.get('asks') else None
                print('[OKX WS]', best_bid, best_ask, '| [MT5]', mt5_tick)
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        ws.stop()
        shutdown_mt5()
        print('Stopped.')
