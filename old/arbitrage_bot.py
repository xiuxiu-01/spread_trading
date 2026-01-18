import time
import json
import os
import ccxt
import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import threading
import websocket
import datetime
import matplotlib.pyplot as plt
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).parent))
import old.okx_rest as okx_rest  # added: reuse REST helpers with fallback
from gateway.okx_gateway import OKXGateway
from gateway.mt5_gateway import MT5Gateway

# Load env
load_dotenv()

CONFIG = {
    'okx_api_key': os.getenv('OKX_API_KEY',''),
    'okx_api_secret': os.getenv('OKX_API_SECRET',''),
    'okx_passphrase': os.getenv('OKX_API_PASSPHRASE',''),
    'mt5_price_file': os.path.expanduser('~/Library/Application Support/MetaQuotes/Terminal/*/MQL5/Files/mt5_xau_price.json'),
    'mt5_cmd_file': os.path.expanduser('~/Library/Application Support/MetaQuotes/Terminal/*/MQL5/Files/mt5_cmd.json'),
    'symbol_mt5': 'XAU',
    'symbol_okx': 'PAXG/USDT',
    'spread_threshold': 0.5,  # USD
    'order_size': 0.01,  # amount in XAU lots or PAXG quantity depending on platform
    'okx_record_file': 'ticks.jsonl',  # added: WS record file
}

# Connect to OKX
# replace local implementation to honor proxies and optional base URL

def create_okx():
    base = os.getenv('OKX_API_BASE')
    proxies = {
        'http': os.getenv('HTTP_PROXY') or os.getenv('http_proxy'),
        'https': os.getenv('HTTPS_PROXY') or os.getenv('https_proxy'),
    }
    opts = {
        'apiKey': CONFIG['okx_api_key'],
        'secret': CONFIG['okx_api_secret'],
        'password': CONFIG['okx_passphrase'],
        'enableRateLimit': True,
    }
    if any(proxies.values()):
        opts['proxies'] = proxies
    okx = ccxt.okx(opts)
    if base:
        okx.urls = okx.urls.copy()
        api = okx.urls.get('api', {})
        api = api.copy() if isinstance(api, dict) else {}
        api['public'] = base
        api['private'] = base
        okx.urls['api'] = api
    return okx

# read mt5 price file (search latest file matching pattern)
import glob

def read_mt5_price():
    # use MetaTrader5 API instead of file-based exchange with EA
    # Ensure MT5 is initialized (noop if already initialized)
    if not mt5.initialize():
        print('mt5 initialize failed', mt5.last_error())
        # still try to proceed; caller will handle None
        return None

    symbol = CONFIG['symbol_mt5']
    # ensure symbol is available in Market Watch
    try:
        mt5.symbol_select(symbol, True)
    except Exception:
        pass

    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        # sometimes symbol_info_tick returns None if symbol not found
        print('mt5 tick fetch failed for', symbol)
        return None
    return {'bid': float(tick.bid), 'ask': float(tick.ask)}


# use REST orderbook polling with fallback from okx_rest

def get_okx_orderbook(okx, limit=5):
    try:
        ob = okx_rest.get_orderbook(okx, CONFIG['symbol_okx'], limit)
        best_bid = ob['bids'][0][0] if ob.get('bids') else None
        best_ask = ob['asks'][0][0] if ob.get('asks') else None
        return {'bid': best_bid, 'ask': best_ask, 'raw': ob}
    except Exception as e:
        print('okx orderbook error', e)
        return None

# map symbol formats
OKX_INST_MAP = {
    'PAXG/USDT': 'PAXG-USDT'
}

class OKXWS:
    def __init__(self, instId, record_file=None):
        # allow override, and use AWS WS if API base hints so
        ws_env = os.getenv('OKX_WS_URL')
        if ws_env:
            self.url = ws_env
        else:
            api_base = os.getenv('OKX_API_BASE') or ''
            if 'aws.okx.com' in api_base:
                self.url = 'wss://wsaws.okx.com:8443/ws/v5/public'
            else:
                self.url = 'wss://ws.okx.com:8443/ws/v5/public'
        self.instId = instId
        self.record_file = record_file
        self.ws = None
        self.latest = None
        self._stop = False

    def _on_open(self, ws):
        sub = {
            "op": "subscribe",
            "args": [
                {"channel": "books", "instId": self.instId, "sz": "5"}
            ]
        }
        ws.send(json.dumps(sub))
        print('OKX WS subscribed to', self.instId)

    def _on_message(self, ws, message):
        try:
            obj = json.loads(message)
            # data messages have 'arg' or 'data'
            if 'data' in obj and obj['data']:
                data = obj['data'][0]
                # standardize to {'bids':[[p,q],...], 'asks':[[p,q],...]}
                bids = [[float(x[0]), float(x[1])] for x in (data.get('bids') or [])]
                asks = [[float(x[0]), float(x[1])] for x in (data.get('asks') or [])]
                snap = {'ts': datetime.datetime.utcnow().isoformat(), 'bids': bids, 'asks': asks}
                self.latest = snap
                # optionally record together with MT5 tick
                if self.record_file:
                    mt5tick = read_mt5_price()
                    record = {'ts': snap['ts'], 'okx': snap, 'mt5': mt5tick}
                    try:
                        with open(self.record_file, 'a') as f:
                            f.write(json.dumps(record) + '\n')
                    except Exception as e:
                        print('record write err', e)
        except Exception as e:
            print('ws msg parse error', e)

    def _on_error(self, ws, error):
        print('OKX WS error', error)

    def _on_close(self, ws, close_status_code, close_msg):
        print('OKX WS closed', close_status_code, close_msg)

    def start(self):
        self._stop = False
        def run():
            while not self._stop:
                try:
                    self.ws = websocket.WebSocketApp(self.url,
                                                     on_open=self._on_open,
                                                     on_message=self._on_message,
                                                     on_error=self._on_error,
                                                     on_close=self._on_close)
                    self.ws.run_forever()
                except Exception as e:
                    print('ws run err', e)
                time.sleep(1)
        t = threading.Thread(target=run, daemon=True)
        t.start()
        return t

    def stop(self):
        self._stop = True
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass

# compute fill cost on an orderbook for given side and size
def cost_to_fill_from_ob(ob, side, size):
    # ob: {'bids': [[p,q],...], 'asks': [[p,q],...]}
    remaining = float(size)
    cost = 0.0
    filled = 0.0
    levels = ob['asks'] if side == 'buy' else ob['bids']
    for price, vol in levels:
        take = min(remaining, vol)
        cost += take * price
        filled += take
        remaining -= take
        if remaining <= 1e-12:
            break
    if remaining > 1e-12:
        # not enough depth, assume remaining executed at worst price (last level)
        if levels:
            last_price = levels[-1][0]
            cost += remaining * last_price
            filled += remaining
            remaining = 0.0
    avg_price = cost / filled if filled>0 else None
    return {'filled': filled, 'avg_price': avg_price, 'not_filled': remaining}

# update simulate_roundtrip to use depth-based OKX cost
def simulate_roundtrip(side, size, mt5_price, okx_ob, config):
    fee_okx = config.get('fee_okx', 0.001)
    fee_mt5 = config.get('fee_mt5', 0.0005)
    # mt5_price: {'bid':..., 'ask':...}

    if side == 'sell_mt5_buy_okx':
        # sell on MT5 at mt5_bid (assume full fill), buy on OKX consuming asks
        exec_price_mt5 = float(mt5_price['bid'])
        okx_fill = cost_to_fill_from_ob(okx_ob, 'buy', size)
        exec_price_okx = okx_fill['avg_price']
        if exec_price_okx is None:
            return {'net': -9999, 'reason': 'no depth'}
        gross = (exec_price_mt5 - exec_price_okx) * float(size)
    else:
        exec_price_mt5 = float(mt5_price['ask'])
        okx_fill = cost_to_fill_from_ob(okx_ob, 'sell', size)
        exec_price_okx = okx_fill['avg_price']
        if exec_price_okx is None:
            return {'net': -9999, 'reason': 'no depth'}
        gross = (exec_price_okx - exec_price_mt5) * float(size)

    fee_cost = (abs(exec_price_okx) * float(size) * fee_okx) + (abs(exec_price_mt5) * float(size) * fee_mt5)
    net = gross - fee_cost
    details = {
        'exec_price_mt5': exec_price_mt5,
        'exec_price_okx': exec_price_okx,
        'gross': gross,
        'fee_cost': fee_cost,
        'net': net,
        'depth_filled': okx_fill
    }
    return details

# backtest replay on recorded file and generate report
def run_backtest_from_file(filepath, config):
    print('Running backtest on', filepath)
    times = []
    pnls = []
    equity = [0.0]
    with open(filepath,'r') as f:
        for line in f:
            obj = json.loads(line)
            okx = obj.get('okx')
            mt5p = obj.get('mt5')
            if not okx or not mt5p:
                continue
            mt5_mid = (float(mt5p['bid']) + float(mt5p['ask']))/2.0
            okx_mid = (okx['bids'][0][0] + okx['asks'][0][0]) / 2.0 if okx['bids'] and okx['asks'] else None
            if okx_mid is None:
                continue
            spread = mt5_mid - okx_mid
            ts = obj.get('ts')
            if spread > config['spread_threshold']:
                res = simulate_roundtrip('sell_mt5_buy_okx', config['order_size'], mt5p, okx, config)
            elif spread < -config['spread_threshold']:
                res = simulate_roundtrip('buy_mt5_sell_okx', config['order_size'], mt5p, okx, config)
            else:
                res = None
            if res and 'net' in res:
                pnl = res['net']
                pnls.append(pnl)
                equity.append(equity[-1] + pnl)
                times.append(ts)
    # build df
    if len(equity) <= 1:
        print('No trades in backtest')
        return
    df = pd.DataFrame({'ts': [pd.to_datetime(t) for t in times], 'pnl': pnls})
    df.set_index('ts', inplace=True)
    eq = pd.Series(equity[1:], index=df.index)
    # compute drawdown
    running_max = eq.cummax()
    drawdown = eq - running_max
    max_dd = drawdown.min()
    total = eq.iloc[-1]
    avg = df['pnl'].mean()
    wins = (df['pnl']>0).sum()
    print('BACKTEST RESULTS - total:', total, 'avg:', avg, 'trades:', len(df), 'wins:', wins, 'max_dd:', max_dd)
    # plot
    plt.figure(figsize=(10,6))
    eq.plot(title='Equity Curve')
    plt.xlabel('time')
    plt.ylabel('equity')
    out = 'backtest_report.png'
    plt.savefig(out)
    print('Saved report to', out)

def place_mt5_order(side, volume):
    """Place a market order on MT5 using MetaTrader5 package.
    side: 'buy' or 'sell'
    volume: lot size (float)
    """
    symbol = CONFIG['symbol_mt5']
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        print('mt5 tick fetch failed for order')
        return None

    price = float(tick.ask) if side == 'buy' else float(tick.bid)
    order_type = mt5.ORDER_TYPE_BUY if side == 'buy' else mt5.ORDER_TYPE_SELL

    request = {
        'action': mt5.TRADE_ACTION_DEAL,
        'symbol': symbol,
        'volume': float(volume),
        'type': order_type,
        'price': price,
        'deviation': 20,
        'magic': 234000,
        'comment': 'arb_bot',
        'type_filling': mt5.ORDER_FILLING_FOK,
    }

    result = mt5.order_send(request)
    # result is a trade result structure; print for debugging
    print('mt5 order_send result:', result)
    return result

def main():
    okx_gw = OKXGateway(symbol=CONFIG['symbol_okx'])
    mt5_gw = MT5Gateway(symbol=CONFIG['symbol_mt5'])

    # simulation / stats
    simulate = True
    stats = {'trades': 0, 'pnls': []}

    # start OKX WebSocket depth subscriber
    instId = OKX_INST_MAP[CONFIG['symbol_okx']]
    ws = OKXWS(instId, CONFIG['okx_record_file'])
    ws.start()

    try:
        while True:
            mt5_price = mt5_gw.get_tick()
            ob = okx_gw.get_orderbook()

            if not mt5_price or not ob:
                time.sleep(0.5)
                continue

            mt5_mid = (float(mt5_price['bid']) + float(mt5_price['ask'])) / 2.0
            okx_mid = (float(ob['bids'][0][0]) + float(ob['asks'][0][0])) / 2.0
            spread = mt5_mid - okx_mid

            print('mt5_mid', mt5_mid, 'okx_mid', okx_mid, 'spread', spread)

            if spread > CONFIG['spread_threshold']:
                print('Arb opportunity: sell MT5 buy OKX')
                if simulate:
                    res = simulate_roundtrip('sell_mt5_buy_okx', CONFIG['order_size'], mt5_price, ob, CONFIG)
                    stats['trades'] += 1
                    stats['pnls'].append(res['net'])
                    print('sim result', res)
                else:
                    mt5_gw.place_market_order('sell', CONFIG['order_size'])
                    try:
                        okx_gw.place_limit_order('buy', CONFIG['order_size'], price=ob['bids'][0][0], post_only=True)
                    except Exception as e:
                        print('okx order error', e)
            elif spread < -CONFIG['spread_threshold']:
                print('Arb opportunity: buy MT5 sell OKX')
                if simulate:
                    res = simulate_roundtrip('buy_mt5_sell_okx', CONFIG['order_size'], mt5_price, ob, CONFIG)
                    stats['trades'] += 1
                    stats['pnls'].append(res['net'])
                    print('sim result', res)
                else:
                    mt5_gw.place_market_order('buy', CONFIG['order_size'])
                    try:
                        okx_gw.place_limit_order('sell', CONFIG['order_size'], price=ob['asks'][0][0], post_only=True)
                    except Exception as e:
                        print('okx order error', e)

            if stats['trades'] and stats['trades'] % 10 == 0:
                total = sum(stats['pnls'])
                avg = total / len(stats['pnls']) if stats['pnls'] else 0
                wins = len([p for p in stats['pnls'] if p>0])
                print(f"SIM STATS - trades: {stats['trades']} total_pnl: {total:.6f} avg_pnl: {avg:.6f} wins: {wins}/{len(stats['pnls'])}")

            time.sleep(0.5)
    except KeyboardInterrupt:
        print('Stopping, shutting down mt5')
    finally:
        try:
            mt5_gw.shutdown()
        except Exception:
            pass

if __name__=='__main__':
    main()
