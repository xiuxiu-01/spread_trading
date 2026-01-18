import os
import sys
import time
import threading
from collections import deque
from datetime import datetime
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Ensure project root on path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from gateway.okx_gateway import OKXGateway
from gateway.mt5_gateway import MT5Gateway

load_dotenv()

# Parameters (can be overridden by env)
EMA_PERIOD = int(os.getenv('EMA_PERIOD', '60'))  # 60 1m bars
FIRST_SPREAD = float(os.getenv('FIRST_SPREAD', '1.0'))
NEXT_SPREAD = float(os.getenv('NEXT_SPREAD', '1.0'))
TAKE_PROFIT = float(os.getenv('TAKE_PROFIT', '0.5'))
MAX_POS = int(os.getenv('MAX_POS', '3'))

SYMBOL_OKX = os.getenv('OKX_SYMBOL', 'PAXG/USDT')
SYMBOL_MT5 = os.getenv('MT5_SYMBOL', 'XAU')

# Helpers
class Ring:
    def __init__(self, maxlen):
        self.buf = deque(maxlen=maxlen)
    def append(self, v):
        self.buf.append(v)
    def list(self):
        return list(self.buf)

def ema(values, period):
    if not values:
        return []
    k = 2 / (period + 1)
    out = []
    for i, v in enumerate(values):
        if i == 0:
            out.append(v)
        else:
            out.append(v * k + out[-1] * (1 - k))
    return out

def floor_minute(ts):
    return ts.replace(second=0, microsecond=0)

def main():
    okx = OKXGateway(symbol=SYMBOL_OKX)
    okx.start_ws()
    mt5 = MT5Gateway(symbol=SYMBOL_MT5)

    # 1m candles buffers
    mt5_closes = []
    okx_closes = []
    mt5_times = []
    okx_times = []

    current_min = None
    mt5_last = None
    okx_last = None

    # Positions state per level
    opened_levels_pos = set()  # tuples ('long'/'short', level_index)

    # Plot setup
    plt.ion()
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    ax1.set_title('OKX & MT5 1m Close')
    ax2.set_title('Spread EMA and Bands')
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    def update_plots():
        ax1.clear()
        ax2.clear()
        ax1.set_title('OKX & MT5 1m Close')
        ax2.set_title('Spread EMA and Bands')
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        # Plot closes
        if mt5_times and mt5_closes:
            ax1.plot(mt5_times, mt5_closes, label='MT5 close', color='gold')
        if okx_times and okx_closes:
            ax1.plot(okx_times, okx_closes, label='OKX close', color='green')
        ax1.legend(loc='upper left')
        # Spread close = mt5_close - okx_close, EMA and bands
        if len(mt5_closes) and len(okx_closes):
            n = min(len(mt5_closes), len(okx_closes))
            spreads = [mt5_closes[i] - okx_closes[i] for i in range(n)]
            times = mt5_times[:n]
            ema_vals = ema(spreads, EMA_PERIOD)
            ax2.plot(times, spreads, label='spread', color='blue')
            ax2.plot(times, ema_vals, label='ema', color='black')
            # Bands based on MAX_POS, FIRST_SPREAD, NEXT_SPREAD
            for lvl in range(1, MAX_POS + 1):
                upper = [e + FIRST_SPREAD + (lvl - 1) * NEXT_SPREAD for e in ema_vals]
                lower = [e - FIRST_SPREAD - (lvl - 1) * NEXT_SPREAD for e in ema_vals]
                ax2.plot(times, upper, linestyle='--', color='red', alpha=0.6)
                ax2.plot(times, lower, linestyle='--', color='orange', alpha=0.6)
            ax2.legend(loc='upper left')
        fig.tight_layout()
        plt.pause(0.001)

    def mt5_loop():
        nonlocal mt5_last, current_min
        for tick in mt5.stream_ticks():
            mt5_last = {'bid': tick.get('bid'), 'ask': tick.get('ask')}
            now = datetime.now()
            m = floor_minute(now)
            if current_min is None:
                current_min = m
            if m != current_min:
                # close previous minute using last mid or ask/bid as close
                if mt5_last and mt5_last.get('bid') is not None and mt5_last.get('ask') is not None:
                    close = (mt5_last['bid'] + mt5_last['ask']) / 2.0
                    mt5_closes.append(close)
                    mt5_times.append(current_min)
                current_min = m
                update_plots()

    def okx_loop():
        nonlocal okx_last, current_min
        while True:
            ob = okx.latest_ws_snapshot() or {}
            bids = ob.get('bids') or []
            asks = ob.get('asks') or []
            if bids and asks:
                best_bid = bids[0][0]
                best_ask = asks[0][0]
                okx_last = {'bid': best_bid, 'ask': best_ask}
                now = datetime.now()
                m = floor_minute(now)
                if current_min is None:
                    current_min = m
                if m != current_min:
                    if okx_last:
                        close = (best_bid + best_ask) / 2.0
                        okx_closes.append(close)
                        okx_times.append(current_min)
                    current_min = m
                    update_plots()
            time.sleep(0.3)

    def trading_loop():
        # Real-time entry/exit using current ticks and EMA bands
        while True:
            if mt5_last is None or okx_last is None:
                time.sleep(0.2)
                continue
            # current spread metrics
            # Using latest completed EMA based on closes
            n = min(len(mt5_closes), len(okx_closes))
            if n == 0:
                time.sleep(0.2)
                continue
            spreads = [mt5_closes[i] - okx_closes[i] for i in range(n)]
            ema_vals = ema(spreads, EMA_PERIOD)
            current_ema = ema_vals[-1]
            mt5_bid = mt5_last['bid']
            mt5_ask = mt5_last['ask']
            okx_bid = okx_last['bid']
            okx_ask = okx_last['ask']
            # Direction based on last 1m close diff sign
            close_diff = spreads[-1]

            # Levels pricing
            upper_levels = [current_ema + FIRST_SPREAD + i * NEXT_SPREAD for i in range(MAX_POS)]
            lower_levels = [current_ema - FIRST_SPREAD - i * NEXT_SPREAD for i in range(MAX_POS)]

            # Entry rules
            # Case A: close_diff > 0
            if close_diff > 0:
                # Forward: mt5 bid - okx ask reaches ema + levels => short MT5, long OKX
                diff_fwd = mt5_bid - okx_ask
                for i, lvl in enumerate(upper_levels, start=1):
                    key = ('fwd', i)
                    if diff_fwd >= lvl and key not in opened_levels_pos:
                        print(f"[OPEN FWD L{i}] MT5 short / OKX long at diff {diff_fwd:.4f} >= {lvl:.4f}")
                        opened_levels_pos.add(key)
                        break
                # Reverse: mt5 ask - okx bid reaches ema - levels => long MT5, short OKX
                diff_rev = mt5_ask - okx_bid
                for i, lvl in enumerate(lower_levels, start=1):
                    key = ('rev', i)
                    if diff_rev <= lvl and key not in opened_levels_pos:
                        print(f"[OPEN REV L{i}] MT5 long / OKX short at diff {diff_rev:.4f} <= {lvl:.4f}")
                        opened_levels_pos.add(key)
                        break
            else:
                # close_diff < 0
                # Forward: okx bid - mt5 ask reaches ema + levels => short MT5, long OKX
                diff_fwd = okx_bid - mt5_ask
                for i, lvl in enumerate(upper_levels, start=1):
                    key = ('fwd', i)
                    if diff_fwd >= lvl and key not in opened_levels_pos:
                        print(f"[OPEN FWD L{i}] MT5 short / OKX long at diff {diff_fwd:.4f} >= {lvl:.4f}")
                        opened_levels_pos.add(key)
                        break
                # Reverse: mt5 bid - okx ask reaches ema - levels => long MT5, short OKX
                diff_rev = mt5_bid - okx_ask
                for i, lvl in enumerate(lower_levels, start=1):
                    key = ('rev', i)
                    if diff_rev <= lvl and key not in opened_levels_pos:
                        print(f"[OPEN REV L{i}] MT5 long / OKX short at diff {diff_rev:.4f} <= {lvl:.4f}")
                        opened_levels_pos.add(key)
                        break

            # Take profit checks per your rules (simplified)
            # a) when forward opened on L1, TP when mt5 ask - okx bid reaches (ema + FIRST_SPREAD - TP)
            if ('fwd', 1) in opened_levels_pos:
                tp = current_ema + FIRST_SPREAD - TAKE_PROFIT
                if mt5_ask - okx_bid <= tp:
                    print(f"[CLOSE FWD L1] TP hit at {mt5_ask - okx_bid:.4f} <= {tp:.4f}")
                    opened_levels_pos.remove(('fwd', 1))
            # b) when reverse opened on L2, TP when mt5 bid - okx ask reaches (ema - FIRST_SPREAD + TP)
            if ('rev', 2) in opened_levels_pos:
                tp = current_ema - FIRST_SPREAD + TAKE_PROFIT
                if mt5_bid - okx_ask >= tp:
                    print(f"[CLOSE REV L2] TP hit at {mt5_bid - okx_ask:.4f} >= {tp:.4f}")
                    opened_levels_pos.remove(('rev', 2))
            # c) when forward opened on L3, TP when mt5 bid - okx bid reaches (ema + FIRST_SPREAD - TP)
            if ('fwd', 3) in opened_levels_pos:
                tp = current_ema + FIRST_SPREAD - TAKE_PROFIT
                if mt5_bid - okx_bid <= tp:
                    print(f"[CLOSE FWD L3] TP hit at {mt5_bid - okx_bid:.4f} <= {tp:.4f}")
                    opened_levels_pos.remove(('fwd', 3))
            # d) when reverse opened on L4, TP when mt5 ask - okx ask reaches (ema - FIRST_SPREAD + TP)
            if ('rev', 4) in opened_levels_pos:
                tp = current_ema - FIRST_SPREAD + TAKE_PROFIT
                if mt5_ask - okx_ask >= tp:
                    print(f"[CLOSE REV L4] TP hit at {mt5_ask - okx_ask:.4f} >= {tp:.4f}")
                    opened_levels_pos.remove(('rev', 4))

            time.sleep(0.2)

    # Start threads
    t1 = threading.Thread(target=mt5_loop, daemon=True)
    t2 = threading.Thread(target=okx_loop, daemon=True)
    t3 = threading.Thread(target=trading_loop, daemon=True)
    t1.start(); t2.start(); t3.start()

    print('Strategy running. Close the plot window to stop.')
    try:
        while plt.fignum_exists(fig.number):
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
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
