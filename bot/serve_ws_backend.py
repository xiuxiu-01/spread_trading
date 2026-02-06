import os
import sys
import json
import time
import threading
import gc
from glob import glob
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from gateway.okx_gateway import OKXGateway
from gateway.mt5_gateway import MT5Gateway

import asyncio
import websockets

# New: aiohttp to serve frontend
from aiohttp import web

load_dotenv()

# Persistence configuration
DATA_DIR = os.path.join(ROOT, 'data')
os.makedirs(DATA_DIR, exist_ok=True)
HISTORY_FILE = os.path.join(DATA_DIR, 'history.jsonl')
DAILY_LOG_DIR = os.path.join(DATA_DIR, 'daily_logs')
os.makedirs(DAILY_LOG_DIR, exist_ok=True)

EMA_PERIOD = int(os.getenv('EMA_PERIOD', '120'))
FIRST_SPREAD = float(os.getenv('FIRST_SPREAD', '6.0'))
NEXT_SPREAD = float(os.getenv('NEXT_SPREAD', '3.0'))
TAKE_PROFIT = float(os.getenv('TAKE_PROFIT', '3.0'))
MAX_POS = int(os.getenv('MAX_POS', '6'))

# Memory limits to prevent infinite growth
MAX_HISTORY_BARS = 10000  # ~7 days of 1-minute bars
MAX_ORDERS = 500  # Maximum orders to keep in memory

SYMBOL_OKX = os.getenv('OKX_SYMBOL', 'XAU/USDT:USDT')
SYMBOL_MT5 = os.getenv('MT5_SYMBOL', 'XAU')

clients = set()
params = {
    'emaPeriod': EMA_PERIOD,
    'firstSpread': FIRST_SPREAD,
    'nextSpread': NEXT_SPREAD,
    'takeProfit': TAKE_PROFIT,
    'maxPos': MAX_POS,
    'OKX_SYMBOL': SYMBOL_OKX,
    'MT5_SYMBOL': SYMBOL_MT5,
    'tradeVolume': 0.01, # Default trade volume (lots for MT5, converted for OKX)
    'autoTrade': True, # New parameter for Auto Trade
}
orders = [] # In-memory order history

def trim_orders():
    """Keep only the last MAX_ORDERS orders in memory."""
    global orders
    if len(orders) > MAX_ORDERS:
        orders = orders[-MAX_ORDERS:]
        print(f"[Memory] Trimmed orders to {MAX_ORDERS} records")


def get_order_logs_with_pnl(limit: int = 50) -> dict:
    """
    Read order logs from daily log files and calculate PnL.
    
    Returns:
        dict with:
            - orders: list of order records with individual PnL
            - pnl: summary of PnL (mt5, okx, total)
    """
    all_orders = []
    
    # Find all order log files
    log_pattern = os.path.join(DAILY_LOG_DIR, 'orders_*.jsonl')
    log_files = sorted(glob(log_pattern), reverse=True)  # Most recent first
    
    # Read orders from files until we have enough
    for log_file in log_files:
        if len(all_orders) >= limit:
            break
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                file_orders = []
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            order = json.loads(line)
                            file_orders.append(order)
                        except json.JSONDecodeError:
                            continue
                # Reverse to get most recent first
                file_orders.reverse()
                all_orders.extend(file_orders)
        except Exception as e:
            print(f"[OrderLogs] Failed to read {log_file}: {e}")
    
    # Limit to requested number
    all_orders = all_orders[:limit]
    
    # Calculate PnL for each order
    total_mt5_pnl = 0.0
    total_okx_pnl = 0.0
    
    for order in all_orders:
        mt5_pnl = 0.0
        okx_pnl = 0.0
        
        # Extract MT5 profit from result
        mt5_result = order.get('mt5_result')
        if isinstance(mt5_result, dict):
            mt5_pnl = float(mt5_result.get('profit', 0) or 0)
        
        # Extract OKX PnL from result - look for fillPnl or pnl
        okx_result = order.get('okx_result')
        if isinstance(okx_result, dict):
            # Check for fillPnl in info (from ccxt)
            info = okx_result.get('info', {})
            if isinstance(info, dict):
                okx_pnl = float(info.get('fillPnl', 0) or 0)
            # Also check direct pnl field
            if okx_pnl == 0:
                okx_pnl = float(okx_result.get('pnl', 0) or 0)
        
        order['mt5_pnl'] = mt5_pnl
        order['okx_pnl'] = okx_pnl
        
        # Only count filled orders for PnL
        if order.get('status') == 'filled_all':
            total_mt5_pnl += mt5_pnl
            total_okx_pnl += okx_pnl
    
    return {
        'orders': all_orders,
        'pnl': {
            'mt5': total_mt5_pnl,
            'okx': total_okx_pnl,
            'total': total_mt5_pnl + total_okx_pnl,
        }
    }


def get_memory_stats():
    """Get memory usage statistics."""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        mem = process.memory_info()
        return {
            'rss_mb': mem.rss / 1024 / 1024,
            'vms_mb': mem.vms / 1024 / 1024,
        }
    except ImportError:
        # psutil not available, use basic tracking
        return {'objects': len(gc.get_objects())}

async def broadcast(msg):
    dead = set()
    for ws in list(clients):
        try:
            # Support both aiohttp and websockets interfaces
            if hasattr(ws, 'send_json'):
                await ws.send_json(msg)
            else:
                await ws.send(json.dumps(msg))
        except (ConnectionResetError, ConnectionError, OSError):
            # Client disconnected - silently remove
            dead.add(ws)
        except Exception:
            dead.add(ws)
    for d in dead:
        clients.discard(d)

def floor_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)

def compute_ema(series, period):
    if not series:
        return []
    k = 2.0 / (period + 1)
    out = []
    last = None
    for v in series:
        if v is None:
            # carry last for stability
            v = last if last is not None else 0.0
        last = v if last is None else (v * k + last * (1 - k))
        out.append(last)
    return out

class Aggregator:
    def __init__(self, ema_period: int):
        self.lock = threading.Lock()
        self.ema_period = ema_period
        self.last_minute = None
        
        # Current minute buffers
        # Format: {'open': float, 'high': float, 'low': float, 'close': float, 'vol': int}
        self.buf_mt5 = None
        self.buf_okx = None
        
        # Last known closes (persist across minutes)
        self.last_mt5_close = None
        self.last_okx_close = None
        
        # NEW: Store last bid/ask for spread calculation
        self.last_mt5_bid = None
        self.last_mt5_ask = None
        self.last_okx_bid = None
        self.last_okx_ask = None
        
        self.times = []
        self.mt5_stats = [] # Store OHLCV dicts
        self.okx_stats = [] # Store OHLCV dicts
        self.spreads = []
        self.ema = []
        
        # NEW: Two spread lines for active trading
        self.spread_sell = []  # MT5 bid - OKX ask (主动卖MT5买OKX)
        self.spread_buy = []   # MT5 ask - OKX bid (主动买MT5卖OKX)
    
    def trim_to_max(self):
        """Trim all lists to MAX_HISTORY_BARS to prevent memory growth."""
        if len(self.times) > MAX_HISTORY_BARS:
            excess = len(self.times) - MAX_HISTORY_BARS
            self.times = self.times[excess:]
            self.mt5_stats = self.mt5_stats[excess:]
            self.okx_stats = self.okx_stats[excess:]
            self.spreads = self.spreads[excess:]
            self.ema = self.ema[excess:]
            self.spread_sell = self.spread_sell[excess:]
            self.spread_buy = self.spread_buy[excess:]
            print(f"[Memory] Trimmed history to {MAX_HISTORY_BARS} bars")

    def load_from_file(self):
        if not os.path.exists(HISTORY_FILE):
            return
        print(f"Loading history from {HISTORY_FILE}...")
        
        # Use a dictionary to deduplicate by timestamp, keeping the LAST entry
        history_map = {}
        
        try:
            with open(HISTORY_FILE, 'r') as f:
                for line in f:
                    if not line.strip(): continue
                    try:
                        data = json.loads(line)
                        ts = data.get('ts')
                        if ts:
                            history_map[ts] = data
                    except Exception as loop_e:
                        print(f"Skipping corrupt line: {loop_e}")
            
            # Reconstruct lists sorted by timestamp
            sorted_keys = sorted(history_map.keys())
            
            self.times = []
            self.mt5_stats = []
            self.okx_stats = []
            self.spreads = []
            self.ema = []
            self.spread_sell = []
            self.spread_buy = []
            
            for ts in sorted_keys:
                d = history_map[ts]
                self.times.append(ts)
                self.mt5_stats.append(d['mt5'])
                self.okx_stats.append(d['okx'])
                self.spreads.append(d['spread'])
                # Load spread_sell/spread_buy if available, else use mid spread
                self.spread_sell.append(d.get('spread_sell', d['spread']))
                self.spread_buy.append(d.get('spread_buy', d['spread']))
                # Recompute EMA if missing or corrupt, but usually load it
                if 'ema' in d:
                    self.ema.append(d['ema'])
                else:
                    self.ema.append(None) # Will be recomputed
            
            # Recompute EMA from loaded spreads to ensure consistency
            self.ema = compute_ema(self.spreads, self.ema_period)
            
            if self.times:
                print(f"Loaded {len(self.times)} bars after deduplication. Last: {self.times[-1]}")
                # Restore last_close prices from history
                if self.mt5_stats:
                    self.last_mt5_close = self.mt5_stats[-1].get('close')
                if self.okx_stats:
                    self.last_okx_close = self.okx_stats[-1].get('close')

        except Exception as e:
            print(f"Error loading history: {e}")

    def save_bar(self, bar):
        try:
            with open(HISTORY_FILE, 'a') as f:
                f.write(json.dumps(bar) + '\n')
        except Exception as e:
            print(f"Error saving bar: {e}")

    def rewrite_file(self):
        try:
            with open(HISTORY_FILE, 'w') as f:
                for i in range(len(self.times)):
                    row = {
                        'ts': self.times[i],
                        'mt5': self.mt5_stats[i],
                        'okx': self.okx_stats[i],
                        'spread': self.spreads[i],
                        'spread_sell': self.spread_sell[i] if i < len(self.spread_sell) else self.spreads[i],
                        'spread_buy': self.spread_buy[i] if i < len(self.spread_buy) else self.spreads[i],
                        'ema': self.ema[i]
                    }
                    f.write(json.dumps(row) + '\n')
            print(f"Rewrote history file with {len(self.times)} records.")
        except Exception as e:
            print(f"Error rewriting file: {e}")

    def get_last_timestamp(self) -> datetime:
        if not self.times:
            return None
        return datetime.fromisoformat(self.times[-1])

    def merge_external_history(self, mt5_hist, okx_hist):
        def clean_candle(c):
            vol = c.get('vol') if 'vol' in c else c.get('volume', 0)
            return {
                'open': float(c['open']),
                'high': float(c['high']),
                'low': float(c['low']),
                'close': float(c['close']),
                'vol': int(vol)
            }

        existing = {}
        for i, t_str in enumerate(self.times):
            dt = datetime.fromisoformat(t_str)
            ts = int(dt.timestamp())
            existing[ts] = {
                'mt5': self.mt5_stats[i],
                'okx': self.okx_stats[i]
            }

        new_mt5 = {x['time']: clean_candle(x) for x in mt5_hist}
        new_okx = {x['time']: clean_candle(x) for x in okx_hist}

        all_ts = sorted(list(set(existing.keys()) | set(new_mt5.keys()) | set(new_okx.keys())))
        
        self.times = []
        self.mt5_stats = []
        self.okx_stats = []
        self.spreads = []
        self.spread_sell = []
        self.spread_buy = []
        self.ema = []

        for ts in all_ts:
            if ts in new_mt5:
                c_mt5 = new_mt5[ts]
            elif ts in existing:
                c_mt5 = existing[ts]['mt5']
            else:
                continue

            if ts in new_okx:
                c_okx = new_okx[ts]
            elif ts in existing:
                c_okx = existing[ts]['okx']
            else:
                continue 

            dt_iso = datetime.fromtimestamp(ts, timezone.utc).isoformat()
            self.times.append(dt_iso)
            self.mt5_stats.append(c_mt5)
            self.okx_stats.append(c_okx)
            
            spread = c_mt5['close'] - c_okx['close']
            self.spreads.append(spread)
            # For historical data, we don't have bid/ask, use mid spread
            self.spread_sell.append(spread)
            self.spread_buy.append(spread)
        
        self.ema = compute_ema(self.spreads, self.ema_period)
        self.trim_to_max()  # Limit memory usage
        self.rewrite_file()

    def get_history_payload(self):
        ts_ints = []
        mt5_out = []
        okx_out = []
        spread_out = []
        spread_sell_out = []
        spread_buy_out = []
        ema_out = []
        
        for i, t_str in enumerate(self.times):
            dt = datetime.fromisoformat(t_str)
            # Ensure UTC for consistency
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
                
            ts_val = int(dt.timestamp())
            ts_ints.append(ts_val)
            
            m = self.mt5_stats[i].copy()
            m['time'] = ts_val
            mt5_out.append(m)
            
            o = self.okx_stats[i].copy()
            o['time'] = ts_val
            okx_out.append(o)

            spread_out.append({'time': ts_val, 'value': self.spreads[i]})
            
            # Two spread lines for active trading
            sell_val = self.spread_sell[i] if i < len(self.spread_sell) else self.spreads[i]
            buy_val = self.spread_buy[i] if i < len(self.spread_buy) else self.spreads[i]
            spread_sell_out.append({'time': ts_val, 'value': sell_val})
            spread_buy_out.append({'time': ts_val, 'value': buy_val})
            
            ema_out.append({'time': ts_val, 'value': self.ema[i]})
            
        return {
            'ts': ts_ints,
            'mt5': mt5_out,
            'okx': okx_out,
            'spread': spread_out,
            'spread_sell': spread_sell_out,  # MT5 bid - OKX ask
            'spread_buy': spread_buy_out,    # MT5 ask - OKX bid
            'ema': ema_out,
        }

    def _sanitize_mid(self, v):
        try:
            if v is None:
                return None
            v = float(v)
            if not (v > 0 and v < 1e9):
                return None
            return v
        except Exception:
            return None

    def _update_candle(self, buf, price):
        if buf is None:
            return {'open': price, 'high': price, 'low': price, 'close': price, 'vol': 1}
        
        buf['close'] = price
        buf['high'] = max(buf['high'], price)
        buf['low'] = min(buf['low'], price)
        buf['vol'] += 1
        return buf

    def on_mt5_tick(self, bid, ask, ts: datetime, last: float = None):
        with self.lock:
            # Filter out invalid ticks (zero or negative) to prevent wick artifacts
            if not bid or bid <= 0 or not ask or ask <= 0:
                return None

            # Prefer 'last' price for close, else mid
            price = last if last and last > 0 else (bid + ask) / 2.0
            mid = self._sanitize_mid(price)

            emit_payload = None
            
            # Check for emission BEFORE adding the tick to the buffer
            if mid is not None:
                emit_payload = self._maybe_emit(ts)
                self.buf_mt5 = self._update_candle(self.buf_mt5, mid)
                self.last_mt5_close = mid
                # Store bid/ask for spread calculation
                self.last_mt5_bid = float(bid)
                self.last_mt5_ask = float(ask)
                
            return emit_payload

    def on_okx_tick(self, bid, ask, ts: datetime, last: float = None):
        with self.lock:
             # Filter out invalid ticks (zero or negative) to prevent wick artifacts
            if not bid or bid <= 0 or not ask or ask <= 0:
                return None

            # Prefer 'last' price for close, else mid
            price = last if last and last > 0 else (bid + ask) / 2.0
            mid = self._sanitize_mid(price)
            
            emit_payload = None

            # Check for emission BEFORE adding the tick to the buffer
            if mid is not None:
                emit_payload = self._maybe_emit(ts)
                self.buf_okx = self._update_candle(self.buf_okx, mid)
                self.last_okx_close = mid
                # Store bid/ask for spread calculation
                self.last_okx_bid = float(bid)
                self.last_okx_ask = float(ask)
                
            return emit_payload

    def _maybe_emit(self, ts: datetime):
        minute = floor_minute(ts)
        # Initialize last_minute on first run provided we have a valid timestamp
        if self.last_minute is None:
            self.last_minute = minute
            return None

        # Check for new minute
        if minute > self.last_minute:
            # Finalize the bar for self.last_minute
            def finalize(buf, last_close):
                if buf: return buf
                p = last_close if last_close is not None else 0.0
                return {'open': p, 'high': p, 'low': p, 'close': p, 'vol': 0}

            c_mt5 = finalize(self.buf_mt5, self.last_mt5_close)
            c_okx = finalize(self.buf_okx, self.last_okx_close)
            
            # Ensure timestamp is UTC aware for ISO format
            bar_ts = self.last_minute
            if bar_ts.tzinfo is None:
                bar_ts = bar_ts.replace(tzinfo=timezone.utc)
            ts_iso = bar_ts.isoformat()

            res = None
            if c_mt5['close'] > 0 and c_okx['close'] > 0:
                # Save to memory history
                self.times.append(ts_iso)
                self.mt5_stats.append(c_mt5)
                self.okx_stats.append(c_okx)
                
                spread = c_mt5['close'] - c_okx['close']
                self.spreads.append(spread)
                
                # Calculate spread_sell and spread_buy using last bid/ask
                # spread_sell = MT5 bid - OKX ask (主动卖MT5，主动买OKX)
                # spread_buy = MT5 ask - OKX bid (主动买MT5，主动卖OKX)
                mt5_bid = self.last_mt5_bid if self.last_mt5_bid else c_mt5['close']
                mt5_ask = self.last_mt5_ask if self.last_mt5_ask else c_mt5['close']
                okx_bid = self.last_okx_bid if self.last_okx_bid else c_okx['close']
                okx_ask = self.last_okx_ask if self.last_okx_ask else c_okx['close']
                
                spread_sell = mt5_bid - okx_ask  # 卖MT5买OKX的实际spread
                spread_buy = mt5_ask - okx_bid   # 买MT5卖OKX的实际spread
                self.spread_sell.append(spread_sell)
                self.spread_buy.append(spread_buy)
                
                # Optimized EMA append:
                last_ema = self.ema[-1] if self.ema else spread
                k = 2.0 / (self.ema_period + 1)
                new_ema = spread * k + last_ema * (1 - k)
                self.ema.append(new_ema)
                
                ema_val = new_ema
                
                print(f"Emitting bar: {ts_iso} Spread: {spread:.2f} Sell: {spread_sell:.2f} Buy: {spread_buy:.2f} EMA: {ema_val:.2f}")
                
                res = {
                    'ts': ts_iso,
                    'mt5': c_mt5,
                    'okx': c_okx,
                    'spread': spread,
                    'spread_sell': spread_sell,
                    'spread_buy': spread_buy,
                    'ema': ema_val,
                }
                
                # Trim memory to prevent unbounded growth
                self.trim_to_max()
            else:
                # Log usage but don't emit incomplete bars
                print(f"Skipping bar {ts_iso}: Missing data (MT5={c_mt5['close']:.2f}, OKX={c_okx['close']:.2f})")
            
            # CRITICAL: Advance the minute and reset buffers
            self.last_minute = minute
            self.buf_mt5 = None
            self.buf_okx = None
            
            return res

        return None

    def _decorate_bar(self, bar):
        ema = bar['ema']
        spread = bar['spread']
        bands = []
        signal_action = None # 'long' or 'short' or None
        signal_level = 0

        if ema is not None and spread is not None:
            first_spread = float(params['firstSpread'])
            next_spread = float(params['nextSpread'])
            max_pos = int(params.get('maxPos') or 3)
            
            # Check levels for signals
            found_short = False
            for lvl in range(max_pos, 0, -1): # Check highest first
                 upper = ema + first_spread + (lvl - 1) * next_spread
                 if spread > upper:
                     signal_action = 'short'
                     signal_level = lvl
                     found_short = True
                     break
            
            if not found_short:
                # Check Long Levels
                for lvl in range(max_pos, 0, -1):
                     lower = ema - first_spread - (lvl - 1) * next_spread
                     if spread < lower:
                         signal_action = 'long'
                         signal_level = lvl
                         break

        for lvl in range(1, int(params['maxPos']) + 1):
            upper = ema + float(params['firstSpread']) + (lvl - 1) * float(params['nextSpread'])
            lower = ema - float(params['firstSpread']) - (lvl - 1) * float(params['nextSpread'])
            bands.append({'upper': upper, 'lower': lower, 'lvl': lvl})

        # Automated Trading Logic
        if signal_action and params.get('autoTrade', False):
             pass

        return {**bar, 'bands': bands, 'signal': signal_action, 'level': signal_level}

class Feeder:
    def __init__(self):
        self.okx = OKXGateway(symbol=SYMBOL_OKX)
        self.okx.start_ws()
        try:
            self.okx.wait_for_ws_snapshot(timeout=5.0)
        except Exception:
            pass
        self.mt5 = MT5Gateway(symbol=SYMBOL_MT5)
        self.agg = Aggregator(EMA_PERIOD)
        self.last_resync_time = 0
        self.account_state = {
            'balance': {'mt5': 0.0, 'okx': 0.0},
            'positions': {'mt5': [], 'okx': []}
        }
        
        # Real-time tick state for display
        self.realtime_state = {
            'mt5_bid': None, 'mt5_ask': None,
            'okx_bid': None, 'okx_ask': None,
            'spread_sell': None,  # MT5 bid - OKX ask
            'spread_buy': None,   # MT5 ask - OKX bid
            'spread_mid': None,
            'ema': None,
            'ts': None
        }
        self.last_signal_time = 0  # Prevent signal spam
        
        # Rolling spread window for trading signals (5 second average)
        self.spread_history = []  # List of (timestamp, spread_sell, spread_buy, spread_mid)
        self.spread_window_seconds = 5  # Rolling window size in seconds
        
        # Daily log file tracking
        self.last_daily_log_minute = None
        
        # Thread-safe tick update flags
        self._tick_updated = False
        self._tick_lock = threading.Lock()

    def _get_daily_log_path(self):
        """Get daily log file path based on current date."""
        today = datetime.now().strftime('%Y-%m-%d')
        return os.path.join(DAILY_LOG_DIR, f'spread_data_{today}.jsonl')
    
    def _get_orders_log_path(self):
        """Get daily orders log file path."""
        today = datetime.now().strftime('%Y-%m-%d')
        return os.path.join(DAILY_LOG_DIR, f'orders_{today}.jsonl')
    
    def _save_minute_data(self, data: dict):
        """Save minute data to daily log file."""
        try:
            log_path = self._get_daily_log_path()
            with open(log_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(data, ensure_ascii=False) + '\n')
        except Exception as e:
            print(f"[DailyLog] Failed to save: {e}")
    
    def _save_order_log(self, order_record: dict, signal_info: dict = None):
        """Save order to daily orders log file.
        
        Args:
            order_record: The order execution result
            signal_info: Optional signal trigger information
        """
        try:
            log_path = self._get_orders_log_path()
            
            # 获取当前实时价格
            mt5_bid = self.realtime_state.get('mt5_bid')
            mt5_ask = self.realtime_state.get('mt5_ask')
            okx_bid = self.realtime_state.get('okx_bid')
            okx_ask = self.realtime_state.get('okx_ask')
            
            # 构建完整的订单日志
            log_entry = {
                'ts': order_record.get('ts'),
                'order_id': order_record.get('id'),
                'direction': order_record.get('direction'),
                'level': order_record.get('level'),
                'vol': order_record.get('vol'),
                'status': order_record.get('status'),
                # 实时价格
                'prices': {
                    'mt5_bid': mt5_bid,
                    'mt5_ask': mt5_ask,
                    'okx_bid': okx_bid,
                    'okx_ask': okx_ask,
                    'spread_sell': mt5_bid - okx_ask if mt5_bid and okx_ask else None,
                    'spread_buy': mt5_ask - okx_bid if mt5_ask and okx_bid else None,
                },
                # EMA
                'ema': self.agg.ema[-1] if self.agg.ema else None,
                # 信号触发原因
                'signal': signal_info,
                # 执行结果
                'mt5_result': order_record.get('mt5_res'),
                'okx_result': order_record.get('okx_res'),
                'error': order_record.get('error'),
            }
            
            with open(log_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, ensure_ascii=False, default=str) + '\n')
            
            print(f"[OrderLog] Saved order to {log_path}")
        except Exception as e:
            print(f"[OrderLog] Failed to save: {e}")

    async def run_account_monitor(self):
        """Periodically fetch and broadcast account info."""
        print("Account Monitor started")
        last_mt5_net = None
        last_okx_net = None
        memory_log_counter = 0
        while True:
            try:
                # Fetch balance
                mt5_bal = await asyncio.to_thread(self.mt5.get_balance)
                okx_bal = await asyncio.to_thread(self.okx.get_balance)
                
                # Get net exposure (already calculated)
                mt5_net, okx_net, _, _ = await asyncio.to_thread(self.get_net_exposure)
                # okx_net is in oz, convert to lots for display (1 lot = 100 oz)
                okx_net_lots = okx_net / 100.0
                
                # Only log when position changes
                if last_mt5_net != mt5_net or last_okx_net != okx_net:
                    print(f"[Account] MT5: {mt5_bal:.2f}, OKX: {okx_bal:.2f} | Net: MT5={mt5_net:.2f} lots, OKX={okx_net:.2f} oz ({okx_net_lots:.2f} lots)")
                    last_mt5_net = mt5_net
                    last_okx_net = okx_net

                # Log memory stats every 60 seconds (12 * 5sec = 60sec)
                memory_log_counter += 1
                if memory_log_counter >= 12:
                    memory_log_counter = 0
                    mem = get_memory_stats()
                    gc.collect()  # Force garbage collection periodically
                    bars = len(self.agg.times)
                    order_cnt = len(orders)
                    ws_clients = len(clients)
                    spread_hist = len(self.spread_history)
                    print(f"[Stats] Memory: {mem} | Bars: {bars} | Orders: {order_cnt} | WS Clients: {ws_clients} | SpreadHist: {spread_hist}")

                new_state = {
                    'balance': {'mt5': mt5_bal, 'okx': okx_bal},
                    'net': {'mt5': mt5_net, 'okx': okx_net_lots},  # Both in lots for display
                    'ts': datetime.now(timezone.utc).isoformat()
                }
                
                self.account_state = new_state
                # Broadcast
                await broadcast({'type': 'account', 'payload': new_state})
                
                await asyncio.sleep(5) # Update every 5 seconds
            except Exception as e:
                print(f"Account monitor error: {e}")
                await asyncio.sleep(5)

    def get_net_exposure(self):
        """
        Returns (mt5_net_vol, okx_net_qty).
        mt5_net_vol: + for Buy, - for Sell
        okx_net_qty: + for Long, - for Short
        """
        mt5_pos = self.mt5.get_positions()
        okx_pos = self.okx.get_positions()
        
        mt5_net = sum([p['volume'] if p['type'] == 'buy' else -p['volume'] for p in mt5_pos])
        
        okx_net = 0.0
        for p in okx_pos:
            qty = p['amount']
            if p['side'] == 'short':
                qty = -qty
            okx_net += qty
            
        return mt5_net, okx_net, mt5_pos, okx_pos

    def execute_trade(self, direction: str, level: int = 1, signal_info: dict = None):
        """
        Execute a spread trade with independent leg management to handle out-of-sync states.
        direction: 'long' (Buy Spread: Buy MT5, Sell OKX) or 'short' (Sell Spread: Sell MT5, Buy OKX)
        level: 1, 2, 3... determines sizing (size = level * base_vol) for accumulation
        signal_info: Optional dict with signal trigger details (spread values, EMA, etc.)
        
        Order execution: OKX first, then MT5 (only if OKX succeeds)
        """
        # Check if MT5 market is open
        if not self.mt5.is_market_open():
            print("[Execute] MT5 market is closed, skipping trade")
            return {'status': 'skipped_market_closed'}
        
        # Initialize order record structure early
        timestamp = datetime.now(timezone.utc).isoformat()
        order_record = {
            'id': int(time.time() * 1000),
            'ts': timestamp,
            'direction': direction,
            'level': level,
            'vol': 0.0,
            'status': 'pending',
            'mt5_res': None,
            'okx_res': None,
            'error': None
        }

        try:
            mt5_net, okx_net, _, _ = self.get_net_exposure()
            base_vol = float(params.get('tradeVolume', 0.01))
            
            # OKX uses oz (amount), MT5 uses lots. 1 lot = 100 oz
            # For display consistency, convert OKX to lot equivalent
            QTY_MULT = 100.0
            okx_net_in_lots = okx_net / QTY_MULT  # Convert OKX oz to lot equivalent
                
            print(f"[Execute] Direction: {direction}, Level: {level}, BaseVol: {base_vol}, MaxPos: {params.get('maxPos')}")
            print(f"[Execute] Current Position - MT5: {mt5_net:.2f} lots, OKX: {okx_net:.2f} oz ({okx_net_in_lots:.2f} lots equiv)")

            max_pos = int(params.get('maxPos', 3))
            
            # Calculate Target Volume for this level (in lots)
            target_vol_mt5 = min(level, max_pos) * base_vol

            LIMIT = base_vol * 0.1  # 10% tolerance
            
            target_net_mt5 = 0.0
            if direction == 'long':
                target_net_mt5 = target_vol_mt5
            else:
                target_net_mt5 = -target_vol_mt5
            
            # For a proper hedge: MT5 long + OKX short (or vice versa)
            # Expected OKX position should be opposite of MT5
            expected_okx_in_lots = -target_net_mt5
            
            delta_mt5 = target_net_mt5 - mt5_net
            delta_okx_in_lots = expected_okx_in_lots - okx_net_in_lots
            
            print(f"[Execute] Target - MT5: {target_net_mt5:.2f} lots, OKX: {expected_okx_in_lots:.2f} lots equiv")
            print(f"[Execute] Delta - MT5: {delta_mt5:.2f}, OKX: {delta_okx_in_lots:.2f} lots equiv")
            
            # Check if both legs are already at target
            if abs(delta_mt5) < LIMIT and abs(delta_okx_in_lots) < LIMIT:
                print(f"[Execute] Already at target exposure (MT5: {mt5_net:.2f}/{target_net_mt5:.2f}, OKX: {okx_net_in_lots:.2f}/{expected_okx_in_lots:.2f}). Skipping.")
                return {'status': 'skipped_already_filled'}
                
            action_mt5 = None
            vol_mt5 = 0.0
            
            if delta_mt5 > LIMIT: # Need to buy
                action_mt5 = 'buy'
                vol_mt5 = delta_mt5
            elif delta_mt5 < -LIMIT: # Need to sell
                action_mt5 = 'sell'
                vol_mt5 = abs(delta_mt5)
                
            vol_mt5 = round(vol_mt5, 2)
            if vol_mt5 < 0.01:
                return {'status': 'skipped_small_volume'}

            order_record['vol'] = vol_mt5

            # QTY_MULT already defined above
            action_okx = None
            amt_okx = 0.0
            
            # Use delta_okx_in_lots converted back to oz for OKX order
            if delta_okx_in_lots > LIMIT:
                action_okx = 'buy'
                amt_okx = abs(delta_okx_in_lots) * QTY_MULT
            elif delta_okx_in_lots < -LIMIT:
                action_okx = 'sell'
                amt_okx = abs(delta_okx_in_lots) * QTY_MULT

            # ========== OKX FIRST ==========
            okx_executed_amt = 0.0
            is_okx_success = False
            
            if action_okx:
                # Calculate OKX amount based on MT5 target volume
                amt_okx = vol_mt5 * QTY_MULT
                print(f"[OKX FIRST] Executing OKX: {action_okx} {amt_okx}")
                
                try:
                    is_spot = ('/' in self.okx.symbol and ':' not in self.okx.symbol and '-SWAP' not in self.okx.symbol)
                    
                    if is_spot:
                        if action_okx == 'buy':
                            quote_ccy = 'USDT'  
                            if '/' in self.okx.symbol:
                                quote_ccy = self.okx.symbol.split('/')[1]
                            
                            free_quote = self.okx.get_balance()
                            ticker = self.okx.get_ticker()
                            price = ticker.get('last') or 0.0
                            
                            if price > 0:
                                cost = amt_okx * price
                                if free_quote < cost:
                                    print(f"!! OKX Insufficient Funds: Have {free_quote} {quote_ccy}, Need ~{cost} !!")
                                    order_record['status'] = 'failed_okx_funds'
                                    order_record['error'] = f'Insufficient {quote_ccy}: {free_quote} < {cost}'
                                    orders.append(order_record)
                                    return order_record

                        elif action_okx == 'sell': 
                            base_ccy = 'XAU'
                            if '/' in self.okx.symbol:
                                base_ccy = self.okx.symbol.split('/')[0]
                            elif '-' in self.okx.symbol:
                                base_ccy = self.okx.symbol.split('-')[0]

                            free_base = self.okx.get_asset_balance(base_ccy)
                            
                            print(f"OKX Check: Selling {amt_okx} {base_ccy}. Have {free_base}.")

                            if free_base < amt_okx:
                                diff = amt_okx - free_base
                                if diff < 0.001 and free_base > 0:
                                    print(f"OKX quantity adjustment: {amt_okx} -> {free_base} (Close ALL avail)")
                                    amt_okx = free_base
                                else:
                                    print(f"!! OKX Insufficient Asset: Have {free_base} {base_ccy}, Need {amt_okx} !!")
                                    order_record['status'] = 'failed_okx_balance'
                                    order_record['error'] = f'Insufficient {base_ccy}: {free_base} < {amt_okx}'
                                    orders.append(order_record)
                                    return order_record

                    okx_res = self.okx.place_market_order(action_okx, amt_okx)
                    order_record['okx_res'] = okx_res
                    
                    # Check OKX success
                    if okx_res and isinstance(okx_res, dict):
                        if okx_res.get('id') or okx_res.get('status') == 'closed':
                            is_okx_success = True
                            filled_val = okx_res.get('filled')
                            okx_executed_amt = float(filled_val) if filled_val is not None else amt_okx
                            print(f"[OKX SUCCESS] Filled: {okx_executed_amt}")
                        else:
                            print(f"[OKX] Response without clear success: {okx_res}")
                            # Assume success if no error
                            is_okx_success = True
                            okx_executed_amt = amt_okx
                    else:
                        print(f"[OKX] Unexpected response: {okx_res}")
                        is_okx_success = True  # Assume success for now
                        okx_executed_amt = amt_okx
                        
                except Exception as e_okx:
                    print(f"OKX Order Failed: {e_okx}")
                    order_record['error'] = str(e_okx)
                    order_record['status'] = 'failed_okx'
                    orders.append(order_record)
                    return order_record
            else:
                order_record['okx_res'] = 'skipped'
                is_okx_success = True  # No OKX action needed

            # ========== MT5 SECOND (only if OKX succeeded) ==========
            if not is_okx_success:
                order_record['status'] = 'failed_okx'
                orders.append(order_record)
                return order_record
            
            mt5_executed_vol = 0.0
            is_mt5_success = False

            if action_mt5:
                # Adjust MT5 volume based on OKX executed amount
                if okx_executed_amt > 0:
                    adjusted_mt5_vol = okx_executed_amt / QTY_MULT
                    print(f"[MT5 SECOND] OKX Filled: {okx_executed_amt} -> MT5 Adjusted: {adjusted_mt5_vol}")
                    vol_mt5 = round(adjusted_mt5_vol, 2)
                
                print(f"[MT5 SECOND] Executing MT5: {action_mt5} {vol_mt5}")
                
                try:
                    mt5_res = self.mt5.place_market_order(action_mt5, vol_mt5)
                    print(f"[MT5] Raw response: {mt5_res}, type: {type(mt5_res)}")
                    if mt5_res and hasattr(mt5_res, 'retcode'):
                        print(f"[MT5] Retcode: {mt5_res.retcode}, Comment: {getattr(mt5_res, 'comment', 'N/A')}")
                except Exception as e:
                    print(f"MT5 Order Exception: {e}")
                    import traceback
                    traceback.print_exc()
                    mt5_res = None

                if mt5_res and hasattr(mt5_res, 'retcode') and mt5_res.retcode == 10009:
                    is_mt5_success = True
                    mt5_executed_vol = mt5_res.volume
                elif isinstance(mt5_res, dict) and mt5_res.get('retcode') == 10009:
                    is_mt5_success = True
                    mt5_executed_vol = mt5_res.get('volume', vol_mt5)
                else:
                    print(f"!! MT5 Order Failed (OKX already executed!): {mt5_res} !!")
                    # WARNING: OKX已成交但MT5失败，需要人工处理
                    is_mt5_success = False
                    mt5_res = mt5_res or {'retcode': -1, 'comment': 'FAILED'}

                def serialize_mt5(res):
                    if hasattr(res, '_asdict'): return res._asdict()
                    if isinstance(res, dict): return res
                    return str(res)
                
                order_record['mt5_res'] = serialize_mt5(mt5_res)
                
                if not is_mt5_success:
                    order_record['status'] = 'failed_mt5_after_okx'
                    order_record['error'] = 'OKX已成交但MT5失败！需要手动处理！'
                    orders.append(order_record)
                    return order_record
            else:
                order_record['mt5_res'] = 'skipped'

            order_record['status'] = 'filled_all'

        except Exception as e:
            print(f"Trade execution critical error: {e}")
            order_record['status'] = 'failed_all'
            order_record['error'] = str(e)
            
        orders.append(order_record)
        trim_orders()
        
        # 保存订单到日志文件
        self._save_order_log(order_record, signal_info)
        
        return order_record

    def close_all(self):
        """Close all positions in MT5 and OKX."""
        print("Closing ALL positions...")
        results = {'mt5': [], 'okx': []}
        
        try:
            mt5_pos = self.mt5.get_positions()
            for p in mt5_pos:
                side = 'sell' if p['type'] == 'buy' else 'buy'
                vol = p['volume']
                print(f"Closing MT5 {p['ticket']}: {side} {vol}")
                res = self.mt5.place_market_order(side, vol)
                results['mt5'].append(str(res))
        except Exception as e:
            print(f"Close All MT5 error: {e}")
            results['mt5'].append(str(e))
            
        try:
            okx_pos = self.okx.get_positions()
            for p in okx_pos:
                side = 'sell' if p['side'] == 'long' else 'buy'
                amt = p['amount']
                print(f"Closing OKX {p['id']}: {side} {amt}")
                res = self.okx.place_market_order(side, amt)
                results['okx'].append(res)
        except Exception as e:
            print(f"Close All OKX error: {e}")
            results['okx'].append(str(e))
            
        return results

    def start(self, loop):
        # Real-time tick thread for MT5 - directly updates shared state
        def mt5_tick_thread():
            print("MT5 Realtime Tick Thread started") 
            try:
                for tick in self.mt5.stream_ticks():
                    bid = tick.get('bid')
                    ask = tick.get('ask')
                    if bid and ask:
                        with self._tick_lock:
                            self.realtime_state['mt5_bid'] = float(bid)
                            self.realtime_state['mt5_ask'] = float(ask)
                            self._tick_updated = True
            except Exception as e:
                print(f"MT5 Tick Thread error: {e}")
        threading.Thread(target=mt5_tick_thread, daemon=True).start()

        # Real-time tick thread for OKX - directly updates shared state
        def okx_tick_thread():
            print("OKX Realtime Tick Thread started")
            while True:
                try:
                    ob = self.okx.latest_ws_snapshot() or {}
                    bids = ob.get('bids') or []
                    asks = ob.get('asks') or []
                    if bids and asks:
                        best_bid = float(bids[0][0])
                        best_ask = float(asks[0][0])
                        with self._tick_lock:
                            self.realtime_state['okx_bid'] = best_bid
                            self.realtime_state['okx_ask'] = best_ask
                            self._tick_updated = True
                    time.sleep(0.3)  # Faster updates for real-time display
                except Exception as e:
                    print(f"OKX Tick Thread error: {e}")
                    time.sleep(1)
        threading.Thread(target=okx_tick_thread, daemon=True).start()

        # K-line fetch thread - runs at :05 of each minute
        def kline_fetch_thread():
            print("K-line Fetch Thread started (fetches at :05 each minute)")
            while True:
                try:
                    if not self.mt5.is_market_open():
                        time.sleep(10)
                        continue
                    
                    now = datetime.now(timezone.utc)
                    # Wait until :05 of the minute
                    seconds_to_wait = (5 - now.second) % 60
                    if seconds_to_wait > 0 and seconds_to_wait < 55:
                        time.sleep(seconds_to_wait)
                    
                    print(f"[KLine Thread] Triggering fetch at {datetime.now(timezone.utc)}")
                    # Fetch last completed 1m bar
                    loop.call_soon_threadsafe(
                        lambda: asyncio.create_task(self.fetch_and_emit_kline())
                    )
                    
                    # Sleep until next minute
                    time.sleep(55)
                except Exception as e:
                    print(f"K-line Fetch Thread error: {e}")
                    time.sleep(10)
        threading.Thread(target=kline_fetch_thread, daemon=True).start()

    async def run_tick_processor(self):
        """Single async task that periodically processes tick updates from threads."""
        print("Tick Processor started (100ms interval)")
        while True:
            try:
                # Check if we have new tick data
                should_process = False
                with self._tick_lock:
                    if self._tick_updated:
                        self._tick_updated = False
                        should_process = True
                
                if should_process:
                    self.realtime_state['ts'] = datetime.now(timezone.utc).isoformat()
                    await self._update_realtime_spread()
                
                await asyncio.sleep(0.1)  # 100ms interval - good balance between responsiveness and efficiency
            except Exception as e:
                print(f"Tick Processor error: {e}")
                await asyncio.sleep(1)
    
    async def _update_realtime_spread(self):
        """Calculate and broadcast real-time spread, check strategy signals."""
        mt5_bid = self.realtime_state['mt5_bid']
        mt5_ask = self.realtime_state['mt5_ask']
        okx_bid = self.realtime_state['okx_bid']
        okx_ask = self.realtime_state['okx_ask']
        
        if not all([mt5_bid, mt5_ask, okx_bid, okx_ask]):
            return
        
        # Calculate spreads
        spread_sell = mt5_bid - okx_ask  # 做空spread: 卖MT5买OKX
        spread_buy = mt5_ask - okx_bid   # 做多spread: 买MT5卖OKX
        spread_mid = (mt5_bid + mt5_ask) / 2 - (okx_bid + okx_ask) / 2
        
        self.realtime_state['spread_sell'] = spread_sell
        self.realtime_state['spread_buy'] = spread_buy
        self.realtime_state['spread_mid'] = spread_mid
        
        # Update rolling spread window
        now_ts = time.time()
        self.spread_history.append((now_ts, spread_sell, spread_buy, spread_mid))
        # Remove old entries outside the window
        cutoff = now_ts - self.spread_window_seconds
        self.spread_history = [(t, ss, sb, sm) for t, ss, sb, sm in self.spread_history if t >= cutoff]
        
        # Calculate rolling averages
        if self.spread_history:
            avg_spread_sell = sum(ss for _, ss, _, _ in self.spread_history) / len(self.spread_history)
            avg_spread_buy = sum(sb for _, _, sb, _ in self.spread_history) / len(self.spread_history)
            avg_spread_mid = sum(sm for _, _, _, sm in self.spread_history) / len(self.spread_history)
        else:
            avg_spread_sell, avg_spread_buy, avg_spread_mid = spread_sell, spread_buy, spread_mid
        
        self.realtime_state['avg_spread_sell'] = avg_spread_sell
        self.realtime_state['avg_spread_buy'] = avg_spread_buy
        self.realtime_state['avg_spread_mid'] = avg_spread_mid
        
        # Get current EMA from history
        ema = self.agg.ema[-1] if self.agg.ema else spread_mid
        self.realtime_state['ema'] = ema
        
        # Calculate minute-level time for chart (floor to current minute)
        minute_time = int(now_ts // 60) * 60
        
        # Broadcast tick data (include rolling averages)
        await broadcast({
            'type': 'tick',
            'payload': {
                'mt5_bid': mt5_bid,
                'mt5_ask': mt5_ask,
                'okx_bid': okx_bid,
                'okx_ask': okx_ask,
                'spread_sell': spread_sell,
                'spread_buy': spread_buy,
                'spread_mid': spread_mid,
                'avg_spread_sell': avg_spread_sell,
                'avg_spread_buy': avg_spread_buy,
                'avg_spread_mid': avg_spread_mid,
                'ema': ema,
                'ts': self.realtime_state['ts'],
                'time': minute_time  # Minute-level time for chart series
            }
        })
        
        # Real-time strategy check (use rolling averages for stability)
        # spread_sell for short signals, spread_buy for long signals
        await self._check_realtime_signal(avg_spread_sell, avg_spread_buy, ema)
    
    async def _check_realtime_signal(self, spread_sell, spread_buy, ema):
        """Check for trading signals in real-time (tick-level trigger).
        
        spread_sell: MT5 bid - OKX ask (用于判断做空信号，因为做空时卖MT5买OKX)
        spread_buy: MT5 ask - OKX bid (用于判断做多信号，因为做多时买MT5卖OKX)
        """
        if not params.get('autoTrade', False):
            return
        
        # Check if market is open before allowing signals
        if not self.mt5.is_market_open():
            return
        
        # Prevent signal spam - minimum 60 seconds (1 minute) between signals
        now = time.time()
        if now - self.last_signal_time < 60:
            return
        
        first_spread = float(params['firstSpread'])
        next_spread = float(params['nextSpread'])
        max_pos = int(params.get('maxPos', 3))
        base_vol = float(params.get('tradeVolume', 0.01))  # Base trade volume per level
        
        # Get current net position (MT5 net in lots)
        try:
            mt5_net, okx_net, _, _ = self.get_net_exposure()
            # mt5_net: positive = long spread, negative = short spread
            # Calculate current level based on base volume (e.g., 0.03 lots / 0.01 base = level 3)
            current_pos_level = int(round(abs(mt5_net) / base_vol)) if base_vol > 0 else 0
            current_direction = 'long' if mt5_net > 0.001 else ('short' if mt5_net < -0.001 else 'neutral')
        except Exception as e:
            print(f"[AutoTrade] Failed to get position: {e}")
            return
        
        signal_action = None
        signal_level = 0
        trigger_spread = None  # The spread value that triggered the signal
        
        # Check for short signals (spread_sell too high)
        # 做空spread = 卖MT5买OKX，所以用spread_sell (MT5 bid - OKX ask)
        for lvl in range(max_pos, 0, -1):
            upper = ema + first_spread + (lvl - 1) * next_spread
            if spread_sell > upper:
                signal_action = 'short'
                signal_level = lvl
                trigger_spread = spread_sell
                break
        
        # Check for long signals (spread_buy too low)
        # 做多spread = 买MT5卖OKX，所以用spread_buy (MT5 ask - OKX bid)
        if not signal_action:
            for lvl in range(max_pos, 0, -1):
                lower = ema - first_spread - (lvl - 1) * next_spread
                if spread_buy < lower:
                    signal_action = 'long'
                    signal_level = lvl
                    trigger_spread = spread_buy
                    break
        
        # Only trade if we need to increase position in the signal direction
        # Don't trigger if:
        # 1. Already have position >= signal_level in the same direction
        # 2. Same direction but lower level (pullback within same side)
        # DO trigger if:
        # 1. No position
        # 2. Same direction and need to add (level increase)
        # 3. Opposite direction signal - CLOSE and REVERSE
        if signal_action:
            should_trade = False
            
            if current_direction == 'neutral':
                # No position, can trade
                should_trade = True
            elif current_direction == signal_action:
                # Same direction - only trade if current level < signal level (adding position)
                if current_pos_level < signal_level:
                    should_trade = True
                    print(f"[AutoTrade] Adding to {signal_action}: L{current_pos_level} -> L{signal_level}")
                # else: Skip silently - already at sufficient level
            else:
                # Opposite direction - CLOSE current position and REVERSE
                # e.g., Have short L2, signal is long L1 -> close short and open long
                should_trade = True
                print(f"[AutoTrade] REVERSING: {current_direction} L{current_pos_level} -> {signal_action} L{signal_level}")
            
            if should_trade:
                self.last_signal_time = now
                spread_type = 'sell' if signal_action == 'short' else 'buy'
                log_msg = f"[AutoTrade] 触发 {signal_action.upper()} L{signal_level} | spread_{spread_type}: {trigger_spread:.2f} vs EMA: {ema:.2f}"
                print(log_msg)
                
                # 构建信号信息用于订单日志
                signal_info = {
                    'trigger': 'auto',
                    'action': signal_action,
                    'level': signal_level,
                    'spread_type': spread_type,
                    'trigger_spread': round(trigger_spread, 2),
                    'ema': round(ema, 2),
                    'spread_sell': round(spread_sell, 2),
                    'spread_buy': round(spread_buy, 2),
                    'first_spread': first_spread,
                    'next_spread': next_spread,
                    'threshold': round(ema + first_spread + (signal_level - 1) * next_spread, 2) if signal_action == 'short' else round(ema - first_spread - (signal_level - 1) * next_spread, 2),
                    'current_direction': current_direction,
                    'current_level': current_pos_level,
                }
                
                async def trade_task():
                    try:
                        await asyncio.to_thread(self.execute_trade, signal_action, level=signal_level, signal_info=signal_info)
                    except Exception as e:
                        print(f"AutoTrade execution error: {e}")
                    await broadcast({'type': 'orders', 'payload': orders})
                
                asyncio.create_task(trade_task())

    async def fetch_and_emit_kline(self):
        """Fetch latest 1m K-line from both exchanges and emit bar."""
        try:
            now = datetime.now(timezone.utc)
            # Get bar from 2 minutes ago to ensure it's complete
            end_time = now.replace(second=0, microsecond=0)
            start_time = end_time - timedelta(minutes=2)
            
            print(f"[KLine] Fetching {start_time} to {end_time}")
            
            # Fetch MT5 K-line
            mt5_bars = await asyncio.to_thread(
                self.mt5.get_historical_data,
                start_time.replace(tzinfo=None),
                end_time.replace(tzinfo=None),
                'UTC'
            )
            
            # Fetch OKX K-line
            okx_bars = await asyncio.to_thread(
                self.okx.get_historical_data,
                start_time.replace(tzinfo=None),
                end_time.replace(tzinfo=None)
            )
            
            if not mt5_bars or not okx_bars:
                print(f"[KLine] No data: MT5={len(mt5_bars) if mt5_bars else 0}, OKX={len(okx_bars) if okx_bars else 0}")
                return
            
            # Get the last complete bar
            mt5_bar = mt5_bars[-1]
            okx_bar = okx_bars[-1]
            
            # Check if we already have this bar
            bar_ts = mt5_bar['time']
            bar_iso = datetime.fromtimestamp(bar_ts, timezone.utc).isoformat()
            
            if self.agg.times and bar_iso <= self.agg.times[-1]:
                return  # Already have this bar
            
            # Build bar data
            c_mt5 = {
                'open': float(mt5_bar['open']),
                'high': float(mt5_bar['high']),
                'low': float(mt5_bar['low']),
                'close': float(mt5_bar['close']),
                'vol': int(mt5_bar.get('volume', 0))
            }
            c_okx = {
                'open': float(okx_bar['open']),
                'high': float(okx_bar['high']),
                'low': float(okx_bar['low']),
                'close': float(okx_bar['close']),
                'vol': int(okx_bar.get('volume', 0))
            }
            
            spread = c_mt5['close'] - c_okx['close']
            
            # Use realtime bid/ask for spread_sell/spread_buy
            mt5_bid = self.realtime_state.get('mt5_bid') or c_mt5['close']
            mt5_ask = self.realtime_state.get('mt5_ask') or c_mt5['close']
            okx_bid = self.realtime_state.get('okx_bid') or c_okx['close']
            okx_ask = self.realtime_state.get('okx_ask') or c_okx['close']
            
            spread_sell = mt5_bid - okx_ask
            spread_buy = mt5_ask - okx_bid
            
            # Update aggregator
            self.agg.times.append(bar_iso)
            self.agg.mt5_stats.append(c_mt5)
            self.agg.okx_stats.append(c_okx)
            self.agg.spreads.append(spread)
            self.agg.spread_sell.append(spread_sell)
            self.agg.spread_buy.append(spread_buy)
            
            # Calculate new EMA
            last_ema = self.agg.ema[-1] if self.agg.ema else spread
            k = 2.0 / (self.agg.ema_period + 1)
            new_ema = spread * k + last_ema * (1 - k)
            self.agg.ema.append(new_ema)
            
            # Use 5s rolling average spread if available, else fall back to realtime
            avg_spread_sell = self.realtime_state.get('avg_spread_sell', spread_sell)
            avg_spread_buy = self.realtime_state.get('avg_spread_buy', spread_buy)
            
            bar = {
                'ts': bar_iso,
                'mt5': c_mt5,
                'okx': c_okx,
                'spread': spread,
                'spread_sell': avg_spread_sell,  # Use 5s average
                'spread_buy': avg_spread_buy,    # Use 5s average
                'ema': new_ema,
            }
            
            # Save and broadcast
            self.agg.save_bar(bar)
            decorated = self.agg._decorate_bar(bar)
            
            # Save to daily log file (每分钟保存一次)
            current_minute = datetime.now().strftime('%Y-%m-%d %H:%M')
            if self.last_daily_log_minute != current_minute:
                self.last_daily_log_minute = current_minute
                daily_data = {
                    'ts': bar_iso,
                    'local_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'mt5_bid': mt5_bid,
                    'mt5_ask': mt5_ask,
                    'okx_bid': okx_bid,
                    'okx_ask': okx_ask,
                    'spread_sell': round(spread_sell, 2),
                    'spread_buy': round(spread_buy, 2),
                    'spread_mid': round(spread, 2),
                    'ema': round(new_ema, 2),
                    'mt5_close': c_mt5['close'],
                    'okx_close': c_okx['close'],
                }
                self._save_minute_data(daily_data)
            
            print(f"[KLine] New bar: {bar_iso} | Spread: {spread:.2f} | EMA: {new_ema:.2f}")
            await broadcast({'type': 'bar', 'payload': decorated})
            
        except Exception as e:
            print(f"[KLine] Fetch error: {e}")

    async def process_mt5_tick(self, bid, ask, ts, last=None):
        # Legacy - kept for compatibility but not used in new architecture
        pass
    
    async def process_okx_tick(self, bid, ask, ts, last=None):
        # Legacy - kept for compatibility but not used in new architecture
        pass

    async def run_scheduler(self):
        print("Scheduler started (Market Monitor)")
        
        while True:
            try:
                now = datetime.now(timezone.utc)
                is_open = self.mt5.is_market_open()
                
                is_sunday = (now.weekday() == 6)
                is_pre_open = (is_sunday and now.hour == 21 and now.minute >= 55)
                
                time_since_last = time.time() - self.last_resync_time
                
                if (is_pre_open or is_open) and time_since_last > 3600 * 4: 
                     if is_pre_open or (is_open and time_since_last > 3600 * 24 * 2): 
                        print("Scheduled Resync triggering...")
                        await self.do_resync()
                        self.last_resync_time = time.time()
                
                await asyncio.sleep(60)
            except Exception as e:
                print(f"Scheduler error: {e}", file=sys.stderr)
                await asyncio.sleep(60)

    async def do_resync(self):
         print("Performing scheduled history synchronization...")
         
         last_ts = self.agg.get_last_timestamp()
         if not last_ts:
             return 
             
         start_time = last_ts + timedelta(minutes=1)
         end_time = datetime.now(timezone.utc)
         
         if start_time >= end_time:
             print("Data is up to date.")
             return
             
         print(f"Syncing gap: {start_time} to {end_time}")
         
         loop = asyncio.get_running_loop()
         try:
            mt5_gw = MT5Gateway(SYMBOL_MT5)
            okx_gw = OKXGateway(SYMBOL_OKX)
            
            def fetch_job():
                start_naive = start_time.replace(tzinfo=None)
                end_naive = end_time.replace(tzinfo=None)
                
                m = mt5_gw.get_historical_data(start_naive, end_naive, tz='UTC')
                o = okx_gw.get_historical_data(start_naive, end_naive)
                
                def clean(arr):
                    for x in arr:
                        if 'time' not in x and 'ts' in x: 
                             pass 
                    return arr
                return m, o

            mt5_hist, okx_hist = await loop.run_in_executor(None, fetch_job)
            
            if mt5_hist or okx_hist:
                self.agg.merge_external_history(mt5_hist, okx_hist)
                print(f"Resynced {len(mt5_hist)} MT5 and {len(okx_hist)} OKX bars.")
            
         except Exception as e:
             print(f"Resync failed: {e}")

feeder = None

async def handler(ws, path):
    clients.add(ws)
    await ws.send(json.dumps({ 'type': 'params', 'payload': params }))
    
    await ws.send_json({ 'type': 'orders', 'payload': orders })
    
    if feeder and feeder.account_state:
        await ws.send_json({ 'type': 'account', 'payload': feeder.account_state })

    try:
        async for message in ws:
            try:
                msg = json.loads(message)
                if msg.get('type') == 'params':
                    payload = msg.get('payload') or {}
                    params.update(payload)
                    await broadcast({ 'type': 'params', 'payload': params })
                
                elif msg.get('type') == 'make_long':
                    if feeder:
                        signal_info = {'trigger': 'manual', 'action': 'long', 'level': 1}
                        res = feeder.execute_trade('long', level=1, signal_info=signal_info) 
                        await broadcast({'type': 'orders', 'payload': orders})
                        
                elif msg.get('type') == 'make_short':
                    print("Manual API Short Request")
                    if feeder:
                        signal_info = {'trigger': 'manual', 'action': 'short', 'level': 1}
                        feeder.execute_trade('short', level=1, signal_info=signal_info)
                        await broadcast({'type': 'orders', 'payload': orders})

                elif msg.get('type') == 'close_all':
                    print("Manual API Close All Request")
                    res = feeder.close_all()
                
                elif msg.get('type') == 'refresh_history':
                    print("Refresh History Request - recalculating with new params")
                    if feeder:
                        # Recalculate EMA with new period
                        feeder.agg.ema_period = params.get('emaPeriod', EMA_PERIOD)
                        feeder.agg.ema = compute_ema(feeder.agg.spreads, feeder.agg.ema_period)
                        
                        # Build and send history
                        history = feeder.agg.get_history_payload()
                        await ws.send_json({ 'type': 'history', 'payload': history })
                
                elif msg.get('type') == 'get_order_logs':
                    limit = msg.get('limit', 50)
                    order_logs_data = get_order_logs_with_pnl(limit)
                    await ws.send_json({ 'type': 'order_logs', 'payload': order_logs_data })
                        
            except Exception as e:
                print('ws msg err', e)
    finally:
        clients.discard(ws)

async def http_index(request):
    index_path = os.path.join(ROOT, 'web', 'index.html')
    return web.FileResponse(index_path)

async def http_chart(request):
    path = os.path.join(ROOT, 'web', 'chart.umd.js')
    if os.path.exists(path):
        return web.FileResponse(path)
    return web.Response(status=404, text='chart.js not found')

async def http_lightweight_charts(request):
    path = os.path.join(ROOT, 'web', 'lightweight-charts.js')
    if os.path.exists(path):
        return web.FileResponse(path)
    return web.Response(status=404, text='lightweight-charts.js not found')

async def http_backtest_results(request):
    path = os.path.join(ROOT, 'web', 'backtest_results.json')
    if os.path.exists(path):
        return web.FileResponse(path)
    return web.Response(status=404, text='Results not found')

async def http_ws(request):
    ws = web.WebSocketResponse(heartbeat=30)  # 30s heartbeat to detect dead connections
    await ws.prepare(request)
    clients.add(ws)

    await ws.send_json({ 'type': 'params', 'payload': params })
    
    if feeder and feeder.account_state:
        await ws.send_json({ 'type': 'account', 'payload': feeder.account_state })

    # Send Orders History
    await ws.send_json({ 'type': 'orders', 'payload': orders })

    if feeder and feeder.agg:
        try:
            hist_payload = feeder.agg.get_history_payload()
            await ws.send_json({
                'type': 'history',
                'payload': hist_payload
            })
        except Exception as e:
            print(f"Failed to send memory history: {e}")
    else:
        print("Feeder not ready, skipping history")

    if feeder and feeder.account_state:
        await ws.send_json({ 'type': 'account', 'payload': feeder.account_state })

    try:
        async for message in ws:
            if message.type == web.WSMsgType.TEXT:
                try:
                    msg = json.loads(message.data)
                    if msg.get('type') == 'params':
                        payload = msg.get('payload') or {}
                        params.update(payload)
                        if feeder and 'emaPeriod' in payload:
                            feeder.agg.ema_period = int(payload['emaPeriod'])
                        await broadcast({ 'type': 'params', 'payload': params })
                    
                    elif msg.get('type') == 'make_long':
                        if feeder:
                            signal_info = {'trigger': 'manual', 'action': 'long', 'level': 1}
                            res = feeder.execute_trade('long', level=1, signal_info=signal_info)
                            await broadcast({'type': 'orders', 'payload': orders})
                            # Send trade result to client
                            await ws.send_json({'type': 'trade_result', 'payload': res})
                            
                    elif msg.get('type') == 'make_short':
                        if feeder:
                            signal_info = {'trigger': 'manual', 'action': 'short', 'level': 1}
                            res = feeder.execute_trade('short', level=1, signal_info=signal_info)
                            await broadcast({'type': 'orders', 'payload': orders})
                            # Send trade result to client
                            await ws.send_json({'type': 'trade_result', 'payload': res})

                    elif msg.get('type') == 'close_all':
                        print("Manual HTTP WS Close All Request")
                        feeder.close_all()
                    
                    elif msg.get('type') == 'refresh_history':
                        print("Refresh History Request (http_ws) - recalculating with new params")
                        if feeder:
                            # Recalculate EMA with new period
                            feeder.agg.ema_period = params.get('emaPeriod', EMA_PERIOD)
                            feeder.agg.ema = compute_ema(feeder.agg.spreads, feeder.agg.ema_period)
                            
                            # Build and send history
                            history = feeder.agg.get_history_payload()
                            await ws.send_json({ 'type': 'history', 'payload': history })
                    
                    elif msg.get('type') == 'get_order_logs':
                        limit = msg.get('limit', 50)
                        order_logs_data = get_order_logs_with_pnl(limit)
                        await ws.send_json({ 'type': 'order_logs', 'payload': order_logs_data })

                except Exception as e:
                    print('http ws msg err', e)
            elif message.type == web.WSMsgType.ERROR:
                break
    finally:
        clients.discard(ws)
    return ws

async def main_async():
    global feeder
    loop = asyncio.get_event_loop()
    feeder = Feeder()

    feeder.agg.load_from_file()

    last_file_ts = feeder.agg.get_last_timestamp()
    
    end_time = datetime.now(timezone.utc) + timedelta(hours=8)
    
    if last_file_ts:
        start_time = last_file_ts + timedelta(minutes=1)
        if start_time > datetime.now(timezone.utc):
            print("Local data is up to date.")
            start_time = None
        else:
            print(f"Gap detected. Fetching from {start_time}...")
    else:
        start_time = datetime.now(timezone.utc) - timedelta(days=2)
        print("No local data. Fetching last 2 days...")

    if start_time:
        mt5_gateway = MT5Gateway(SYMBOL_MT5)
        okx_gateway = OKXGateway(SYMBOL_OKX)
        try:
            start_naive = start_time.replace(tzinfo=None)
            end_naive = end_time.replace(tzinfo=None)

            def clean_hist(hist_list):
                 cleaned = []
                 for item in hist_list:
                     if 'time' not in item and 'ts' in item:
                         dt = datetime.fromisoformat(item['ts'])
                         if dt.tzinfo:
                             dt = dt.astimezone(timezone.utc)
                         item['time'] = int(dt.timestamp())
                     cleaned.append(item)
                 return cleaned

            print(f"Fetching MT5 from {start_naive}...")
            mt5_hist_raw = mt5_gateway.get_historical_data(start_naive, end_naive, tz='UTC')
            mt5_hist = clean_hist(mt5_hist_raw)

            print(f"Fetching OKX from {start_naive}...")
            okx_hist_raw = okx_gateway.get_historical_data(start_naive, end_naive)
            okx_hist = clean_hist(okx_hist_raw)

            feeder.agg.merge_external_history(mt5_hist, okx_hist)
            print(f"History synchronized. Total bars: {len(feeder.agg.times)}")

        except Exception as e:
            print(f"Bootstrap history warning: {e}")

    feeder.start(loop)
    
    asyncio.create_task(feeder.run_scheduler())
    
    asyncio.create_task(feeder.run_account_monitor())
    
    asyncio.create_task(feeder.run_tick_processor())

    app = web.Application()
    app.router.add_get('/', http_index)
    app.router.add_get('/ws', http_ws)
    app.router.add_get('/chart.umd.js', http_chart)
    app.router.add_get('/lightweight-charts.js', http_lightweight_charts)
    app.router.add_get('/backtest_results.json', http_backtest_results)

    host = os.getenv('HTTP_HOST', '0.0.0.0')
    port = int(os.getenv('HTTP_PORT', '8765'))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f'Frontend + WS running on http://{host}:{port} (WS at /ws)')

    while True:
        await asyncio.sleep(3600)

if __name__ == '__main__':
    asyncio.run(main_async())

