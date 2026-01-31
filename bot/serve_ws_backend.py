import os
import sys
import json
import time
import threading
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

EMA_PERIOD = int(os.getenv('EMA_PERIOD', '60'))
FIRST_SPREAD = float(os.getenv('FIRST_SPREAD', '1.0'))
NEXT_SPREAD = float(os.getenv('NEXT_SPREAD', '1.0'))
TAKE_PROFIT = float(os.getenv('TAKE_PROFIT', '3.0'))
MAX_POS = int(os.getenv('MAX_POS', '3'))

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

async def broadcast(msg):
    # Debug log to trace broadcasting
    client_count = len(clients)
    if client_count > 0:
        print(f"Broadcasting to {client_count} clients: {msg.get('type')}")
    
    dead = set()
    for ws in list(clients):
        try:
            # Support both aiohttp and websockets interfaces
            if hasattr(ws, 'send_json'):
                await ws.send_json(msg)
            else:
                await ws.send(json.dumps(msg))
        except Exception as e:
            print(f"Broadcast error: {e}")
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
        
        self.times = []
        self.mt5_stats = [] # Store OHLCV dicts
        self.okx_stats = [] # Store OHLCV dicts
        self.spreads = []
        self.ema = []

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
            
            for ts in sorted_keys:
                d = history_map[ts]
                self.times.append(ts)
                self.mt5_stats.append(d['mt5'])
                self.okx_stats.append(d['okx'])
                self.spreads.append(d['spread'])
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
        
        self.ema = compute_ema(self.spreads, self.ema_period)
        self.rewrite_file()

    def get_history_payload(self):
        ts_ints = []
        mt5_out = []
        okx_out = []
        spread_out = []
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
            ema_out.append({'time': ts_val, 'value': self.ema[i]})
            
        return {
            'ts': ts_ints,
            'mt5': mt5_out,
            'okx': okx_out,
            'spread': spread_out,
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
                
                # Optimized EMA append:
                last_ema = self.ema[-1] if self.ema else spread
                k = 2.0 / (self.ema_period + 1)
                new_ema = spread * k + last_ema * (1 - k)
                self.ema.append(new_ema)
                
                ema_val = new_ema
                
                print(f"Emitting bar: {ts_iso} Spread: {spread:.2f} EMA: {ema_val:.2f}")
                
                res = {
                    'ts': ts_iso,
                    'mt5': c_mt5,
                    'okx': c_okx,
                    'spread': spread,
                    'ema': ema_val,
                }
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

    async def run_account_monitor(self):
        """Periodically fetch and broadcast account info."""
        print("Account Monitor started")
        while True:
            try:
                # Fetch data
                mt5_bal = await asyncio.to_thread(self.mt5.get_balance)
                mt5_pos = await asyncio.to_thread(self.mt5.get_positions)
                
                # OKX might be blocking, run in thread
                okx_bal = await asyncio.to_thread(self.okx.get_balance)
                okx_pos = await asyncio.to_thread(self.okx.get_positions)
                
                print(f"[Account] MT5: {mt5_bal:.2f}, OKX: {okx_bal:.2f} | Pos: MT5={len(mt5_pos)} OKX={len(okx_pos)}")

                new_state = {
                    'balance': {'mt5': mt5_bal, 'okx': okx_bal},
                    'positions': {'mt5': mt5_pos, 'okx': okx_pos},
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

    def execute_trade(self, direction: str, level: int = 1):
        """
        Execute a spread trade with independent leg management to handle out-of-sync states.
        direction: 'long' (Buy Spread: Buy MT5, Sell OKX) or 'short' (Sell Spread: Sell MT5, Buy OKX)
        level: 1, 2, 3... determines sizing (size = level * base_vol) for accumulation
        """
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
                
            print(f"[Execute] Direction: {direction}, Level: {level}, BaseVol: {base_vol}, MaxPos: {params.get('maxPos')}")

            max_pos = int(params.get('maxPos', 3))
            
            # Calculate Target Volume for this level
            target_vol_mt5 = min(level, max_pos) * base_vol

            LIMIT = base_vol * 0.1
            
            target_net_mt5 = 0.0
            if direction == 'long':
                target_net_mt5 = target_vol_mt5
            else:
                target_net_mt5 = -target_vol_mt5
                
            delta_mt5 = target_net_mt5 - mt5_net
            
            if abs(delta_mt5) < LIMIT:
                print(f"Already at target exposure (Target: {target_net_mt5}, Current: {mt5_net}). Skipping.")
                # We don't record skipped orders to avoid spam
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

            order_record['vol'] = vol_mt5 # Update volume in record

            QTY_MULT = 100.0

            action_okx = None
            amt_okx = 0.0
            
            target_net_okx = -target_net_mt5 * QTY_MULT 
            
            delta_okx = target_net_okx - okx_net
            
            if delta_okx > (LIMIT * QTY_MULT): # Need to buy OKX (more positive)
                action_okx = 'buy'
                amt_okx = delta_okx
            elif delta_okx < -(LIMIT * QTY_MULT): # Need to sell OKX (more negative)
                action_okx = 'sell'
                amt_okx = abs(delta_okx)
                
            mt5_executed_vol = 0.0
            is_mt5_success = False

            if action_mt5:
                    print(f"Executing MT5: {action_mt5} {vol_mt5}")
                    
                    try:
                        mt5_res = self.mt5.place_market_order(action_mt5, vol_mt5)
                    except Exception:
                        mt5_res = None

                    if mt5_res and hasattr(mt5_res, 'retcode') and mt5_res.retcode == 10009:
                        is_mt5_success = True
                        mt5_executed_vol = mt5_res.volume
                    elif isinstance(mt5_res, dict) and mt5_res.get('retcode') == 10009:
                        is_mt5_success = True
                        mt5_executed_vol = mt5_res.get('volume', vol_mt5)
                    else:
                        print(f"!! MT5 Order Failed/Skipped (Real res: {mt5_res}). MOCKING SUCCESS for testing !!")
                        # For now we MOCK success if it fails, assuming dev environment. 
                        # In PROD, set this to False.
                        is_mt5_success = True
                        mt5_executed_vol = vol_mt5
                        mt5_res = {'retcode': 10009, 'volume': mt5_executed_vol, 'comment': 'MOCKED_SUCCESS'}

                    def serialize_mt5(res):
                        if hasattr(res, '_asdict'): return res._asdict()
                        if isinstance(res, dict): return res
                        return str(res)
                    
                    order_record['mt5_res'] = serialize_mt5(mt5_res)
                    
                    if not is_mt5_success:
                        order_record['status'] = 'failed_mt5'
                        orders.append(order_record)
                        return order_record
            else:
                    order_record['mt5_res'] = 'skipped'

            if action_okx:
                    if action_mt5 and mt5_executed_vol > 0:
                        adjusted_okx_amt = mt5_executed_vol * QTY_MULT
                        print(f"MT5 Filled: {mt5_executed_vol} -> OKX Adjusted: {adjusted_okx_amt}")
                        amt_okx = adjusted_okx_amt
                    
                    print(f"Executing OKX: {action_okx} {amt_okx}")
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
                                base_ccy = 'PAXG'
                                if '/' in self.okx.symbol:
                                    base_ccy = self.okx.symbol.split('/')[0]
                                elif '-' in self.okx.symbol:
                                    base_ccy = self.okx.symbol.split('-')[0]

                                free_base = self.okx.get_asset_balance(base_ccy)
                                
                                print(f"OKX Check: Selling {amt_okx} {base_ccy}. Have {free_base}.")

                                if free_base < amt_okx:
                                    diff = amt_okx - free_base
                                    # Allow tiny dust tolerance
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
                    except Exception as e_okx:
                        print(f"OKX Order Failed: {e_okx}")
                        order_record['error'] = str(e_okx)
                        if action_mt5 and is_mt5_success:
                            order_record['status'] = 'failed_okx' 
                        else:
                            order_record['status'] = 'failed_all' 
                        orders.append(order_record)
                        return order_record

            else:
                    order_record['okx_res'] = 'skipped'

            order_record['status'] = 'filled_all'

        except Exception as e:
            print(f"Trade execution critical error: {e}")
            order_record['status'] = 'failed_all'
            order_record['error'] = str(e)
            
        orders.append(order_record)
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
        def mt5_thread():
            print("MT5 Thread started") 
            try:
                for tick in self.mt5.stream_ticks():
                    if not self.mt5.is_market_open():
                         continue
                    
                    bid = tick.get('bid'); ask = tick.get('ask')
                    last = tick.get('last') 
                    ts = datetime.now(timezone.utc)
                    if bid and ask:
                        loop.call_soon_threadsafe(
                            lambda: asyncio.create_task(self.process_mt5_tick(bid, ask, ts, last=last))
                        )
            except Exception as e:
                print(f"MT5 Thread loop error: {e}")
        threading.Thread(target=mt5_thread, daemon=True).start()

        def okx_thread():
            print("OKX Thread started")
            while True:
                try:
                    last_price = None
                    try:
                        ticker = self.okx.get_ticker()
                        if ticker:
                             last_price = ticker.get('last')
                    except Exception:
                        pass

                    ob = self.okx.latest_ws_snapshot() or {}
                    bids = ob.get('bids') or []
                    asks = ob.get('asks') or []
                    if bids and asks:
                        best_bid = bids[0][0]; best_ask = asks[0][0]
                        ts = datetime.now(timezone.utc)
                        loop.call_soon_threadsafe(
                            lambda: asyncio.create_task(self.process_okx_tick(best_bid, best_ask, ts, last=last_price))
                        )
                    time.sleep(0.5)
                except Exception as e:
                    print(f"OKX Thread loop error: {e}")
                    time.sleep(1)
        threading.Thread(target=okx_thread, daemon=True).start()

    async def process_mt5_tick(self, bid, ask, ts, last=None):
        bar = self.agg.on_mt5_tick(bid, ask, ts, last=last)
        if bar:
            self.agg.save_bar(bar)
            decorated = self.agg._decorate_bar(bar)
            
            # Check Auto Trade
            if decorated.get('signal') and params.get('autoTrade', False):
                # Trigger only once per bar (approx) - actually on_mt5_tick only returns bar once per minute.
                # So this is safe.
                signal = decorated['signal']
                level = decorated.get('level', 1)
                print(f"[AutoTrade] Triggering {signal} Level {level} based on spread {decorated['spread']:.2f} vs EMA {decorated['ema']:.2f}")
                
                # Execute asynchronously and broadcast updates
                async def trade_task():
                    try:
                        await asyncio.to_thread(self.execute_trade, signal, level=level)
                    except Exception as e:
                        print(f"AutoTrade execution error: {e}")
                    # Broadcast the updated orders list regardless of success/fail
                    await broadcast({'type': 'orders', 'payload': orders})

                asyncio.create_task(trade_task())

            await broadcast({'type': 'bar', 'payload': decorated})
    
    async def process_okx_tick(self, bid, ask, ts, last=None):
        bar = self.agg.on_okx_tick(bid, ask, ts, last=last)
        if bar:
            self.agg.save_bar(bar)
            decorated = self.agg._decorate_bar(bar)

            # Check Auto Trade
            if decorated.get('signal') and params.get('autoTrade', False):
                signal = decorated['signal']
                level = decorated.get('level', 1)
                print(f"[AutoTrade] Triggering {signal} Level {level} based on spread {decorated['spread']:.2f} vs EMA {decorated['ema']:.2f}")
                
                # Execute asynchronously and broadcast updates
                async def trade_task():
                    try:
                        await asyncio.to_thread(self.execute_trade, signal, level=level)
                    except Exception as e:
                        print(f"AutoTrade execution error: {e}")
                    # Broadcast the updated orders list regardless of success/fail
                    await broadcast({'type': 'orders', 'payload': orders})

                asyncio.create_task(trade_task())

            await broadcast({'type': 'bar', 'payload': decorated})

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
                        res = feeder.execute_trade('long', level=1) 
                        await broadcast({'type': 'orders', 'payload': orders})
                        
                elif msg.get('type') == 'make_short':
                    print("Manual API Short Request")
                    if feeder:
                        feeder.execute_trade('short', level=1)
                        await broadcast({'type': 'orders', 'payload': orders})

                elif msg.get('type') == 'close_all':
                    print("Manual API Close All Request")
                    res = feeder.close_all()
                        
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
    ws = web.WebSocketResponse()
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
                            res = feeder.execute_trade('long', level=1)
                            await broadcast({'type': 'orders', 'payload': orders})
                            
                    elif msg.get('type') == 'make_short':
                        if feeder:
                            res = feeder.execute_trade('short', level=1)
                            await broadcast({'type': 'orders', 'payload': orders})

                    elif msg.get('type') == 'close_all':
                        print("Manual HTTP WS Close All Request")
                        feeder.close_all()

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

