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
FIRST_SPREAD = float(os.getenv('FIRST_SPREAD', '3.0'))
NEXT_SPREAD = float(os.getenv('NEXT_SPREAD', '1.0'))
TAKE_PROFIT = float(os.getenv('TAKE_PROFIT', '6.0'))
MAX_POS = int(os.getenv('MAX_POS', '3'))

SYMBOL_OKX = os.getenv('OKX_SYMBOL', 'PAXG/USDT')
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
}

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
        try:
            with open(HISTORY_FILE, 'r') as f:
                count = 0
                for line in f:
                    if not line.strip(): continue
                    try:
                        data = json.loads(line)
                        self.times.append(data['ts'])
                        self.mt5_stats.append(data['mt5'])
                        self.okx_stats.append(data['okx'])
                        self.spreads.append(data['spread'])
                        self.ema.append(data['ema'])
                        count += 1
                    except Exception as loop_e:
                        print(f"Skipping corrupt line: {loop_e}")
            if self.times:
                print(f"Loaded {count} bars. Last: {self.times[-1]}")
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
        
        for i, t_str in enumerate(self.times):
            dt = datetime.fromisoformat(t_str)
            ts_val = int(dt.timestamp())
            ts_ints.append(ts_val)
            
            m = self.mt5_stats[i].copy()
            m['time'] = ts_val
            mt5_out.append(m)
            
            o = self.okx_stats[i].copy()
            o['time'] = ts_val
            okx_out.append(o)
            
        return {
            'ts': ts_ints,
            'mt5': mt5_out,
            'okx': okx_out,
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

    def on_mt5_tick(self, bid, ask, ts: datetime):
        with self.lock:
            mid = self._sanitize_mid(((bid or 0) + (ask or 0)) / 2.0 if bid is not None and ask is not None else None)
            if mid is not None:
                self.buf_mt5 = self._update_candle(self.buf_mt5, mid)
                self.last_mt5_close = mid
            return self._maybe_emit(ts)

    def on_okx_tick(self, bid, ask, ts: datetime):
        with self.lock:
            mid = self._sanitize_mid(((bid or 0) + (ask or 0)) / 2.0 if bid is not None and ask is not None else None)
            if mid is not None:
                self.buf_okx = self._update_candle(self.buf_okx, mid)
                self.last_okx_close = mid
            return self._maybe_emit(ts)

    def _maybe_emit(self, ts: datetime):
        minute = floor_minute(ts)
        if self.last_minute is None:
            self.last_minute = minute
            return None
        if minute != self.last_minute:
            def finalize(buf, last_close):
                if buf: return buf
                p = last_close if last_close is not None else 0.0
                return {'open': p, 'high': p, 'low': p, 'close': p, 'vol': 0}

            c_mt5 = finalize(self.buf_mt5, self.last_mt5_close)
            c_okx = finalize(self.buf_okx, self.last_okx_close)

            if c_mt5['close'] > 0 and c_okx['close'] > 0:
                self.times.append(self.last_minute.isoformat())
                self.mt5_stats.append(c_mt5)
                self.okx_stats.append(c_okx)
                
                spread = c_mt5['close'] - c_okx['close']
                self.spreads.append(spread)
                self.ema = compute_ema(self.spreads, self.ema_period)
                print(f"Emitting bar: {self.last_minute} Spread: {spread:.2f} Vol: MT5={c_mt5['vol']} OKX={c_okx['vol']}")
            else:
                print(f"Skipping bar: {self.last_minute} (Missing data)")
                # If skipping, we must NOT return a bar to be saved or broadcasted
                # Update last_minute and clear buffers, but return None early
                self.last_minute = minute
                self.buf_mt5 = None
                self.buf_okx = None
                return None

            self.last_minute = minute
            self.buf_mt5 = None
            self.buf_okx = None
            
            if self.spreads and len(self.spreads) > 0:
                idx = len(self.spreads) - 1
                if self.times[idx] == self.times[-1]:
                    return {
                        'ts': self.times[idx],
                        'mt5': self.mt5_stats[idx],
                        'okx': self.okx_stats[idx],
                        'spread': self.spreads[idx],
                        'ema': self.ema[idx],
                    }
        return None

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

    def start(self, loop):
        # MT5 ticks
        def mt5_thread():
            print("MT5 Thread started") 
            try:
                for tick in self.mt5.stream_ticks():
                    # Check market hours
                    if not self.mt5.is_market_open():
                         continue
                    
                    bid = tick.get('bid'); ask = tick.get('ask')
                    ts = datetime.now(timezone.utc)
                    bar = self.agg.on_mt5_tick(bid, ask, ts)
                    if bar:
                        self.agg.save_bar(bar)
                        asyncio.run_coroutine_threadsafe(broadcast({'type': 'bar', 'payload': self._decorate_bar(bar)}), loop)
            except Exception as e:
                print(f"MT5 Thread loop error: {e}")
        threading.Thread(target=mt5_thread, daemon=True).start()

        def okx_thread():
            print("OKX Thread started")
            while True:
                try:
                    ob = self.okx.latest_ws_snapshot() or {}
                    bids = ob.get('bids') or []
                    asks = ob.get('asks') or []
                    if bids and asks:
                        best_bid = bids[0][0]; best_ask = asks[0][0]
                        ts = datetime.now(timezone.utc)
                        bar = self.agg.on_okx_tick(best_bid, best_ask, ts)
                        if bar:
                            self.agg.save_bar(bar)
                            asyncio.run_coroutine_threadsafe(broadcast({'type': 'bar', 'payload': self._decorate_bar(bar)}), loop)
                    time.sleep(0.5)
                except Exception as e:
                    print(f"OKX Thread loop error: {e}")
                    time.sleep(1)
        threading.Thread(target=okx_thread, daemon=True).start()

    async def run_scheduler(self):
        """Monitor market open/close and trigger resync."""
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

    def _decorate_bar(self, bar):
        ema = bar['ema']
        bands = []
        for lvl in range(1, params['maxPos'] + 1):
            upper = ema + params['firstSpread'] + (lvl - 1) * params['nextSpread']
            lower = ema - params['firstSpread'] - (lvl - 1) * params['nextSpread']
            bands.append({'upper': upper, 'lower': lower, 'lvl': lvl})
        return {**bar, 'bands': bands}

feeder = None

async def handler(ws, path):
    clients.add(ws)
    await ws.send(json.dumps({ 'type': 'params', 'payload': params }))
    try:
        async for message in ws:
            try:
                msg = json.loads(message)
                if msg.get('type') == 'params':
                    payload = msg.get('payload') or {}
                    params.update(payload)
                    await broadcast({ 'type': 'params', 'payload': params })
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

async def http_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    clients.add(ws)

    await ws.send_json({ 'type': 'params', 'payload': params })

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
    
    # Start scheduler
    asyncio.create_task(feeder.run_scheduler())

    app = web.Application()
    app.router.add_get('/', http_index)
    app.router.add_get('/ws', http_ws)
    app.router.add_get('/chart.umd.js', http_chart)
    app.router.add_get('/lightweight-charts.js', http_lightweight_charts)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8765)
    await site.start()
    print('Frontend + WS on http://localhost:8765 (WS at /ws)')

    while True:
        await asyncio.sleep(3600)

if __name__ == '__main__':
    asyncio.run(main_async())

