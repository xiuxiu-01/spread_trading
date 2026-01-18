import os
import sys
import json
import time
import threading
from datetime import datetime, timedelta
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
            # Emit bar
            # Logic: If no data for this minute, clone the last close as O=H=L=C, vol=0
            
            # Helper to finalize candle
            def finalize(buf, last_close):
                if buf: return buf
                p = last_close if last_close is not None else 0.0
                return {'open': p, 'high': p, 'low': p, 'close': p, 'vol': 0}

            c_mt5 = finalize(self.buf_mt5, self.last_mt5_close)
            c_okx = finalize(self.buf_okx, self.last_okx_close)

            # Ensure we have valid prices to calc spread
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

    def start(self, loop):
        # MT5 ticks
        def mt5_thread():
            print("MT5 Thread started") 
            try:
                for tick in self.mt5.stream_ticks():
                    bid = tick.get('bid'); ask = tick.get('ask')
                    ts = datetime.now()
                    bar = self.agg.on_mt5_tick(bid, ask, ts)
                    if bar:
                        asyncio.run_coroutine_threadsafe(broadcast({'type': 'bar', 'payload': self._decorate_bar(bar)}), loop)
            except Exception as e:
                print(f"MT5 Thread loop error: {e}")
        threading.Thread(target=mt5_thread, daemon=True).start()

        # OKX ws -> best bid/ask
        def okx_thread():
            print("OKX Thread started")
            while True:
                try:
                    ob = self.okx.latest_ws_snapshot() or {}
                    bids = ob.get('bids') or []
                    asks = ob.get('asks') or []
                    if bids and asks:
                        best_bid = bids[0][0]; best_ask = asks[0][0]
                        ts = datetime.now()
                        bar = self.agg.on_okx_tick(best_bid, best_ask, ts)
                        if bar:
                            asyncio.run_coroutine_threadsafe(broadcast({'type': 'bar', 'payload': self._decorate_bar(bar)}), loop)
                    time.sleep(0.5) # slow down slightly to match mt5 poll roughly
                except Exception as e:
                    print(f"OKX Thread loop error: {e}")
                    time.sleep(1)
        threading.Thread(target=okx_thread, daemon=True).start()

    def _decorate_bar(self, bar):
        # bands based on current params
        ema = bar['ema']
        bands = []
        for lvl in range(1, params['maxPos'] + 1):
            upper = ema + params['firstSpread'] + (lvl - 1) * params['nextSpread']
            lower = ema - params['firstSpread'] - (lvl - 1) * params['nextSpread']
            bands.append({'upper': upper, 'lower': lower, 'lvl': lvl})
        return {**bar, 'bands': bands}

feeder = None

# Original websockets handler preserved for compatibility
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

# New: aiohttp app serving index.html and bridging to websockets handler
async def http_index(request):
    index_path = os.path.join(ROOT, 'web', 'index.html')
    return web.FileResponse(index_path)

# Serve local Chart.js if available to avoid CDN issues
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
    try:
        async for message in ws:
            if message.type == web.WSMsgType.TEXT:
                try:
                    msg = json.loads(message.data)
                    if msg.get('type') == 'params':
                        payload = msg.get('payload') or {}
                        params.update(payload)
                        # update aggregator period if changed
                        if feeder and 'emaPeriod' in payload:
                            feeder.agg.ema_period = int(payload['emaPeriod'])
                        # broadcast new params
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

    # --- NEW: bootstrap with MT5 historical 1m bars (server time) ---
    try:
        hist_minutes = int(os.getenv('BOOTSTRAP_MINUTES', str(max(EMA_PERIOD * 5, 500))))
        mt5_hist = feeder.mt5.fetch_ohlcv_1m(minutes=hist_minutes)
        if mt5_hist:
            # Seed aggregator state so EMA/spread start with context
            for bar in mt5_hist:
                ts = datetime.fromisoformat(bar['ts'])
                feeder.agg.times.append(bar['ts'])
                feeder.agg.mt5_stats.append({
                    'open': bar['open'],
                    'high': bar['high'],
                    'low': bar['low'],
                    'close': bar['close'],
                    'vol': bar.get('vol', 0),
                })
                # OKX history is optional; initialize with MT5 close so spread starts at 0 until OKX catches up
                c = float(bar['close'])
                feeder.agg.okx_stats.append({
                    'open': c,
                    'high': c,
                    'low': c,
                    'close': c,
                    'vol': 0,
                })
                feeder.agg.spreads.append(0.0)
            feeder.agg.ema = compute_ema(feeder.agg.spreads, feeder.agg.ema_period)

            # Broadcast to UI: historical candles (mt5 only + ts)
            asyncio.create_task(broadcast({
                'type': 'history',
                'payload': {
                    'ts': [b['ts'] for b in mt5_hist],
                    'mt5': mt5_hist,
                }
            }))
            print(f"Bootstrapped MT5 history: {len(mt5_hist)} bars")
    except Exception as e:
        print(f"Bootstrap history failed: {e}")

    feeder.start(loop)

    # Start aiohttp app
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

    # Keep running
    while True:
        await asyncio.sleep(3600)

if __name__ == '__main__':
    asyncio.run(main_async())

