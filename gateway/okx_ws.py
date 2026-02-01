import json
import threading
import time
import websocket
import datetime
import os
from dotenv import load_dotenv

load_dotenv()

OKX_INST_MAP = {
    'XAU/USDT': 'XAU-USDT',
    "XAU/USDT:USDT": "XAU-USDT-SWAP",
}

class OKXWS:
    def __init__(self, instId, record_file=None):
        # WS URL override via env; choose aws endpoint if configured
        env_ws = os.getenv('OKX_WS_URL')
        endpoints = [env_ws] if env_ws else []
        # add common endpoints
        endpoints += [
            'wss://ws.okx.com:8443/ws/v5/public',
            'wss://wsaws.okx.com:8443/ws/v5/public',
        
        ]
        # dedupe while preserving order
        seen = set()
        self._endpoints = []
        for u in endpoints:
            if u and u not in seen:
                self._endpoints.append(u)
                seen.add(u)
        # default current url
        self.url = self._endpoints[0]
        self.instId = instId
        self.record_file = record_file
        self.ws = None
        self.latest = None
        self._stop = False
        # optional proxy support
        self._proxy_host, self._proxy_port = None, None
        self._proxy_type = (os.getenv('PROXY_TYPE') or '').lower()  # 'http', 'socks4', 'socks5'
        https_proxy = os.getenv('HTTPS_PROXY') or os.getenv('https_proxy')
        http_proxy = os.getenv('HTTP_PROXY') or os.getenv('http_proxy')
        proxy = https_proxy or http_proxy
        if proxy and '://' in proxy:
            try:
                scheme, rest = proxy.split('://', 1)
                # infer proxy type from scheme if not explicitly set
                if not self._proxy_type:
                    if 'socks5' in scheme:
                        self._proxy_type = 'socks5'
                    elif 'socks4' in scheme:
                        self._proxy_type = 'socks4'
                    else:
                        self._proxy_type = 'http'
                hostport = rest.split('@',1)[-1]
                hp = hostport.split('/') [0]
                host, port = hp.split(':') if ':' in hp else (hp, '443')
                self._proxy_host, self._proxy_port = host, int(port)
            except Exception:
                pass
        print(f"[OKXWS] using url={self.url} proxy={self._proxy_type or 'none'} {self._proxy_host}:{self._proxy_port or ''} (from .env)")

    def _on_open(self, ws):
        sub = {
            "op": "subscribe",
            "args": [
                {"channel": "books", "instId": self.instId, "sz": "5"}
            ]
        }
        ws.send(json.dumps(sub))

    def _on_message(self, ws, message):
        try:
            obj = json.loads(message)
            if 'event' in obj and obj.get('event') == 'subscribe':
                return
            if 'data' in obj and obj['data']:
                data = obj['data'][0]
                bids = [[float(x[0]), float(x[1])] for x in (data.get('bids') or [])]
                asks = [[float(x[0]), float(x[1])] for x in (data.get('asks') or [])]
                snap = {'ts': datetime.datetime.utcnow().isoformat(), 'bids': bids, 'asks': asks}
                self.latest = snap
                if self.record_file:
                    try:
                        with open(self.record_file, 'a') as f:
                            f.write(json.dumps(snap) + '\n')
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
            backoff = 1.0
            i = 0
            while not self._stop:
                try:
                    # rotate endpoints on retries
                    self.url = self._endpoints[i % len(self._endpoints)]
                    print(f"[OKXWS] connecting {self.url}")
                    
                    conn_start = time.time()

                    self.ws = websocket.WebSocketApp(self.url,
                                                     on_open=self._on_open,
                                                     on_message=self._on_message,
                                                     on_error=self._on_error,
                                                     on_close=self._on_close)
                    kwargs = {
                        'http_proxy_host': self._proxy_host,
                        'http_proxy_port': self._proxy_port,
                        'ping_interval': 20,
                        'ping_timeout': 10,
                    }
                    if self._proxy_type:
                        kwargs['proxy_type'] = self._proxy_type
                    self.ws.run_forever(**kwargs)
                    
                    # If we stayed connected for >10s, reset backoff
                    if time.time() - conn_start > 10.0:
                        backoff = 1.0
                    else:
                        backoff = min(backoff * 2, 60.0)
                        
                    i += 1
                except Exception as e:
                    print('ws run err', e)
                    backoff = min(backoff * 2, 60.0)
                
                if not self._stop:
                    time.sleep(backoff)
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
