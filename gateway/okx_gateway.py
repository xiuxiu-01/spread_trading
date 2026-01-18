import os
import ccxt
from typing import Optional, Dict, Any
from dotenv import load_dotenv

load_dotenv()

class OKXGateway:
    """Unified OKX gateway for market data and trading, with optional WS streaming."""
    def __init__(self, symbol='PAXG/USDT'):
        self.symbol = symbol
        proxies = {
            'http': os.getenv('HTTP_PROXY'),
            'https': os.getenv('HTTPS_PROXY'),
        }
        opts: Dict[str, Any] = {
            'apiKey': os.getenv('OKX_API_KEY'),
            'secret': os.getenv('OKX_API_SECRET'),
            'password': os.getenv('OKX_API_PASSPHRASE'),
            'enableRateLimit': True,
        }
        if any(proxies.values()):
            opts['proxies'] = proxies
        self.client = ccxt.okx(opts)
        base = os.getenv('OKX_API_BASE')
        if base:
            self.client.urls = self.client.urls.copy()
            api = self.client.urls.get('api', {})
            api = api.copy() if isinstance(api, dict) else {}
            api['public'] = base
            api['private'] = base
            self.client.urls['api'] = api

        # WS streaming support
        self._ws = None
        self._latest_ws: Optional[Dict[str, Any]] = None

    # Market data
    def get_orderbook(self, limit: int = 5) -> Dict[str, Any]:
        ob = self.client.fetch_order_book(self.symbol, limit)
        return {
            'bids': [[float(p), float(q)] for p, q in ob.get('bids', [])],
            'asks': [[float(p), float(q)] for p, q in ob.get('asks', [])],
        }

    def get_ticker(self) -> Dict[str, Any]:
        t = self.client.fetch_ticker(self.symbol)
        last = float(t['last']) if t and t.get('last') else None
        return {'last': last, 'raw': t}

    # WS streaming
    def start_ws(self, record_file: Optional[str] = None) -> None:
        try:
            try:
                from okx_ws import OKXWS, OKX_INST_MAP
            except Exception:
                from gateway.okx_ws import OKXWS, OKX_INST_MAP
            instId = OKX_INST_MAP.get(self.symbol, self.symbol.replace('/', '-'))
            self._ws = OKXWS(instId, record_file=record_file)
            self._ws.start()
            # Attach a small poller to mirror snapshots into gateway for unified interface
            def poll_latest():
                import time
                while True:
                    try:
                        if self._ws and self._ws.latest:
                            self._latest_ws = self._ws.latest
                        time.sleep(0.5)
                    except Exception:
                        break
            import threading
            t = threading.Thread(target=poll_latest, daemon=True)
            t.start()
        except Exception as e:
            print(f"[OKXGateway] WS start failed: {e}")

    def stop_ws(self) -> None:
        try:
            if self._ws:
                self._ws.stop()
        except Exception:
            pass

    def latest_ws_snapshot(self) -> Optional[Dict[str, Any]]:
        return self._latest_ws

    def wait_for_ws_snapshot(self, timeout: float = 10.0) -> bool:
        """Block until first WS snapshot arrives or timeout."""
        import time
        start = time.time()
        while time.time() - start < timeout:
            if self._latest_ws:
                return True
            time.sleep(0.2)
        return False

    # Trading
    def place_limit_order(self, side: str, amount: float, price: float, post_only: bool = True):
        params = {'postOnly': True} if post_only else {}
        return self.client.create_order(symbol=self.symbol,
                                        type='limit',
                                        side=side,
                                        amount=amount,
                                        price=price,
                                        params=params)
