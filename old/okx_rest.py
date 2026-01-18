import os
import ccxt
import requests


def create_okx():
    base = os.getenv('OKX_API_BASE')  # e.g., https://aws.okx.com or https://www.okx.com
    proxies = {
        'http': os.getenv('HTTP_PROXY') or os.getenv('http_proxy'),
        'https': os.getenv('HTTPS_PROXY') or os.getenv('https_proxy'),
    }
    opts = {
        'enableRateLimit': True,
    }
    if any(proxies.values()):
        opts['proxies'] = proxies
    okx = ccxt.okx(opts)
    # optional: override base URLs
    if base:
        okx.urls = okx.urls.copy()
        api = okx.urls.get('api', {})
        api = api.copy() if isinstance(api, dict) else {}
        api['public'] = base
        api['private'] = base
        okx.urls['api'] = api
    return okx


def get_orderbook(okx, symbol, limit=5):
    try:
        ob = okx.fetch_order_book(symbol, limit)
        bids = [[float(p), float(q)] for p, q in ob.get('bids', [])]
        asks = [[float(p), float(q)] for p, q in ob.get('asks', [])]
        return {'bids': bids, 'asks': asks, 'bids_raw': ob.get('bids', []), 'asks_raw': ob.get('asks', [])}
    except Exception:
        # fallback: direct REST without ccxt market loading
        inst = symbol.replace('/', '-')
        base = os.getenv('OKX_API_BASE') or 'https://www.okx.com'
        url = f"{base}/api/v5/market/books?instId={inst}&sz={limit}"
        proxies = {
            'http': os.getenv('HTTP_PROXY') or os.getenv('http_proxy'),
            'https': os.getenv('HTTPS_PROXY') or os.getenv('https_proxy'),
        }
        resp = requests.get(url, timeout=10, proxies={k: v for k, v in proxies.items() if v})
        resp.raise_for_status()
        data = resp.json()
        first = (data.get('data') or [{}])[0]
        bids = []
        for level in (first.get('bids') or []):
            if not level:
                continue
            p = float(level[0])
            q = float(level[1]) if len(level) > 1 else 0.0
            bids.append([p, q])
        asks = []
        for level in (first.get('asks') or []):
            if not level:
                continue
            p = float(level[0])
            q = float(level[1]) if len(level) > 1 else 0.0
            asks.append([p, q])
        return {'bids': bids, 'asks': asks}
