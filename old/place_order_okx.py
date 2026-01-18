import os
import sys
import pathlib
from dotenv import load_dotenv

sys.path.append(str(pathlib.Path(__file__).parent))
import ccxt
from old.okx_rest import create_okx

"""
Place a small post-only limit order on OKX to avoid immediate fill.
Usage:
  python bot/place_order_okx.py [buy|sell] [amount] [symbol]
Defaults:
  side=buy, amount=0.001, symbol=PAXG/USDT
Requires .env:
  OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE
Optional:
  OKX_API_BASE, HTTP_PROXY/HTTPS_PROXY
"""

load_dotenv()

def main():
    side = sys.argv[1] if len(sys.argv) > 1 else 'buy'
    amount = float(sys.argv[2]) if len(sys.argv) > 2 else 0.001
    symbol = sys.argv[3] if len(sys.argv) > 3 else 'PAXG/USDT'

    # Ensure keys exist
    if not (os.getenv('OKX_API_KEY') and os.getenv('OKX_API_SECRET') and os.getenv('OKX_API_PASSPHRASE')):
        print('Missing OKX credentials in .env: OKX_API_KEY / OKX_API_SECRET / OKX_API_PASSPHRASE')
        sys.exit(1)

    okx = ccxt.okx({
        'apiKey': os.getenv('OKX_API_KEY'),
        'secret': os.getenv('OKX_API_SECRET'),
        'password': os.getenv('OKX_API_PASSPHRASE'),
        'enableRateLimit': True,
        'proxies': {
            'http': os.getenv('HTTP_PROXY'),
            'https': os.getenv('HTTPS_PROXY'),
        },
    })

    # Optional base/proxies picked up via okx_rest.create_okx if needed
    base = os.getenv('OKX_API_BASE')
    if base:
        okx.urls = okx.urls.copy()
        api = okx.urls.get('api', {})
        api = api.copy() if isinstance(api, dict) else {}
        api['public'] = base
        api['private'] = base
        okx.urls['api'] = api

    # Fetch current ticker to place far-from-market post-only order
    try:
        ticker = okx.fetch_ticker(symbol)
        last = float(ticker['last']) if ticker and ticker.get('last') else None
    except Exception:
        # fallback: use orderbook mid
        ob = okx.fetch_order_book(symbol, limit=5)
        if ob.get('bids') and ob.get('asks'):
            last = (ob['bids'][0][0] + ob['asks'][0][0]) / 2.0
        else:
            last = None

    if last is None:
        print('Unable to get market price for', symbol)
        sys.exit(1)

    # Set a price unlikely to cross, and postOnly to avoid taker fill
    # For buy: set price well below last; for sell: well above last
    offset = 50.0  # USD offset; adjust as needed
    price = last - offset if side == 'buy' else last + offset

    print(f"Placing OKX post-only limit order: side={side} amount={amount} symbol={symbol} price={price:.2f} (last={last:.2f})")
    try:
        order = okx.create_order(symbol=symbol,
                                 type='limit',
                                 side=side,
                                 amount=amount,
                                 price=price,
                                 params={'postOnly': True})
        print('Order placed:', order)
    except Exception as e:
        print('Order error:', e)
        sys.exit(1)

if __name__ == '__main__':
    main()
