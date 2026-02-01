import os
from dotenv import load_dotenv
load_dotenv()

import ccxt

print(f'ccxt version: {ccxt.__version__}')

opts = {
    'enableRateLimit': True,
}
api_key = os.getenv('OKX_API_KEY')
api_secret = os.getenv('OKX_API_SECRET')
api_passphrase = os.getenv('OKX_API_PASSPHRASE')
if api_key:
    opts['apiKey'] = api_key
if api_secret:
    opts['secret'] = api_secret
if api_passphrase:
    opts['password'] = api_passphrase

print('Creating OKX client...')
client = ccxt.okx(opts)

print('Attempting to load markets (this triggers the bug)...')
try:
    client.load_markets()
    print('Markets loaded successfully!')
    print(f'Total markets: {len(client.markets)}')
except Exception as e:
    print(f'Error: {type(e).__name__}: {e}')
    import traceback
    traceback.print_exc()
