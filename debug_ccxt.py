import os
from dotenv import load_dotenv
load_dotenv()

import ccxt

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

# Try to get raw market data to see what's returning None
for inst_type in ['SPOT', 'SWAP', 'FUTURES', 'OPTION']:
    print(f'\nFetching {inst_type} instruments...')
    try:
        response = client.publicGetPublicInstruments({'instType': inst_type})
        data = response.get('data', [])
        print(f'Got {len(data)} {inst_type} instruments')
        
        # Check for None values in baseCcy or quoteCcy
        bad = [i for i in data if not i.get('baseCcy') and not i.get('ctValCcy')]
        if bad:
            print(f'Found {len(bad)} instruments with missing base currency:')
            for b in bad[:3]:
                print(f'  instId={b.get("instId")}, baseCcy={b.get("baseCcy")}, quoteCcy={b.get("quoteCcy")}, ctValCcy={b.get("ctValCcy")}')
    except Exception as e:
        print(f'Error fetching {inst_type}: {e}')
