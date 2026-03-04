import ccxt
import json

bitget = ccxt.bitget()
bitget.load_markets()

candidates = []
for sym, m in bitget.markets.items():
    if 'XAU' in sym or 'XAUT' in sym or 'XAU' in m.get('id', ''):
        candidates.append((sym, m))

if not candidates:
    print('No XAU-like symbols found on Bitget')
else:
    for sym, m in candidates:
        print('---')
        print('symbol:', sym)
        print('type:', m.get('type'))
        # common fields
        print('contractSize:', m.get('contractSize') or m.get('contract_size'))
        print('precision:', m.get('precision'))
        print('info keys:', list(m.get('info', {}).keys())[:10])
        print('raw info sample:', json.dumps(m.get('info', {}), ensure_ascii=False)[:1000])
