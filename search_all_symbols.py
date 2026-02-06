"""全面搜索各交易所的贵金属和加密货币合约"""
import ccxt

def search_symbols(exchange, keywords):
    """搜索包含关键词的所有合约"""
    results = {}
    for kw in keywords:
        matches = []
        for sym, market in exchange.markets.items():
            if kw.upper() in sym.upper():
                matches.append({
                    'symbol': sym,
                    'type': market.get('type', '?'),
                    'active': market.get('active', False),
                    'base': market.get('base', ''),
                    'quote': market.get('quote', ''),
                })
        results[kw] = matches
    return results

print("=" * 70)
print("全面搜索各交易所合约")
print("=" * 70)

keywords = ['XAU', 'XAG', 'GOLD', 'SILVER', 'PAXG', 'XAUT']

# OKX
print("\n" + "=" * 70)
print("OKX")
print("=" * 70)
try:
    okx = ccxt.okx()
    okx.load_markets()
    print(f"总合约数: {len(okx.markets)}")
    
    results = search_symbols(okx, keywords)
    for kw, matches in results.items():
        if matches:
            print(f"\n  [{kw}] 找到 {len(matches)} 个:")
            for m in matches[:10]:
                print(f"    {m['symbol']:30} type={m['type']:8} active={m['active']}")
except Exception as e:
    print(f"  Error: {e}")

# Binance
print("\n" + "=" * 70)
print("Binance")
print("=" * 70)
try:
    binance = ccxt.binance()
    binance.load_markets()
    print(f"总合约数: {len(binance.markets)}")
    
    results = search_symbols(binance, keywords)
    for kw, matches in results.items():
        if matches:
            print(f"\n  [{kw}] 找到 {len(matches)} 个:")
            for m in matches[:10]:
                print(f"    {m['symbol']:30} type={m['type']:8} active={m['active']}")
except Exception as e:
    print(f"  Error: {e}")

# Bybit
print("\n" + "=" * 70)
print("Bybit")
print("=" * 70)
try:
    bybit = ccxt.bybit()
    bybit.load_markets()
    print(f"总合约数: {len(bybit.markets)}")
    
    results = search_symbols(bybit, keywords)
    for kw, matches in results.items():
        if matches:
            print(f"\n  [{kw}] 找到 {len(matches)} 个:")
            for m in matches[:10]:
                print(f"    {m['symbol']:30} type={m['type']:8} active={m['active']}")
except Exception as e:
    print(f"  Error: {e}")

# Gate.io
print("\n" + "=" * 70)
print("Gate.io")
print("=" * 70)
try:
    gate = ccxt.gate()
    gate.load_markets()
    print(f"总合约数: {len(gate.markets)}")
    
    results = search_symbols(gate, keywords)
    for kw, matches in results.items():
        if matches:
            print(f"\n  [{kw}] 找到 {len(matches)} 个:")
            for m in matches[:10]:
                print(f"    {m['symbol']:30} type={m['type']:8} active={m['active']}")
except Exception as e:
    print(f"  Error: {e}")

# Bitget
print("\n" + "=" * 70)
print("Bitget")
print("=" * 70)
try:
    bitget = ccxt.bitget()
    bitget.load_markets()
    print(f"总合约数: {len(bitget.markets)}")
    
    results = search_symbols(bitget, keywords)
    for kw, matches in results.items():
        if matches:
            print(f"\n  [{kw}] 找到 {len(matches)} 个:")
            for m in matches[:10]:
                print(f"    {m['symbol']:30} type={m['type']:8} active={m['active']}")
except Exception as e:
    print(f"  Error: {e}")

# BitMart
print("\n" + "=" * 70)
print("BitMart")
print("=" * 70)
try:
    bitmart = ccxt.bitmart()
    bitmart.load_markets()
    print(f"总合约数: {len(bitmart.markets)}")
    
    results = search_symbols(bitmart, keywords)
    for kw, matches in results.items():
        if matches:
            print(f"\n  [{kw}] 找到 {len(matches)} 个:")
            for m in matches[:10]:
                print(f"    {m['symbol']:30} type={m['type']:8} active={m['active']}")
except Exception as e:
    print(f"  Error: {e}")

# LBank
print("\n" + "=" * 70)
print("LBank")
print("=" * 70)
try:
    lbank = ccxt.lbank()
    lbank.load_markets()
    print(f"总合约数: {len(lbank.markets)}")
    
    results = search_symbols(lbank, keywords)
    for kw, matches in results.items():
        if matches:
            print(f"\n  [{kw}] 找到 {len(matches)} 个:")
            for m in matches[:10]:
                print(f"    {m['symbol']:30} type={m['type']:8} active={m['active']}")
except Exception as e:
    print(f"  Error: {e}")

print("\n" + "=" * 70)
print("搜索完成")
print("=" * 70)
