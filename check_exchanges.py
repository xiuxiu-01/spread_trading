"""检查各交易所是否有配置的合约"""
import ccxt

print("=" * 60)
print("检查各交易所合约是否存在")
print("=" * 60)

# OKX
print("\n=== OKX ===")
try:
    okx = ccxt.okx()
    okx.load_markets()
    test_symbols = ['XAU/USDT:USDT', 'XAG/USDT:USDT', 'BTC/USDT:USDT', 'ETH/USDT:USDT']
    for sym in test_symbols:
        if sym in okx.markets:
            m = okx.markets[sym]
            print(f"  ✅ {sym} - type={m['type']} active={m['active']}")
        else:
            print(f"  ❌ {sym} - NOT FOUND")
            # 搜索相似的
            similar = [s for s in okx.markets.keys() if 'XAU' in s or 'XAG' in s][:5]
            if similar:
                print(f"     Similar: {similar}")
except Exception as e:
    print(f"  Error: {e}")

# Binance
print("\n=== Binance ===")
try:
    binance = ccxt.binance()
    binance.load_markets()
    test_symbols = ['PAXG/USDT', 'BTC/USDT', 'ETH/USDT', 'EUR/USDT']
    for sym in test_symbols:
        if sym in binance.markets:
            m = binance.markets[sym]
            print(f"  ✅ {sym} - type={m['type']} active={m['active']}")
        else:
            print(f"  ❌ {sym} - NOT FOUND")
except Exception as e:
    print(f"  Error: {e}")

# Bybit
print("\n=== Bybit ===")
try:
    bybit = ccxt.bybit()
    bybit.load_markets()
    test_symbols = ['XAUUSDT', 'XAU/USDT:USDT', 'XAGUSDT', 'XAG/USDT:USDT', 'BTC/USDT:USDT', 'ETH/USDT:USDT']
    for sym in test_symbols:
        if sym in bybit.markets:
            m = bybit.markets[sym]
            print(f"  ✅ {sym} - type={m['type']} active={m['active']}")
        else:
            print(f"  ❌ {sym} - NOT FOUND")
            # 搜索相似的
            if 'XAU' in sym or 'XAG' in sym:
                similar = [s for s in bybit.markets.keys() if 'XAU' in s or 'XAG' in s][:5]
                if similar:
                    print(f"     Similar: {similar}")
except Exception as e:
    print(f"  Error: {e}")

# Gate.io
print("\n=== Gate.io ===")
try:
    gate = ccxt.gate()
    gate.load_markets()
    test_symbols = ['PAXG/USDT', 'PAXG_USDT', 'BTC/USDT', 'ETH/USDT', 'EUR/USDT', 'EUR_USDT']
    for sym in test_symbols:
        if sym in gate.markets:
            m = gate.markets[sym]
            print(f"  ✅ {sym} - type={m['type']} active={m['active']}")
        else:
            print(f"  ❌ {sym} - NOT FOUND")
except Exception as e:
    print(f"  Error: {e}")

# Bitget
print("\n=== Bitget ===")
try:
    bitget = ccxt.bitget()
    bitget.load_markets()
    test_symbols = ['XAUUSDT', 'XAU/USDT:USDT', 'XAGUSDT', 'XAG/USDT:USDT', 'BTC/USDT:USDT', 'ETH/USDT:USDT']
    for sym in test_symbols:
        if sym in bitget.markets:
            m = bitget.markets[sym]
            print(f"  ✅ {sym} - type={m['type']} active={m['active']}")
        else:
            print(f"  ❌ {sym} - NOT FOUND")
            if 'XAU' in sym or 'XAG' in sym:
                similar = [s for s in bitget.markets.keys() if 'XAU' in s or 'XAG' in s][:5]
                if similar:
                    print(f"     Similar: {similar}")
except Exception as e:
    print(f"  Error: {e}")

print("\n" + "=" * 60)
print("检查完成")
