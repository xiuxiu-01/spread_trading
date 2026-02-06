"""
验证每个合约是否能获取到盘口数据和1分钟K线数据
使用同步方法
"""
import ccxt

# 要测试的合约配置
CONTRACTS = {
    "XAUUSD": {
        "okx": "XAU/USDT:USDT",
        "binance": "XAU/USDT:USDT",
        "bybit": "PAXG/USDT:USDT",
        "gate": "XAU/USDT:USDT",
        "bitget": "XAU/USDT:USDT",
        "bitmart": "XAU/USDT:USDT",
        "lbank": "GOLD/USDT:USDT",
    },
    "XAGUSD": {
        "okx": "XAG/USDT:USDT",
        "binance": "XAG/USDT:USDT",
        "gate": "XAG/USDT:USDT",
        "bitget": "XAG/USDT:USDT",
        "bitmart": "XAG/USDT:USDT",
        "lbank": "SILVER/USDT:USDT",
    },
}

def test_exchange(exchange_id: str, symbol: str, asset: str):
    """测试单个交易所的合约"""
    result = {
        "exchange": exchange_id,
        "symbol": symbol,
        "asset": asset,
        "orderbook": False,
        "kline": False,
        "bid": None,
        "ask": None,
        "last_close": None,
        "error": None
    }
    
    try:
        # 创建交易所实例
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class({
            'enableRateLimit': True,
            'timeout': 15000,
        })
        
        # 加载市场
        exchange.load_markets()
        
        if symbol not in exchange.markets:
            result["error"] = "Symbol not found in markets"
            return result
        
        # 测试盘口数据
        try:
            orderbook = exchange.fetch_order_book(symbol, limit=5)
            if orderbook and orderbook.get('bids') and orderbook.get('asks'):
                result["orderbook"] = True
                result["bid"] = orderbook['bids'][0][0] if orderbook['bids'] else None
                result["ask"] = orderbook['asks'][0][0] if orderbook['asks'] else None
        except Exception as e:
            result["error"] = f"Orderbook error: {str(e)[:50]}"
        
        # 测试K线数据
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, '1m', limit=5)
            if ohlcv and len(ohlcv) > 0:
                result["kline"] = True
                result["last_close"] = ohlcv[-1][4]  # 最后一根K线的收盘价
        except Exception as e:
            if not result["error"]:
                result["error"] = f"OHLCV error: {str(e)[:50]}"
        
        return result
        
    except Exception as e:
        result["error"] = str(e)[:80]
        return result

def main():
    print("=" * 100)
    print("验证贵金属合约市场数据 (盘口 + 1分钟K线)")
    print("=" * 100)
    
    all_results = []
    
    for asset, exchanges in CONTRACTS.items():
        print(f"\n{'='*50}")
        print(f"📊 {asset}")
        print(f"{'='*50}")
        
        for exchange_id, symbol in exchanges.items():
            print(f"\n  测试 {exchange_id.upper():10} | {symbol}...", end=" ", flush=True)
            
            r = test_exchange(exchange_id, symbol, asset)
            all_results.append(r)
            
            ob_status = "✅" if r.get("orderbook") else "❌"
            kl_status = "✅" if r.get("kline") else "❌"
            
            print(f"盘口:{ob_status} K线:{kl_status}")
            
            if r.get("bid") and r.get("ask"):
                print(f"           Bid: {r['bid']:.4f} | Ask: {r['ask']:.4f}")
            if r.get("last_close"):
                print(f"           Last Close: {r['last_close']:.4f}")
            if r.get("error"):
                print(f"           ⚠️  {r['error']}")
    
    # 汇总
    print("\n" + "=" * 100)
    print("📋 汇总结果")
    print("=" * 100)
    
    print(f"\n{'Asset':<10} {'Exchange':<12} {'Symbol':<22} {'盘口':<6} {'K线':<6} {'状态'}")
    print("-" * 80)
    
    for r in all_results:
        ob = "✅" if r.get("orderbook") else "❌"
        kl = "✅" if r.get("kline") else "❌"
        status = "可用 ✅" if (r.get("orderbook") and r.get("kline")) else "不可用 ❌"
        print(f"{r.get('asset','?'):<10} {r.get('exchange','?'):<12} {r.get('symbol','?'):<22} {ob:<6} {kl:<6} {status}")
    
    # 统计可用合约
    print("\n" + "=" * 100)
    print("🎯 可用于套利的合约组合")
    print("=" * 100)
    
    for asset in CONTRACTS.keys():
        valid_exchanges = [r for r in all_results if r.get('asset') == asset and r.get('orderbook') and r.get('kline')]
        print(f"\n{asset}:")
        if len(valid_exchanges) >= 2:
            for r in valid_exchanges:
                print(f"  ✅ {r.get('exchange','?'):10} -> {r.get('symbol','?')}")
            print(f"  🔗 可组合 {len(valid_exchanges) * (len(valid_exchanges)-1) // 2} 个套利对")
        else:
            print(f"  ⚠️ 只有 {len(valid_exchanges)} 个可用交易所，无法套利")

if __name__ == "__main__":
    main()
