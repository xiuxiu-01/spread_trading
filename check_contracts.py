
import ccxt
import time

def check_okx_instruments():
    print("Connecting to OKX...")
    okx = ccxt.okx()
    try:
        markets = okx.load_markets()
        
        print(f"Total markets: {len(markets)}")
        
        # Search for gold/silver
        for s in markets:
            if 'XAU' in s or 'XAG' in s:
                if 'USDT' in s and ':' in s: # swa
                    print(f"Found: {s}")
                    
        symbols = ['XAU/USDT:USDT', 'XAG/USDT:USDT']
        
        for sym in symbols:
            if sym in markets:
                info = markets[sym]
                contract_size = info.get('contractSize')
                print(f"\nSymbol: {sym}")
                print(f"Contract Size: {contract_size}")
                # OKX raw info usually has ctVal (contract value)
                raw = info.get('info', {})
                print(f"ctVal: {raw.get('ctVal')}")
                print(f"ctMult: {raw.get('ctMult')}")
            else:
                print(f"\nSymbol {sym} not found")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_okx_instruments()
