import os
import ccxt
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from datetime import datetime

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

    def get_historical_data(self, start: datetime, end: datetime) -> list:
        """Fetch historical 1-minute OHLCV data from OKX with pagination."""
        try:
            # Ensure naive UTC timestamps
            if start.tzinfo:
                start = start.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            if end.tzinfo:
                end = end.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            
            start_ts = int(start.timestamp() * 1000)
            end_ts = int(end.timestamp() * 1000)
            
            all_data = []
            
            # OKX limit per request (usually 100 for candles)
            LIMIT = 100
            
            current_since = start_ts
            
            while current_since < end_ts:
                # Fetch batch
                ohlcv = self.client.fetch_ohlcv(self.symbol, timeframe='1m', since=current_since, limit=LIMIT)
                
                if not ohlcv:
                    break
                    
                for entry in ohlcv:
                    ts = entry[0]
                    # Filter out any data beyond end_time
                    if ts > end_ts:
                        continue
                        
                    all_data.append({
                        'time': ts / 1000, # Convert to seconds for consistency
                        'open': float(entry[1]),
                        'high': float(entry[2]),
                        'low': float(entry[3]),
                        'close': float(entry[4]),
                        'volume': float(entry[5]),
                    })
                
                # Update cursor
                last_ts = ohlcv[-1][0]
                if last_ts <= current_since:
                   # Prevent infinite loop if exchange returns same data
                   current_since += 60 * 1000 * LIMIT 
                else:
                    current_since = last_ts + 1  # Move past the last record

                # Rate limit protection
                import time
                time.sleep(0.1)

            return all_data
            
        except ccxt.NetworkError as e:
            print(f"[OKXGateway] Network error: {e}")
        except ccxt.ExchangeError as e:
            print(f"[OKXGateway] Exchange error: {e}")
        except Exception as e:
            print(f"[OKXGateway] Unexpected error: {e}")
        return []

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
    def get_balance(self) -> float:
        """Return free USDT balance (or relevant quote currency)."""
        try:
            # We assume quote currency is USDT for PAXG/USDT
            # Robustly parse quote currency, handling '/' or '-' or failing gracefully
            quote = 'USDT' # Default fallback
            if '/' in self.symbol:
                quote = self.symbol.split('/')[1]
            elif '-' in self.symbol:
                quote = self.symbol.split('-')[1]

            bal = self.client.fetch_balance()
            if quote in bal:
                return float(bal[quote]['free'])
            # fallback if structure differs
            return float(bal.get('free', {}).get(quote, 0.0))
        except Exception as e:
            print(f"[OKXGateway] get_balance error: {e}")
            return 0.0

    def get_asset_balance(self, currency: str) -> float:
        """Return free balance of a specific asset."""
        try:
            bal = self.client.fetch_balance()
            if currency in bal:
                 return float(bal[currency]['free'])
            return float(bal.get('free', {}).get(currency, 0.0))
        except Exception as e:
            print(f"[OKXGateway] get_asset_balance({currency}) error: {e}")
            return 0.0

    def get_positions(self) -> list:
        """Return open positions for this symbol."""
        try:
            # Try to fetch derivatives positions (Swap/Futures/Margin)
            raw_positions = []
            try:
                raw_positions = self.client.fetch_positions([self.symbol])
            except Exception as e:
                # If fetch_positions fails (e.g. not supported for this symbol type), we continue
                # print(f"[OKXGateway] fetch_positions debug: {e}")
                pass

            out = []
            for p in raw_positions:
                # ccxt unifies position structure
                # Typically side is 'long' or 'short'
                out.append({
                    'id': p.get('id'),
                    'side': p.get('side'), # long/short
                    'amount': float(p.get('contracts') or p.get('amount') or 0),
                    'openPrice': float(p.get('entryPrice') or 0),
                    'unrealizedPnl': float(p.get('unrealizedPnl') or 0),
                    'leverage': p.get('leverage'),
                })
            
            # If no derivative positions, check if we have Spot assets (Base Currency)
            # This is a heuristic to show Spot holdings as "Long Positions"
            if not out and '/' in self.symbol:
                # e.g. PAXG/USDT -> Base: PAXG
                parts = self.symbol.split('/')
                if len(parts) >= 1:
                    base = parts[0]
                    # We need to fetch balance again or cache it. 
                    # For simplicity, we fetch it (rate limit allowing)
                    bal = self.client.fetch_balance()
                    
                    # Check free balance (only free can be sold immediately)
                    # We use 'free' instead of 'total' to avoid "Insufficient balance" when trying to sell locked funds.
                    amount = float(bal.get(base, {}).get('free', 0.0))
                    
                    # Filter out dust (e.g. < 0.0001)
                    if amount > 0.0001:
                        out.append({
                            'id': f'spot-{base}',
                            'side': 'long', 
                            'amount': amount,
                            'openPrice': 0.0, # Cannot determine easily for Spot wallet
                            'unrealizedPnl': 0.0,
                            'leverage': 1.0,
                        })

            return out
        except Exception as e:
            print(f"[OKXGateway] get_positions error: {e}")
            return []

    def place_market_order(self, side: str, amount: float):
        # 确定我们是在交易现货还是永续/期货
        # 如果代码中包含 -SWAP 或者是期货，通常 create_order('market') 不需要更改，
        # 但有时在双向持仓模式下需要指定 'positionSide' 为 'long'/'short'。
        # 检查我们是单向持仓模式还是双向持仓模式？
        # 为简单起见，如果未指定，我们假设是单向模式或标准净持仓模式。
        # 但用户提到了“期货”。
        
        # 如果是期货，平仓时可能需要指定 'reduceOnly'？
        # 但在这里我们通过简单的市价单进行开仓/平仓。
        
        # 确保数量在精度范围内
        # CCXT 会处理这个问题，但有时传递格式化的字符串更安全
        # self.client.load_markets() # Should be loaded
        try:
             amount = self.client.amount_to_precision(self.symbol, amount)
        except Exception:
             pass 
        #如果可以平的仓位低于0.001但是amount为
        params = {}
        # 对于现货市价买单，如果要按基础货币数量购买而不是按计价货币金额购买，通常需要指定 'tgtCcy'
        # 在 OKX 上买入现货时：
        # 如果我们要买入 X 数量的基础货币（例如 1 PAXG），我们将 'tgtCcy' 设置为 'base_ccy'。
        # 如果省略它，行为取决于交易所的默认值（市价买入通常默认为计价货币金额）。
        # 我们显式将其设置为 base_ccy 以匹配我们的 'amount' 语义（即数量）。
        if side == 'buy' and '/' in self.symbol: # Spot check
             params['tgtCcy'] = 'base_ccy'
             # 注意：OKX 现货买入时，交易手续费通常会从收到的基础资产（如PAXG）中扣除。
             # 因此，如果你买入 1.0 PAXG，实际到账可能只有 0.999 PAXG（假设 0.1% 类手续费）。
             # 这会导致持仓量略少于下单量，且与 MT5（通常从余额扣除手续费不影响持仓量）表现不同。
             # Note: OKX Spot Buy deducts fees from the received base asset.
             # Buying 1.0 might result in 0.999 balance. This is expected behavior.
        
        return self.client.create_order(symbol=self.symbol,
                                        type='market',
                                        side=side,
                                        amount=amount,
                                        params=params)

    def place_limit_order(self, side: str, amount: float, price: float, post_only: bool = True):
        params = {'postOnly': True} if post_only else {}
        return self.client.create_order(symbol=self.symbol,
                                        type='limit',
                                        side=side,
                                        amount=amount,
                                        price=price,
                                        params=params)
