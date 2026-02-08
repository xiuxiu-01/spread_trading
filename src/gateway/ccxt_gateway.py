"""
CCXT Pro Gateway - Universal exchange connector.

Supports multiple exchanges through ccxt.pro library:
- OKX
- Gate.io
- Bitget
- BitMart
- Binance
- LBank
- Bybit
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import aiohttp

from .base import BaseGateway, GatewayStatus
from ..models import Tick, Order, OrderSide, OrderStatus, Position, PositionSide
from ..config.logging_config import get_logger

logger = get_logger("gateway.ccxt")

# Try to import ccxt.pro
try:
    import ccxt.pro as ccxtpro
    CCXT_AVAILABLE = True
except ImportError:
    try:
        import ccxt.async_support as ccxtpro
        CCXT_AVAILABLE = True
    except ImportError:
        ccxtpro = None
        CCXT_AVAILABLE = False


# Exchange class mapping
EXCHANGE_CLASSES = {
    "okx": "okx",
    "gate": "gateio",
    "gateio": "gateio",
    "bitget": "bitget",
    "bitmart": "bitmart",
    "binance": "binance",
    "lbank": "lbank",
    "bybit": "bybit",
}


class CCXTGateway(BaseGateway):
    """
    Universal exchange gateway using CCXT Pro.
    
    Provides a unified interface for multiple cryptocurrency exchanges
    with WebSocket support for real-time data.
    """
    
    def __init__(
        self,
        exchange_name: str,
        symbol: str,
        api_key: str = "",
        api_secret: str = "",
        password: str = "",
        sandbox: bool = False,
        options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize CCXT gateway.
        
        Args:
            exchange_name: Exchange identifier (e.g., "okx", "binance")
            symbol: Trading symbol in CCXT format (e.g., "XAU/USDT:USDT")
            api_key: API key
            api_secret: API secret
            password: API password/passphrase (for exchanges that require it)
            sandbox: Use sandbox/testnet mode
            options: Additional exchange-specific options
        """
        # Normalize exchange name
        exchange_id = EXCHANGE_CLASSES.get(exchange_name.lower(), exchange_name.lower())
        super().__init__(name=exchange_name.lower(), symbol=symbol)
        
        self.exchange_id = exchange_id
        self.api_key = api_key
        self.api_secret = api_secret
        self.password = password
        self.sandbox = sandbox
        self.options = options or {}
        
        self._exchange: Optional[Any] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._running = False
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Cache
        self._last_tick: Optional[Tick] = None
        self._orderbook: Dict[str, Any] = {}
        self._positions: Dict[str, Position] = {}
    
    def _create_exchange(self) -> Any:
        """Create CCXT exchange instance."""
        if not CCXT_AVAILABLE:
            raise ImportError("ccxt.pro is not installed. Run: pip install ccxt")
        
        exchange_class = getattr(ccxtpro, self.exchange_id, None)
        if not exchange_class:
            raise ValueError(f"Exchange '{self.exchange_id}' not supported by CCXT")
        
        # Determine market type from symbol
        # Symbols like "XAU/USDT:USDT" or "BTC/USDT:USDT" are perpetual swaps
        is_swap = ":USDT" in self.symbol or ":USD" in self.symbol
        market_type = "swap" if is_swap else "spot"
        
        config = {
            "apiKey": self.api_key,
            "secret": self.api_secret,
            "enableRateLimit": True,
            "options": {
                "defaultType": market_type,
                **self.options
            }
        }
        
        # OKX-specific: Force swap market type
        if self.exchange_id == "okx" and is_swap:
            config["options"]["defaultMarket"] = "swap"
            # OKX requires instType for API calls
            config["options"]["instType"] = "SWAP"
            config["options"]["fetchMarkets"] = ["swap"]
        
        # Binance-specific: Force perpetual futures
        if self.exchange_id == "binance" and is_swap:
            config["options"]["defaultType"] = "future"
            config["options"]["adjustForTimeDifference"] = True
        
        # Add password if provided (OKX, etc.)
        if self.password:
            config["password"] = self.password
        
        # Add proxy settings if available
        import os
        http_proxy = os.getenv('HTTP_PROXY')
        https_proxy = os.getenv('HTTPS_PROXY')
        if http_proxy or https_proxy:
            proxies = {}
            if http_proxy:
                proxies['http'] = http_proxy
            if https_proxy:
                proxies['https'] = https_proxy
            config['proxies'] = proxies
        
        # Create aiohttp session with ThreadedResolver to avoid aiodns DNS issues on Windows
        connector = aiohttp.TCPConnector(resolver=aiohttp.resolver.ThreadedResolver())
        self._session = aiohttp.ClientSession(connector=connector)
        config['session'] = self._session
        
        exchange = exchange_class(config)
        
        # Set sandbox mode
        if self.sandbox:
            exchange.set_sandbox_mode(True)
        
        logger.info(f"Created {self.exchange_id} exchange with market type: {market_type}")
        return exchange
    
    async def connect(self) -> bool:
        """Connect to exchange."""
        try:
            self._set_status(GatewayStatus.CONNECTING)
            logger.info(f"Connecting to {self.name}...")
            
            self._exchange = self._create_exchange()
            
            # Load markets to verify connection
            # For swap/perpetual contracts, we need to load the right market type
            is_swap = ":USDT" in self.symbol or ":USD" in self.symbol
            
            if self.exchange_id == "okx" and is_swap:
                # OKX: load only swap markets by passing params
                logger.info(f"Loading OKX swap markets...")
                await self._exchange.load_markets(params={"instType": "SWAP"})
            else:
                await self._exchange.load_markets()
            
            # Verify symbol exists
            if self.symbol not in self._exchange.markets:
                # Try to find similar symbol
                symbol_base = self.symbol.split('/')[0] if '/' in self.symbol else self.symbol
                available = [s for s in self._exchange.markets if symbol_base in s][:10]
                logger.warning(f"Symbol {self.symbol} not found. Similar: {available}")
                # Don't fail, just warn
            else:
                logger.info(f"Symbol {self.symbol} found in {self.name} markets")
            
            self._set_status(GatewayStatus.CONNECTED)
            logger.info(f"{self.name} connected successfully")
            return True
            
        except Exception as e:
            logger.error(f"{self.name} connection failed: {e}")
            self._set_status(GatewayStatus.ERROR)
            await self._emit_error(str(e))
            return False
    
    async def disconnect(self):
        """Disconnect from exchange."""
        self._running = False
        
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        
        if self._exchange:
            try:
                await self._exchange.close()
            except Exception as e:
                logger.warning(f"Error closing {self.name}: {e}")
        
        # Close the aiohttp session
        if self._session:
            try:
                await self._session.close()
            except Exception as e:
                logger.warning(f"Error closing session for {self.name}: {e}")
            self._session = None
        
        self._exchange = None
        self._set_status(GatewayStatus.DISCONNECTED)
        logger.info(f"{self.name} disconnected")
    
    async def is_connected(self) -> bool:
        """Check if connected."""
        return self._exchange is not None and self.status == GatewayStatus.CONNECTED
    
    async def get_ticker(self) -> Optional[Tick]:
        """Get current ticker."""
        if not self._exchange:
            return None
        
        try:
            ticker = await self._exchange.fetch_ticker(self.symbol)
            
            tick = Tick(
                exchange=self.name,
                symbol=self.symbol,
                bid=float(ticker.get("bid", 0) or 0),
                ask=float(ticker.get("ask", 0) or 0),
                timestamp=datetime.now(timezone.utc),
                last=float(ticker.get("last", 0) or 0),
                volume=float(ticker.get("baseVolume", 0) or 0)
            )
            self._last_tick = tick
            return tick
            
        except Exception as e:
            logger.error(f"{self.name} get_ticker error: {e}")
            return None
    
    async def subscribe_ticker(self):
        """Subscribe to real-time ticker via WebSocket."""
        if not self._exchange:
            logger.error("Exchange not connected")
            return
        
        self._running = True
        self._ws_task = asyncio.create_task(self._ticker_loop())
    
    async def _ticker_loop(self):
        """WebSocket ticker subscription loop."""
        logger.info(f"Starting {self.name} WebSocket ticker subscription")
        
        while self._running:
            try:
                # Use watch_order_book for best bid/ask
                orderbook = await self._exchange.watch_order_book(self.symbol)
                
                if orderbook.get("bids") and orderbook.get("asks"):
                    best_bid = float(orderbook["bids"][0][0])
                    best_ask = float(orderbook["asks"][0][0])
                    
                    tick = Tick(
                        exchange=self.name,
                        symbol=self.symbol,
                        bid=best_bid,
                        ask=best_ask,
                        timestamp=datetime.now(timezone.utc),
                        last=(best_bid + best_ask) / 2
                    )
                    self._last_tick = tick
                    self._orderbook = orderbook
                    await self._emit_tick(tick)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"{self.name} WebSocket error: {e}")
                await self._emit_error(str(e))
                await asyncio.sleep(1)
        
        logger.info(f"{self.name} WebSocket subscription stopped")
    
    async def get_klines(
        self,
        timeframe: str = "1m",
        limit: int = 100,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get historical klines with pagination."""
        if not self._exchange:
            return []
        
        try:
            # CCXT expects milliseconds
            since = start_time * 1000 if start_time else None
            end_ts_ms = end_time * 1000 if end_time else None
            
            all_ohlcv = []
            remaining = limit
            current_since = since
            
            # Map timeframe to milliseconds for incrementing `since`
            tf_seconds = self._exchange.parse_timeframe(timeframe)
            tf_ms = tf_seconds * 1000

            while remaining > 0:
                # Some exchanges limit to 100 or 300 per request. 
                # Requesting larger batches than exchange limit will just return limit.
                # We request up to 1000, assuming CCXT/Exchange truncates as needed.
                batch_limit = min(remaining, 1000) 
                
                batch = await self._exchange.fetch_ohlcv(
                    self.symbol,
                    timeframe=timeframe,
                    limit=batch_limit,
                    since=current_since
                )
                
                if not batch:
                    break
                    
                # If we have an end_time, filter out bars beyond it
                if end_ts_ms:
                    batch = [b for b in batch if b[0] <= end_ts_ms]
                    if not batch:
                        break
                
                all_ohlcv.extend(batch)
                remaining -= len(batch)
                
                # Update since for next batch (last bar time + 1 timeframe)
                last_bar_time = batch[-1][0]
                current_since = last_bar_time + tf_ms
                
                # If we got fewer bars than requested in this batch (and didn't ask for huge amount),
                # we probably reached head of market data
                if len(batch) < batch_limit and len(batch) < 100: # heuristic
                     # However, be careful: if we asked for 50 and got 50, we might need more.
                     # If we asked for 1000 and got 100 (exchange limit), we continue.
                     # If we asked for 1000 and got 5, we are done.
                     if len(batch) < 100: # Assuming 100 is a common low limit
                         break
                
                # Safety break for infinite loops if exchange implies strict limits
                if len(all_ohlcv) >= limit:
                    break
            
            # Trim if exceeded
            if len(all_ohlcv) > limit:
                all_ohlcv = all_ohlcv[:limit]

            return [{
                "time": int(bar[0] / 1000),  # Convert ms to seconds
                "open": float(bar[1]),
                "high": float(bar[2]),
                "low": float(bar[3]),
                "close": float(bar[4]),
                "volume": float(bar[5])
            } for bar in all_ohlcv]
            
        except Exception as e:
            logger.error(f"{self.name} get_klines error: {e}")
            return []
    
    async def place_order(
        self,
        side: OrderSide,
        volume: float,
        price: Optional[float] = None,
        order_type: str = "market",
        dry_run: bool = False
    ) -> Order:
        """Place order on exchange."""
        order = Order(
            exchange=self.name,
            symbol=self.symbol,
            side=side,
            volume=volume,
            price=price,
            order_type=order_type
        )
        
        # Dry run mode - simulate order without sending
        if dry_run:
            order.mark_submitted(f"DRY_RUN_{id(order)}")
            simulated_price = self._last_tick.ask if side == OrderSide.BUY else self._last_tick.bid if self._last_tick else price or 0
            order.mark_filled(simulated_price)
            logger.info(f"[DRY_RUN] {self.name} order simulated: {side.value} {volume} @ {simulated_price}")
            await self._emit_order(order)
            return order
        
        if not self._exchange:
            order.mark_failed("Exchange not connected")
            return order
        
        try:
            ccxt_side = "buy" if side == OrderSide.BUY else "sell"
            
            if order_type == "market":
                result = await self._exchange.create_market_order(
                    self.symbol, ccxt_side, volume
                )
            else:
                result = await self._exchange.create_limit_order(
                    self.symbol, ccxt_side, volume, price
                )
            
            order.mark_submitted(result.get("id", ""))
            
            # Check if filled
            if result.get("status") == "closed":
                avg_price = float(result.get("average", result.get("price", 0)) or 0)
                fee = float(result.get("fee", {}).get("cost", 0) or 0)
                order.mark_filled(avg_price, fee)
                logger.info(f"{self.name} order filled: {side.value} {volume} @ {avg_price}")
            
            await self._emit_order(order)
            return order
            
        except Exception as e:
            order.mark_failed(str(e))
            logger.error(f"{self.name} place_order error: {e}")
            await self._emit_order(order)
            return order
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel order."""
        if not self._exchange:
            return False
        
        try:
            await self._exchange.cancel_order(order_id, self.symbol)
            return True
        except Exception as e:
            logger.error(f"{self.name} cancel_order error: {e}")
            return False
    
    async def get_position(self) -> Optional[Position]:
        """Get current position."""
        if not self._exchange:
            return None
        
        try:
            positions = await self._exchange.fetch_positions([self.symbol])
            
            for pos in positions:
                if pos.get("symbol") == self.symbol:
                    contracts = float(pos.get("contracts", 0) or 0)
                    
                    if contracts == 0:
                        return Position(exchange=self.name, symbol=self.symbol)
                    
                    side = PositionSide.LONG if pos.get("side") == "long" else PositionSide.SHORT
                    
                    return Position(
                        exchange=self.name,
                        symbol=self.symbol,
                        side=side,
                        volume=abs(contracts),
                        avg_entry_price=float(pos.get("entryPrice", 0) or 0),
                        unrealized_pnl=float(pos.get("unrealizedPnl", 0) or 0)
                    )
            
            return Position(exchange=self.name, symbol=self.symbol)
            
        except Exception as e:
            logger.error(f"{self.name} get_position error: {e}")
            return None
    
    async def get_balance(self) -> Dict[str, float]:
        """Get account balance."""
        if not self._exchange:
            return {}
        
        try:
            balance = await self._exchange.fetch_balance()
            
            # Get USDT balance for trading
            usdt = balance.get("USDT", {})
            
            return {
                "total": float(usdt.get("total", 0) or 0),
                "free": float(usdt.get("free", 0) or 0),
                "used": float(usdt.get("used", 0) or 0),
                "currency": "USDT"
            }
            
        except Exception as e:
            logger.error(f"{self.name} get_balance error: {e}")
            return {}
    
    @classmethod
    def get_supported_exchanges(cls) -> List[str]:
        """Get list of supported exchanges."""
        return list(EXCHANGE_CLASSES.keys())
    
    @classmethod
    def create_from_config(cls, config: "ExchangeConfig", symbol: str) -> "CCXTGateway":
        """Create gateway from ExchangeConfig."""
        return cls(
            exchange_name=config.name,
            symbol=symbol,
            api_key=config.api_key,
            api_secret=config.api_secret,
            password=config.password,
            sandbox=config.sandbox,
            options=config.options
        )
