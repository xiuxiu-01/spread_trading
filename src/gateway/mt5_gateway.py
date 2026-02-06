"""
MT5 Gateway implementation using MetaTrader5 Python library.

Directly connects to MT5 terminal using the official Python package.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor

from .base import BaseGateway, GatewayStatus
from ..models import Tick, Order, OrderSide, OrderStatus, Position, PositionSide
from ..config.logging_config import get_logger

logger = get_logger("gateway.mt5")

# Try to import MetaTrader5
try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    mt5 = None
    MT5_AVAILABLE = False


class MT5Gateway(BaseGateway):
    """
    MetaTrader 5 Gateway using official Python library.
    
    Directly connects to MT5 terminal for real-time quotes and trading.
    Requires MetaTrader5 package: pip install MetaTrader5
    """
    
    def __init__(
        self,
        symbol: str = "XAUUSD",
        host: str = "127.0.0.1",  # Kept for compatibility, not used
        port: int = 18812,        # Kept for compatibility, not used
        timeout: float = 5.0
    ):
        super().__init__(name="mt5", symbol=symbol)
        self.timeout = timeout
        
        # Thread pool for running MT5 calls in threads (MT5 lib is not async)
        self._executor = ThreadPoolExecutor(max_workers=2)
        
        # Cache
        self._last_tick: Optional[Tick] = None
        self._position: Optional[Position] = None
        self._initialized = False
    
    def _run_sync(self, func, *args, **kwargs):
        """Run a synchronous function in thread pool."""
        loop = asyncio.get_event_loop()
        return loop.run_in_executor(self._executor, lambda: func(*args, **kwargs))
    
    async def connect(self) -> bool:
        """Connect to MT5 terminal."""
        if not MT5_AVAILABLE:
            logger.error("MetaTrader5 library not installed. Run: pip install MetaTrader5")
            self._set_status(GatewayStatus.ERROR)
            await self._emit_error("MetaTrader5 library not installed")
            return False
        
        try:
            self._set_status(GatewayStatus.CONNECTING)
            logger.info(f"Connecting to MT5 terminal...")
            
            # Initialize MT5 in thread pool
            initialized = await self._run_sync(mt5.initialize)
            
            if not initialized:
                error = mt5.last_error()
                logger.error(f"MT5 initialization failed: {error}")
                self._set_status(GatewayStatus.ERROR)
                await self._emit_error(f"MT5 init failed: {error}")
                return False
            
            # Enable symbol
            await self._run_sync(mt5.symbol_select, self.symbol, True)
            
            # Verify we can get a tick
            tick = await self._run_sync(mt5.symbol_info_tick, self.symbol)
            if tick is None:
                logger.warning(f"Could not get tick for {self.symbol}, but connection OK")
            
            self._initialized = True
            self._set_status(GatewayStatus.CONNECTED)
            logger.info(f"MT5 connected successfully (symbol: {self.symbol})")
            
            # Log account info
            account = await self._run_sync(mt5.account_info)
            if account:
                logger.info(f"MT5 Account: {account.login} | Balance: {account.balance} {account.currency}")
            
            return True
            
        except Exception as e:
            logger.error(f"MT5 connection failed: {e}")
            self._set_status(GatewayStatus.ERROR)
            await self._emit_error(str(e))
            return False
    
    async def disconnect(self):
        """Disconnect from MT5 terminal."""
        try:
            if self._initialized:
                await self._run_sync(mt5.shutdown)
                self._initialized = False
        except Exception as e:
            logger.warning(f"Error during MT5 shutdown: {e}")
        
        self._set_status(GatewayStatus.DISCONNECTED)
        logger.info("MT5 disconnected")
    
    async def is_connected(self) -> bool:
        """Check if connected to MT5."""
        if not self._initialized:
            return False
        try:
            # Try to get terminal info
            info = await self._run_sync(mt5.terminal_info)
            return info is not None
        except Exception:
            return False
    
    async def get_ticker(self) -> Optional[Tick]:
        """Get current MT5 ticker."""
        try:
            tick_data = await self._run_sync(mt5.symbol_info_tick, self.symbol)
            
            if tick_data is None:
                return None
            
            tick = Tick(
                exchange="mt5",
                symbol=self.symbol,
                bid=float(tick_data.bid),
                ask=float(tick_data.ask),
                timestamp=datetime.now(timezone.utc),
                last=float(tick_data.last) if tick_data.last > 0 else (tick_data.bid + tick_data.ask) / 2.0
            )
            self._last_tick = tick
            return tick
            
        except Exception as e:
            logger.error(f"MT5 get_ticker error: {e}")
            await self._emit_error(str(e))
            return None
    
    async def subscribe_ticker(self):
        """
        Subscribe to ticker updates.
        
        Note: MT5 gateway uses polling instead of WebSocket subscription.
        Call get_ticker() periodically for updates.
        """
        logger.info("MT5 uses polling for ticker updates")
    
    async def get_klines(
        self,
        timeframe: str = "1m",
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get MT5 historical klines."""
        try:
            # Map timeframe to MT5 constant
            tf_map = {
                "1m": mt5.TIMEFRAME_M1,
                "5m": mt5.TIMEFRAME_M5,
                "15m": mt5.TIMEFRAME_M15,
                "30m": mt5.TIMEFRAME_M30,
                "1h": mt5.TIMEFRAME_H1,
                "4h": mt5.TIMEFRAME_H4,
                "1d": mt5.TIMEFRAME_D1
            }
            mt5_tf = tf_map.get(timeframe, mt5.TIMEFRAME_M1)
            
            rates = await self._run_sync(
                mt5.copy_rates_from_pos,
                self.symbol,
                mt5_tf,
                0,
                limit
            )
            
            if rates is None or len(rates) == 0:
                return []
            
            return [{
                "time": int(bar[0]),
                "open": float(bar[1]),
                "high": float(bar[2]),
                "low": float(bar[3]),
                "close": float(bar[4]),
                "volume": int(bar[5])
            } for bar in rates]
            
        except Exception as e:
            logger.error(f"MT5 get_klines error: {e}")
            return []
    
    async def place_order(
        self,
        side: OrderSide,
        volume: float,
        price: Optional[float] = None,
        order_type: str = "market",
        dry_run: bool = False
    ) -> Order:
        """Place order on MT5."""
        order = Order(
            exchange="mt5",
            symbol=self.symbol,
            side=side,
            volume=volume,
            price=price,
            order_type=order_type
        )
        
        # Dry run mode - simulate order without sending
        if dry_run:
            order.mark_submitted(f"DRY_RUN_{id(order)}")
            tick = await self.get_ticker()
            simulated_price = tick.ask if side == OrderSide.BUY else tick.bid if tick else price or 0
            order.mark_filled(simulated_price)
            logger.info(f"[DRY_RUN] MT5 order simulated: {side.value} {volume} @ {simulated_price}")
            await self._emit_order(order)
            return order
        
        try:
            # Get current tick for price
            tick_data = await self._run_sync(mt5.symbol_info_tick, self.symbol)
            if tick_data is None:
                order.mark_failed("Could not get tick data")
                await self._emit_order(order)
                return order
            
            # Determine order price
            if price is None:
                price = float(tick_data.ask) if side == OrderSide.BUY else float(tick_data.bid)
            
            # Build order request
            order_type_mt5 = mt5.ORDER_TYPE_BUY if side == OrderSide.BUY else mt5.ORDER_TYPE_SELL
            
            request = {
                'action': mt5.TRADE_ACTION_DEAL,
                'symbol': self.symbol,
                'volume': float(volume),
                'type': order_type_mt5,
                'price': price,
                'deviation': 20,
                'magic': 234000,
                'comment': 'spread_trading',
                'type_filling': mt5.ORDER_FILLING_IOC,
                'type_time': mt5.ORDER_TIME_GTC,
            }
            
            result = await self._run_sync(mt5.order_send, request)
            
            if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                order.mark_submitted(str(result.order))
                order.mark_filled(float(result.price))
                logger.info(f"MT5 order filled: {side.value} {volume} @ {result.price}")
            else:
                error = f"retcode={result.retcode if result else 'None'}"
                if result:
                    error += f", comment={result.comment}"
                order.mark_failed(error)
                logger.error(f"MT5 order failed: {error}")
            
            await self._emit_order(order)
            return order
            
        except Exception as e:
            order.mark_failed(str(e))
            logger.error(f"MT5 place_order error: {e}")
            await self._emit_order(order)
            return order
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel MT5 order."""
        try:
            request = {
                'action': mt5.TRADE_ACTION_REMOVE,
                'order': int(order_id),
            }
            result = await self._run_sync(mt5.order_send, request)
            return result and result.retcode == mt5.TRADE_RETCODE_DONE
        except Exception as e:
            logger.error(f"MT5 cancel_order error: {e}")
            return False
    
    async def get_position(self) -> Optional[Position]:
        """Get MT5 position for symbol."""
        try:
            positions = await self._run_sync(mt5.positions_get, symbol=self.symbol)
            
            if positions is None or len(positions) == 0:
                return Position(exchange="mt5", symbol=self.symbol)
            
            # Aggregate positions
            total_volume = 0.0
            total_profit = 0.0
            weighted_price = 0.0
            side = PositionSide.FLAT
            
            for pos in positions:
                if pos.type == mt5.POSITION_TYPE_BUY:
                    total_volume += pos.volume
                    weighted_price += pos.price_open * pos.volume
                    side = PositionSide.LONG
                else:
                    total_volume -= pos.volume
                    weighted_price += pos.price_open * pos.volume
                    side = PositionSide.SHORT
                total_profit += pos.profit
            
            if abs(total_volume) < 0.001:
                side = PositionSide.FLAT
            elif total_volume < 0:
                side = PositionSide.SHORT
                total_volume = abs(total_volume)
            
            avg_price = weighted_price / sum(p.volume for p in positions) if positions else 0
            
            position = Position(
                exchange="mt5",
                symbol=self.symbol,
                side=side,
                volume=abs(total_volume),
                avg_entry_price=avg_price,
                unrealized_pnl=total_profit
            )
            self._position = position
            return position
            
        except Exception as e:
            logger.error(f"MT5 get_position error: {e}")
            return None
    
    async def get_balance(self) -> Dict[str, float]:
        """Get MT5 account balance."""
        try:
            account = await self._run_sync(mt5.account_info)
            
            if account:
                return {
                    "balance": float(account.balance),
                    "equity": float(account.equity),
                    "margin": float(account.margin),
                    "free_margin": float(account.margin_free),
                    "currency": account.currency
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"MT5 get_balance error: {e}")
            return {}
    
    def is_market_open(self) -> bool:
        """
        Check if market is currently open.
        
        XAUUSD trading hours (approximate):
        - Opens: Sunday 22:00 UTC
        - Closes: Friday 21:00 UTC
        - Daily break: 21:00-22:00 UTC
        """
        now = datetime.now(timezone.utc)
        weekday = now.weekday()
        hour = now.hour
        
        # Saturday - closed
        if weekday == 5:
            return False
        
        # Sunday - opens at 22:00 UTC
        if weekday == 6:
            return hour >= 22
        
        # Friday - closes at 21:00 UTC
        if weekday == 4:
            return hour < 21
        
        # Mon-Thu: closed 21:00-22:00 for daily break
        if hour == 21:
            return False
        
        return True
