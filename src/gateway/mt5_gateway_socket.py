"""
MT5 Gateway implementation using JSON-RPC over TCP.
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from .base import BaseGateway, GatewayStatus
from ..models import Tick, Order, OrderSide, OrderStatus, Position, PositionSide
from ..config.logging_config import get_logger

logger = get_logger("gateway.mt5")


class MT5Gateway(BaseGateway):
    """
    MetaTrader 5 Gateway using JSON-RPC protocol.
    
    Connects to MT5 EA via TCP socket for real-time quotes and trading.
    """
    
    def __init__(
        self,
        symbol: str = "XAUUSD",
        host: str = "127.0.0.1",
        port: int = 18812,
        timeout: float = 5.0
    ):
        super().__init__(name="mt5", symbol=symbol)
        self.host = host
        self.port = port
        self.timeout = timeout
        
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._request_id = 0
        self._lock = asyncio.Lock()
        
        # Cache
        self._last_tick: Optional[Tick] = None
        self._position: Optional[Position] = None
    
    async def connect(self) -> bool:
        """Connect to MT5 EA server."""
        try:
            self._set_status(GatewayStatus.CONNECTING)
            logger.info(f"Connecting to MT5 at {self.host}:{self.port}")
            
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=self.timeout
            )
            
            self._set_status(GatewayStatus.CONNECTED)
            logger.info("MT5 connected successfully")
            return True
            
        except asyncio.TimeoutError:
            logger.error("MT5 connection timeout")
            self._set_status(GatewayStatus.ERROR)
            await self._emit_error("Connection timeout")
            return False
            
        except Exception as e:
            logger.error(f"MT5 connection failed: {e}")
            self._set_status(GatewayStatus.ERROR)
            await self._emit_error(str(e))
            return False
    
    async def disconnect(self):
        """Disconnect from MT5 EA server."""
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception as e:
                logger.warning(f"Error closing MT5 connection: {e}")
        
        self._reader = None
        self._writer = None
        self._set_status(GatewayStatus.DISCONNECTED)
        logger.info("MT5 disconnected")
    
    async def is_connected(self) -> bool:
        """Check if connected to MT5."""
        if not self._writer:
            return False
        try:
            # Try a simple request
            await self._rpc_call("ping", {}, timeout=2.0)
            return True
        except Exception:
            return False
    
    async def _rpc_call(
        self,
        method: str,
        params: Dict[str, Any],
        timeout: Optional[float] = None
    ) -> Any:
        """Make JSON-RPC call to MT5 EA."""
        if not self._writer or not self._reader:
            raise ConnectionError("Not connected to MT5")
        
        async with self._lock:
            self._request_id += 1
            request = {
                "jsonrpc": "2.0",
                "id": self._request_id,
                "method": method,
                "params": params
            }
            
            try:
                # Send request
                data = json.dumps(request).encode() + b"\n"
                self._writer.write(data)
                await self._writer.drain()
                
                # Read response
                response_data = await asyncio.wait_for(
                    self._reader.readline(),
                    timeout=timeout or self.timeout
                )
                
                if not response_data:
                    raise ConnectionError("Empty response from MT5")
                
                response = json.loads(response_data.decode())
                
                if "error" in response:
                    raise Exception(response["error"].get("message", "Unknown error"))
                
                return response.get("result")
                
            except asyncio.TimeoutError:
                raise TimeoutError(f"MT5 RPC timeout: {method}")
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON response: {e}")
    
    async def get_ticker(self) -> Optional[Tick]:
        """Get current MT5 ticker."""
        try:
            result = await self._rpc_call("quote", {"symbol": self.symbol})
            
            if result and result.get("bid") and result.get("ask"):
                tick = Tick(
                    exchange="mt5",
                    symbol=self.symbol,
                    bid=float(result["bid"]),
                    ask=float(result["ask"]),
                    timestamp=datetime.now(timezone.utc),
                    last=float(result.get("last", result["bid"]))
                )
                self._last_tick = tick
                return tick
            
            return None
            
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
            # Map timeframe to MT5 format
            tf_map = {
                "1m": "M1", "5m": "M5", "15m": "M15",
                "1h": "H1", "4h": "H4", "1d": "D1"
            }
            mt5_tf = tf_map.get(timeframe, "M1")
            
            result = await self._rpc_call("klines", {
                "symbol": self.symbol,
                "timeframe": mt5_tf,
                "limit": limit
            })
            
            if result and isinstance(result, list):
                return [{
                    "time": bar.get("time"),
                    "open": float(bar.get("open", 0)),
                    "high": float(bar.get("high", 0)),
                    "low": float(bar.get("low", 0)),
                    "close": float(bar.get("close", 0)),
                    "volume": int(bar.get("volume", 0))
                } for bar in result]
            
            return []
            
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
            simulated_price = self._last_tick.ask if side == OrderSide.BUY else self._last_tick.bid if self._last_tick else price or 0
            order.mark_filled(simulated_price)
            logger.info(f"[DRY_RUN] MT5 order simulated: {side.value} {volume} @ {simulated_price}")
            await self._emit_order(order)
            return order
        
        try:
            # MT5 action mapping
            action = "buy" if side == OrderSide.BUY else "sell"
            
            result = await self._rpc_call("trade", {
                "action": action,
                "symbol": self.symbol,
                "volume": volume,
                "price": price,
                "type": order_type
            })
            
            if result and result.get("success"):
                order.mark_submitted(str(result.get("ticket", "")))
                
                # If market order, assume filled immediately
                if order_type == "market":
                    fill_price = float(result.get("price", price or 0))
                    order.mark_filled(fill_price)
                    logger.info(f"MT5 order filled: {side.value} {volume} @ {fill_price}")
            else:
                error = result.get("error", "Unknown error") if result else "No response"
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
            result = await self._rpc_call("cancel", {"ticket": int(order_id)})
            return result and result.get("success", False)
        except Exception as e:
            logger.error(f"MT5 cancel_order error: {e}")
            return False
    
    async def get_position(self) -> Optional[Position]:
        """Get MT5 position for symbol."""
        try:
            result = await self._rpc_call("positions", {"symbol": self.symbol})
            
            if result and result.get("positions"):
                pos_data = result["positions"][0]  # Take first position
                
                side = PositionSide.FLAT
                if pos_data.get("type") == "buy":
                    side = PositionSide.LONG
                elif pos_data.get("type") == "sell":
                    side = PositionSide.SHORT
                
                position = Position(
                    exchange="mt5",
                    symbol=self.symbol,
                    side=side,
                    volume=float(pos_data.get("volume", 0)),
                    avg_entry_price=float(pos_data.get("price", 0)),
                    unrealized_pnl=float(pos_data.get("profit", 0))
                )
                self._position = position
                return position
            
            return Position(exchange="mt5", symbol=self.symbol)
            
        except Exception as e:
            logger.error(f"MT5 get_position error: {e}")
            return None
    
    async def get_balance(self) -> Dict[str, float]:
        """Get MT5 account balance."""
        try:
            result = await self._rpc_call("account", {})
            
            if result:
                return {
                    "balance": float(result.get("balance", 0)),
                    "equity": float(result.get("equity", 0)),
                    "margin": float(result.get("margin", 0)),
                    "free_margin": float(result.get("freeMargin", 0)),
                    "currency": result.get("currency", "USD")
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"MT5 get_balance error: {e}")
            return {}
    
    def is_market_open(self) -> bool:
        """
        Check if market is currently open.
        
        IC Markets XAUUSD trading hours:
        - Daily maintenance: Beijing 05:55 - 07:01 = UTC 21:55 - 23:01
        - Weekend: Friday UTC 22:00 close, Monday UTC 23:01 open
        """
        now = datetime.now(timezone.utc)
        weekday = now.weekday()  # Mon=0, Sun=6
        hour = now.hour
        minute = now.minute
        
        # Daily maintenance window: UTC 21:55 - 23:01 (Beijing 05:55 - 07:01)
        if hour == 21 and minute >= 55:
            return False
        if hour == 22:
            return False
        if hour == 23 and minute < 1:
            return False
        
        # Saturday: Closed all day
        if weekday == 5:
            return False
            
        # Sunday: Closed before 23:01 UTC (Beijing Monday 07:01)
        if weekday == 6:
            return False
        
        # Friday after 22:00 UTC: Closed for weekend
        if weekday == 4 and hour >= 22:
            return False
            
        return True
