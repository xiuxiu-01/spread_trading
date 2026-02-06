"""
Base gateway interface for all exchange connectors.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, Callable, Awaitable, List

from ..models import Tick, Order, OrderSide, Position


class GatewayStatus(Enum):
    """Gateway connection status."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"


@dataclass
class GatewayInfo:
    """Gateway information."""
    name: str
    exchange: str
    status: GatewayStatus = GatewayStatus.DISCONNECTED
    connected_at: Optional[datetime] = None
    last_tick_at: Optional[datetime] = None
    tick_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None


class BaseGateway(ABC):
    """
    Abstract base class for exchange gateways.
    
    All exchange connectors should inherit from this class and implement
    the required abstract methods.
    """
    
    def __init__(self, name: str, symbol: str):
        """
        Initialize gateway.
        
        Args:
            name: Gateway name (e.g., "okx", "mt5")
            symbol: Trading symbol (e.g., "XAU/USDT:USDT", "XAUUSD")
        """
        self.name = name
        self.symbol = symbol
        self.status = GatewayStatus.DISCONNECTED
        self.info = GatewayInfo(name=name, exchange=name)
        
        # Callbacks
        self._on_tick: Optional[Callable[[Tick], Awaitable[None]]] = None
        self._on_order: Optional[Callable[[Order], Awaitable[None]]] = None
        self._on_position: Optional[Callable[[Position], Awaitable[None]]] = None
        self._on_error: Optional[Callable[[str], Awaitable[None]]] = None
    
    # --- Connection Management ---
    
    @abstractmethod
    async def connect(self) -> bool:
        """
        Establish connection to the exchange.
        
        Returns:
            True if connection successful, False otherwise.
        """
        pass
    
    @abstractmethod
    async def disconnect(self):
        """Disconnect from the exchange."""
        pass
    
    @abstractmethod
    async def is_connected(self) -> bool:
        """Check if gateway is connected."""
        pass
    
    # --- Market Data ---
    
    @abstractmethod
    async def get_ticker(self) -> Optional[Tick]:
        """
        Get current ticker (bid/ask/last).
        
        Returns:
            Tick object with current prices, or None if unavailable.
        """
        pass
    
    @abstractmethod
    async def subscribe_ticker(self):
        """Subscribe to real-time ticker updates."""
        pass
    
    @abstractmethod
    async def get_klines(
        self, 
        timeframe: str = "1m", 
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get historical kline/candlestick data.
        
        Args:
            timeframe: Timeframe (e.g., "1m", "5m", "1h")
            limit: Number of candles to fetch
        
        Returns:
            List of OHLCV dictionaries.
        """
        pass
    
    # --- Trading ---
    
    @abstractmethod
    async def place_order(
        self,
        side: OrderSide,
        volume: float,
        price: Optional[float] = None,
        order_type: str = "market",
        dry_run: bool = False
    ) -> Order:
        """
        Place a trading order.
        
        Args:
            side: Order side (BUY or SELL)
            volume: Order volume
            price: Limit price (None for market orders)
            order_type: Order type ("market" or "limit")
            dry_run: If True, simulate order without sending to exchange
        
        Returns:
            Order object with execution details.
        """
        pass
    
    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an open order.
        
        Args:
            order_id: Order ID to cancel
        
        Returns:
            True if cancellation successful.
        """
        pass
    
    @abstractmethod
    async def get_position(self) -> Optional[Position]:
        """
        Get current position for the symbol.
        
        Returns:
            Position object, or None if no position.
        """
        pass
    
    @abstractmethod
    async def get_balance(self) -> Dict[str, float]:
        """
        Get account balance.
        
        Returns:
            Dictionary with balance information.
        """
        pass
    
    # --- Callbacks ---
    
    def on_tick(self, callback: Callable[[Tick], Awaitable[None]]):
        """Register tick callback."""
        self._on_tick = callback
    
    def on_order(self, callback: Callable[[Order], Awaitable[None]]):
        """Register order update callback."""
        self._on_order = callback
    
    def on_position(self, callback: Callable[[Position], Awaitable[None]]):
        """Register position update callback."""
        self._on_position = callback
    
    def on_error(self, callback: Callable[[str], Awaitable[None]]):
        """Register error callback."""
        self._on_error = callback
    
    # --- Internal helpers ---
    
    async def _emit_tick(self, tick: Tick):
        """Emit tick to callback."""
        self.info.last_tick_at = datetime.now()
        self.info.tick_count += 1
        if self._on_tick:
            await self._on_tick(tick)
    
    async def _emit_order(self, order: Order):
        """Emit order to callback."""
        if self._on_order:
            await self._on_order(order)
    
    async def _emit_position(self, position: Position):
        """Emit position to callback."""
        if self._on_position:
            await self._on_position(position)
    
    async def _emit_error(self, error: str):
        """Emit error to callback."""
        self.info.error_count += 1
        self.info.last_error = error
        if self._on_error:
            await self._on_error(error)
    
    def _set_status(self, status: GatewayStatus):
        """Update gateway status."""
        self.status = status
        self.info.status = status
        if status == GatewayStatus.CONNECTED:
            self.info.connected_at = datetime.now()
    
    def get_info(self) -> dict:
        """Get gateway info as dictionary."""
        return {
            "name": self.info.name,
            "exchange": self.info.exchange,
            "symbol": self.symbol,
            "status": self.status.value,
            "connected_at": self.info.connected_at.isoformat() if self.info.connected_at else None,
            "last_tick_at": self.info.last_tick_at.isoformat() if self.info.last_tick_at else None,
            "tick_count": self.info.tick_count,
            "error_count": self.info.error_count,
            "last_error": self.info.last_error,
        }
