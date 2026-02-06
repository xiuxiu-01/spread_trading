"""
Order model.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any
import uuid


class OrderSide(Enum):
    """Order side."""
    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    """Order status."""
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    FAILED = "failed"


@dataclass
class Order:
    """Represents a trading order."""
    
    exchange: str
    symbol: str
    side: OrderSide
    volume: float
    price: Optional[float] = None  # None for market orders
    order_type: str = "market"
    
    # Order lifecycle
    order_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    exchange_order_id: Optional[str] = None
    status: OrderStatus = OrderStatus.PENDING
    
    # Execution details
    filled_volume: float = 0.0
    avg_price: Optional[float] = None
    commission: float = 0.0
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)
    submitted_at: Optional[datetime] = None
    filled_at: Optional[datetime] = None
    
    # Metadata
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_complete(self) -> bool:
        """Check if order is complete (filled, cancelled, or failed)."""
        return self.status in (
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.FAILED,
        )
    
    @property
    def remaining_volume(self) -> float:
        """Get remaining volume to fill."""
        return self.volume - self.filled_volume
    
    def mark_submitted(self, exchange_order_id: str):
        """Mark order as submitted."""
        self.status = OrderStatus.SUBMITTED
        self.exchange_order_id = exchange_order_id
        self.submitted_at = datetime.now()
    
    def mark_filled(self, avg_price: float, commission: float = 0.0):
        """Mark order as filled."""
        self.status = OrderStatus.FILLED
        self.filled_volume = self.volume
        self.avg_price = avg_price
        self.commission = commission
        self.filled_at = datetime.now()
    
    def mark_partial(self, filled_volume: float, avg_price: float):
        """Mark order as partially filled."""
        self.status = OrderStatus.PARTIAL
        self.filled_volume = filled_volume
        self.avg_price = avg_price
    
    def mark_failed(self, error: str):
        """Mark order as failed."""
        self.status = OrderStatus.FAILED
        self.error = error
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "order_id": self.order_id,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "side": self.side.value,
            "volume": self.volume,
            "price": self.price,
            "order_type": self.order_type,
            "status": self.status.value,
            "filled_volume": self.filled_volume,
            "avg_price": self.avg_price,
            "commission": self.commission,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
            "filled_at": self.filled_at.isoformat() if self.filled_at else None,
        }
