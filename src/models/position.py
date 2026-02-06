"""
Position model.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any


class PositionSide(Enum):
    """Position side."""
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


@dataclass
class Position:
    """Represents a trading position."""
    
    exchange: str
    symbol: str
    side: PositionSide = PositionSide.FLAT
    volume: float = 0.0
    avg_entry_price: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    
    # Position details
    level: int = 0  # Arbitrage level (1-n)
    entry_time: Optional[datetime] = None
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_flat(self) -> bool:
        """Check if position is flat (no exposure)."""
        return self.volume == 0 or self.side == PositionSide.FLAT
    
    @property
    def market_value(self) -> float:
        """Calculate position market value at entry."""
        return self.volume * self.avg_entry_price
    
    def update_from_fill(self, side: PositionSide, volume: float, price: float):
        """Update position from order fill."""
        if self.is_flat:
            # Opening new position
            self.side = side
            self.volume = volume
            self.avg_entry_price = price
            self.entry_time = datetime.now()
        elif self.side == side:
            # Adding to position
            total_value = (self.volume * self.avg_entry_price) + (volume * price)
            self.volume += volume
            self.avg_entry_price = total_value / self.volume if self.volume > 0 else 0
        else:
            # Reducing position
            if volume >= self.volume:
                # Closing or reversing
                remaining = volume - self.volume
                if remaining > 0:
                    # Reverse position
                    self.side = side
                    self.volume = remaining
                    self.avg_entry_price = price
                    self.entry_time = datetime.now()
                else:
                    # Close position
                    self.side = PositionSide.FLAT
                    self.volume = 0
                    self.avg_entry_price = 0
            else:
                # Partial close
                self.volume -= volume
    
    def calculate_pnl(self, current_price: float) -> float:
        """Calculate unrealized PnL at current price."""
        if self.is_flat:
            return 0.0
        
        price_diff = current_price - self.avg_entry_price
        if self.side == PositionSide.SHORT:
            price_diff = -price_diff
        
        return price_diff * self.volume
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "side": self.side.value,
            "volume": self.volume,
            "avg_entry_price": self.avg_entry_price,
            "unrealized_pnl": self.unrealized_pnl,
            "realized_pnl": self.realized_pnl,
            "level": self.level,
            "entry_time": self.entry_time.isoformat() if self.entry_time else None,
        }
