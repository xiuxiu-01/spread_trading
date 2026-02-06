"""
Tick data model.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Tick:
    """Represents a price tick from an exchange."""
    
    exchange: str
    symbol: str
    bid: float
    ask: float
    timestamp: datetime
    last: Optional[float] = None
    volume: Optional[float] = None
    
    @property
    def mid(self) -> float:
        """Calculate mid price."""
        return (self.bid + self.ask) / 2
    
    @property
    def spread(self) -> float:
        """Calculate bid-ask spread."""
        return self.ask - self.bid
    
    @property
    def spread_pct(self) -> float:
        """Calculate spread as percentage of mid price."""
        mid = self.mid
        if mid == 0:
            return 0
        return (self.spread / mid) * 100
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "bid": self.bid,
            "ask": self.ask,
            "mid": self.mid,
            "last": self.last,
            "volume": self.volume,
            "timestamp": self.timestamp.isoformat(),
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Tick":
        """Create Tick from dictionary."""
        return cls(
            exchange=data["exchange"],
            symbol=data["symbol"],
            bid=data["bid"],
            ask=data["ask"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            last=data.get("last"),
            volume=data.get("volume"),
        )
