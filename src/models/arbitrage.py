"""
Arbitrage task model for tracking running arbitrage pairs.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List
import uuid


class ArbitrageStatus(Enum):
    """Arbitrage task status."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    ERROR = "error"
    STOPPED = "stopped"


@dataclass
class ArbitrageTask:
    """Represents an arbitrage trading task between two exchanges."""
    
    # Task identification
    task_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str = ""
    
    # Exchange pair
    exchange_a: str = ""  # e.g., "mt5"
    exchange_b: str = ""  # e.g., "okx"
    symbol_a: str = ""    # e.g., "XAUUSD"
    symbol_b: str = ""    # e.g., "XAU/USDT:USDT"
    
    # Status
    status: ArbitrageStatus = ArbitrageStatus.IDLE
    
    # Real-time data
    spread_sell: float = 0.0  # Sell A, Buy B spread
    spread_buy: float = 0.0   # Buy A, Sell B spread
    avg_spread_sell: float = 0.0
    avg_spread_buy: float = 0.0
    ema: float = 0.0
    
    # Position
    current_level: int = 0
    direction: str = ""  # "long" or "short" or ""
    
    # Statistics
    total_trades: int = 0
    total_pnl: float = 0.0
    win_trades: int = 0
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    last_trade_at: Optional[datetime] = None
    last_tick_at: Optional[datetime] = None
    
    # Error tracking
    last_error: Optional[str] = None
    error_count: int = 0
    
    # Configuration snapshot
    config: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.name:
            self.name = f"{self.exchange_a}-{self.exchange_b}"
    
    @property
    def is_active(self) -> bool:
        """Check if task is actively running."""
        return self.status == ArbitrageStatus.RUNNING
    
    @property
    def has_position(self) -> bool:
        """Check if task has open position."""
        return self.current_level > 0
    
    @property
    def win_rate(self) -> float:
        """Calculate win rate percentage."""
        if self.total_trades == 0:
            return 0.0
        return (self.win_trades / self.total_trades) * 100
    
    @property
    def uptime_seconds(self) -> float:
        """Calculate uptime in seconds."""
        if not self.started_at:
            return 0.0
        return (datetime.now() - self.started_at).total_seconds()
    
    def start(self):
        """Start the arbitrage task."""
        self.status = ArbitrageStatus.RUNNING
        self.started_at = datetime.now()
        self.error_count = 0
        self.last_error = None
    
    def stop(self):
        """Stop the arbitrage task."""
        self.status = ArbitrageStatus.STOPPED
    
    def pause(self):
        """Pause the arbitrage task."""
        self.status = ArbitrageStatus.PAUSED
    
    def resume(self):
        """Resume the arbitrage task."""
        self.status = ArbitrageStatus.RUNNING
    
    def set_error(self, error: str):
        """Set error status."""
        self.status = ArbitrageStatus.ERROR
        self.last_error = error
        self.error_count += 1
    
    def update_tick(self, spread_sell: float, spread_buy: float, 
                    avg_spread_sell: float, avg_spread_buy: float, ema: float):
        """Update real-time tick data."""
        self.spread_sell = spread_sell
        self.spread_buy = spread_buy
        self.avg_spread_sell = avg_spread_sell
        self.avg_spread_buy = avg_spread_buy
        self.ema = ema
        self.last_tick_at = datetime.now()
    
    def record_trade(self, pnl: float, is_win: bool):
        """Record a completed trade."""
        self.total_trades += 1
        self.total_pnl += pnl
        if is_win:
            self.win_trades += 1
        self.last_trade_at = datetime.now()
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        # Get symbol from config first, then try to extract from task_id
        symbol = self.config.get("symbol", "")
        if not symbol:
            # Try to extract from task_id (e.g., "mt5-okx-xauusd-abc123")
            parts = self.task_id.split("-")
            if len(parts) >= 3:
                symbol = parts[2].upper()
            else:
                symbol = self.symbol_a
        
        return {
            "task_id": self.task_id,
            "name": self.name,
            "exchange_a": self.exchange_a,
            "exchange_b": self.exchange_b,
            "symbol": symbol,
            "symbol_a": self.symbol_a,
            "symbol_b": self.symbol_b,
            "status": self.status.value,
            "spread_sell": round(self.spread_sell, 2),
            "spread_buy": round(self.spread_buy, 2),
            "avg_spread_sell": round(self.avg_spread_sell, 2),
            "avg_spread_buy": round(self.avg_spread_buy, 2),
            "current_spread": round(self.avg_spread_sell, 2),  # Alias for frontend
            "ema": round(self.ema, 2),
            "current_level": self.current_level,
            "direction": self.direction,
            "total_trades": self.total_trades,
            "total_pnl": round(self.total_pnl, 2),
            "win_rate": round(self.win_rate, 1),
            "uptime": self.uptime_seconds,
            "last_error": self.last_error,
            "error_count": self.error_count,
            "config": self.config,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "last_trade_at": self.last_trade_at.isoformat() if self.last_trade_at else None,
            "last_tick_at": self.last_tick_at.isoformat() if self.last_tick_at else None,
        }
    
    def to_summary(self) -> dict:
        """Get a summary for dashboard display."""
        return {
            "task_id": self.task_id,
            "name": self.name,
            "status": self.status.value,
            "pair": f"{self.exchange_a}/{self.exchange_b}",
            "symbols": f"{self.symbol_a} ↔ {self.symbol_b}",
            "spread": f"{self.avg_spread_sell:.2f} / {self.avg_spread_buy:.2f}",
            "position": f"L{self.current_level} {self.direction}" if self.has_position else "Flat",
            "pnl": f"{self.total_pnl:+.2f}",
            "trades": self.total_trades,
        }
