"""
Trading strategy implementations.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any, List

from ..config.logging_config import get_logger

logger = get_logger("core.strategy")


class SignalType(Enum):
    """Trading signal type."""
    NONE = "none"
    LONG = "long"    # Buy MT5, Sell OKX
    SHORT = "short"  # Sell MT5, Buy OKX


@dataclass
class Signal:
    """Trading signal."""
    type: SignalType
    level: int = 0
    spread: float = 0
    ema: float = 0
    threshold: float = 0
    reason: str = ""
    
    @property
    def is_valid(self) -> bool:
        return self.type != SignalType.NONE


class BaseStrategy(ABC):
    """Base class for trading strategies."""
    
    def __init__(self, params: Dict[str, Any]):
        self.params = params
    
    @abstractmethod
    def check_signal(
        self,
        spread_sell: float,
        spread_buy: float,
        ema: float,
        current_level: int,
        current_direction: str
    ) -> Signal:
        """
        Check for trading signal.
        
        Args:
            spread_sell: Current sell spread (MT5 bid - OKX ask)
            spread_buy: Current buy spread (MT5 ask - OKX bid)
            ema: Current EMA value
            current_level: Current position level (0 = flat)
            current_direction: Current direction ("long", "short", or "")
        
        Returns:
            Signal object
        """
        pass
    
    @abstractmethod
    def check_take_profit(
        self,
        spread_sell: float,
        spread_buy: float,
        ema: float,
        current_level: int,
        current_direction: str
    ) -> bool:
        """
        Check if take profit condition is met.
        
        Returns:
            True if should close position
        """
        pass


class EMASpreadStrategy(BaseStrategy):
    """
    EMA-based spread trading strategy.
    
    Entry Logic:
    - Long (Buy MT5, Sell OKX): When spread_buy < EMA - threshold
    - Short (Sell MT5, Buy OKX): When spread_sell > EMA + threshold
    
    Exit Logic:
    - Close when spread crosses back to EMA ± take_profit
    """
    
    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.first_spread = float(params.get("firstSpread", 3.0))
        self.next_spread = float(params.get("nextSpread", 1.0))
        self.take_profit = float(params.get("takeProfit", 0.5))
        self.max_pos = int(params.get("maxPos", 3))
    
    def update_params(self, params: Dict[str, Any]):
        """Update strategy parameters."""
        if "firstSpread" in params:
            self.first_spread = float(params["firstSpread"])
        if "nextSpread" in params:
            self.next_spread = float(params["nextSpread"])
        if "takeProfit" in params:
            self.take_profit = float(params["takeProfit"])
        if "maxPos" in params:
            self.max_pos = int(params["maxPos"])
        self.params.update(params)
    
    def get_threshold(self, level: int) -> float:
        """Calculate threshold for given level."""
        return self.first_spread + (level - 1) * self.next_spread
    
    def check_signal(
        self,
        spread_sell: float,
        spread_buy: float,
        ema: float,
        current_level: int,
        current_direction: str
    ) -> Signal:
        """Check for entry or add-on signal."""
        
        # If already at max position, no new signals
        if current_level >= self.max_pos:
            return Signal(type=SignalType.NONE)
        
        next_level = current_level + 1
        threshold = self.get_threshold(next_level)
        
        # Check for short signal (sell MT5, buy OKX)
        # Triggered when spread_sell > ema + threshold
        short_trigger = ema + threshold
        if spread_sell > short_trigger:
            # Can only go short if flat or already short
            if current_direction in ("", "short"):
                return Signal(
                    type=SignalType.SHORT,
                    level=next_level,
                    spread=spread_sell,
                    ema=ema,
                    threshold=short_trigger,
                    reason=f"Spread {spread_sell:.2f} > Trigger {short_trigger:.2f} (EMA {ema:.2f} + {threshold:.2f})"
                )
        
        # Check for long signal (buy MT5, sell OKX)
        # Triggered when spread_buy < ema - threshold
        long_trigger = ema - threshold
        if spread_buy < long_trigger:
            # Can only go long if flat or already long
            if current_direction in ("", "long"):
                return Signal(
                    type=SignalType.LONG,
                    level=next_level,
                    spread=spread_buy,
                    ema=ema,
                    threshold=long_trigger,
                    reason=f"Spread {spread_buy:.2f} < Trigger {long_trigger:.2f} (EMA {ema:.2f} - {threshold:.2f})"
                )
        
        return Signal(type=SignalType.NONE)
    
    def check_take_profit(
        self,
        spread_sell: float,
        spread_buy: float,
        ema: float,
        current_level: int,
        current_direction: str
    ) -> bool:
        """Check if take profit condition is met."""
        
        if current_level == 0:
            return False
        
        tp_threshold = self.take_profit
        
        if current_direction == "short":
            # Close short when spread_sell drops below ema + take_profit
            # (spread is converging back to EMA)
            tp_level = ema + tp_threshold
            if spread_sell <= tp_level:
                logger.info(f"Take profit SHORT: spread {spread_sell:.2f} <= {tp_level:.2f}")
                return True
        
        elif current_direction == "long":
            # Close long when spread_buy rises above ema - take_profit
            # (spread is converging back to EMA)
            tp_level = ema - tp_threshold
            if spread_buy >= tp_level:
                logger.info(f"Take profit LONG: spread {spread_buy:.2f} >= {tp_level:.2f}")
                return True
        
        return False
    
    def get_bands(self, ema: float) -> List[Dict[str, Any]]:
        """Get threshold bands for visualization."""
        bands = []
        for lvl in range(1, self.max_pos + 1):
            threshold = self.get_threshold(lvl)
            bands.append({
                "level": lvl,
                "upper": ema + threshold,
                "lower": ema - threshold,
                "threshold": threshold
            })
        return bands
