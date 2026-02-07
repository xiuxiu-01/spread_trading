"""
K-Line data aggregator for spread calculation and persistence.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from collections import deque

from ..config.logging_config import get_logger

logger = get_logger("core.aggregator")


class KLineAggregator:
    """
    Aggregates K-line data from multiple sources and calculates spreads.
    
    Handles:
    - Historical data loading
    - Real-time data aggregation
    - Spread calculation
    - EMA calculation
    - Data persistence
    """
    
    def __init__(
        self,
        data_dir: Path,
        ema_period: int = 20,
        max_bars: int = 5000,
        filename: str = "history.jsonl"
    ):
        """
        Initialize aggregator.
        
        Args:
            data_dir: Directory for data persistence
            ema_period: EMA calculation period
            max_bars: Maximum bars to keep in memory
            filename: Name of the history file
        """
        self.data_dir = data_dir
        self.ema_period = ema_period
        self.max_bars = max_bars
        
        # Data storage
        self.times: List[str] = []
        self.mt5_bars: List[Dict[str, float]] = []
        self.okx_bars: List[Dict[str, float]] = []
        self.spreads: List[float] = []
        self.spread_sell: List[float] = []  # MT5 bid - OKX ask
        self.spread_buy: List[float] = []   # MT5 ask - OKX bid
        self.ema: List[float] = []
        
        # Real-time spread buffer (for rolling average)
        self.spread_buffer: deque = deque(maxlen=50)  # ~5 seconds at 10 ticks/sec
        
        # File paths
        self.history_file = data_dir / filename
        self.daily_logs_dir = data_dir / "daily_logs"
        self.daily_logs_dir.mkdir(parents=True, exist_ok=True)
    
    def load_history(self, days: int = 7) -> int:
        """
        Load historical data from files.
        
        Args:
            days: Number of days to load
        
        Returns:
            Number of bars loaded
        """
        self._clear()
        loaded = 0
        
        try:
            # Load from main history file
            if self.history_file.exists():
                with open(self.history_file, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            data = json.loads(line.strip())
                            self._add_bar_from_dict(data)
                            loaded += 1
                        except (json.JSONDecodeError, KeyError):
                            continue
            
            # Trim to max bars
            self._trim_to_max()
            
            logger.info(f"Loaded {loaded} bars from history")
            return loaded
            
        except Exception as e:
            logger.error(f"Error loading history: {e}")
            return 0
    
    def _add_bar_from_dict(self, data: Dict[str, Any]):
        """Add bar data from dictionary."""
        self.times.append(data.get("ts", data.get("time", "")))
        
        # MT5 data
        mt5 = data.get("mt5", {})
        if isinstance(mt5, dict):
            self.mt5_bars.append(mt5)
        else:
            self.mt5_bars.append({
                "open": data.get("mt5_close", 0),
                "high": data.get("mt5_close", 0),
                "low": data.get("mt5_close", 0),
                "close": data.get("mt5_close", 0)
            })
        
        # OKX data
        okx = data.get("okx", {})
        if isinstance(okx, dict):
            self.okx_bars.append(okx)
        else:
            self.okx_bars.append({
                "open": data.get("okx_close", 0),
                "high": data.get("okx_close", 0),
                "low": data.get("okx_close", 0),
                "close": data.get("okx_close", 0)
            })
        
        # Spreads
        spread = data.get("spread", 0)
        self.spreads.append(spread)
        self.spread_sell.append(data.get("spread_sell", spread))
        self.spread_buy.append(data.get("spread_buy", spread))
        
        # EMA
        ema_val = data.get("ema")
        if ema_val is not None:
            self.ema.append(ema_val)
        elif self.ema:
            # Calculate EMA if not provided
            k = 2.0 / (self.ema_period + 1)
            new_ema = spread * k + self.ema[-1] * (1 - k)
            self.ema.append(new_ema)
        else:
            self.ema.append(spread)
    
    def _clear(self):
        """Clear all data."""
        self.times.clear()
        self.mt5_bars.clear()
        self.okx_bars.clear()
        self.spreads.clear()
        self.spread_sell.clear()
        self.spread_buy.clear()
        self.ema.clear()
    
    def _trim_to_max(self):
        """Trim data to max_bars."""
        if len(self.times) > self.max_bars:
            excess = len(self.times) - self.max_bars
            self.times = self.times[excess:]
            self.mt5_bars = self.mt5_bars[excess:]
            self.okx_bars = self.okx_bars[excess:]
            self.spreads = self.spreads[excess:]
            self.spread_sell = self.spread_sell[excess:]
            self.spread_buy = self.spread_buy[excess:]
            self.ema = self.ema[excess:]
    
    def add_bar(
        self,
        timestamp: str,
        mt5_bar: Dict[str, float],
        okx_bar: Dict[str, float],
        spread_sell: float,
        spread_buy: float
    ) -> Dict[str, Any]:
        """
        Add new bar and calculate indicators.
        
        Args:
            timestamp: ISO format timestamp
            mt5_bar: MT5 OHLC data
            okx_bar: OKX OHLC data
            spread_sell: Sell spread (MT5 bid - OKX ask)
            spread_buy: Buy spread (MT5 ask - OKX bid)
        
        Returns:
            Complete bar data with calculations
        """
        # Calculate mid spread
        spread = (mt5_bar.get("close", 0) - okx_bar.get("close", 0))
        
        # Calculate EMA
        if self.ema:
            k = 2.0 / (self.ema_period + 1)
            new_ema = spread * k + self.ema[-1] * (1 - k)
        else:
            new_ema = spread
        
        # Store data
        self.times.append(timestamp)
        self.mt5_bars.append(mt5_bar)
        self.okx_bars.append(okx_bar)
        self.spreads.append(spread)
        self.spread_sell.append(spread_sell)
        self.spread_buy.append(spread_buy)
        self.ema.append(new_ema)
        
        # Trim if needed
        self._trim_to_max()
        
        bar = {
            "ts": timestamp,
            "mt5": mt5_bar,
            "okx": okx_bar,
            "spread": spread,
            "spread_sell": spread_sell,
            "spread_buy": spread_buy,
            "ema": new_ema
        }
        
        # Save to file
        self.save_bar(bar)
        
        return bar
    
    def save_bar(self, bar: Dict[str, Any]):
        """Save bar to history file."""
        try:
            with open(self.history_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(bar) + "\n")
        except Exception as e:
            logger.error(f"Error saving bar: {e}")
    
    def save_daily_data(self, data: Dict[str, Any]):
        """Save data to daily log file."""
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            daily_file = self.daily_logs_dir / f"spread_data_{today}.jsonl"
            
            with open(daily_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(data) + "\n")
                
        except Exception as e:
            logger.error(f"Error saving daily data: {e}")
    
    def add_tick_spread(self, spread_sell: float, spread_buy: float, spread_mid: float):
        """Add tick spread to rolling buffer."""
        self.spread_buffer.append((
            datetime.now().timestamp(),
            spread_sell,
            spread_buy,
            spread_mid
        ))
    
    def get_rolling_average(self, window_seconds: float = 5.0) -> tuple:
        """
        Get rolling average of spreads.
        
        Args:
            window_seconds: Time window for average
        
        Returns:
            Tuple of (avg_spread_sell, avg_spread_buy, avg_spread_mid)
        """
        if not self.spread_buffer:
            return 0, 0, 0
        
        now = datetime.now().timestamp()
        cutoff = now - window_seconds
        
        # Filter recent spreads
        recent = [(ss, sb, sm) for ts, ss, sb, sm in self.spread_buffer if ts >= cutoff]
        
        if not recent:
            # Use last value if no recent data
            _, ss, sb, sm = self.spread_buffer[-1]
            return ss, sb, sm
        
        n = len(recent)
        avg_sell = sum(ss for ss, _, _ in recent) / n
        avg_buy = sum(sb for _, sb, _ in recent) / n
        avg_mid = sum(sm for _, _, sm in recent) / n
        
        return avg_sell, avg_buy, avg_mid
    
    def get_history_payload(self) -> Dict[str, Any]:
        """
        Get history data for WebSocket transmission.
        
        Returns:
            Dictionary with all historical data formatted for frontend
        """
        n = len(self.times)
        
        return {
            "mt5": [
                {
                    "time": self._iso_to_timestamp(self.times[i]),
                    **self.mt5_bars[i]
                }
                for i in range(n)
            ],
            "okx": [
                {
                    "time": self._iso_to_timestamp(self.times[i]),
                    **self.okx_bars[i]
                }
                for i in range(n)
            ],
            "spread": [
                {"time": self._iso_to_timestamp(self.times[i]), "value": self.spreads[i]}
                for i in range(n)
            ],
            "spread_sell": [
                {"time": self._iso_to_timestamp(self.times[i]), "value": self.spread_sell[i]}
                for i in range(n)
            ],
            "spread_buy": [
                {"time": self._iso_to_timestamp(self.times[i]), "value": self.spread_buy[i]}
                for i in range(n)
            ],
            "ema": [
                {"time": self._iso_to_timestamp(self.times[i]), "value": self.ema[i]}
                for i in range(n)
            ]
        }
    
    def _iso_to_timestamp(self, iso_str: str) -> int:
        """Convert ISO timestamp to Unix timestamp."""
        try:
            dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except Exception:
            return 0
    
    @property
    def last_ema(self) -> float:
        """Get last EMA value."""
        return self.ema[-1] if self.ema else 0
    
    @property
    def bar_count(self) -> int:
        """Get number of bars."""
        return len(self.times)
