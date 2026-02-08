"""
Configuration settings for the trading system.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class ExchangeConfig:
    """Exchange-specific configuration."""
    name: str
    api_key: str = ""
    api_secret: str = ""
    password: str = ""  # For exchanges that require passphrase
    sandbox: bool = False
    enabled: bool = True
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MT5Config:
    """MT5 configuration."""
    host: str = "127.0.0.1"
    port: int = 18812
    symbol: str = "XAUUSD"
    enabled: bool = True
    utc_offset: int = 0  # Hours to subtract from MT5 server time to get UTC


@dataclass
class StrategyConfig:
    """Strategy parameters."""
    ema_period: int = 20
    first_spread: float = 3.0
    next_spread: float = 1.0
    take_profit: float = 0.5
    max_pos: int = 3
    trade_volume: float = 0.01
    auto_trade: bool = False
    dry_run: bool = True  # If True, orders will NOT be sent to exchange


@dataclass
class ServerConfig:
    """WebSocket server configuration."""
    host: str = "0.0.0.0"
    port: int = 8766  # Changed from 8765 to avoid conflict
    cors_origins: list = field(default_factory=lambda: ["*"])


@dataclass
class Settings:
    """Main settings container."""
    
    # Base paths
    base_dir: Path = field(default_factory=lambda: Path(__file__).parent.parent.parent)
    data_dir: Path = field(default_factory=lambda: Path(__file__).parent.parent.parent / "data")
    
    # Server
    server: ServerConfig = field(default_factory=ServerConfig)
    
    # MT5
    mt5: MT5Config = field(default_factory=MT5Config)
    
    # Strategy
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    
    # Exchanges (loaded from environment or config file)
    exchanges: Dict[str, ExchangeConfig] = field(default_factory=dict)
    
    def __post_init__(self):
        """Load configuration from environment variables."""
        self._load_from_env()
        self._ensure_dirs()
    
    def _load_from_env(self):
        """Load settings from environment variables."""
        # MT5
        self.mt5.host = os.getenv("MT5_HOST", self.mt5.host)
        self.mt5.port = int(os.getenv("MT5_PORT", self.mt5.port))
        self.mt5.symbol = os.getenv("MT5_SYMBOL", self.mt5.symbol)
        self.mt5.utc_offset = int(os.getenv("MT5_UTC_OFFSET_HOURS", "0"))
        
        # Server
        self.server.host = os.getenv("WS_HOST", self.server.host)
        self.server.port = int(os.getenv("WS_PORT", self.server.port))
        
        # Strategy
        self.strategy.ema_period = int(os.getenv("EMA_PERIOD", self.strategy.ema_period))
        self.strategy.first_spread = float(os.getenv("FIRST_SPREAD", self.strategy.first_spread))
        self.strategy.next_spread = float(os.getenv("NEXT_SPREAD", self.strategy.next_spread))
        self.strategy.take_profit = float(os.getenv("TAKE_PROFIT", self.strategy.take_profit))
        self.strategy.max_pos = int(os.getenv("MAX_POS", self.strategy.max_pos))
        
        # Load exchange configurations
        self._load_exchanges()
    
    def _load_exchanges(self):
        """Load exchange configurations from environment."""
        exchange_names = ["okx", "gate", "bitget", "bitmart", "binance", "lbank", "bybit"]
        
        for name in exchange_names:
            upper_name = name.upper()
            api_key = os.getenv(f"{upper_name}_API_KEY", "")
            api_secret = os.getenv(f"{upper_name}_API_SECRET", "")
            password = os.getenv(f"{upper_name}_PASSWORD", "") or \
                      os.getenv(f"{upper_name}_PASSPHRASE", "") or \
                      os.getenv(f"{upper_name}_API_PASSPHRASE", "")
            
            sandbox = os.getenv(f"{upper_name}_SANDBOX", "false").lower() == "true"
            enabled = os.getenv(f"{upper_name}_ENABLED", "false").lower() == "true"
            
            # Special handling for OKX (default enabled if credentials exist)
            if name == "okx" and api_key:
                enabled = os.getenv(f"{upper_name}_ENABLED", "true").lower() == "true"
            
            self.exchanges[name] = ExchangeConfig(
                name=name,
                api_key=api_key,
                api_secret=api_secret,
                password=password,
                sandbox=sandbox,
                enabled=enabled
            )
    
    def _ensure_dirs(self):
        """Ensure required directories exist."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / "daily_logs").mkdir(parents=True, exist_ok=True)
    
    def get_exchange(self, name: str) -> Optional[ExchangeConfig]:
        """Get exchange configuration by name."""
        return self.exchanges.get(name.lower())
    
    def get_enabled_exchanges(self) -> Dict[str, ExchangeConfig]:
        """Get all enabled exchanges."""
        return {k: v for k, v in self.exchanges.items() if v.enabled}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert settings to dictionary for JSON serialization."""
        return {
            "MT5_SYMBOL": self.mt5.symbol,
            "emaPeriod": self.strategy.ema_period,
            "firstSpread": self.strategy.first_spread,
            "nextSpread": self.strategy.next_spread,
            "takeProfit": self.strategy.take_profit,
            "maxPos": self.strategy.max_pos,
            "tradeVolume": self.strategy.trade_volume,
            "autoTrade": self.strategy.auto_trade,
            "exchanges": list(self.get_enabled_exchanges().keys()),
        }


# Global settings instance
settings = Settings()
