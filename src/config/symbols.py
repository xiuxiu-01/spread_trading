"""
Symbol configuration for multi-exchange arbitrage.

Defines trading pairs across different exchanges for each asset class.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class SymbolMapping:
    """Mapping of a symbol across different exchanges."""
    name: str  # Display name (e.g., "Gold", "Silver")
    code: str  # Internal code (e.g., "XAUUSD", "XAGUSD")
    
    # Symbol on each exchange
    mt5: str = ""
    okx: str = ""
    binance: str = ""
    bybit: str = ""
    gate: str = ""
    bitget: str = ""
    bitmart: str = ""
    lbank: str = ""
    
    # Contract specifications
    mt5_lot_size: float = 0.01  # Minimum lot size on MT5
    exchange_lot_size: float = 1.0  # Minimum lot size on crypto exchanges
    
    # Conversion factor (e.g., 1 MT5 lot = X exchange contracts)
    lot_ratio: float = 1.0
    
    # Default spread parameters
    default_first_spread: float = 3.0
    default_next_spread: float = 1.0
    default_take_profit: float = 0.5
    
    def get_symbol(self, exchange: str) -> Optional[str]:
        """Get symbol for a specific exchange."""
        return getattr(self, exchange.lower(), None)
    
    def is_supported(self, exchange: str) -> bool:
        """Check if this symbol is supported on the exchange."""
        symbol = self.get_symbol(exchange)
        return bool(symbol)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON."""
        return {
            "name": self.name,
            "code": self.code,
            "exchanges": {
                "mt5": self.mt5,
                "okx": self.okx,
                "binance": self.binance,
                "bybit": self.bybit,
                "gate": self.gate,
                "bitget": self.bitget,
                "bitmart": self.bitmart,
                "lbank": self.lbank,
            },
            "mt5_lot_size": self.mt5_lot_size,
            "exchange_lot_size": self.exchange_lot_size,
            "lot_ratio": self.lot_ratio,
            "default_params": {
                "firstSpread": self.default_first_spread,
                "nextSpread": self.default_next_spread,
                "takeProfit": self.default_take_profit,
            }
        }


# =============================================================================
# PRECIOUS METALS
# =============================================================================

XAUUSD = SymbolMapping(
    name="Gold (XAU/USD)",
    code="XAUUSD",
    mt5="XAUUSD",
    okx="XAU/USDT:USDT",      # OKX Gold perpetual swap
    binance="PAXG/USDT",       # Binance uses PAXG (Pax Gold token)
    bybit="XAUUSDT",           # Bybit Gold perpetual
    gate="PAXG_USDT",          # Gate.io PAXG
    bitget="XAUUSDT",          # Bitget Gold perpetual  
    bitmart="",                # BitMart - no gold contract
    lbank="",                  # LBank - no gold contract
    mt5_lot_size=0.01,
    exchange_lot_size=1,       # 1 contract = 1 oz
    lot_ratio=100,             # 0.01 MT5 lot = 1 exchange contract
    default_first_spread=3.0,
    default_next_spread=1.0,
    default_take_profit=0.5,
)

XAGUSD = SymbolMapping(
    name="Silver (XAG/USD)",
    code="XAGUSD",
    mt5="XAGUSD",
    okx="XAG/USDT:USDT",      # OKX Silver perpetual (if available)
    binance="",                # No silver on Binance spot/futures
    bybit="XAGUSDT",           # Bybit Silver perpetual
    gate="",                   # Gate.io - no silver
    bitget="XAGUSDT",          # Bitget Silver perpetual
    bitmart="",
    lbank="",
    mt5_lot_size=0.01,
    exchange_lot_size=1,
    lot_ratio=100,
    default_first_spread=0.15,  # Silver has smaller spreads
    default_next_spread=0.05,
    default_take_profit=0.03,
)


# =============================================================================
# FOREX MAJORS (if crypto exchanges have forex pairs)
# =============================================================================

EURUSD = SymbolMapping(
    name="EUR/USD",
    code="EURUSD",
    mt5="EURUSD",
    okx="",                    # Most crypto exchanges don't have forex
    binance="EUR/USDT",        # Binance has EUR/USDT
    bybit="",
    gate="EUR_USDT",
    bitget="",
    bitmart="",
    lbank="",
    mt5_lot_size=0.01,
    exchange_lot_size=1,
    lot_ratio=100000,          # Forex lot conversion
    default_first_spread=0.0005,
    default_next_spread=0.0002,
    default_take_profit=0.0001,
)


# =============================================================================
# CRYPTO (for crypto-to-crypto arbitrage)
# =============================================================================

BTCUSD = SymbolMapping(
    name="Bitcoin (BTC/USD)",
    code="BTCUSD",
    mt5="BTCUSD",              # If MT5 broker supports crypto
    okx="BTC/USDT:USDT",
    binance="BTC/USDT",
    bybit="BTCUSDT",
    gate="BTC_USDT",
    bitget="BTCUSDT",
    bitmart="BTC_USDT",
    lbank="btc_usdt",
    mt5_lot_size=0.01,
    exchange_lot_size=0.001,
    lot_ratio=10,
    default_first_spread=50.0,
    default_next_spread=20.0,
    default_take_profit=10.0,
)

ETHUSD = SymbolMapping(
    name="Ethereum (ETH/USD)",
    code="ETHUSD",
    mt5="ETHUSD",
    okx="ETH/USDT:USDT",
    binance="ETH/USDT",
    bybit="ETHUSDT",
    gate="ETH_USDT",
    bitget="ETHUSDT",
    bitmart="ETH_USDT",
    lbank="eth_usdt",
    mt5_lot_size=0.01,
    exchange_lot_size=0.01,
    lot_ratio=10,
    default_first_spread=5.0,
    default_next_spread=2.0,
    default_take_profit=1.0,
)


# =============================================================================
# SYMBOL REGISTRY
# =============================================================================

# All available symbols
SYMBOL_REGISTRY: Dict[str, SymbolMapping] = {
    "XAUUSD": XAUUSD,
    "XAGUSD": XAGUSD,
    "EURUSD": EURUSD,
    "BTCUSD": BTCUSD,
    "ETHUSD": ETHUSD,
}

# Supported exchanges
SUPPORTED_EXCHANGES = [
    {"id": "mt5", "name": "MetaTrader 5", "icon": "📊", "type": "forex"},
    {"id": "okx", "name": "OKX", "icon": "🟠", "type": "crypto"},
    {"id": "binance", "name": "Binance", "icon": "🟡", "type": "crypto"},
    {"id": "bybit", "name": "Bybit", "icon": "🔵", "type": "crypto"},
    {"id": "gate", "name": "Gate.io", "icon": "🟢", "type": "crypto"},
    {"id": "bitget", "name": "Bitget", "icon": "🟣", "type": "crypto"},
    {"id": "bitmart", "name": "BitMart", "icon": "⚫", "type": "crypto"},
    {"id": "lbank", "name": "LBank", "icon": "🔴", "type": "crypto"},
]


def get_symbol(code: str) -> Optional[SymbolMapping]:
    """Get symbol mapping by code."""
    return SYMBOL_REGISTRY.get(code.upper())


def get_supported_pairs(symbol_code: str) -> List[Dict]:
    """
    Get all supported exchange pairs for a symbol.
    
    Returns list of valid (exchange_a, exchange_b) combinations.
    """
    symbol = get_symbol(symbol_code)
    if not symbol:
        return []
    
    pairs = []
    exchanges = [e["id"] for e in SUPPORTED_EXCHANGES]
    
    for i, ex_a in enumerate(exchanges):
        if not symbol.is_supported(ex_a):
            continue
        for ex_b in exchanges[i+1:]:
            if not symbol.is_supported(ex_b):
                continue
            pairs.append({
                "exchange_a": ex_a,
                "exchange_b": ex_b,
                "symbol_a": symbol.get_symbol(ex_a),
                "symbol_b": symbol.get_symbol(ex_b),
            })
    
    return pairs


def get_all_symbols_info() -> List[Dict]:
    """Get all symbols info for frontend."""
    result = []
    for code, symbol in SYMBOL_REGISTRY.items():
        info = symbol.to_dict()
        # Add list of supported exchanges
        info["supported_exchanges"] = [
            ex["id"] for ex in SUPPORTED_EXCHANGES 
            if symbol.is_supported(ex["id"])
        ]
        result.append(info)
    return result


def get_exchanges_info() -> List[Dict]:
    """Get all exchanges info for frontend."""
    return SUPPORTED_EXCHANGES.copy()
