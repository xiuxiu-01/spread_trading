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

# Verified 2026-02-06: All contracts tested for orderbook + 1m kline data
XAUUSD = SymbolMapping(
    name="Gold (XAU/USD)",
    code="XAUUSD",
    mt5="XAUUSD",
    okx="XAU/USDT:USDT",      # OKX Gold perpetual swap ✅ ~4893
    binance="XAU/USDT:USDT",   # Binance Gold perpetual swap ✅ ~4914
    bybit="PAXG/USDT:USDT",    # Bybit PAXG (Pax Gold) swap ✅ ~4915
    gate="XAU/USDT:USDT",      # Gate.io Gold swap ✅ ~4918
    bitget="XAU/USDT:USDT",    # Bitget Gold perpetual swap ✅ ~2900 (real gold, not tokenized XAUT)
    bitmart="XAU/USDT:USDT",   # BitMart Gold swap ✅ ~4917
    lbank="",                  # LBank ❌ K线数据不可用
    mt5_lot_size=0.01,
    exchange_lot_size=1,       # 1 contract = 1 oz
    lot_ratio=100,             # 0.01 MT5 lot = 1 exchange contract
    default_first_spread=3.0,
    default_next_spread=1.0,
    default_take_profit=0.5,
)

# Verified 2026-02-06: All contracts tested for orderbook + 1m kline data
XAGUSD = SymbolMapping(
    name="Silver (XAG/USD)",
    code="XAGUSD",
    mt5="XAGUSD",
    okx="XAG/USDT:USDT",      # OKX Silver perpetual swap ✅ ~76.18
    binance="XAG/USDT:USDT",   # Binance Silver perpetual swap ✅ ~76.19
    bybit="",                  # Bybit 没有 XAG 合约
    gate="XAG/USDT:USDT",      # Gate.io Silver swap ✅ ~76.20
    bitget="XAG/USDT:USDT",    # Bitget Silver perpetual swap ✅ ~76.20
    bitmart="XAG/USDT:USDT",   # BitMart Silver swap ✅ ~76.17
    lbank="",                  # LBank ❌ K线数据不可用
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
    okx="",                    # OKX 没有 EUR 合约
    binance="EUR/USDT",        # Binance EUR/USDT ✅
    bybit="",                  # Bybit 没有 EUR 合约
    gate="",                   # Gate.io 没有 EUR 合约
    bitget="",                 # Bitget 没有 EUR 合约
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
    okx="BTC/USDT:USDT",       # OKX perpetual swap ✅
    binance="BTC/USDT",        # Binance spot ✅
    bybit="BTC/USDT:USDT",     # Bybit perpetual swap ✅
    gate="BTC/USDT",           # Gate.io spot ✅
    bitget="BTC/USDT:USDT",    # Bitget perpetual swap ✅
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
    okx="ETH/USDT:USDT",       # OKX perpetual swap ✅
    binance="ETH/USDT",        # Binance spot ✅
    bybit="ETH/USDT:USDT",     # Bybit perpetual swap ✅
    gate="ETH/USDT",           # Gate.io spot ✅
    bitget="ETH/USDT:USDT",    # Bitget perpetual swap ✅
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
