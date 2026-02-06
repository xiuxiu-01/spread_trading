"""
Exchange verification service.

Verifies API keys and account balance before creating tasks.
"""

import os
from typing import Dict, Any, Tuple, Optional
from dataclasses import dataclass
from pathlib import Path

# Load .env file
try:
    from dotenv import load_dotenv
    # Find .env file in project root
    env_path = Path(__file__).parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass

from ..config import settings
from ..config.logging_config import get_logger

logger = get_logger("services.exchange_verifier")

# Try to import ccxt
try:
    import ccxt
    CCXT_AVAILABLE = True
except ImportError:
    ccxt = None
    CCXT_AVAILABLE = False


@dataclass
class VerificationResult:
    """Result of exchange verification."""
    success: bool
    exchange: str
    has_api_key: bool = False
    can_connect: bool = False
    balance: Optional[Dict[str, float]] = None
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "exchange": self.exchange,
            "has_api_key": self.has_api_key,
            "can_connect": self.can_connect,
            "balance": self.balance,
            "error": self.error
        }


# Exchange class mapping
EXCHANGE_CLASSES = {
    "okx": "okx",
    "gate": "gateio",
    "gateio": "gateio",
    "bitget": "bitget",
    "bitmart": "bitmart",
    "binance": "binance",
    "lbank": "lbank",
    "bybit": "bybit",
}

# API key environment variable mappings
API_KEY_ENV = {
    "okx": {
        "key": "OKX_API_KEY",
        "secret": "OKX_API_SECRET",
        "password": "OKX_API_PASSPHRASE",
    },
    "binance": {
        "key": "BINANCE_API_KEY",
        "secret": "BINANCE_API_SECRET",
    },
    "bybit": {
        "key": "BYBIT_API_KEY",
        "secret": "BYBIT_API_SECRET",
    },
    "gate": {
        "key": "GATE_API_KEY",
        "secret": "GATE_API_SECRET",
    },
    "gateio": {
        "key": "GATE_API_KEY",
        "secret": "GATE_API_SECRET",
    },
    "bitget": {
        "key": "BITGET_API_KEY",
        "secret": "BITGET_API_SECRET",
        "password": "BITGET_API_PASSPHRASE",
    },
    "bitmart": {
        "key": "BITMART_API_KEY",
        "secret": "BITMART_API_SECRET",
        "password": "BITMART_API_MEMO",
    },
    "lbank": {
        "key": "LBANK_API_KEY",
        "secret": "LBANK_API_SECRET",
    },
}


def get_api_credentials(exchange: str) -> Tuple[str, str, str]:
    """
    Get API credentials for an exchange from environment variables.
    
    Returns:
        Tuple of (api_key, api_secret, password/passphrase)
    """
    exchange_lower = exchange.lower()
    env_map = API_KEY_ENV.get(exchange_lower, {})
    
    if not env_map:
        # Try generic format
        upper = exchange.upper()
        env_map = {
            "key": f"{upper}_API_KEY",
            "secret": f"{upper}_API_SECRET",
            "password": f"{upper}_API_PASSPHRASE",
        }
    
    api_key = os.getenv(env_map.get("key", ""), "")
    api_secret = os.getenv(env_map.get("secret", ""), "")
    password = os.getenv(env_map.get("password", ""), "")
    
    return api_key, api_secret, password


def has_api_key(exchange: str) -> bool:
    """Check if API key exists for an exchange."""
    # MT5 doesn't need API key check here
    if exchange.lower() == "mt5":
        return True
    
    api_key, api_secret, _ = get_api_credentials(exchange)
    return bool(api_key and api_secret)


async def verify_exchange(exchange: str, symbol: str = "") -> VerificationResult:
    """
    Verify an exchange connection and fetch balance.
    
    Args:
        exchange: Exchange name (e.g., "okx", "binance")
        symbol: Optional symbol to check (not used for balance check)
    
    Returns:
        VerificationResult with connection status and balance
    """
    exchange_lower = exchange.lower()
    
    # MT5 verification (skip for now, assume OK)
    if exchange_lower == "mt5":
        return VerificationResult(
            success=True,
            exchange="mt5",
            has_api_key=True,
            can_connect=True,
            balance={"USDT": 0}  # MT5 balance check not implemented
        )
    
    # Check CCXT availability
    if not CCXT_AVAILABLE:
        return VerificationResult(
            success=False,
            exchange=exchange,
            error="ccxt library not installed"
        )
    
    # Check API credentials
    api_key, api_secret, password = get_api_credentials(exchange)
    
    if not api_key or not api_secret:
        return VerificationResult(
            success=False,
            exchange=exchange,
            has_api_key=False,
            error=f"API key not found in .env. Required: {API_KEY_ENV.get(exchange_lower, {}).get('key', f'{exchange.upper()}_API_KEY')} and {API_KEY_ENV.get(exchange_lower, {}).get('secret', f'{exchange.upper()}_API_SECRET')}"
        )
    
    result = VerificationResult(
        success=False,
        exchange=exchange,
        has_api_key=True
    )
    
    try:
        # Create exchange instance
        exchange_id = EXCHANGE_CLASSES.get(exchange_lower, exchange_lower)
        exchange_class = getattr(ccxt, exchange_id, None)
        
        if not exchange_class:
            result.error = f"Exchange '{exchange}' not supported by CCXT"
            return result
        
        config = {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "timeout": 15000,
            "options": {
                "defaultType": "swap",  # For perpetual futures
            }
        }
        
        if password:
            config["password"] = password
        
        ex = exchange_class(config)
        
        # Load markets first
        ex.load_markets()
        
        # Fetch balance
        balance = ex.fetch_balance()
        
        result.can_connect = True
        
        # Extract relevant balance info
        usdt_balance = balance.get("USDT", {})
        
        # Handle different balance structures
        if isinstance(usdt_balance, dict):
            total = usdt_balance.get("total", 0)
            free = usdt_balance.get("free", 0)
        else:
            total = float(usdt_balance) if usdt_balance else 0
            free = total
        
        result.balance = {
            "USDT_total": float(total) if total else 0,
            "USDT_free": float(free) if free else 0,
        }
        
        result.success = True
        logger.info(f"✅ {exchange} verified: USDT balance = {result.balance}")
        
    except ccxt.AuthenticationError as e:
        result.error = f"Authentication failed: Invalid API key or secret"
        logger.error(f"❌ {exchange} auth error: {e}")
    except ccxt.PermissionDenied as e:
        result.error = f"Permission denied: API key may lack required permissions"
        logger.error(f"❌ {exchange} permission error: {e}")
    except ccxt.ExchangeError as e:
        result.error = f"Exchange error: {str(e)[:100]}"
        logger.error(f"❌ {exchange} exchange error: {e}")
    except Exception as e:
        result.error = f"Connection error: {str(e)[:100]}"
        logger.error(f"❌ {exchange} error: {e}")
    
    return result


async def verify_task_exchanges(
    exchange_a: str,
    exchange_b: str,
    symbol_a: str = "",
    symbol_b: str = ""
) -> Tuple[bool, Dict[str, VerificationResult]]:
    """
    Verify both exchanges for a task.
    
    Returns:
        Tuple of (all_ok, results_dict)
    """
    results = {}
    
    # Verify exchange A
    result_a = await verify_exchange(exchange_a, symbol_a)
    results[exchange_a] = result_a
    
    # Verify exchange B
    result_b = await verify_exchange(exchange_b, symbol_b)
    results[exchange_b] = result_b
    
    all_ok = result_a.success and result_b.success
    
    if all_ok:
        logger.info(f"✅ Both exchanges verified: {exchange_a} and {exchange_b}")
    else:
        if not result_a.success:
            logger.error(f"❌ {exchange_a} verification failed: {result_a.error}")
        if not result_b.success:
            logger.error(f"❌ {exchange_b} verification failed: {result_b.error}")
    
    return all_ok, results
