"""
Test exchange verification and task creation.
"""
import asyncio
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

from src.services import verify_task_exchanges, has_api_key, get_api_credentials

async def test_verification():
    print("=" * 80)
    print("Exchange Verification Test")
    print("=" * 80)
    
    # Test API key detection
    print("\n📋 Checking API keys in .env:")
    exchanges = ["okx", "binance", "bybit", "gate", "bitget", "bitmart", "lbank"]
    
    for ex in exchanges:
        has_key = has_api_key(ex)
        key, secret, pwd = get_api_credentials(ex)
        status = "✅" if has_key else "❌"
        key_preview = f"{key[:8]}..." if key else "NOT FOUND"
        print(f"  {status} {ex:10} - API Key: {key_preview}")
    
    print("\n" + "=" * 80)
    print("Testing Exchange Verification (mt5 + okx)")
    print("=" * 80)
    
    # Test mt5 + okx verification
    all_ok, results = await verify_task_exchanges("mt5", "okx", "XAUUSD", "XAU/USDT:USDT")
    
    print(f"\nResult: {'✅ PASSED' if all_ok else '❌ FAILED'}")
    print("\nDetails:")
    for ex_name, result in results.items():
        print(f"\n  {ex_name.upper()}:")
        print(f"    Has API Key: {result.has_api_key}")
        print(f"    Can Connect: {result.can_connect}")
        if result.balance:
            print(f"    Balance: {result.balance}")
        if result.error:
            print(f"    Error: {result.error}")
    
    # Test without valid credentials
    print("\n" + "=" * 80)
    print("Testing Exchange Without API Key (binance)")
    print("=" * 80)
    
    all_ok2, results2 = await verify_task_exchanges("mt5", "binance", "XAUUSD", "XAU/USDT:USDT")
    
    print(f"\nResult: {'✅ PASSED' if all_ok2 else '❌ FAILED (Expected)'}")
    for ex_name, result in results2.items():
        if not result.success:
            print(f"\n  {ex_name.upper()}: {result.error}")
    
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print("""
The verification system checks:
1. API key exists in .env file
2. Can connect to exchange with the key
3. Can fetch account balance

Only if BOTH exchanges pass, the task can be created.
""")

if __name__ == "__main__":
    asyncio.run(test_verification())
