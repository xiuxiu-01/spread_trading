import asyncio
from src.services.arbitrage_manager import ArbitrageManager

async def main():
    manager = ArbitrageManager()
    await manager.close_all_positions()

if __name__ == "__main__":
    asyncio.run(main())
