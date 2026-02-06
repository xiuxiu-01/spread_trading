"""
Spread Trading System - Main Entry Point

Usage:
    python -m src.main
    python src/main.py

Options:
    --host      WebSocket server host (default: 0.0.0.0)
    --port      WebSocket server port (default: 8766)
    --debug     Enable debug logging
"""

import asyncio
import signal
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import settings, setup_logging
from src.config.logging_config import get_logger
from src.core import KLineAggregator
from src.services import ArbitrageManager
from src.server import WebSocketServer, MessageHandler

logger = get_logger("main")


class SpreadTradingApp:
    """Main application class."""
    
    def __init__(self):
        self.running = False
        
        # Initialize components
        self.aggregator = KLineAggregator(
            data_dir=settings.data_dir,
            ema_period=settings.strategy.ema_period
        )
        
        self.manager = ArbitrageManager(data_dir=settings.data_dir)
        
        self.server = WebSocketServer(
            host=settings.server.host,
            port=settings.server.port
        )
        
        self.handler = None
    
    async def start(self):
        """Start the application."""
        logger.info("=" * 60)
        logger.info("Spread Trading System v2.0")
        logger.info("=" * 60)
        
        # Load historical data
        bar_count = self.aggregator.load_history()
        logger.info(f"Loaded {bar_count} historical bars")
        
        # Setup message handler
        self.handler = MessageHandler(
            manager=self.manager,
            aggregator=self.aggregator,
            send_func=self.server.send_to
        )
        
        # Set server message handler
        async def on_message(websocket, data):
            await self.handler.handle(websocket, data)
            
        async def on_connect(websocket, path):
            # Send initial state to new clients
            await self.handler.send_initial_state(websocket)
        
        self.server.message_handler = on_message
        
        # Register manager callbacks
        self.manager.on_tick(self._on_tick)
        self.manager.on_bar(self._on_bar)
        self.manager.on_trade(self._on_trade)
        
        # Start WebSocket server
        await self.server.start()
        
        # Create default task (MT5 + OKX)
        await self._create_default_task()
        
        self.running = True
        logger.info("Application started successfully")
        
        # Keep running until stopped
        while self.running:
            await asyncio.sleep(1)
    
    async def _create_default_task(self):
        """Create the default MT5-OKX arbitrage task."""
        try:
            # Create task even if OKX not configured (for testing/monitoring)
            okx_config = settings.get_exchange("okx")
            has_okx = okx_config and okx_config.api_key
            
            if not has_okx:
                logger.warning("OKX not configured - task will be created but not auto-started")
            
            task = await self.manager.create_task(
                task_id="mt5-okx",
                exchange_a="mt5",
                exchange_b="okx",
                symbol_a=settings.mt5.symbol,
                symbol_b="XAU/USDT:USDT",
                config={
                    "emaPeriod": settings.strategy.ema_period,
                    "firstSpread": settings.strategy.first_spread,
                    "nextSpread": settings.strategy.next_spread,
                    "takeProfit": settings.strategy.take_profit,
                    "maxPos": settings.strategy.max_pos,
                    "autoTrade": settings.strategy.auto_trade,
                    "dryRun": settings.strategy.dry_run,
                }
            )
            
            logger.info(f"Created default task: {task.task_id}")
            
            # Auto-start if configured
            # await self.manager.start_task(task.task_id)
            
        except Exception as e:
            logger.error(f"Error creating default task: {e}")
    
    async def _on_tick(self, task_id: str, tick_data: dict):
        """Handle tick updates from manager."""
        await self.server.broadcast({
            "type": "tick",
            "payload": tick_data
        })
    
    async def _on_bar(self, task_id: str, bar_data: dict):
        """Handle bar updates from manager."""
        await self.server.broadcast({
            "type": "bar",
            "payload": bar_data
        })
    
    async def _on_trade(self, task_id: str, trade_data: dict):
        """Handle trade events from manager."""
        await self.server.broadcast({
            "type": "trade_result",
            "payload": trade_data
        })
    
    async def stop(self):
        """Stop the application."""
        logger.info("Stopping application...")
        self.running = False
        
        # Stop all tasks
        for task in self.manager.get_running_tasks():
            await self.manager.stop_task(task.task_id)
        
        # Stop server
        await self.server.stop()
        
        logger.info("Application stopped")


async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Spread Trading System")
    parser.add_argument("--host", default="0.0.0.0", help="WebSocket host")
    parser.add_argument("--port", type=int, default=8766, help="WebSocket port")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    # Setup logging
    log_level = "DEBUG" if args.debug else "INFO"
    setup_logging(level=log_level)
    
    # Update settings
    settings.server.host = args.host
    settings.server.port = args.port
    
    # Create app
    app = SpreadTradingApp()
    
    # Setup signal handlers
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        logger.info("Received shutdown signal")
        asyncio.create_task(app.stop())
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass
    
    try:
        await app.start()
    except KeyboardInterrupt:
        await app.stop()
    except Exception as e:
        logger.error(f"Application error: {e}")
        await app.stop()
        raise


if __name__ == "__main__":
    asyncio.run(main())
