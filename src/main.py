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
        self._monitor_task: Optional[asyncio.Task] = None
    
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
            
        async def on_connect(websocket):
            # Send initial state to new clients
            await self.handler.send_initial_state(websocket)
        
        self.server.message_handler = on_message
        self.server.connect_handler = on_connect
        
        # Register manager callbacks
        self.manager.on_tick(self._on_tick)
        self.manager.on_bar(self._on_bar)
        self.manager.on_trade(self._on_trade)
        
        # Start WebSocket server
        await self.server.start()
        
        # Restore persisted tasks
        await self.manager.restart_tasks()

        # Start monitor task to log runtime metrics periodically
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        
        # Default task creation removed as per user request
        # if not self.manager.tasks:
        #     await self._create_default_task()
        
        self.running = True
        logger.info("Application started successfully")
        
        # Keep running until stopped
        while self.running:
            await asyncio.sleep(1)
    
    async def _create_default_task(self):
        """Create the default MT5-OKX arbitrage tasks (Gold and Silver)."""
        try:
            # Create task even if OKX not configured (for testing/monitoring)
            okx_config = settings.get_exchange("okx")
            
            # --- GOLD (XAU) ---
            # MT5 1 Lot = 100 oz
            # OKX 1 Contract = 0.001 oz
            # Multiplier = 100 / 0.001 = 100,000
            
            task_xau = await self.manager.create_task(
                task_id="mt5-okx-xau",
                exchange_a="mt5",
                exchange_b="okx",
                symbol_a="XAUUSD",
                symbol_b="XAU/USDT:USDT",
                config={
                    "emaPeriod": settings.strategy.ema_period,
                    "firstSpread": settings.strategy.first_spread,
                    "nextSpread": settings.strategy.next_spread,
                    "takeProfit": settings.strategy.take_profit,
                    "maxPos": settings.strategy.max_pos,
                    "autoTrade": settings.strategy.auto_trade,
                    "dryRun": settings.strategy.dry_run,
                    "tradeVolume": 0.01,
                    "qtyMultiplier": 100000.0,
                }
            )
            logger.info(f"Created default XAU task: {task_xau.task_id}")

            # --- SILVER (XAG) ---
            # MT5 1 Lot = 5000 oz
            # OKX 1 Contract = 0.01 oz
            # Multiplier = 5000 / 0.01 = 500,000
            
            # Use smaller spreads for Silver
            task_xag = await self.manager.create_task(
                task_id="mt5-okx-xag",
                exchange_a="mt5",
                exchange_b="okx",
                symbol_a="XAGUSD",
                symbol_b="XAG/USDT:USDT",
                config={
                    "emaPeriod": 120,
                    "firstSpread": 0.05,  # Silver spreads are smaller
                    "nextSpread": 0.03,
                    "takeProfit": 0.08,
                    "maxPos": 3,
                    "autoTrade": False,
                    "dryRun": True,
                    "tradeVolume": 0.01,
                    "qtyMultiplier": 500000.0,
                }
            )
            logger.info(f"Created default XAG task: {task_xag.task_id}")

            # Start tasks
            await self.manager.start_task("mt5-okx-xau")
            await self.manager.start_task("mt5-okx-xag")
            
        except Exception as e:
            logger.error(f"Error creating default tasks: {e}")
    
    async def _on_tick(self, task_id: str, tick_data: dict):
        """Handle tick updates from manager."""
        await self.server.broadcast({
            "type": "tick",
            "task_id": task_id,
            "payload": tick_data
        })
    
    async def _on_bar(self, task_id: str, bar_data: dict):
        """Handle bar updates from manager."""
        await self.server.broadcast({
            "type": "bar",
            "task_id": task_id,
            "payload": bar_data
        })
    
    async def _on_trade(self, task_id: str, trade_data: dict):
        """Handle trade events from manager."""
        await self.server.broadcast({
            "type": "trade_result",
            "task_id": task_id,
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

    async def _monitor_loop(self, interval: int = 300):
        """Periodic runtime monitor to help diagnose long-running leaks.

        Logs:
        - threading active count
        - asyncio all_tasks count
        - websocket client count
        - number of gateways and their statuses
        - optional psutil stats (if psutil installed)
        """
        import threading
        try:
            import psutil
        except Exception:
            psutil = None

        while self.running:
            try:
                # Threading
                tcount = threading.active_count()

                # Asyncio tasks
                try:
                    task_count = len(asyncio.all_tasks())
                except Exception:
                    task_count = 0

                # WebSocket clients
                ws_clients = self.server.client_count if hasattr(self.server, 'client_count') else 0

                # Gateways info
                gw_count = 0
                gw_statuses = {}
                try:
                    gw_dict = getattr(self.manager, 'gateways', {})
                    gw_count = sum(len(v) for v in gw_dict.values())
                    for tid, gws in (gw_dict.items() if isinstance(gw_dict, dict) else []):
                        for side, gw in gws.items():
                            key = f"{tid}:{side}:{getattr(gw, 'name', 'gw')}"
                            gw_statuses[key] = getattr(gw, 'status', None).value if getattr(gw, 'status', None) else str(getattr(gw, 'status', None))
                except Exception:
                    gw_count = 0

                info_parts = [f"threads={tcount}", f"async_tasks={task_count}", f"ws_clients={ws_clients}", f"gateways={gw_count}"]
                if gw_statuses:
                    info_parts.append(f"gw_statuses={list(gw_statuses.items())[:6]}")

                # psutil details if available
                if psutil:
                    proc = psutil.Process()
                    try:
                        fds = getattr(proc, 'num_fds', lambda: None)()
                    except Exception:
                        fds = None
                    conns = len(proc.connections()) if hasattr(proc, 'connections') else None
                    info_parts.append(f"fds={fds}")
                    info_parts.append(f"conns={conns}")

                logger.info("[monitor] " + " | ".join(str(p) for p in info_parts))

            except Exception as e:
                logger.warning(f"Monitor loop error: {e}")

            await asyncio.sleep(interval)


async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Spread Trading System")
    parser.add_argument("--host", default="0.0.0.0", help="WebSocket host")
    parser.add_argument("--port", type=int, default=8766, help="WebSocket port")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()
    
    # Setup logging (write to console and to data/app.log)
    log_level = "DEBUG" if args.debug else "INFO"
    setup_logging(level=log_level, log_file=settings.data_dir / "app.log")
    
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
