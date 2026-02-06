"""
WebSocket message handlers.
"""

from typing import Dict, Any, Callable, Awaitable, Optional

from ..config import settings
from ..config.logging_config import get_logger
from ..config.symbols import get_all_symbols_info, get_exchanges_info
from ..services import ArbitrageManager
from ..core import KLineAggregator

logger = get_logger("server.handlers")


class MessageHandler:
    """
    Handles WebSocket messages from clients.
    
    Message types:
    - params: Update strategy parameters
    - get_history: Request historical data
    - get_orders: Request order history
    - get_dashboard: Request dashboard data
    - make_long: Manual long trade
    - make_short: Manual short trade
    - close_all: Close all positions
    - start_task: Start arbitrage task
    - stop_task: Stop arbitrage task
    """
    
    def __init__(
        self,
        manager: ArbitrageManager,
        aggregator: KLineAggregator,
        send_func: Callable[[Any, Dict], Awaitable[None]]
    ):
        """
        Initialize message handler.
        
        Args:
            manager: Arbitrage manager instance
            aggregator: K-line aggregator instance
            send_func: Function to send messages to client
        """
        self.manager = manager
        self.aggregator = aggregator
        self.send = send_func
        
        # Current state
        self.auto_trade = settings.strategy.auto_trade
        self.trade_volume = settings.strategy.trade_volume
        
        # Register handlers
        self.handlers: Dict[str, Callable] = {
            "params": self._handle_params,
            "get_settings": self._handle_get_settings,
            "get_history": self._handle_get_history,
            "refresh_history": self._handle_get_history,
            "get_orders": self._handle_get_orders,
            "get_order_logs": self._handle_get_orders,
            "get_dashboard": self._handle_get_dashboard,
            "get_symbols": self._handle_get_symbols,
            "get_exchanges": self._handle_get_exchanges,
            "make_long": self._handle_make_long,
            "make_short": self._handle_make_short,
            "close_all": self._handle_close_all,
            "close_position": self._handle_close_position,
            "start_task": self._handle_start_task,
            "stop_task": self._handle_stop_task,
            "create_task": self._handle_create_task,
            "remove_task": self._handle_remove_task,
            "get_tasks": self._handle_get_tasks,
            "update_settings": self._handle_update_settings,
        }
    
    async def handle(self, websocket: Any, message: Dict[str, Any]):
        """
        Handle incoming message.
        
        Args:
            websocket: Client websocket
            message: Parsed message dictionary
        """
        msg_type = message.get("type", "")
        payload = message.get("payload", {})
        
        handler = self.handlers.get(msg_type)
        
        if handler:
            try:
                await handler(websocket, payload)
            except Exception as e:
                logger.error(f"Handler error for {msg_type}: {e}")
                await self.send(websocket, {
                    "type": "error",
                    "payload": {"message": str(e)}
                })
        else:
            logger.warning(f"Unknown message type: {msg_type}")
    
    async def _handle_get_settings(self, websocket: Any, payload: Dict):
        """Handle get_settings request."""
        await self.send(websocket, {
            "type": "settings",
            "payload": settings.to_dict()
        })
    
    async def _handle_params(self, websocket: Any, payload: Dict):
        """Handle params update."""
        # Update settings
        if "emaPeriod" in payload:
            settings.strategy.ema_period = int(payload["emaPeriod"])
        if "firstSpread" in payload:
            settings.strategy.first_spread = float(payload["firstSpread"])
        if "nextSpread" in payload:
            settings.strategy.next_spread = float(payload["nextSpread"])
        if "takeProfit" in payload:
            settings.strategy.take_profit = float(payload["takeProfit"])
        if "maxPos" in payload:
            settings.strategy.max_pos = int(payload["maxPos"])
        if "tradeVolume" in payload:
            self.trade_volume = float(payload["tradeVolume"])
            settings.strategy.trade_volume = self.trade_volume
        if "autoTrade" in payload:
            self.auto_trade = bool(payload["autoTrade"])
            settings.strategy.auto_trade = self.auto_trade
        
        # Update strategies in manager
        for task_id, strategy in self.manager.strategies.items():
            strategy.update_params(payload)
        
        # Send updated params back
        await self.send(websocket, {
            "type": "params",
            "payload": settings.to_dict()
        })
        
        logger.info(f"Parameters updated: {payload}")
    
    async def _handle_get_history(self, websocket: Any, payload: Dict):
        """Handle history request."""
        history = self.aggregator.get_history_payload()
        
        await self.send(websocket, {
            "type": "history",
            "payload": history
        })
    
    async def _handle_get_orders(self, websocket: Any, payload: Dict):
        """Handle order history request."""
        # Get from manager or order service
        orders = []
        
        await self.send(websocket, {
            "type": "order_logs",
            "payload": orders
        })
    
    async def _handle_get_dashboard(self, websocket: Any, payload: Dict):
        """Handle dashboard data request."""
        dashboard = self.manager.get_dashboard_data()
        
        await self.send(websocket, {
            "type": "dashboard",
            "payload": dashboard
        })
    
    async def _handle_get_symbols(self, websocket: Any, payload: Dict):
        """Handle get symbols request."""
        symbols = get_all_symbols_info()
        
        await self.send(websocket, {
            "type": "symbols",
            "payload": symbols
        })
    
    async def _handle_get_exchanges(self, websocket: Any, payload: Dict):
        """Handle get exchanges request."""
        exchanges = get_exchanges_info()
        
        await self.send(websocket, {
            "type": "exchanges",
            "payload": exchanges
        })
    
    async def _handle_close_position(self, websocket: Any, payload: Dict):
        """Handle close position request for a specific task."""
        task_id = payload.get("task_id")
        
        if not task_id:
            await self.send(websocket, {
                "type": "error",
                "payload": {"message": "task_id required"}
            })
            return
        
        await self.manager._close_position(task_id)
        
        await self.send(websocket, {
            "type": "trade_result",
            "payload": {"status": "position_closed", "task_id": task_id}
        })
    
    async def _handle_update_settings(self, websocket: Any, payload: Dict):
        """Handle global settings update."""
        if "dryRun" in payload:
            settings.strategy.dry_run = bool(payload["dryRun"])
        if "defaultVolume" in payload:
            settings.strategy.trade_volume = float(payload["defaultVolume"])
        
        await self.send(websocket, {
            "type": "settings_updated",
            "payload": {"success": True}
        })
    
    async def _handle_make_long(self, websocket: Any, payload: Dict):
        """Handle manual long trade."""
        # Get active task
        tasks = self.manager.get_running_tasks()
        
        if not tasks:
            await self.send(websocket, {
                "type": "trade_result",
                "payload": {"status": "error", "error": "No active tasks"}
            })
            return
        
        task = tasks[0]
        
        # TODO: Execute long trade
        logger.info(f"Manual LONG requested for {task.task_id}")
        
        await self.send(websocket, {
            "type": "trade_result",
            "payload": {"status": "submitted", "direction": "long"}
        })
    
    async def _handle_make_short(self, websocket: Any, payload: Dict):
        """Handle manual short trade."""
        tasks = self.manager.get_running_tasks()
        
        if not tasks:
            await self.send(websocket, {
                "type": "trade_result",
                "payload": {"status": "error", "error": "No active tasks"}
            })
            return
        
        task = tasks[0]
        
        # TODO: Execute short trade
        logger.info(f"Manual SHORT requested for {task.task_id}")
        
        await self.send(websocket, {
            "type": "trade_result",
            "payload": {"status": "submitted", "direction": "short"}
        })
    
    async def _handle_close_all(self, websocket: Any, payload: Dict):
        """Handle close all positions."""
        for task in self.manager.get_running_tasks():
            if task.has_position:
                await self.manager._close_position(task.task_id)
        
        await self.send(websocket, {
            "type": "trade_result",
            "payload": {"status": "closed_all"}
        })
    
    async def _handle_start_task(self, websocket: Any, payload: Dict):
        """Handle start task request."""
        task_id = payload.get("task_id")
        
        if not task_id:
            await self.send(websocket, {
                "type": "error",
                "payload": {"message": "task_id required"}
            })
            return
        
        success = await self.manager.start_task(task_id)
        
        await self.send(websocket, {
            "type": "task_status",
            "payload": {
                "task_id": task_id,
                "success": success,
                "action": "start"
            }
        })
    
    async def _handle_stop_task(self, websocket: Any, payload: Dict):
        """Handle stop task request."""
        task_id = payload.get("task_id")
        
        if not task_id:
            await self.send(websocket, {
                "type": "error",
                "payload": {"message": "task_id required"}
            })
            return
        
        success = await self.manager.stop_task(task_id)
        
        await self.send(websocket, {
            "type": "task_status",
            "payload": {
                "task_id": task_id,
                "success": success,
                "action": "stop"
            }
        })
    
    async def _handle_create_task(self, websocket: Any, payload: Dict):
        """Handle create task request."""
        try:
            # Include symbol in config for reference
            config = payload.get("config", {})
            if "symbol" not in config and "symbol" in payload:
                config["symbol"] = payload.get("symbol")
            
            task = await self.manager.create_task(
                task_id=payload.get("task_id", ""),
                exchange_a=payload.get("exchange_a", "mt5"),
                exchange_b=payload.get("exchange_b", "okx"),
                symbol_a=payload.get("symbol_a", "XAUUSD"),
                symbol_b=payload.get("symbol_b", "XAU/USDT:USDT"),
                config=config
            )
            
            await self.send(websocket, {
                "type": "task_created",
                "payload": task.to_dict()
            })
            
            # Auto-start if requested
            if payload.get("autoStart", False):
                await self.manager.start_task(task.task_id)
                await self.send(websocket, {
                    "type": "task_status",
                    "payload": {
                        "task_id": task.task_id,
                        "success": True,
                        "action": "start"
                    }
                })
            
        except Exception as e:
            logger.error(f"Failed to create task: {e}")
            await self.send(websocket, {
                "type": "error",
                "payload": {"message": str(e)}
            })
    
    async def _handle_remove_task(self, websocket: Any, payload: Dict):
        """Handle remove task request."""
        task_id = payload.get("task_id")
        
        if task_id:
            await self.manager.remove_task(task_id)
        
        await self.send(websocket, {
            "type": "task_removed",
            "payload": {"task_id": task_id}
        })
    
    async def _handle_get_tasks(self, websocket: Any, payload: Dict):
        """Handle get tasks request."""
        tasks = [t.to_dict() for t in self.manager.get_all_tasks()]
        
        await self.send(websocket, {
            "type": "tasks",
            "payload": tasks
        })
    
    async def send_initial_state(self, websocket: Any):
        """Send initial state to new client."""
        # Send params
        await self.send(websocket, {
            "type": "params",
            "payload": settings.to_dict()
        })
        
        # Send history
        await self._handle_get_history(websocket, {})
        
        # Send dashboard
        await self._handle_get_dashboard(websocket, {})
        
        # Send tasks
        await self._handle_get_tasks(websocket, {})
