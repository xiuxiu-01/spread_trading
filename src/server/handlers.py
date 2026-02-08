"""
WebSocket message handlers.
"""

from typing import Dict, Any, Callable, Awaitable, Optional

from ..config import settings
from ..config.logging_config import get_logger
from ..config.symbols import get_all_symbols_info, get_exchanges_info
from ..services import ArbitrageManager, verify_task_exchanges, task_persistence
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
            "get_account": self._handle_get_account,
            "get_params": self._handle_get_params,
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
            "sync_history": self._handle_sync_history,
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
        """Handle parameters update."""
        logger.info(f"Received params update: {payload}")
        
        # Check if this is a task-specific update
        task_id = payload.get("task_id")
        
        if task_id and task_id in self.manager.tasks:
            new_params = {}
            # Copy known params
            for key in ["emaPeriod", "firstSpread", "nextSpread", "takeProfit", "maxPos", "tradeVolume", "autoTrade", "qtyMultiplier", "dryRun"]:
                if key in payload:
                    if key in ["emaPeriod", "maxPos"]:
                        new_params[key] = int(payload[key])
                    elif key in ["autoTrade", "dryRun"]:
                        new_params[key] = bool(payload[key])
                    else:
                        new_params[key] = float(payload[key])
            
            logger.info(f"Updating params for {task_id}: {new_params}")
            self.manager.update_task_params(task_id, new_params)
            
            task = self.manager.tasks.get(task_id)
            if task:
                await self.send(websocket, {
                    "type": "params_updated",
                    "payload": {"task_id": task_id, "config": task.config}
                })
            return

        # Legacy Global Update
        # If we receive global params (no task_id), we apply them to ALL tasks (existing behavior)
        new_params = {}
        if "emaPeriod" in payload:
            settings.strategy.ema_period = int(payload["emaPeriod"])
            new_params["emaPeriod"] = settings.strategy.ema_period
        if "firstSpread" in payload:
            settings.strategy.first_spread = float(payload["firstSpread"])
            new_params["firstSpread"] = settings.strategy.first_spread
        if "nextSpread" in payload:
            settings.strategy.next_spread = float(payload["nextSpread"])
            new_params["nextSpread"] = settings.strategy.next_spread
        if "takeProfit" in payload:
            settings.strategy.take_profit = float(payload["takeProfit"])
            new_params["takeProfit"] = settings.strategy.take_profit
        if "maxPos" in payload:
            settings.strategy.max_pos = int(payload["maxPos"])
            new_params["maxPos"] = settings.strategy.max_pos
        if "tradeVolume" in payload:
            self.trade_volume = float(payload["tradeVolume"])
            settings.strategy.trade_volume = self.trade_volume
            new_params["tradeVolume"] = self.trade_volume
        if "qtyMultiplier" in payload:
            qty_mult = float(payload["qtyMultiplier"])
            # Update setting if it exists in settings model, otherwise just pass to task
            if hasattr(settings.strategy, "qty_multiplier"):
                settings.strategy.qty_multiplier = qty_mult
            new_params["qtyMultiplier"] = qty_mult
        if "autoTrade" in payload:
            self.auto_trade = bool(payload["autoTrade"])
            new_params["autoTrade"] = self.auto_trade
            
        if new_params:
            for t_id, task in self.manager.tasks.items():
                self.manager.update_task_params(t_id, new_params)
            
            # Broadcast update
            await self.send(websocket, {
                "type": "params_updated",
                "payload": {"task_id": "GLOBAL", "config": new_params}
            })

    async def _handle_sync_history(self, websocket: Any, payload: Dict):
        """Handle sync history request."""
        task_id = payload.get("task_id")
        days = payload.get("days", 1)
        
        if not task_id:
             # Try first running task
             tasks = self.manager.get_running_tasks()
             if tasks:
                 task_id = tasks[0].task_id
        
        if task_id:
            logger.info(f"Manual history sync requested for {task_id}")
            # Trigger sync (1440 mins per day)
            await self.manager._sync_history(task_id, limit=1440 * days)
            
            # Send updated history
            agg = self.manager.aggregators.get(task_id)
            if agg:
                await self.send(websocket, {
                    "type": "history",
                    "task_id": task_id,
                    "payload": agg.get_history_payload()
                })
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
        # Check if task_id is provided
        task_id = payload.get("task_id")
        
        if task_id:
            # Get specific task aggregator
            agg = self.manager.aggregators.get(task_id)
            if agg:
                history = agg.get_history_payload()
                await self.send(websocket, {
                    "type": "history",
                    "task_id": task_id,
                    "payload": history
                })
            else:
                await self.send(websocket, {
                    "type": "error",
                    "payload": {"message": f"Aggregator not found for {task_id}"}
                })
        else:
            # Fallback or get all? 
            # If no task_id, try to send history for the default global aggregator (if used)
            # Or iterate all active tasks and send multiple history messages
            
            # Sending global aggregator history (Mainly for single-task legacy support)
            history = self.aggregator.get_history_payload()
            await self.send(websocket, {
                "type": "history",
                "payload": history
            })
            
            # Also send individual task histories
            for t_id, agg in self.manager.aggregators.items():
                hist = agg.get_history_payload()
                await self.send(websocket, {
                    "type": "history",
                    "task_id": t_id,
                    "payload": hist
                })
    
    async def _handle_get_orders(self, websocket: Any, payload: Dict):
        """Handle order history request."""
        # Get from order service - Now fetching COMPOSITE TRADES
        requested_task_id = payload.get("task_id") # Note: frontend not sending task_id yet for this call? 
        # But we added it in a previous step to frontend. Let's assume it might not be there for global view.
        # Actually payload is from get_order_logs call.
        
        # If payload has task_id (added in frontend cleanup), filter. If not, return all (or handle global view)
        target_task_id = requested_task_id # Can be None
        
        orders = self.manager.order_service.get_recent_trades(limit=50, task_id=target_task_id)
        
        # Calculate Simple PnL Summary specific to this session/view
        mt5_pnl = 0.0
        okx_pnl = 0.0
        
        # Note: We can only calc pnl if we have close trades or mark-to-market.
        # Currently we just sum available fields if they exist?
        # Actually the frontend expects {orders: [], pnl: {}}
        
        response_payload = {
            "orders": orders,
            "pnl": {
                "mt5": mt5_pnl,
                "okx": okx_pnl,
                "total": mt5_pnl + okx_pnl
            }
        }
        
        await self.send(websocket, {
            "type": "order_logs",
            "payload": response_payload
        })
        
    async def _handle_get_account(self, websocket: Any, payload: Dict):
        """Handle access account info via gateways."""
        accounts = {}
        positions = {}
        
        # Iterate all tasks to find unique gateways or filter by requested task
        requested_task_id = payload.get("task_id")
        all_tasks = self.manager.get_all_tasks()
        
        target_tasks = all_tasks
        if requested_task_id:
            target_tasks = [t for t in all_tasks if t.task_id == requested_task_id]
        
        # 1. Get Balances (Account Level - Use any functional gateway from target tasks)
        # Note: We really only need one successful fetch per exchange "account"
        # Since all tasks usually share the same "mt5" and "okx" account, 
        # fetching via the current task's gateway is sufficient.
        
        for task in target_tasks:
            gateways = self.manager.gateways.get(task.task_id, {})
            
            # Gateway A (MT5)
            gw_a = gateways.get("a")
            if gw_a:
                key = "mt5"
                # Get Balance
                if key not in accounts:
                    try:
                        if hasattr(gw_a, 'get_balance'):
                             info = await gw_a.get_balance()
                             accounts[key] = info
                        elif hasattr(gw_a, 'get_account_info'):
                             info = await gw_a.get_account_info()
                             accounts[key] = info
                    except Exception as e:
                        logger.error(f"Failed to get {key} account: {e}")
                
                # Get Position for this task if needed, but currently we aggregate per exchange
                # MT5 usually returns net position per symbol.
                try:
                    # Specific to MT5 handling logic
                    pass 
                except:
                    pass

            # Gateway B (OKX)
            gw_b = gateways.get("b")
            if gw_b:
                key = "okx"
                if key not in accounts:
                    try:
                        bal = await gw_b.get_balance()
                        accounts[key] = bal
                    except Exception as e:
                        logger.error(f"Failed to get {key} account: {e}")

        # Current logic for NET LOTS (Position)
        # We need to fetch current Positions from the Manager's State / Orders
        # Or fetch directly from Gateways if possible
        
        # MT5 Position (Total across all tasks for simplicity, or specific if needed)
        # BUT frontend expects "mt5" and "okx" keys in 'net' object.
        
        mt5_net_lots = 0.0
        okx_net_lots = 0.0
        
        # Calculate from active tasks
        # Removed broken loop that used get_position_size
        
        # Fetch actual positions from gateways to ensure accuracy (esp if restarted)
        for task in target_tasks:
            gateways = self.manager.gateways.get(task.task_id, {})
            gw_a = gateways.get("a") # MT5
            gw_b = gateways.get("b") # OKX
            
            if gw_a:
                try: 
                    # MT5 Gateway get_position uses self.symbol, takes no args
                    pos = await gw_a.get_position()
                    if pos: 
                        # MT5 Position volume is already in Lots
                        # Check side
                        sign = 1 if pos.side.value == "long" else -1
                        mt5_net_lots += (pos.volume * sign)
                except Exception as e:
                    logger.debug(f"MT5 get_pos error: {e}")

            if gw_b:
                try:
                    pos = await gw_b.get_position()
                    if pos and pos.volume > 0:
                        # OKX returns Number of Contracts (e.g. 100)
                        # We need to convert to Lots if user wants alignment
                        # Multiplier e.g. 100,000
                        sign = 1 if pos.side.value == "long" else -1
                        contracts = pos.volume
                        
                        multiplier = task.config.get("qtyMultiplier", 1.0)
                        if multiplier > 0:
                             # Display Equivalent Lots
                             okx_net_lots += (contracts / multiplier * sign)
                        else:
                             okx_net_lots += (contracts * sign)

                except Exception as e:
                    logger.debug(f"OKX get_pos error: {e}")

        # Format for frontend
        formatted_payload = {
            "balance": {
                "mt5": accounts.get("mt5", {}).get("equity", 0),  
                "okx": accounts.get("okx", {}).get("total", 0)
            },
            "net": {
                "mt5": mt5_net_lots,
                "okx": okx_net_lots
            }
        }
        
        await self.send(websocket, {
            "type": "account",
            "payload": formatted_payload
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
        task_id = payload.get("task_id")
        
        target_task = None
        if task_id:
            target_task = self.manager.get_task(task_id)
        else:
             # Fallback to first running task (Legacy behavior)
             tasks = self.manager.get_running_tasks()
             if tasks:
                 target_task = tasks[0]
        
        if not target_task:
            await self.send(websocket, {
                "type": "trade_result",
                "payload": {"status": "error", "error": f"Task not found: {task_id or 'any'}"}
            })
            return
        
        logger.info(f"Manual LONG requested for {target_task.task_id}")
        
        # Parse volume if provided in payload (passed via params update usually, but check here)
        # Actually messageHandler stores self.trade_volume from params
        # But let's rely on Manager's config which should be updated.
        
        result = await self.manager.execute_manual_trade(target_task.task_id, "long")
        
        await self.send(websocket, {
            "type": "trade_result",
            "payload": {
                "status": "filled" if result.get("success") else "failed", 
                "direction": "long",
                "error": result.get("error")
            }
        })
    
    async def _handle_make_short(self, websocket: Any, payload: Dict):
        """Handle manual short trade."""
        task_id = payload.get("task_id")
        
        target_task = None
        if task_id:
            target_task = self.manager.get_task(task_id)
        else:
             tasks = self.manager.get_running_tasks()
             if tasks:
                 target_task = tasks[0]
        
        if not target_task:
            await self.send(websocket, {
                "type": "trade_result",
                "payload": {"status": "error", "error": f"Task not found: {task_id or 'any'}"}
            })
            return
        
        logger.info(f"Manual SHORT requested for {target_task.task_id}")
        
        result = await self.manager.execute_manual_trade(target_task.task_id, "short")
        
        await self.send(websocket, {
            "type": "trade_result",
            "payload": {
                "status": "filled" if result.get("success") else "failed", 
                "direction": "short",
                "error": result.get("error")
            }
        })
    
    async def _handle_close_all(self, websocket: Any, payload: Dict):
        """Handle close all positions."""
        # Handle specific task close if provided
        task_id = payload.get("task_id")
        
        target_tasks = []
        if task_id:
            t = self.manager.get_task(task_id)
            if t: target_tasks.append(t)
        else:
            target_tasks = self.manager.get_running_tasks()
            
        for task in target_tasks:
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
        """
        Handle create task request.
        
        Before creating a task:
        1. Check if API keys exist for both exchanges in .env
        2. Verify connection and fetch balance from both exchanges
        3. Only create task if both exchanges are accessible
        4. Persist task to disk
        """
        exchange_a = payload.get("exchange_a", "mt5")
        exchange_b = payload.get("exchange_b", "okx")
        symbol_a = payload.get("symbol_a", "XAUUSD")
        symbol_b = payload.get("symbol_b", "XAU/USDT:USDT")
        
        logger.info(f"Creating task: {exchange_a}/{symbol_a} <-> {exchange_b}/{symbol_b}")
        
        # Step 1 & 2: Verify both exchanges (API keys + balance)
        try:
            all_ok, verification_results = await verify_task_exchanges(
                exchange_a, exchange_b, symbol_a, symbol_b
            )
            
            if not all_ok:
                # Build error message with details
                errors = []
                balances = {}
                
                for ex_name, result in verification_results.items():
                    if not result.success:
                        errors.append(f"{ex_name}: {result.error}")
                    if result.balance:
                        balances[ex_name] = result.balance
                
                error_msg = "Exchange verification failed:\n" + "\n".join(errors)
                logger.error(error_msg)
                
                await self.send(websocket, {
                    "type": "task_creation_failed",
                    "payload": {
                        "error": error_msg,
                        "verification": {
                            name: r.to_dict() for name, r in verification_results.items()
                        }
                    }
                })
                return
            
            # Log balances
            for ex_name, result in verification_results.items():
                if result.balance:
                    logger.info(f"✅ {ex_name} balance: {result.balance}")
        
        except Exception as e:
            logger.error(f"Verification error: {e}")
            await self.send(websocket, {
                "type": "task_creation_failed",
                "payload": {"error": f"Verification failed: {str(e)}"}
            })
            return
        
        # Step 3: Create the task
        try:
            config = payload.get("config", {})
            if "symbol" not in config and "symbol" in payload:
                config["symbol"] = payload.get("symbol")
            
            # Ensure qtyMultiplier is set
            if "qtyMultiplier" not in config:
                # Default logic: XAU -> 100000, XAG -> 500000, others -> 1.0 or user defined in settings
                if "XAU" in symbol_a or "XAU" in symbol_b:
                    config["qtyMultiplier"] = getattr(settings.strategy, "qty_multiplier_xau", 100000.0)
                elif "XAG" in symbol_a or "XAG" in symbol_b:
                    config["qtyMultiplier"] = getattr(settings.strategy, "qty_multiplier_xag", 500000.0)
                else:
                    config["qtyMultiplier"] = getattr(settings.strategy, "qty_multiplier", 100.0)

            # Add verified balances to config
            config["verified_balances"] = {
                name: r.balance for name, r in verification_results.items()
            }
            
            task = await self.manager.create_task(
                task_id=payload.get("task_id", ""),
                exchange_a=exchange_a,
                exchange_b=exchange_b,
                symbol_a=symbol_a,
                symbol_b=symbol_b,
                config=config
            )
            
            # Step 4: Persist to disk
            task_persistence.add_task(task, self.manager.tasks)
            logger.info(f"Task {task.task_id} created and persisted")
            
            await self.send(websocket, {
                "type": "task_created",
                "payload": {
                    **task.to_dict(),
                    "verification": {
                        name: r.to_dict() for name, r in verification_results.items()
                    }
                }
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
            # Also remove from persistence
            task_persistence.save_tasks(self.manager.tasks)
        
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
    

    async def _handle_get_params(self, websocket: Any, payload: Dict):
        """Handle get params request."""
        task_id = payload.get("task_id")
        
        if task_id:
            task = self.manager.get_task(task_id)
            if task:
                # Merge task config with some global settings if needed, 
                # but primarily return task config.
                response = task.config.copy()
                # Ensure we include task_id so frontend knows context
                response["task_id"] = task_id
                
                await self.send(websocket, {
                    "type": "params",
                    "payload": response
                })
        else:
            # Fallback to global settings or legacy behavior
            # If no task specified, maybe return the global settings object
            # matching get_settings, but formatted for params UI
            await self.send(websocket, {
                "type": "params",
                "payload": settings.to_dict()
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
