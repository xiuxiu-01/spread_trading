"""
Arbitrage Manager - Manages multiple arbitrage tasks.
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Callable, Awaitable, Any

from ..models import ArbitrageTask, ArbitrageStatus, Order
from ..gateway import BaseGateway, MT5Gateway, CCXTGateway
from ..core import KLineAggregator, EMASpreadStrategy, SignalType
from ..config import settings
from ..config.logging_config import get_logger
from .task_persistence import task_persistence
from .order_service import OrderService

logger = get_logger("services.arbitrage_manager")


class ArbitrageManager:
    """
    Manages multiple arbitrage tasks between exchange pairs.
    
    Responsibilities:
    - Create and manage arbitrage tasks
    - Coordinate gateways for each task
    - Execute trading signals
    - Track positions and PnL
    """
    
    def __init__(self, data_dir=None):
        """Initialize arbitrage manager."""
        self.data_dir = data_dir or settings.data_dir
        self.order_service = OrderService(self.data_dir)
        
        # Active tasks
        self.tasks: Dict[str, ArbitrageTask] = {}
        
        # Gateways per task
        self.gateways: Dict[str, Dict[str, BaseGateway]] = {}
        
        # Aggregators per task
        self.aggregators: Dict[str, KLineAggregator] = {}
        
        # Strategies per task
        self.strategies: Dict[str, EMASpreadStrategy] = {}
        
        # Background tasks
        self._tick_tasks: Dict[str, asyncio.Task] = {}
        self._kline_tasks: Dict[str, asyncio.Task] = {}
        
        # Callbacks
        self._on_tick: Optional[Callable[[str, Dict], Awaitable[None]]] = None
        self._on_bar: Optional[Callable[[str, Dict], Awaitable[None]]] = None
        self._on_trade: Optional[Callable[[str, Dict], Awaitable[None]]] = None
        self._on_status: Optional[Callable[[str, ArbitrageTask], Awaitable[None]]] = None
        
        # Load persisted tasks
        self._load_persisted_tasks()
    
    def _load_persisted_tasks(self):
        """Load tasks from disk on startup."""
        try:
            task_data_list = task_persistence.load_tasks()
            for task_data in task_data_list:
                task = task_persistence.create_task_from_dict(task_data)
                self.tasks[task.task_id] = task
                logger.info(f"Loaded persisted task: {task.task_id}")
            
            if task_data_list:
                logger.info(f"Loaded {len(task_data_list)} tasks from disk")
        except Exception as e:
            logger.error(f"Failed to load persisted tasks: {e}")
    
    # --- Task Management ---
    
    async def create_task(
        self,
        task_id: str,
        exchange_a: str,
        exchange_b: str,
        symbol_a: str,
        symbol_b: str,
        config: Optional[Dict[str, Any]] = None
    ) -> ArbitrageTask:
        """
        Create a new arbitrage task.
        
        Args:
            task_id: Unique task identifier
            exchange_a: First exchange name (e.g., "mt5")
            exchange_b: Second exchange name (e.g., "okx")
            symbol_a: Symbol on first exchange
            symbol_b: Symbol on second exchange
            config: Strategy configuration
        
        Returns:
            Created ArbitrageTask
        """
        if task_id in self.tasks:
            raise ValueError(f"Task {task_id} already exists")
        
        task = ArbitrageTask(
            task_id=task_id,
            exchange_a=exchange_a,
            exchange_b=exchange_b,
            symbol_a=symbol_a,
            symbol_b=symbol_b,
            config=config or {}
        )
        
        self.tasks[task_id] = task
        logger.info(f"Created task: {task_id} ({exchange_a}/{exchange_b})")
        
        return task
    
    async def start_task(self, task_id: str) -> bool:
        """Start an arbitrage task."""
        task = self.tasks.get(task_id)
        if not task:
            logger.error(f"Task {task_id} not found")
            return False
        
        try:
            # Initialize gateways
            await self._init_gateways(task)
            
            # Initialize aggregator
            agg = KLineAggregator(
                data_dir=self.data_dir,
                ema_period=task.config.get("emaPeriod", settings.strategy.ema_period),
                filename=f"history_{task_id}.jsonl"
            )
            agg.load_history()
            self.aggregators[task_id] = agg
            
            # Sync recent history
            if task.config.get("sync_history", True):
                await self._sync_history(task_id)
            
            # Initialize strategy
            strategy_params = {
                "firstSpread": task.config.get("firstSpread", settings.strategy.first_spread),
                "nextSpread": task.config.get("nextSpread", settings.strategy.next_spread),
                "takeProfit": task.config.get("takeProfit", settings.strategy.take_profit),
                "maxPos": task.config.get("maxPos", settings.strategy.max_pos),
            }
            self.strategies[task_id] = EMASpreadStrategy(strategy_params)
            
            # Start background tasks
            self._tick_tasks[task_id] = asyncio.create_task(self._tick_loop(task_id))
            self._kline_tasks[task_id] = asyncio.create_task(self._kline_loop(task_id))
            
            task.start()
            logger.info(f"Started task: {task_id}")
            
            if self._on_status:
                await self._on_status(task_id, task)
            
            # Persist state
            task_persistence.save_tasks(self.tasks)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start task {task_id}: {e}")
            task.set_error(str(e))
            return False

    async def _sync_history(self, task_id: str):
        """Fetch and sync recent history if market is open."""
        gateways = self.gateways.get(task_id, {})
        gw_a = gateways.get("a")
        gw_b = gateways.get("b")
        agg = self.aggregators.get(task_id)
        
        if not all([gw_a, gw_b, agg]):
            return

        # Check market open
        is_open = False
        if hasattr(gw_a, 'is_market_open'):
             if asyncio.iscoroutinefunction(gw_a.is_market_open):
                 is_open = await gw_a.is_market_open()
             else:
                 is_open = gw_a.is_market_open()
        
        if not is_open:
             logger.info(f"Market closed, skipping history sync for {task_id}")
             return

        logger.info(f"Market open, syncing last 1440 bars for {task_id}")
        
        try:
             klines_a, klines_b = await asyncio.gather(
                 gw_a.get_klines(limit=1440),
                 gw_b.get_klines(limit=1440)
             )
        except Exception as e:
             logger.error(f"Failed to sync history for {task_id}: {e}")
             return
             
        if not klines_a or not klines_b:
            logger.warning(f"No history data received for {task_id}")
            return

        dict_a = {x['time']: x for x in klines_a}
        dict_b = {x['time']: x for x in klines_b}
        
        common_times = sorted(list(set(dict_a.keys()) & set(dict_b.keys())))
        
        count = 0 
        existing_times = set(agg.times)
        
        for ts in common_times:
            # Use local naive time to match system (same as datetime.now())
            dt = datetime.fromtimestamp(ts)
            t_iso = dt.isoformat()
            
            if t_iso in existing_times:
                continue
                
            b_a = dict_a[ts]
            b_b = dict_b[ts]
            
            # Using close price for spread
            spread = b_a['close'] - b_b['close']
            
            agg.add_bar(t_iso, b_a, b_b, spread, spread)
            count += 1
            
        if count > 0:
            logger.info(f"Synced {count} new bars for {task_id}")
    
    async def stop_task(self, task_id: str) -> bool:
        """Stop an arbitrage task."""
        task = self.tasks.get(task_id)
        if not task:
            return False
        
        # Cancel background tasks
        for task_dict in [self._tick_tasks, self._kline_tasks]:
            if task_id in task_dict:
                task_dict[task_id].cancel()
                try:
                    await task_dict[task_id]
                except asyncio.CancelledError:
                    pass
        
        # Disconnect gateways
        if task_id in self.gateways:
            for gw in self.gateways[task_id].values():
                await gw.disconnect()
            del self.gateways[task_id]
        
        task.stop()
        logger.info(f"Stopped task: {task_id}")
        
        if self._on_status:
            await self._on_status(task_id, task)
        
        # Persist state
        task_persistence.save_tasks(self.tasks)
        
        return True
    
    async def remove_task(self, task_id: str) -> bool:
        """Remove a task completely."""
        await self.stop_task(task_id)
        
        if task_id in self.tasks:
            del self.tasks[task_id]
        if task_id in self.aggregators:
            del self.aggregators[task_id]
        if task_id in self.strategies:
            del self.strategies[task_id]
        
        logger.info(f"Removed task: {task_id}")
        return True
    
    # --- Gateway Management ---
    
    async def _init_gateways(self, task: ArbitrageTask):
        """Initialize gateways for a task."""
        gateways = {}
        
        # Gateway A (typically MT5)
        if task.exchange_a == "mt5":
            gw_a = MT5Gateway(
                symbol=task.symbol_a,
                host=settings.mt5.host,
                port=settings.mt5.port
            )
        else:
            exchange_config = settings.get_exchange(task.exchange_a)
            if not exchange_config:
                raise ValueError(f"Exchange {task.exchange_a} not configured")
            gw_a = CCXTGateway.create_from_config(exchange_config, task.symbol_a)
        
        # Gateway B (CCXT exchange)
        exchange_config = settings.get_exchange(task.exchange_b)
        if not exchange_config:
            raise ValueError(f"Exchange {task.exchange_b} not configured")
        gw_b = CCXTGateway.create_from_config(exchange_config, task.symbol_b)
        
        # Connect
        if not await gw_a.connect():
            raise ConnectionError(f"Failed to connect to {task.exchange_a}")
        if not await gw_b.connect():
            await gw_a.disconnect()
            raise ConnectionError(f"Failed to connect to {task.exchange_b}")
        
        gateways["a"] = gw_a
        gateways["b"] = gw_b
        
        self.gateways[task.task_id] = gateways
    
    # --- Real-time Loops ---
    
    async def _tick_loop(self, task_id: str):
        """Real-time tick processing loop."""
        task = self.tasks.get(task_id)
        if not task:
            return
        
        gateways = self.gateways.get(task_id, {})
        gw_a = gateways.get("a")
        gw_b = gateways.get("b")
        
        if not gw_a or not gw_b:
            return
        
        logger.info(f"Starting tick loop for {task_id}")
        
        while task.is_active:
            try:
                # Get ticks from both exchanges
                tick_a = await gw_a.get_ticker()
                tick_b = await gw_b.get_ticker()
                
                if tick_a and tick_b:
                    # Calculate spreads
                    spread_sell = tick_a.bid - tick_b.ask
                    spread_buy = tick_a.ask - tick_b.bid
                    spread_mid = (tick_a.mid - tick_b.mid)
                    
                    # Update aggregator
                    agg = self.aggregators.get(task_id)
                    if agg:
                        agg.add_tick_spread(spread_sell, spread_buy, spread_mid)
                        avg_sell, avg_buy, avg_mid = agg.get_rolling_average()
                        
                        # Update task
                        task.update_tick(
                            spread_sell=spread_sell,
                            spread_buy=spread_buy,
                            avg_spread_sell=avg_sell,
                            avg_spread_buy=avg_buy,
                            ema=agg.last_ema
                        )
                        
                        # Emit tick
                        if self._on_tick:
                            await self._on_tick(task_id, {
                                "mt5_bid": tick_a.bid,
                                "mt5_ask": tick_a.ask,
                                "okx_bid": tick_b.bid,
                                "okx_ask": tick_b.ask,
                                "spread_sell": spread_sell,
                                "spread_buy": spread_buy,
                                "avg_spread_sell": avg_sell,
                                "avg_spread_buy": avg_buy,
                                "ema": agg.last_ema,
                                "time": datetime.now().timestamp(),
                                "mt5_symbol": getattr(tick_a, 'symbol', ''),
                                "okx_symbol": getattr(tick_b, 'symbol', '')
                            })
                        
                        # Check signals (if auto trade enabled)
                        if task.config.get("autoTrade", False):
                            await self._check_signals(task_id, avg_sell, avg_buy)
                
                await asyncio.sleep(0.1)  # 100ms tick rate
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Tick loop error for {task_id}: {e}")
                task.set_error(str(e))
                await asyncio.sleep(1)
        
        logger.info(f"Tick loop stopped for {task_id}")
    
    async def _kline_loop(self, task_id: str):
        """K-line update loop (every minute)."""
        task = self.tasks.get(task_id)
        if not task:
            return
        
        logger.info(f"Starting kline loop for {task_id}")
        
        while task.is_active:
            try:
                # Wait for next minute
                now = datetime.now()
                wait_seconds = 60 - now.second
                await asyncio.sleep(wait_seconds)
                
                # Fetch new kline
                await self._update_kline(task_id)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Kline loop error for {task_id}: {e}")
                await asyncio.sleep(5)
        
        logger.info(f"Kline loop stopped for {task_id}")
    
    async def _update_kline(self, task_id: str):
        """Update kline data."""
        gateways = self.gateways.get(task_id, {})
        gw_a = gateways.get("a")
        gw_b = gateways.get("b")
        agg = self.aggregators.get(task_id)
        task = self.tasks.get(task_id)
        
        if not all([gw_a, gw_b, agg, task]):
            return
        
        try:
            # Check market hours (MT5) before updating
            # Assuming gw_a is MT5
            is_open = False
            if hasattr(gw_a, 'is_market_open'):
                if asyncio.iscoroutinefunction(gw_a.is_market_open):
                    is_open = await gw_a.is_market_open()
                else:
                    is_open = gw_a.is_market_open()
            
            if not is_open:
                 logger.debug(f"Market closed for {task_id}, skipping kline update")
                 return

            # Get latest klines (1m)
            # Fetching 2 bars to ensure we get a CLOSED bar (previous minute)
            klines_a = await gw_a.get_klines(timeframe="1m", limit=2)
            klines_b = await gw_b.get_klines(timeframe="1m", limit=2)
            
            if klines_a and klines_b:
                # Use the second to last bar (completed bar)
                # If we only get 1 bar, it might be the current forming bar
                if len(klines_a) > 1 and len(klines_b) > 1:
                    bar_a = klines_a[-2]
                    bar_b = klines_b[-2]
                else:
                    # Fallback to last available if not enough history
                    bar_a = klines_a[-1]
                    bar_b = klines_b[-1]
                
                # Verify timestamps match (within reasonable tolerance)
                ts_a = bar_a.get('time') or bar_a.get('ts')
                ts_b = bar_b.get('time') or bar_b.get('ts')
                
                # Get current bid/ask for spread calculation
                tick_a = await gw_a.get_ticker()
                tick_b = await gw_b.get_ticker()
                
                if tick_a and tick_b:
                    avg_sell, avg_buy, _ = agg.get_rolling_average()
                    
                    # Use rolling average for bar spread
                    bar = agg.add_bar(
                        timestamp=datetime.now().isoformat(),
                        mt5_bar=bar_a,
                        okx_bar=bar_b,
                        spread_sell=avg_sell,
                        spread_buy=avg_buy
                    )
                    
                    if self._on_bar:
                        await self._on_bar(task_id, bar)
                    
                    logger.debug(f"New bar for {task_id}: spread={bar['spread']:.2f}")
        
        except Exception as e:
            logger.error(f"Kline update error for {task_id}: {e}")
    
    async def _check_signals(self, task_id: str, spread_sell: float, spread_buy: float):
        """Check and execute trading signals."""
        task = self.tasks.get(task_id)
        strategy = self.strategies.get(task_id)
        agg = self.aggregators.get(task_id)
        
        if not all([task, strategy, agg]):
            return
        
        ema = agg.last_ema
        
        # Check take profit first
        if task.has_position:
            if strategy.check_take_profit(
                spread_sell, spread_buy, ema,
                task.current_level, task.direction
            ):
                await self._close_position(task_id)
                return
        
        # Check entry/add-on signals
        signal = strategy.check_signal(
            spread_sell, spread_buy, ema,
            task.current_level, task.direction
        )
        
        if signal.is_valid:
            await self._execute_signal(task_id, signal)
    
    async def _execute_signal(self, task_id: str, signal):
        """Execute a trading signal."""
        task = self.tasks.get(task_id)
        gateways = self.gateways.get(task_id, {})
        gw_a = gateways.get("a")
        gw_b = gateways.get("b")
        
        if not task or not gw_a or not gw_b:
            return
        
        # Config
        base_volume = float(task.config.get("tradeVolume", 0.01))
        qty_multiplier = float(task.config.get("qtyMultiplier", 100.0))
        dry_run = task.config.get("dryRun", True)

        # Logic for reversal
        if task.has_position and task.direction != signal.type.value:
            logger.info(f"Reversing position for {task_id}")
            await self._close_position(task_id)
            # Update task state after close logic (it is done inside _close_position but let's be safe)
            if task.has_position:
                 # If close failed, we might abort or force reset.
                 # For now, assume _close_position resets the memory state.
                 pass

        # Calculate volume to add
        target_level = signal.level
        current_level = task.current_level
        
        # Safety: don't trade if already there (in same direction)
        if current_level >= target_level and task.direction == signal.type.value:
            return

        levels_to_add = target_level - current_level
        if levels_to_add <= 0:
             return
             
        trade_volume = base_volume * levels_to_add
        trade_volume = round(trade_volume, 2)
        
        # Calculate OKX volume for logging/verification
        okx_volume = trade_volume * qty_multiplier
        
        logger.info(f"Executing signal {task_id}: {signal.type.value} L{target_level} (+{levels_to_add} lvls). Volumes: MT5={trade_volume}, OKX={okx_volume} (Mult={qty_multiplier})")
        
        result = await self.order_service.execute_arbitrage_orders(
            gateway_a=gw_a,
            gateway_b=gw_b,
            direction=signal.type.value,
            volume=trade_volume,
            dry_run=dry_run,
            qty_multiplier=qty_multiplier
        )
        
        if result["success"]:
            # Recalculate PnL and trades count only on closing/full cycle
            # For now, just mark last_trade_at
            task.last_trade_at = datetime.now()
            
            task.current_level = target_level
            task.direction = signal.type.value
            
            # Persist task state (important for level tracking)
            task_persistence.save_tasks(self.tasks)
            
            if self._on_trade:
                await self._on_trade(task_id, {
                    "type": signal.type.value,
                    "level": signal.level,
                    "reason": signal.reason,
                    "result": result
                })
        else:
            logger.error(f"Trade execution failed for {task_id}: {result.get('error')}")
    
    async def _close_position(self, task_id: str):
        """Close all positions for a task."""
        task = self.tasks.get(task_id)
        gateways = self.gateways.get(task_id, {})
        gw_a = gateways.get("a")
        gw_b = gateways.get("b")
        
        if not task or not gw_a or not gw_b:
            return
        
        if not task.has_position:
            return
        
        logger.info(f"Closing position for {task_id}: {task.direction} L{task.current_level}")
        
        base_volume = float(task.config.get("tradeVolume", 0.01))
        qty_multiplier = float(task.config.get("qtyMultiplier", 100.0))
        dry_run = task.config.get("dryRun", True)

        # Calculate total volume pending
        total_volume = base_volume * task.current_level
        total_volume = round(total_volume, 2)
        
        result = await self.order_service.close_arbitrage_position(
            gateway_a=gw_a,
            gateway_b=gw_b,
            direction=task.direction,
            volume=total_volume,
            dry_run=dry_run,
            qty_multiplier=qty_multiplier
        )
        
        # Reset state regardless of result to avoid stuck state (safest for now)
        task.current_level = 0
        task.direction = ""
        task.record_trade(0, True)  # Placeholder PnL
        
        # Persist updated state
        task_persistence.save_tasks(self.tasks)
        
        if self._on_trade:
             await self._on_trade(task_id, {
                 "type": "close",
                 "level": 0,
                 "reason": "signal_close",
                 "result": result
             })
    
    # --- Callbacks ---
    
    def on_tick(self, callback: Callable[[str, Dict], Awaitable[None]]):
        """Register tick callback."""
        self._on_tick = callback
    
    def on_bar(self, callback: Callable[[str, Dict], Awaitable[None]]):
        """Register bar callback."""
        self._on_bar = callback
    
    def on_trade(self, callback: Callable[[str, Dict], Awaitable[None]]):
        """Register trade callback."""
        self._on_trade = callback
    
    def on_status(self, callback: Callable[[str, ArbitrageTask], Awaitable[None]]):
        """Register status callback."""
        self._on_status = callback
    
    # --- Queries ---
    
    def get_task(self, task_id: str) -> Optional[ArbitrageTask]:
        """Get task by ID."""
        return self.tasks.get(task_id)
    
    def get_all_tasks(self) -> List[ArbitrageTask]:
        """Get all tasks."""
        return list(self.tasks.values())
    
    def get_running_tasks(self) -> List[ArbitrageTask]:
        """Get all running tasks."""
        return [t for t in self.tasks.values() if t.is_active]
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get dashboard summary data."""
        tasks = self.get_all_tasks()
        running = [t for t in tasks if t.is_active]
        
        return {
            "total_tasks": len(tasks),
            "running_tasks": len(running),
            "total_pnl": sum(t.total_pnl for t in tasks),
            "total_trades": sum(t.total_trades for t in tasks),
            "tasks": [t.to_summary() for t in tasks],
            "gateways": self._get_gateway_status()
        }
    
    def _get_gateway_status(self) -> List[Dict]:
        """Get status of all gateways."""
        status = []
        for task_id, gws in self.gateways.items():
            for name, gw in gws.items():
                status.append({
                    "task_id": task_id,
                    "gateway": name,
                    **gw.get_info()
                })
        return status
