"""
Arbitrage Manager - Manages multiple arbitrage tasks.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Callable, Awaitable, Any

from ..models import ArbitrageTask, ArbitrageStatus, Order
from ..gateway import BaseGateway, MT5Gateway, CCXTGateway
from ..core import KLineAggregator, EMASpreadStrategy, SignalType
from ..config import settings
from ..config.logging_config import get_logger

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
                ema_period=task.config.get("emaPeriod", settings.strategy.ema_period)
            )
            agg.load_history()
            self.aggregators[task_id] = agg
            
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
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start task {task_id}: {e}")
            task.set_error(str(e))
            return False
    
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
                                "time": datetime.now().timestamp()
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
            # Get latest klines
            klines_a = await gw_a.get_klines(timeframe="1m", limit=1)
            klines_b = await gw_b.get_klines(timeframe="1m", limit=1)
            
            if klines_a and klines_b:
                bar_a = klines_a[-1]
                bar_b = klines_b[-1]
                
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
        
        if not task or not gateways:
            return
        
        logger.info(f"Executing signal for {task_id}: {signal.type.value} L{signal.level}")
        
        # TODO: Implement actual order execution
        # This is a placeholder for the trading logic
        
        task.current_level = signal.level
        task.direction = signal.type.value
        
        if self._on_trade:
            await self._on_trade(task_id, {
                "type": signal.type.value,
                "level": signal.level,
                "reason": signal.reason
            })
    
    async def _close_position(self, task_id: str):
        """Close all positions for a task."""
        task = self.tasks.get(task_id)
        if not task:
            return
        
        logger.info(f"Closing position for {task_id}")
        
        # TODO: Implement actual position closing
        
        task.current_level = 0
        task.direction = ""
        task.record_trade(0, True)  # Placeholder PnL
    
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
