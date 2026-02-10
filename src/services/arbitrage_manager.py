"""
Arbitrage Manager - Manages multiple arbitrage tasks.
"""

import asyncio
from datetime import datetime, timezone, timedelta
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
            loaded_count = 0
            
            for task_data in task_data_list:
                task = task_persistence.create_task_from_dict(task_data)
                
                # Check for duplicate before adding
                if task.task_id not in self.tasks:
                    self.tasks[task.task_id] = task
                    loaded_count += 1
                
                # If task was RUNNING, we should restart it
                # Note: We can't await here in __init__, so we'll need to schedule it
                # We handle this in a separate method called after init
            
            if loaded_count:
                logger.info(f"Loaded {loaded_count} tasks from disk")
        except Exception as e:
            logger.error(f"Failed to load persisted tasks: {e}")

    async def restart_tasks(self):
        """Restart active tasks after load."""
        for task_id, task in self.tasks.items():
            if task.status == ArbitrageStatus.RUNNING:
                logger.info(f"Restarting persisted task: {task_id}")
                await self.start_task(task_id)
    
    # --- Task Management ---
    
    async def create_task(
        self,
        task_id: str,
        exchange_a: str,
        exchange_b: str,
        symbol_a: str,
        symbol_b: str,
        config: Optional[Dict[str, Any]] = None,
        name: str = ""
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
            name: Task name (optional)
        
        Returns:
            Created ArbitrageTask
        """
        if task_id in self.tasks:
            raise ValueError(f"Task {task_id} already exists")
        
        task = ArbitrageTask(
            task_id=task_id,
            name=name,
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

    async def _sync_history(self, task_id: str, limit: int = 1440):
        """Fetch and sync recent history if market is open."""
        gateways = self.gateways.get(task_id, {})
        gw_a = gateways.get("a")
        gw_b = gateways.get("b")
        agg = self.aggregators.get(task_id)
        
        if not all([gw_a, gw_b, agg]):
            return

        logger.info(f"Syncing last {limit} bars for {task_id}")
        
        try:
             # 1. Fetch MT5 history first (master timeline)
             klines_a = await gw_a.get_klines(limit=limit)
             
             if not klines_a:
                 logger.warning(f"No history data received from MT5 for {task_id}")
                 return

             # 2. Determine start time for OKX sync
             start_ts = klines_a[0]['time']
             end_ts = klines_a[-1]['time']
             logger.info(f"MT5 History Range: {start_ts} to {end_ts} (Count: {len(klines_a)})")
             
             # 3. Fetch OKX history starting from MT5 start time
             # Calculate required limit based on time difference (minutes) plus padding
             time_diff_min = (end_ts - start_ts) // 60
             safety_limit = int(time_diff_min * 1.2) # 20% padding for gaps
             # Ensure limit is at least the original limit
             fetch_limit = max(limit, safety_limit)
             
             klines_b = await gw_b.get_klines(limit=fetch_limit, start_time=start_ts, end_time=end_ts)

        except Exception as e:
             logger.error(f"Failed to sync history for {task_id}: {e}")
             return
        
        if not klines_a or not klines_b:
            logger.warning(f"No history data received for {task_id}: MT5={len(klines_a if klines_a else [])}, OKX={len(klines_b if klines_b else [])}")
            return

        dict_a = {x['time']: x for x in klines_a}
        dict_b = {x['time']: x for x in klines_b}
        
        common_times = sorted(list(set(dict_a.keys()) & set(dict_b.keys())))
        logger.info(f"Unfiltered bars: MT5={len(klines_a)}, OKX={len(klines_b)}. Common timestamps: {len(common_times)}")

        if len(klines_a) > 0 and len(klines_b) > 0:
             logger.info(f"Sample TS - MT5: {klines_a[-1]['time']}, OKX: {klines_b[-1]['time']}")
        
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

    def update_task_params(self, task_id: str, new_params: Dict[str, Any]) -> bool:
        """
        Update parameters for a running (or stopped) task.
        Also updates the active strategy if it exists.
        """
        task = self.tasks.get(task_id)
        if not task:
            return False
        
        # Update config
        for key, value in new_params.items():
            task.config[key] = value
            
        # Update running strategy
        if task_id in self.strategies:
            self.strategies[task_id].update_params(new_params)
            logger.info(f"Updated active strategy params for {task_id}")
            
        # Persist
        task_persistence.save_tasks(self.tasks)
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
        
        # Start WebSocket subscriptions for CCXT gateways (to avoid REST rate limits)
        use_ws_a = hasattr(gw_a, 'subscribe_ticker')
        use_ws_b = hasattr(gw_b, 'subscribe_ticker')
        
        if use_ws_b and hasattr(gw_b, '_ws_task'):
            # Start WebSocket subscription for OKX/CCXT gateway
            await gw_b.subscribe_ticker()
            logger.info(f"{task_id}: Started WebSocket subscription for {task.exchange_b}")
            # Wait a moment for first tick
            await asyncio.sleep(0.5)
        
        while task.is_active:
            try:
                # Get ticks from both exchanges
                # For MT5: always use REST (get_ticker) as it's local
                # For CCXT/OKX: prefer cached WebSocket tick to avoid rate limits
                tick_a = await gw_a.get_ticker()
                
                # For gateway B (OKX), use cached last_tick from WebSocket if available
                if use_ws_b and gw_b.last_tick:
                    tick_b = gw_b.last_tick
                else:
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
                                "bar_time": int(datetime.now().timestamp() // 60) * 60,
                                "mt5_symbol": getattr(tick_a, 'symbol', ''),
                                "okx_symbol": getattr(tick_b, 'symbol', '')
                            })
                        
                        # Check signals (if auto trade enabled)
                        # Moved Back to Tick Loop, but using Last Closed K-Line EMA
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
        
        # Track last processed bar timestamp to avoid duplicates
        last_bar_ts = 0
        
        while task.is_active:
            try:
                # Wait for next minute + 5 seconds buffer (ensure bar is closed on both exchanges)
                now = datetime.now()
                wait_seconds = 60 - now.second + 5
                if wait_seconds > 60:
                    wait_seconds -= 60
                await asyncio.sleep(wait_seconds)
                
                # Fetch new kline with retry
                success = await self._update_kline_with_retry(task_id, last_bar_ts)
                if success:
                    # Update last processed timestamp
                    agg = self.aggregators.get(task_id)
                    if agg and agg.times:
                        try:
                            last_ts_str = agg.times[-1]
                            last_bar_ts = int(datetime.fromisoformat(last_ts_str.replace('Z', '+00:00')).timestamp())
                        except:
                            pass
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Kline loop error for {task_id}: {e}")
                await asyncio.sleep(5)
        
        logger.info(f"Kline loop stopped for {task_id}")

    async def _update_kline_with_retry(self, task_id: str, last_bar_ts: int, max_retries: int = 3) -> bool:
        """Update kline data with retry mechanism."""
        for attempt in range(max_retries):
            try:
                result = await self._update_kline(task_id, last_bar_ts)
                if result:
                    return True
                
                # Wait before retry
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                    
            except Exception as e:
                logger.warning(f"Kline update attempt {attempt + 1} failed for {task_id}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
        
        return False

    async def _update_kline(self, task_id: str, last_bar_ts: int = 0) -> bool:
        """Update kline data with improved reliability."""
        gateways = self.gateways.get(task_id, {})
        gw_a = gateways.get("a")
        gw_b = gateways.get("b")
        agg = self.aggregators.get(task_id)
        task = self.tasks.get(task_id)
        
        if not all([gw_a, gw_b, agg, task]):
            return False
        
        try:
            # Check market hours (MT5) before updating
            is_open = False
            if hasattr(gw_a, 'is_market_open'):
                if asyncio.iscoroutinefunction(gw_a.is_market_open):
                    is_open = await gw_a.is_market_open()
                else:
                    is_open = gw_a.is_market_open()
            
            if not is_open:
                 logger.debug(f"Market closed for {task_id}, skipping kline update")
                 return False

            # Current minute boundary (any bar >= this is still forming)
            now_ts = int(datetime.now(timezone.utc).timestamp())
            current_minute_start = (now_ts // 60) * 60

            # Get latest klines (1m) - fetch 5 bars for better reliability
            klines_a = await gw_a.get_klines(timeframe="1m", limit=5)
            klines_b = await gw_b.get_klines(timeframe="1m", limit=5)
            
            if not klines_a:
                logger.warning(f"{task_id}: MT5 returned no klines")
                return False
            if not klines_b:
                logger.warning(f"{task_id}: OKX returned no klines")
                return False
            
            # Filter out current (forming) bar from both exchanges
            # Keep only bars that started before current minute
            closed_a = [b for b in klines_a if (b.get('time') or 0) < current_minute_start]
            closed_b = [b for b in klines_b if (b.get('time') or 0) < current_minute_start]
            
            if not closed_a:
                logger.warning(f"{task_id}: MT5 has no closed bars (current_min={current_minute_start})")
                return False
            if not closed_b:
                logger.warning(f"{task_id}: OKX has no closed bars (current_min={current_minute_start})")
                return False
            
            # Use the most recent CLOSED bar from each exchange
            bar_a = closed_a[-1]
            ts_a = bar_a.get('time') or bar_a.get('ts')
            
            # Check if this bar was already processed
            if ts_a and ts_a <= last_bar_ts:
                logger.debug(f"{task_id}: Bar {ts_a} already processed")
                return False
            
            # Find matching OKX bar by timestamp
            bar_b = None
            ts_a_minute = (ts_a // 60) * 60 if ts_a else 0
            
            for okx_bar in reversed(closed_b):
                ts_b = okx_bar.get('time') or okx_bar.get('ts')
                ts_b_minute = (ts_b // 60) * 60 if ts_b else 0
                
                # Exact minute match
                if ts_a_minute == ts_b_minute:
                    bar_b = okx_bar
                    break
            
            if not bar_b:
                # If no exact match, use OKX's most recent closed bar
                bar_b = closed_b[-1]
                ts_b = bar_b.get('time') or 0
                logger.debug(f"{task_id}: No exact OKX match for MT5 ts={ts_a}, using OKX ts={ts_b}")
            
            # Get current tick for spread calculation (or use rolling average)
            avg_sell, avg_buy, _ = agg.get_rolling_average()
            
            # Use MT5 bar timestamp for consistency
            ts_val = ts_a or int(datetime.now().timestamp())
            # 转为东八区本地时间字符串，无时区后缀
            dt = datetime.fromtimestamp(ts_val, timezone.utc) + timedelta(hours=8)
            t_iso = dt.replace(tzinfo=None).isoformat()
            
            # 如果已存在该 timestamp，则替换为最新数据，否则正常添加
            if hasattr(agg, 'bars') and hasattr(agg, 'times'):
                if t_iso in agg.times:
                    idx = agg.times.index(t_iso)
                    bar = agg.add_bar(
                        timestamp=t_iso,
                        mt5_bar=bar_a,
                        okx_bar=bar_b,
                        spread_sell=avg_sell,
                        spread_buy=avg_buy
                    )
                    agg.bars[idx] = bar
                else:
                    bar = agg.add_bar(
                        timestamp=t_iso,
                        mt5_bar=bar_a,
                        okx_bar=bar_b,
                        spread_sell=avg_sell,
                        spread_buy=avg_buy
                    )
            else:
                bar = agg.add_bar(
                    timestamp=t_iso,
                    mt5_bar=bar_a,
                    okx_bar=bar_b,
                    spread_sell=avg_sell,
                    spread_buy=avg_buy
                )
            
            if self._on_bar:
                await self._on_bar(task_id, bar)
            
            logger.info(f"New bar for {task_id}: ts={t_iso}, spread={bar['spread']:.2f}, ema={bar['ema']:.2f}")
            return True
        
        except Exception as e:
            logger.error(f"Kline update error for {task_id}: {e}")
            return False
    
    async def _check_signals(self, task_id: str, spread_sell: float, spread_buy: float):
        """Check and execute trading signals."""
        task = self.tasks.get(task_id)
        strategy = self.strategies.get(task_id)
        agg = self.aggregators.get(task_id)
        gateways = self.gateways.get(task_id, {})
        gw_a = gateways.get("a")
        
        if not all([task, strategy, agg]):
            return
        
        # Check if dry run mode (skip market hours check in dry run)
        dry_run = task.config.get("dryRun", True)
        
        # Check market hours before trading (skip in dry run mode)
        if not dry_run and gw_a and hasattr(gw_a, 'is_market_open'):
            if asyncio.iscoroutinefunction(gw_a.is_market_open):
                is_open = await gw_a.is_market_open()
            else:
                is_open = gw_a.is_market_open()
            
            if not is_open:
                # Market is closed, skip signal check (real trading only)
                return
        
        ema = agg.last_ema
        
        # Check trade cooldown before any auto action
        # Minimum 60 seconds between auto trades to prevent signal confusion
        MIN_TRADE_INTERVAL = 60.0  # seconds
        if task.last_trade_at:
            elapsed = (datetime.now() - task.last_trade_at).total_seconds()
            if elapsed < MIN_TRADE_INTERVAL:
                # Still in cooldown, skip all auto signals
                return
        
        # Note: Take profit is disabled - manual close only
        
        # Check entry/add-on signals
        logger.debug(f"[{task_id}] check_signal: level={task.current_level}, dir='{task.direction}', ema={ema:.2f}, spread_buy={spread_buy:.2f}, spread_sell={spread_sell:.2f}")
        signal = strategy.check_signal(
            spread_sell, spread_buy, ema,
            task.current_level, task.direction
        )
        logger.debug(f"[{task_id}] signal result: type={signal.type}, level={signal.level}, reason={signal.reason}")
        
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
        
        # Note: Cooldown is now checked in _check_signals before calling this method

        # Track volumes for logging
        close_volume = 0.0
        open_volume = 0.0
        is_reversal = False
        
        target_level = signal.level
        
        # Check if this is a reversal (direction change)
        if task.has_position and task.direction != signal.type.value:
            # Reversal: need to close current position + open new position
            # Gateway will handle "close opposite first, then open new" automatically
            close_volume = base_volume * task.current_level
            open_volume = base_volume * target_level
            trade_volume = round(close_volume + open_volume, 2)
            is_reversal = True
            logger.info(f"Reversing position for {task_id}: {task.direction} L{task.current_level} -> {signal.type.value} L{target_level}")
            logger.info(f"  Total volume: {trade_volume} (close {close_volume} + open {open_volume})")
        else:
            # Same direction: just add to position
            current_level = task.current_level
            
            # Safety: don't trade if already at target level
            if current_level >= target_level and task.direction == signal.type.value:
                return
            
            levels_to_add = target_level - current_level
            if levels_to_add <= 0:
                return
            
            open_volume = base_volume * levels_to_add
            trade_volume = round(open_volume, 2)
            logger.info(f"Adding to position for {task_id}: L{current_level} -> L{target_level} (+{levels_to_add} levels)")
        
        # Calculate OKX volume for logging
        okx_volume = trade_volume * qty_multiplier
        
        logger.info(f"Executing signal {task_id}: {signal.type.value} L{target_level}. Volumes: MT5={trade_volume}, OKX={okx_volume}")
        
        result = await self.order_service.execute_arbitrage_orders(
            gateway_a=gw_a,
            gateway_b=gw_b,
            direction=signal.type.value,
            volume=trade_volume,
            dry_run=dry_run,
            qty_multiplier=qty_multiplier
        )
    
        # Recalculate PnL and trades count only on closing/full cycle
        # For now, just mark last_trade_at
        task.last_trade_at = datetime.now()
        
        task.current_level = target_level
        task.direction = signal.type.value
        
        # Persist task state (important for level tracking)
        task_persistence.save_tasks(self.tasks)
        
        # Get current tick prices for complete logging
        tick_a = gw_a.last_tick if hasattr(gw_a, 'last_tick') and gw_a.last_tick else None
        tick_b = gw_b.last_tick if hasattr(gw_b, 'last_tick') and gw_b.last_tick else None
        
        # Construct Composite Trade Log for Auto Trade
        # Build reason string with details
        if is_reversal:
            reason_str = f"自动反转: 平{close_volume}手 + 开{open_volume}手 | {signal.reason}"
        else:
            reason_str = signal.reason
        
        trade_record = {
            "task_id": task_id,
            "ts": datetime.now(timezone.utc).isoformat(),
            "order_id": int(datetime.now().timestamp() * 1000),
            "direction": signal.type.value,
            "level": signal.level,
            "vol": trade_volume,
            "status": bool(result.get("success", False)),
            "prices": {
                "spread_sell": getattr(signal, 'spread_sell', 0) or signal.spread,
                "spread_buy": getattr(signal, 'spread_buy', 0),
                "mt5_bid": tick_a.bid if tick_a else 0,
                "mt5_ask": tick_a.ask if tick_a else 0,
                "okx_bid": tick_b.bid if tick_b else 0,
                "okx_ask": tick_b.ask if tick_b else 0,
            },
            "ema": signal.ema if hasattr(signal, 'ema') else 0,
            "signal": {
                "trigger": "auto_reversal" if is_reversal else "auto",
                "action": signal.type.value,
                "level": signal.level,
                "reason": reason_str,
                "threshold": signal.threshold if hasattr(signal, 'threshold') else 0
            },
            "mt5_result": result.get("order_a", {}),
            "okx_result": result.get("order_b", {}),
            "error": None
        }
            
        self.order_service.log_composite_trade(trade_record)
        
        if self._on_trade:
            await self._on_trade(task_id, trade_record)
    async def execute_manual_trade(self, task_id: str, direction: str, volume: Optional[float] = None) -> Dict[str, Any]:
        """
        Execute a manual trade.
        Updates task state as if it were a Level 1 entry or increments level.
        """
        task = self.tasks.get(task_id)
        if not task:
            return {"success": False, "error": "Task not found"}
        
        # Trade cooldown check - prevent orders too fast
        MIN_TRADE_INTERVAL = 1.0  # 1 second for manual trades
        if task.last_trade_at:
            elapsed = (datetime.now() - task.last_trade_at).total_seconds()
            if elapsed < MIN_TRADE_INTERVAL:
                return {"success": False, "error": f"Trade too fast, wait {MIN_TRADE_INTERVAL - elapsed:.1f}s"}
            
        gateways = self.gateways.get(task_id, {})
        gw_a = gateways.get("a")
        gw_b = gateways.get("b")
        
        if not gw_a or not gw_b:
             return {"success": False, "error": "Gateways not ready"}

        # Config
        base_volume = float(task.config.get("tradeVolume", 0.01))
        qty_multiplier = float(task.config.get("qtyMultiplier", 100.0))
        dry_run = task.config.get("dryRun", True)
        
        open_volume = volume if volume else base_volume
        
        # Log current state for debugging
        logger.info(f"Manual trade check: has_position={task.has_position}, current_dir='{task.direction}', current_level={task.current_level}, requested_dir='{direction}'")
        
        # Track volumes for logging
        close_volume = 0.0
        is_reversal = False
        
        # Check if this is a reversal (direction change)
        # Gateway will handle "close opposite first, then open new" automatically
        if task.has_position and task.direction != direction:
             close_volume = base_volume * task.current_level
             trade_volume = round(close_volume + open_volume, 2)
             is_reversal = True
             logger.info(f"Manual reversal: {task.direction} L{task.current_level} -> {direction}")
             logger.info(f"  Total volume: {trade_volume} (close {close_volume} + open {open_volume})")
        else:
             trade_volume = open_volume
             logger.info(f"Manual trade: {direction} {trade_volume} lots")
        
        result = await self.order_service.execute_arbitrage_orders(
            gateway_a=gw_a,
            gateway_b=gw_b,
            direction=direction,
            volume=trade_volume,
            dry_run=dry_run,
            qty_multiplier=qty_multiplier
        )
        
             # Update Task State
             # For reversal: set to level 1 in new direction
             # For same direction: increment level
        if is_reversal:
            task.direction = direction
            task.current_level = 1
        elif task.direction == direction:
            task.current_level += 1
        else:
            # First position
            task.direction = direction
            task.current_level = 1
            
        task.last_trade_at = datetime.now()
        task_persistence.save_tasks(self.tasks)
        
        # Construct Composite Trade Log
        
        # Fetch current tickers for logging accurate prices
        tick_a = await gw_a.get_ticker()
        tick_b = await gw_b.get_ticker()
        
        current_prices = {
            "mt5_bid": tick_a.bid if tick_a else 0, 
            "mt5_ask": tick_a.ask if tick_a else 0, 
            "okx_bid": tick_b.bid if tick_b else 0, 
            "okx_ask": tick_b.ask if tick_b else 0,
        }
        
        # Calculate spreads from these ticks
        if tick_a and tick_b:
            current_prices["spread_sell"] = tick_a.bid - tick_b.ask
            current_prices["spread_buy"] = tick_a.ask - tick_b.bid
        else:
            current_prices["spread_sell"] = 0
            current_prices["spread_buy"] = 0
        
        # Build reason string with details
        if is_reversal:
            reason_str = f"手动反转: 平{close_volume}手 + 开{open_volume}手"
        else:
            reason_str = "手动交易"
        
        trade_record = {
            "task_id": task_id,
            "ts": datetime.now(timezone.utc).isoformat(),
            "order_id": int(datetime.now().timestamp() * 1000),
            "direction": direction,
            "level": task.current_level,
            "vol": trade_volume,
            "status": bool(result.get("success", False)),
            "prices": current_prices,
            "ema": 0, # Placeholder
            "signal": {
                "trigger": "manual_reversal" if is_reversal else "manual",
                "action": direction,
                "level": task.current_level,
                "reason": reason_str
            },
            "mt5_result": result.get("order_a", {}),
            "okx_result": result.get("order_b", {}),
            "error": None
        }
        
        # Get latest market data if available
        agg = self.aggregators.get(task_id)
        if agg:
            trade_record["ema"] = agg.last_ema
            # We could fetch latest spreads from agg or gateways if needed
        
        self.order_service.log_composite_trade(trade_record)
        
        if self._on_trade:
            await self._on_trade(task_id, trade_record)
        
        return result
    
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
                 "task_id": task_id,
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

    async def close_all_positions(self):
        """批量全平所有任务的持仓（OKX/MT5）"""
        for task_id in self.tasks.keys():
            await self._close_position(task_id)
