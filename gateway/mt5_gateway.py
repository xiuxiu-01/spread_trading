import MetaTrader5 as mt5
from typing import Optional, Dict, Any, Iterator
from dotenv import load_dotenv
load_dotenv()

from datetime import datetime, timedelta, timezone
from typing import List
from pytz import timezone as pytz_timezone

class MT5Gateway:
    """Unified MT5 gateway for market data and trading."""
    def __init__(self, symbol: str = 'XAU'):
        self.symbol = symbol
        if not mt5.initialize():
            raise RuntimeError(f'mt5 initialize failed: {mt5.last_error()}')
        mt5.symbol_select(self.symbol, True)

    def shutdown(self) -> None:
        try:
            mt5.shutdown()
        except Exception:
            pass

    # Market data
    def get_last_price(self) -> Optional[float]:
        tick = mt5.symbol_info_tick(self.symbol)
        if tick is None:
            return None
        return float(tick.last) if tick.last > 0 else (tick.bid + tick.ask) / 2.0

    def get_tick(self) -> Optional[Dict[str, float]]:
        tick = mt5.symbol_info_tick(self.symbol)
        if tick is None:
            return None
        return {'bid': float(tick.bid), 'ask': float(tick.ask)}

    def stream_ticks(self) -> Iterator[Dict[str, float]]:
        import time
        while True:
            t = self.get_tick()
            if t:
                # Add 'last' price if available
                tick = mt5.symbol_info_tick(self.symbol)
                if tick and tick.last > 0:
                     t['last'] = float(tick.last)
                yield t
            time.sleep(0.5)

    # Trading
    def get_balance(self) -> float:
        """Return account equity (or balance if preferred)"""
        if not mt5.initialize():
             return 0.0
        info = mt5.account_info()
        if info is None:
            return 0.0
        return float(info.equity)

    def get_positions(self) -> List[Dict[str, Any]]:
        """Return list of open positions for the symbol."""
        if not mt5.initialize():
            return []
        positions = mt5.positions_get(symbol=self.symbol)
        if positions is None:
            return []
        
        out = []
        for p in positions:
            # p is a named tuple
            out.append({
                'ticket': p.ticket,
                'time': p.time,
                'type': 'buy' if p.type == mt5.POSITION_TYPE_BUY else 'sell',
                'volume': p.volume,
                'price_open': p.price_open,
                'price_current': p.price_current,
                'profit': p.profit,
                'comment': p.comment
            })
        return out

    def place_market_order(self, side: str, volume: float, deviation: int = 20, fill_mode: int = mt5.ORDER_FILLING_IOC):
        """Place a market order. In Hedge mode, will close opposite positions first before opening new ones."""
        tick = mt5.symbol_info_tick(self.symbol)
        if tick is None:
            return None
        
        # Check account mode: 2 = Hedge mode
        account_info = mt5.account_info()
        is_hedge_mode = account_info and account_info.margin_mode == 2
        
        remaining_volume = float(volume)
        results = []
        
        if is_hedge_mode:
            # In Hedge mode, first close opposite positions
            positions = mt5.positions_get(symbol=self.symbol)
            if positions:
                # Determine opposite position type
                # If we want to buy, close sells first; if we want to sell, close buys first
                opposite_type = mt5.POSITION_TYPE_SELL if side == 'buy' else mt5.POSITION_TYPE_BUY
                
                for pos in positions:
                    if pos.type == opposite_type and remaining_volume > 0:
                        close_volume = min(pos.volume, remaining_volume)
                        close_price = float(tick.ask) if side == 'buy' else float(tick.bid)
                        
                        # Close position by specifying position ticket
                        close_request = {
                            'action': mt5.TRADE_ACTION_DEAL,
                            'symbol': self.symbol,
                            'volume': close_volume,
                            'type': mt5.ORDER_TYPE_BUY if side == 'buy' else mt5.ORDER_TYPE_SELL,
                            'position': pos.ticket,  # Specify position to close
                            'price': close_price,
                            'deviation': deviation,
                            'magic': 234000,
                            'comment': 'gateway-close',
                            'type_filling': fill_mode,
                            'type_time': mt5.ORDER_TIME_GTC,
                        }
                        
                        close_result = mt5.order_send(close_request)
                        results.append(close_result)
                        
                        if close_result and close_result.retcode == 10009:
                            print(f"[MT5] Closed position #{pos.ticket}: {close_volume} lots")
                            remaining_volume -= close_volume
                        else:
                            error_code = close_result.retcode if close_result else 'None'
                            error_comment = getattr(close_result, 'comment', 'N/A') if close_result else 'N/A'
                            print(f"[MT5] Failed to close position #{pos.ticket}: retcode={error_code}, comment={error_comment}")
        
        # If there's remaining volume, open new position
        if remaining_volume > 0.001:  # Avoid dust orders
            price = float(tick.ask) if side == 'buy' else float(tick.bid)
            order_type = mt5.ORDER_TYPE_BUY if side == 'buy' else mt5.ORDER_TYPE_SELL
            request = {
                'action': mt5.TRADE_ACTION_DEAL,
                'symbol': self.symbol,
                'volume': float(remaining_volume),
                'type': order_type,
                'price': price,
                'deviation': deviation,
                'magic': 234000,
                'comment': 'gateway',
                'type_filling': fill_mode,
                'type_time': mt5.ORDER_TIME_GTC,
            }
            result = mt5.order_send(request)
            results.append(result)
            
            if result and result.retcode == 10009:
                print(f"[MT5] Opened new position: {side} {remaining_volume} lots")
            else:
                error_code = result.retcode if result else 'None'
                error_comment = getattr(result, 'comment', 'N/A') if result else 'N/A'
                print(f"[MT5] Failed to open position: retcode={error_code}, comment={error_comment}")
        
        # Return the last successful result, or last result if none succeeded
        for r in reversed(results):
            if r and hasattr(r, 'retcode') and r.retcode == 10009:
                return r
        return results[-1] if results else None

    def place_limit_order(self, side: str, volume: float, price: float, deviation: int = 20,
                           fill_mode: int = mt5.ORDER_FILLING_IOC,
                           time_type: int = mt5.ORDER_TIME_GTC,
                           expiration: Optional[int] = None):
        """Place a pending limit order (buy_limit/sell_limit) on MT5.
        side: 'buy' or 'sell'
        volume: lot size
        price: limit price
        deviation: allowed slippage for placement
        fill_mode: filling mode (IOC by default)
        time_type: ORDER_TIME_GTC or ORDER_TIME_SPECIFIED
        expiration: Unix timestamp for ORDER_TIME_SPECIFIED (optional)
        """
        order_type = mt5.ORDER_TYPE_BUY_LIMIT if side == 'buy' else mt5.ORDER_TYPE_SELL_LIMIT
        request = {
            'action': mt5.TRADE_ACTION_PENDING,
            'symbol': self.symbol,
            'volume': float(volume),
            'type': order_type,
            'price': float(price),
            'deviation': deviation,
            'magic': 234001,
            'comment': 'gateway-limit',
            'type_filling': fill_mode,
            'type_time': time_type,
        }
        if time_type == mt5.ORDER_TIME_SPECIFIED and expiration:
            request['expiration'] = int(expiration)
        return mt5.order_send(request)

    def server_time_utc_offset(self) -> Optional[timedelta]:
        """Best-effort: estimate MT5 trade server UTC offset using tick time.

        Returns a timedelta like +02:00. If tick is unavailable, returns None.
        """
        tick = mt5.symbol_info_tick(self.symbol)
        if tick is None:
            return None
        # tick.time is seconds since epoch in server time representation.
        # In practice, MetaTrader5 python returns epoch seconds; comparing to system UTC gives offset.
        server_dt = datetime.fromtimestamp(int(tick.time))
        utc_dt = datetime.utcfromtimestamp(int(tick.time))
        return server_dt - utc_dt

    def server_now(self) -> datetime:
        """Current server time as naive datetime (best-effort)."""
        off = self.server_time_utc_offset() or timedelta(0)
        return datetime.utcnow() + off

    def fetch_ohlcv_1m(self, minutes: int = 500, end: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Fetch historical 1-minute bars from MT5 using *server time*.

        - minutes: number of 1m candles to fetch
        - end: server-time end (naive datetime). Defaults to server_now().

        Returns list of dicts: {ts, open, high, low, close, vol}
        where ts is ISO string (UTC) at bar open. (Corrected from server time)
        """
        if minutes <= 0:
            return []
        
        # Get offset for collection adjustment
        import os
        env_offset = os.getenv('MT5_UTC_OFFSET_HOURS')
        if env_offset is not None:
             offset_hours = float(env_offset)
        else:
             offset_hours = self._estimate_server_offset()
        offset_delta = timedelta(hours=offset_hours)

        # "end" parameter is usually assumed to be server time if passed, or now()
        # But for consistency, let's work backwards from "now"
        end_server = end or (datetime.utcnow() + offset_delta)
        start_server = end_server - timedelta(minutes=int(minutes))

        rates = mt5.copy_rates_range(self.symbol, mt5.TIMEFRAME_M1, start_server, end_server)
        if rates is None:
            return []

        out: List[Dict[str, Any]] = []
        for r in rates:
            # r['time'] is epoch seconds in server time.
            # We want to output UTC ISO string.
            server_ts_int = int(r['time'])
            # Convert server timestamp to UTC timestamp
            utc_ts_int = server_ts_int - int(offset_hours * 3600)
            
            ts_utc = datetime.utcfromtimestamp(utc_ts_int).replace(second=0, microsecond=0)
            
            out.append({
                'ts': ts_utc.replace(tzinfo=timezone.utc).isoformat(), # Explicitly UTC aware
                'open': float(r['open']),
                'high': float(r['high']),
                'low': float(r['low']),
                'close': float(r['close']),
                'vol': int(r.get('tick_volume', 0)),
            })
        return out

    def _estimate_server_offset(self) -> float:
        """Estimate the server offset in hours based on current tick vs UTC time."""
        tick = mt5.symbol_info_tick(self.symbol)
        if tick is None:
            return 0.0
        
        # Current UTC time
        now_utc = datetime.now(timezone.utc).timestamp()
        
        # Tick time (server time in seconds)
        tick_ts = float(tick.time)
        
        diff_seconds = tick_ts - now_utc
        
        # If the tick is older than 10 minutes, market might be closed or low liquidity. 
        # Trusting the offset calc is risky if the gap is large (e.g. weekend).
        if abs(diff_seconds) > 3600 * 10: # > 10 hours discrepancy usually means old tick
            print(f"[MT5] Warning: Last tick is old ({int(diff_seconds/3600)}h ago). Cannot auto-detect timezone offset safely. Defaulting to 0. Set MT5_UTC_OFFSET_HOURS in .env to fix.")
            return 0.0
            
        # Round to nearest half-hour
        hours = round(diff_seconds / 1800) / 2.0
        
        print(f"[MT5] Auto-detected server offset: {hours} hours (based on tick delay {diff_seconds:.1f}s)")
        return hours

    def get_historical_data(self, start: datetime, end: datetime, tz: str = 'UTC') -> List[Dict[str, Any]]:
        """Fetch historical minute data for the symbol, aligned to the specified timezone."""
        
        # Get configured UTC offset for MT5 server
        import os
        env_offset = os.getenv('MT5_UTC_OFFSET_HOURS')
        
        if env_offset is not None:
             offset_hours = float(env_offset)
        else:
             offset_hours = self._estimate_server_offset()
             
        offset_delta = timedelta(hours=offset_hours)

        # Ensure input datetimes are naive UTC
        def to_naive_utc(dt):
            if dt.tzinfo:
                return dt.astimezone(pytz_timezone('UTC')).replace(tzinfo=None)
            return dt

        start_naive = to_naive_utc(start)
        end_naive = to_naive_utc(end)

        # Adjust query times to Server Time
        # If we want 10:00 UTC and server is +2, we ask for 12:00 Server
        start_server = start_naive + offset_delta
        end_server = end_naive + offset_delta

        # Calculate how many bars we need
        minutes_diff = int((end_naive - start_naive).total_seconds() / 60)
        bar_count = max(minutes_diff + 5, 20)  # Extra buffer for safety
        
        # Use copy_rates_from_pos to get recent bars (position 0 = current bar)
        # This is more reliable than copy_rates_from for getting recent data
        rates = mt5.copy_rates_from_pos(self.symbol, mt5.TIMEFRAME_M1, 0, bar_count)
        
        if rates is None or len(rates) == 0:
            # Not raising error, return empty list to be safe
            print(f"Warning: Failed to fetch MT5 historical data: {mt5.last_error()}")
            return []
        
        # Filter bars within our time range using calendar.timegm (UTC-aware)
        import calendar
        start_ts = calendar.timegm(start_server.timetuple())
        end_ts = calendar.timegm(end_server.timetuple())
        
        filtered_rates = [r for r in rates if start_ts <= int(r['time']) < end_ts]
        
        if not filtered_rates:
            # If no bars match exact filter, get bars closest to end_time
            filtered_rates = [r for r in rates if int(r['time']) < end_ts][-bar_count:]

        out = []
        # Pre-calc offset in seconds for adjustment back to UTC
        offset_seconds = int(offset_hours * 3600)

        for r in filtered_rates:
            # r['time'] is Server Time (e.g. 12:00 for a 10:00 UTC bar).
            # We want to return UTC timestamp (10:00).
            server_ts = int(r['time'])
            utc_ts = server_ts - offset_seconds

            # Explicitly cast numpy types to standard python types for JSON serialization
            out.append({
                'time': utc_ts, 
                'open': float(r['open']),
                'high': float(r['high']),
                'low': float(r['low']),
                'close': float(r['close']),
                'volume': float(r['tick_volume']),
            })
            
        return out

    def is_market_open(self) -> bool:
        """Check if the market (Forex/Metals) is currently open in UTC time."""
        # IC Markets daily maintenance:
        # Beijing 05:55 - 07:01 = UTC 21:55 - 23:01 (daily)
        # Weekend: Friday UTC 22:00 close, Sunday UTC 23:01 open
        
        now = datetime.now(timezone.utc)
        weekday = now.weekday()  # Mon=0, Sun=6
        hour = now.hour
        minute = now.minute
        
        # Daily maintenance window: UTC 21:55 - 23:01 (Beijing 05:55 - 07:01)
        if hour == 21 and minute >= 55:
            return False
        if hour == 22:
            return False
        if hour == 23 and minute < 1:
            return False
        
        # Saturday: Closed all day
        if weekday == 5:
            return False
            
        # Sunday: Closed before 23:01 UTC (Beijing 07:01 Monday)
        if weekday == 6:
            return False
        
        # Friday after 22:00 UTC: Closed for weekend
        if weekday == 4 and hour >= 22:
            return False
            
        return True
