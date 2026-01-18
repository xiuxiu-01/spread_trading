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
                yield t
            time.sleep(0.5)

    # Trading
    def place_market_order(self, side: str, volume: float, deviation: int = 20, fill_mode: int = mt5.ORDER_FILLING_IOC):
        tick = mt5.symbol_info_tick(self.symbol)
        if tick is None:
            return None
        price = float(tick.ask) if side == 'buy' else float(tick.bid)
        order_type = mt5.ORDER_TYPE_BUY if side == 'buy' else mt5.ORDER_TYPE_SELL
        request = {
            'action': mt5.TRADE_ACTION_DEAL,
            'symbol': self.symbol,
            'volume': float(volume),
            'type': order_type,
            'price': price,
            'deviation': deviation,
            'magic': 234000,
            'comment': 'gateway',
            'type_filling': fill_mode,
            'type_time': mt5.ORDER_TIME_GTC,
        }
        return mt5.order_send(request)

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
        where ts is ISO string (server time) at bar open.
        """
        if minutes <= 0:
            return []

        end_server = end or self.server_now()
        start_server = end_server - timedelta(minutes=int(minutes))

        rates = mt5.copy_rates_range(self.symbol, mt5.TIMEFRAME_M1, start_server, end_server)
        if rates is None:
            return []

        out: List[Dict[str, Any]] = []
        for r in rates:
            # r['time'] is epoch seconds; interpret as server-local timestamp.
            ts = datetime.fromtimestamp(int(r['time'])).replace(second=0, microsecond=0)
            out.append({
                'ts': ts.isoformat(),
                'open': float(r['open']),
                'high': float(r['high']),
                'low': float(r['low']),
                'close': float(r['close']),
                'vol': int(r.get('tick_volume', 0)),
            })
        return out

    def get_historical_data(self, start: datetime, end: datetime, tz: str = 'UTC') -> List[Dict[str, Any]]:
        """Fetch historical minute data for the symbol, aligned to the specified timezone."""
        
        # Ensure input datetimes are naive UTC if they don't have timezone info (assuming caller provides UTC)
        # or convert to UTC if they are aware.
        def to_naive_utc(dt):
            if dt.tzinfo:
                return dt.astimezone(pytz_timezone('UTC')).replace(tzinfo=None)
            return dt

        start_naive = to_naive_utc(start)
        end_naive = to_naive_utc(end)

        rates = mt5.copy_rates_range(self.symbol, mt5.TIMEFRAME_M1, start_naive, end_naive)
        
        if rates is None:
            # Not raising error, return empty list to be safe
            print(f"Warning: Failed to fetch MT5 historical data: {mt5.last_error()}")
            return []

        out = []
        for r in rates:
            # Convert timestamp to seconds
            # Explicitly cast numpy types to standard python types for JSON serialization
            out.append({
                'time': int(r['time']), # Assuming this is Unix timestamp in seconds
                'open': float(r['open']),
                'high': float(r['high']),
                'low': float(r['low']),
                'close': float(r['close']),
                'volume': float(r['tick_volume']),
            })
            
        return out
