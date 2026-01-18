import MetaTrader5 as mt5

DEFAULT_SYMBOL = 'XAU'

def init_mt5():
    if not mt5.initialize():
        raise RuntimeError(f'mt5 initialize failed: {mt5.last_error()}')

def shutdown_mt5():
    try:
        mt5.shutdown()
    except Exception:
        pass

def get_tick(symbol=DEFAULT_SYMBOL):
    mt5.symbol_select(symbol, True)
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return None
    return {'bid': float(tick.bid), 'ask': float(tick.ask)}

def place_market_order(side, volume, symbol=DEFAULT_SYMBOL, deviation=20):
    mt5.symbol_select(symbol, True)
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return None
    price = float(tick.ask) if side == 'buy' else float(tick.bid)
    order_type = mt5.ORDER_TYPE_BUY if side == 'buy' else mt5.ORDER_TYPE_SELL
    request = {
        'action': mt5.TRADE_ACTION_DEAL,
        'symbol': symbol,
        'volume': float(volume),
        'type': order_type,
        'price': price,
        'deviation': deviation,
        'magic': 234000,
        'comment': 'arb_bot',
        'type_filling': mt5.ORDER_FILLING_IOC,  # changed from FOK to IOC
        'type_time': mt5.ORDER_TIME_GTC,
    }
    return mt5.order_send(request)

def stream_ticks(symbol=DEFAULT_SYMBOL, interval_sec=0.25):
    """Yield real-time ticks by polling symbol_info_tick at interval.
    This is simple and reliable if broker doesn't expose Market Book.
    """
    mt5.symbol_select(symbol, True)
    last = None
    while True:
        tick = mt5.symbol_info_tick(symbol)
        if tick:
            snap = {'bid': float(tick.bid), 'ask': float(tick.ask), 'time': int(getattr(tick, 'time_msc', 0))}
            if snap != last:
                last = snap
                yield snap
        mt5.sleep(int(interval_sec * 1000))
