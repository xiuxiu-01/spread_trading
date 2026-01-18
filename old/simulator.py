import json
import pandas as pd

def cost_to_fill_from_ob(ob, side, size):
    remaining = float(size)
    cost = 0.0
    filled = 0.0
    levels = ob['asks'] if side == 'buy' else ob['bids']
    for price, vol in levels:
        take = min(remaining, vol)
        cost += take * price
        filled += take
        remaining -= take
        if remaining <= 1e-12:
            break
    if remaining > 1e-12:
        if levels:
            last_price = levels[-1][0]
            cost += remaining * last_price
            filled += remaining
            remaining = 0.0
    avg_price = cost / filled if filled>0 else None
    return {'filled': filled, 'avg_price': avg_price, 'not_filled': remaining}


def simulate_roundtrip(side, size, mt5_price, okx_ob, config):
    fee_okx = config.get('fee_okx', 0.001)
    fee_mt5 = config.get('fee_mt5', 0.0005)

    if side == 'sell_mt5_buy_okx':
        exec_price_mt5 = float(mt5_price['bid'])
        okx_fill = cost_to_fill_from_ob(okx_ob, 'buy', size)
        exec_price_okx = okx_fill['avg_price']
        if exec_price_okx is None:
            return {'net': -9999, 'reason': 'no depth'}
        gross = (exec_price_mt5 - exec_price_okx) * float(size)
    else:
        exec_price_mt5 = float(mt5_price['ask'])
        okx_fill = cost_to_fill_from_ob(okx_ob, 'sell', size)
        exec_price_okx = okx_fill['avg_price']
        if exec_price_okx is None:
            return {'net': -9999, 'reason': 'no depth'}
        gross = (exec_price_okx - exec_price_mt5) * float(size)

    fee_cost = (abs(exec_price_okx) * float(size) * fee_okx) + (abs(exec_price_mt5) * float(size) * fee_mt5)
    net = gross - fee_cost
    details = {
        'exec_price_mt5': exec_price_mt5,
        'exec_price_okx': exec_price_okx,
        'gross': gross,
        'fee_cost': fee_cost,
        'net': net,
        'depth_filled': okx_fill
    }
    return details


def run_backtest_from_file(filepath, config):
    times = []
    pnls = []
    equity = [0.0]
    with open(filepath,'r') as f:
        for line in f:
            obj = json.loads(line)
            okx = obj.get('okx')
            mt5p = obj.get('mt5')
            if not okx or not mt5p:
                continue
            mt5_mid = (float(mt5p['bid']) + float(mt5p['ask']))/2.0
            okx_mid = (okx['bids'][0][0] + okx['asks'][0][0]) / 2.0 if okx['bids'] and okx['asks'] else None
            if okx_mid is None:
                continue
            spread = mt5_mid - okx_mid
            ts = obj.get('ts')
            if spread > config['spread_threshold']:
                res = simulate_roundtrip('sell_mt5_buy_okx', config['order_size'], mt5p, okx, config)
            elif spread < -config['spread_threshold']:
                res = simulate_roundtrip('buy_mt5_sell_okx', config['order_size'], mt5p, okx, config)
            else:
                res = None
            if res and 'net' in res:
                pnl = res['net']
                pnls.append(pnl)
                equity.append(equity[-1] + pnl)
                times.append(ts)
    if len(equity) <= 1:
        print('No trades in backtest')
        return
    df = pd.DataFrame({'ts': [pd.to_datetime(t) for t in times], 'pnl': pnls})
    df.set_index('ts', inplace=True)
    eq = pd.Series(equity[1:], index=df.index)
    running_max = eq.cummax()
    drawdown = eq - running_max
    max_dd = drawdown.min()
    total = eq.iloc[-1]
    avg = df['pnl'].mean()
    wins = (df['pnl']>0).sum()
    print('BACKTEST RESULTS - total:', total, 'avg:', avg, 'trades:', len(df), 'wins:', wins, 'max_dd:', max_dd)
    return {'df': df, 'equity': eq}
