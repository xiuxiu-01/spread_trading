"""
计算两个交易所的交易盈亏

功能：
1. 获取MT5的历史成交记录 (history_deals_get)
2. 获取OKX的历史成交记录 (fetch_my_trades)
3. 计算总盈亏

用法：
    python bot/calc_pnl.py                    # 默认获取最近7天
    python bot/calc_pnl.py --days 30          # 获取最近30天
    python bot/calc_pnl.py --from 2026-02-01  # 从指定日期开始
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from dotenv import load_dotenv
load_dotenv()

import MetaTrader5 as mt5

SYMBOL_MT5 = os.getenv('SYMBOL_MT5', 'XAUUSD')
SYMBOL_OKX = os.getenv('SYMBOL_OKX', 'XAU/USDT:USDT')

# OKX 1 contract = 0.001 oz
OKX_CONTRACT_SIZE = 0.001


def get_mt5_deals(from_date: datetime, to_date: datetime) -> List[Dict[str, Any]]:
    """获取MT5历史成交记录"""
    if not mt5.initialize():
        print("MT5 初始化失败")
        return []
    
    # 获取指定时间范围内的所有成交
    deals = mt5.history_deals_get(from_date, to_date, group=f"*{SYMBOL_MT5}*")
    
    if deals is None or len(deals) == 0:
        print(f"MT5 没有找到成交记录")
        return []
    
    result = []
    for d in deals:
        # 过滤掉余额操作等非交易记录
        # DEAL_TYPE_BUY = 0, DEAL_TYPE_SELL = 1
        if d.type not in [0, 1]:
            continue
            
        result.append({
            'ticket': d.ticket,
            'order': d.order,
            'time': datetime.fromtimestamp(d.time, tz=timezone.utc),
            'type': 'buy' if d.type == 0 else 'sell',
            'volume': d.volume,
            'price': d.price,
            'profit': d.profit,
            'commission': d.commission,
            'swap': d.swap,
            'fee': d.fee if hasattr(d, 'fee') else 0,
            'symbol': d.symbol,
            'comment': d.comment,
            'entry': 'in' if d.entry == 0 else ('out' if d.entry == 1 else 'inout'),
        })
    
    return result


def get_okx_trades(from_date: datetime, to_date: datetime) -> List[Dict[str, Any]]:
    """获取OKX历史成交记录（使用fills-history API分页获取全部）"""
    import ccxt
    
    client = ccxt.okx({
        'apiKey': os.getenv('OKX_API_KEY'),
        'secret': os.getenv('OKX_API_SECRET'),
        'password': os.getenv('OKX_API_PASSPHRASE'),
        'options': {'defaultType': 'swap'},
    })
    
    all_trades = []
    from_ts = int(from_date.timestamp() * 1000)
    to_ts = int(to_date.timestamp() * 1000)
    
    # 转换symbol格式: XAU/USDT:USDT -> XAU-USDT-SWAP
    inst_id = SYMBOL_OKX.replace('/', '-').replace(':USDT', '-SWAP')
    
    try:
        # 使用 privateGetTradeFillsHistory API (最近3个月的记录)
        page = 0
        before = ''  # 用于分页的游标
        
        while True:
            page += 1
            params = {
                'instType': 'SWAP',
                'instId': inst_id,
                'limit': '100',
                'begin': str(from_ts),  # 开始时间戳(ms)
                'end': str(to_ts),      # 结束时间戳(ms)
            }
            if before:
                params['before'] = before  # 获取更早的记录
            
            print(f"  获取OKX fills-history 第{page}页...")
            result = client.privateGetTradeFillsHistory(params)
            data = result.get('data', [])
            
            if not data:
                print(f"  第{page}页无数据，结束")
                break
            
            added = 0
            out_of_range = 0
            for d in data:
                ts = int(d.get('fillTime', 0))
                if ts < from_ts:
                    out_of_range += 1
                    continue  # 跳过范围外的（太早）
                if ts > to_ts:
                    continue  # 跳过范围外的（太晚）
                
                fill_sz = float(d.get('fillSz', 0))  # 合约数量
                fill_px = float(d.get('fillPx', 0))  # 成交价格
                fee = float(d.get('fee', 0))         # 手续费（负数）
                fill_pnl = float(d.get('fillPnl', 0))  # 已实现盈亏
                
                # 计算oz数量 (1合约 = 0.001 oz)
                amount_oz = fill_sz * OKX_CONTRACT_SIZE
                # 计算成交额
                cost = amount_oz * fill_px
                
                all_trades.append({
                    'id': d.get('tradeId'),
                    'order': d.get('ordId'),
                    'time': datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
                    'side': d.get('side'),  # 'buy' or 'sell'
                    'amount': fill_sz,      # 合约数
                    'amount_oz': amount_oz, # oz数量
                    'price': fill_px,
                    'cost': cost,           # 成交金额 (USDT)
                    'fee': fee,             # 手续费
                    'pnl': fill_pnl,        # 已实现盈亏
                    'billId': d.get('billId'),
                })
                added += 1
            
            print(f"  第{page}页获取 {len(data)} 条，有效 {added} 条，超范围 {out_of_range} 条")
            
            # 如果这一页全部超出时间范围（太早），则停止分页
            if out_of_range == len(data):
                print(f"  全部超出时间范围，结束分页")
                break
            
            # 使用最后一条的billId作为下一页的游标
            if data:
                before = data[-1].get('billId', '')
                
                # 如果返回不足100条，说明没有更多数据
                if len(data) < 100:
                    print(f"  返回不足100条，结束分页")
                    break
            else:
                break
                
    except Exception as e:
        print(f"获取OKX交易记录失败: {e}")
    
    return all_trades


def calculate_mt5_pnl(deals: List[Dict]) -> Dict:
    """计算MT5盈亏"""
    total_profit = 0.0
    total_commission = 0.0
    total_swap = 0.0
    buy_count = 0
    sell_count = 0
    total_volume = 0.0
    
    for d in deals:
        total_profit += d['profit']
        total_commission += d['commission']
        total_swap += d['swap']
        total_volume += d['volume']
        if d['type'] == 'buy':
            buy_count += 1
        else:
            sell_count += 1
    
    net_pnl = total_profit + total_commission + total_swap
    
    return {
        'total_profit': total_profit,
        'total_commission': total_commission,
        'total_swap': total_swap,
        'net_pnl': net_pnl,
        'trade_count': len(deals),
        'buy_count': buy_count,
        'sell_count': sell_count,
        'total_volume': total_volume,
    }


def calculate_okx_pnl(trades: List[Dict]) -> Dict:
    """计算OKX盈亏（使用API返回的fillPnl字段）"""
    total_fee = 0.0
    total_cost = 0.0
    total_pnl = 0.0
    buy_count = 0
    sell_count = 0
    total_amount_oz = 0.0
    
    for t in trades:
        # 累计费用（API返回的是负数）
        fee = float(t.get('fee', 0) or 0)
        total_fee += abs(fee)
        
        # 累计已实现盈亏
        pnl = float(t.get('pnl', 0) or 0)
        total_pnl += pnl
        
        total_cost += t.get('cost', 0)
        total_amount_oz += t.get('amount_oz', 0)
        
        if t['side'] == 'buy':
            buy_count += 1
        else:
            sell_count += 1
    
    # 净盈亏 = 已实现盈亏 - 手续费 (手续费已经是负数，所以用abs)
    net_pnl = total_pnl - total_fee
    
    return {
        'total_pnl': total_pnl,      # 已实现盈亏
        'total_fee': total_fee,       # 手续费
        'net_pnl': net_pnl,           # 净盈亏
        'total_cost': total_cost,
        'trade_count': len(trades),
        'buy_count': buy_count,
        'sell_count': sell_count,
        'total_amount_oz': total_amount_oz,
    }


def print_deals_table(deals: List[Dict], title: str):
    """打印成交明细表格"""
    print(f"\n{'='*80}")
    print(f"{title}")
    print(f"{'='*80}")
    
    if not deals:
        print("无记录")
        return
    
    print(f"{'时间':<22} {'类型':<6} {'数量':<10} {'价格':<12} {'盈亏':<12} {'备注'}")
    print(f"{'-'*80}")
    
    for d in deals[:50]:  # 只显示前50条
        time_str = d['time'].strftime('%Y-%m-%d %H:%M:%S')
        if 'profit' in d:
            # MT5 格式
            print(f"{time_str:<22} {d['type']:<6} {d['volume']:<10.2f} {d['price']:<12.2f} {d['profit']:<12.2f} {d.get('comment', '')}")
        else:
            # OKX 格式
            print(f"{time_str:<22} {d['side']:<6} {d['amount_oz']:<10.4f} {d['price']:<12.2f} {'N/A':<12} {d.get('symbol', '')}")
    
    if len(deals) > 50:
        print(f"... 还有 {len(deals) - 50} 条记录未显示")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='计算两个交易所的交易盈亏')
    parser.add_argument('--days', type=int, default=7, help='获取最近N天的记录')
    parser.add_argument('--from', dest='from_date', type=str, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--to', dest='to_date', type=str, help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--detail', action='store_true', help='显示详细成交记录')
    
    args = parser.parse_args()
    
    # 计算时间范围
    to_date = datetime.now(timezone.utc)
    if args.to_date:
        to_date = datetime.strptime(args.to_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        to_date = to_date.replace(hour=23, minute=59, second=59)
    
    if args.from_date:
        from_date = datetime.strptime(args.from_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    else:
        from_date = to_date - timedelta(days=args.days)
    
    print(f"\n📊 交易盈亏统计")
    print(f"时间范围: {from_date.strftime('%Y-%m-%d %H:%M')} 至 {to_date.strftime('%Y-%m-%d %H:%M')} UTC")
    print(f"MT5 品种: {SYMBOL_MT5}")
    print(f"OKX 品种: {SYMBOL_OKX}")
    
    # 获取MT5成交
    print(f"\n正在获取 MT5 成交记录...")
    mt5_deals = get_mt5_deals(from_date, to_date)
    print(f"找到 {len(mt5_deals)} 条 MT5 成交记录")
    
    # 获取OKX成交
    print(f"\n正在获取 OKX 成交记录...")
    okx_trades = get_okx_trades(from_date, to_date)
    print(f"找到 {len(okx_trades)} 条 OKX 成交记录")
    
    # 显示详细记录
    if args.detail:
        print_deals_table(mt5_deals, "MT5 成交明细")
        print_deals_table(okx_trades, "OKX 成交明细")
    
    # 计算MT5盈亏
    mt5_pnl = calculate_mt5_pnl(mt5_deals)
    
    # 计算OKX费用统计
    okx_stats = calculate_okx_pnl(okx_trades)
    
    # 打印汇总
    print(f"\n{'='*60}")
    print(f"📈 MT5 盈亏统计 ({SYMBOL_MT5})")
    print(f"{'='*60}")
    print(f"  交易笔数:    {mt5_pnl['trade_count']} (买入: {mt5_pnl['buy_count']}, 卖出: {mt5_pnl['sell_count']})")
    print(f"  总交易量:    {mt5_pnl['total_volume']:.2f} 手")
    print(f"  交易盈亏:    ${mt5_pnl['total_profit']:.2f}")
    print(f"  手续费:      ${mt5_pnl['total_commission']:.2f}")
    print(f"  隔夜费:      ${mt5_pnl['total_swap']:.2f}")
    print(f"  ─────────────────────────────")
    print(f"  净盈亏:      ${mt5_pnl['net_pnl']:.2f}")
    
    print(f"\n{'='*60}")
    print(f"📈 OKX 交易统计 ({SYMBOL_OKX})")
    print(f"{'='*60}")
    print(f"  交易笔数:    {okx_stats['trade_count']} (买入: {okx_stats['buy_count']}, 卖出: {okx_stats['sell_count']})")
    print(f"  总交易量:    {okx_stats['total_amount_oz']:.4f} oz")
    print(f"  总成交额:    ${okx_stats['total_cost']:.2f} USDT")
    print(f"  已实现盈亏:  ${okx_stats['total_pnl']:.2f} USDT")
    print(f"  手续费:      ${okx_stats['total_fee']:.4f} USDT")
    print(f"  ─────────────────────────────")
    print(f"  净盈亏:      ${okx_stats['net_pnl']:.2f} USDT")
    
    # 尝试获取OKX账户盈亏
    try:
        import ccxt
        client = ccxt.okx({
            'apiKey': os.getenv('OKX_API_KEY'),
            'secret': os.getenv('OKX_API_SECRET'),
            'password': os.getenv('OKX_API_PASSPHRASE'),
            'options': {'defaultType': 'swap'},
        })
        
        # 获取账户余额变动/已实现盈亏
        # OKX 可能需要特殊API调用
        balance = client.fetch_balance()
        usdt_total = float(balance.get('USDT', {}).get('total', 0) or 0)
        usdt_free = float(balance.get('USDT', {}).get('free', 0) or 0)
        usdt_used = float(balance.get('USDT', {}).get('used', 0) or 0)
        
        print(f"\n  当前USDT余额:")
        print(f"    总额:      ${usdt_total:.2f}")
        print(f"    可用:      ${usdt_free:.2f}")
        print(f"    冻结:      ${usdt_used:.2f}")
    except Exception as e:
        print(f"\n  (获取OKX余额失败: {e})")
    
    # 综合统计
    print(f"\n{'='*60}")
    print(f"📊 综合统计 (对冲策略)")
    print(f"{'='*60}")
    combined_pnl = mt5_pnl['net_pnl'] + okx_stats['net_pnl']
    combined_fee = abs(mt5_pnl['total_commission']) + okx_stats['total_fee']
    
    print(f"  MT5 净盈亏:  ${mt5_pnl['net_pnl']:.2f}")
    print(f"  OKX 净盈亏:  ${okx_stats['net_pnl']:.2f}")
    print(f"  ─────────────────────────────")
    print(f"  合计盈亏:    ${combined_pnl:.2f}")
    print(f"  ─────────────────────────────")
    print(f"  总手续费:    ${combined_fee:.2f} (MT5: ${abs(mt5_pnl['total_commission']):.2f}, OKX: ${okx_stats['total_fee']:.2f})")
    print(f"  MT5隔夜费:   ${mt5_pnl['total_swap']:.2f}")


if __name__ == '__main__':
    main()
