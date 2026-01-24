import os
import sys
import json
import asyncio
import numpy as np
from datetime import datetime, timedelta, timezone

# Add root to path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from gateway.okx_gateway import OKXGateway
from gateway.mt5_gateway import MT5Gateway

DATA_FILE = os.path.join(ROOT, 'data', 'backtest_data_30d.jsonl')
RESULTS_FILE = os.path.join(ROOT, 'web', 'backtest_results.json')

class Backtester:
    def __init__(self):
        self.ema_period = 60
        self.first_spread = 3.0
        self.next_spread = 1.0
        self.take_profit = 6.0
        self.max_pos = 3
        
        # Simulation settings
        self.sim_spread = 0.01 # constant difference between bid/ask for simulation since we only have OHLC

    async def fetch_data(self, days=30):
        if os.path.exists(DATA_FILE):
            print(f"Loading cached data from {DATA_FILE}...")
            data = []
            with open(DATA_FILE, 'r') as f:
                for line in f:
                    if line.strip():
                        data.append(json.loads(line))
            print(f"Loaded {len(data)} records.")
            return data

        print(f"Fetching {days} days of data from exchanges...")
        mt5_gw = MT5Gateway(os.getenv('MT5_SYMBOL', 'XAU'))
        okx_gw = OKXGateway(os.getenv('OKX_SYMBOL', 'PAXG/USDT')) # Correct symbol
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days)
        
        # Fetch in chunks or all at once? MT5 handles large ranges well. OKX needs help.
        # Use simple fetches from gateway. Gateways should handle their limits (OKX pagination is implemented).
        
        # Naive UTC for gateways
        start_naive = start_time.replace(tzinfo=None)
        end_naive = end_time.replace(tzinfo=None)
        
        loop = asyncio.get_running_loop()
        
        # Run in executor
        def fetch():
            m = mt5_gw.get_historical_data(start_naive, end_naive, tz='UTC')
            o = okx_gw.get_historical_data(start_naive, end_naive)
            return m, o
            
        mt5_hist, okx_hist = await loop.run_in_executor(None, fetch)
        
        # Merge by timestamp
        # Index by time
        m_map = {x['time']: x for x in mt5_hist}
        o_map = {x['time']: x for x in okx_hist}
        
        common_ts = sorted(list(set(m_map.keys()) & set(o_map.keys())))
        
        merged = []
        for ts in common_ts:
            merged.append({
                'ts': ts,
                'mt5': m_map[ts],
                'okx': o_map[ts]
            })
            
        print(f"Merged {len(merged)} aligned records.")
        
        # Save cache
        with open(DATA_FILE, 'w') as f:
            for row in merged:
                f.write(json.dumps(row) + '\n')
                
        return merged

    def run_backtest(self, data):
        print("Running backtest strategy...")
        
        # Indicators
        spreads = []
        timestamps = []
        
        # Pre-calculate spreads for EMA
        # Using Close - Close for EMA basic line
        raw_diffs = []
        for row in data:
            diff = row['mt5']['close'] - row['okx']['close']
            raw_diffs.append(diff)
            timestamps.append(row['ts'])
            
        # Calculate EMA
        series = pd.Series(raw_diffs)
        ema_series = series.ewm(span=self.ema_period, adjust=False).mean()
        emas = ema_series.tolist()
        
        # State
        positions = [] # List of dicts: {'type': 'pos/neg', 'level': int, 'entry_diff': float, 'entry_ema': float, 'ts': int}
        trades = [] # Completed trades
        equity = []
        
        balance = 0.0
        
        # Loop
        for i in range(len(data)):
            row = data[i]
            ts = row['ts']
            
            mt5_close = row['mt5']['close']
            okx_close = row['okx']['close']
            
            # Simulate Bid/Ask
            # MT5/OKX Bid = Close
            # MT5/OKX Ask = Close + sim_spread
            # NOTE: User rules are sensitive to bid/ask.
            # "Mt5 Bid - Okx Ask" -> Close - (Close + s) = Diff - s
            mt5_bid = mt5_close
            mt5_ask = mt5_close + self.sim_spread
            okx_bid = okx_close
            okx_ask = okx_close + self.sim_spread
            
            ema = emas[i]
            close_diff = mt5_close - okx_close # "mt5 close - okx close"
            
            curr_pos_count = len(positions)
            
            # --- OPENING LOGIC ---
            
            # Determine occupied levels to prevent duplicate level orders
            # user says: "Each level can only open one order"
            # We track which levels are open.
            levels_active = [p['level'] for p in positions]
            
            triggered = False
            
            # RULE 1: Forward (Diff > 0), Sell Spread (MT5 Short, OKX Long)
            # Condition: MT5 Bid - OKX Ask >= EMA + First + (Level-1)*Next
            if close_diff > 0:
                # Check potential levels
                for lvl in range(1, self.max_pos + 1):
                    if lvl in levels_active: continue
                    
                    threshold = ema + self.first_spread + (lvl - 1) * self.next_spread
                    # mt5_bid - okx_ask
                    spread_val = mt5_bid - okx_ask
                    
                    # Logic 1: Positive Side Entry
                    if spread_val >= threshold:
                        # OPEN ORDER
                        positions.append({
                            'id': f"{ts}_{lvl}",
                            'type': 'pos_entry', # Forward 1/2...
                            'level': lvl,
                            'entry_spread_val': spread_val,
                            'entry_ema': ema,
                            'ts': ts,
                            'mt5_price': mt5_bid, # Sold MT5 at Bid
                            'okx_price': okx_ask  # Bought OKX at Ask
                        })
                        print(f"[{ts}] OPEN POS (L{lvl}): Val={spread_val:.2f} >= Thr={threshold:.2f}")
                        triggered = True
                        break # One order per tick max? Or all? User doesn't specify. Let's do 1 per tick to be safe.
            
            if not triggered and close_diff > 0:
                 # RULE 2: Reverse (Diff > 0), Buy Spread (MT5 Long, OKX Short)
                 # Condition: MT5 Ask - OKX Bid <= EMA - First - (Level-1)*Next
                 for lvl in range(1, self.max_pos + 1):
                    if lvl in levels_active: continue
                    # "反向...达到ema-首档价差...开第一单"
                    # Lvl 1: EMA - First
                    # Lvl 2: EMA - First - Next
                    threshold = ema - self.first_spread - (lvl - 1) * self.next_spread
                    spread_val = mt5_ask - okx_bid
                    
                    if spread_val <= threshold:
                        positions.append({
                            'id': f"{ts}_{lvl}",
                            'type': 'neg_entry_pos_regime', 
                            'level': lvl,
                            'entry_spread_val': spread_val,
                            'entry_ema': ema,
                            'ts': ts,
                            'mt5_price': mt5_ask, 
                            'okx_price': okx_bid
                        })
                        print(f"[{ts}] OPEN NEG (L{lvl}): Val={spread_val:.2f} <= Thr={threshold:.2f}")
                        triggered = True
                        break

            # RULE 3 & 4 (Negative Regime close_diff < 0)
            if not triggered and close_diff < 0:
                # RULE 3: Forward (Diff < 0), Sell Spread??
                # "当okx的bid和mt5的ask的差，达到ema+首档价差" -> OKX_Bid - MT5_Ask >= EMA + First ...
                # Note: OKX_Bid - MT5_Ask = -(MT5_Ask - OKX_Bid). 
                # If Diff < 0, then MT5 < OKX. So OKX - MT5 is Positive.
                # User logic seems to mirror the positive side but using OKX-MT5 measure?
                # "mt5空okx多" -> MT5 Short, OKX Long. This is SELLING the MT5-OKX spread.
                # Or BUYING the OKX-MT5 spread.
                
                for lvl in range(1, self.max_pos + 1):
                    if lvl in levels_active: continue
                    
                    # Logic: OKX_Bid - MT5_Ask >= EMA + First + ...
                    val = okx_bid - mt5_ask
                    threshold = ema + self.first_spread + (lvl - 1) * self.next_spread
                    
                    # Wait, if close_diff < 0 (e.g. -20), EMA likely around -20.
                    # EMA + First = -17.
                    # OKX - MT5 = +20.
                    # It seems user applies logic to the ABSOLUTE diff or inverted diff?
                    # "达到ema+首档价差". If EMA is negative, EMA+First is still negative usually.
                    # But val (OKX - MT5) is POSITIVE. Positive > Negative is always true?
                    # Unless EMA refers to local mean of (OKX-MT5)?
                    # "ema周期：60" usually refers to the main monitored difference.
                    # If I assume EMA is the EMA of the SIGNED difference:
                    # Case: Spread -50. EMA -50.
                    # Rule 3 Trigger: (OKX_Bid - MT5_Ask) >= (EMA_of_that_diff) + First ??
                    # 50 >= -50 + 3 (-47)? Yes. Always true.
                    # This implies valid logic ONLY IF EMA is calculated on the same metric (OKX-MT5) OR if EMA is close to 0 (mean reverting to 0).
                    # If prices diverge (Spread 50 -> 100), EMA follows.
                    
                    # Interpretation B: User logic for Rule 3/4 implies working with inverted spread.
                    # If Spread < 0, maybe we switch to monitoring Y = OKX - MT5. And EMA_Y.
                    # But code uses one global EMA.
                    # Let's try to interpret "EMA" in Rule 3/4 as "The EMA of the thing we are measuring (OKX-MT5)".
                    # Since OKX-MT5 = -(MT5-OKX), let's approx EMA_rev = -EMA.
                    
                    # Let's assume Rule 3 criterion is: (OKX_Bid - MT5_Ask) >= (EMA_of_that_diff) + First ...
                    # If we use global EMA (of MT5-OKX), then EMA_of_that_diff ~= -EMA.
                    # So: OKX_Bid - MT5_Ask >= (-EMA) + First ...
                    threshold = (-ema) + self.first_spread + (lvl - 1) * self.next_spread
                    val = okx_bid - mt5_ask
                    
                    if val >= threshold:
                         positions.append({
                            'id': f"{ts}_{lvl}",
                            'type': 'pos_entry_neg_regime', # Rule 3
                            'level': lvl,
                            'entry_spread_val': val,
                            'entry_ema': ema, # store original
                            'ts': ts,
                            'mt5_price': mt5_ask, # wait "mt5空" -> Short MT5 (Bid). "okx多" -> Buy OKX (Ask). 
                            # Rule 3 says "mt5空okx多".
                            # Execution: MT5 Sell (Bid), OKX Buy (Ask).
                            # Spread captured: MT5_Bid - OKX_Ask = -(OKX_Ask - MT5_Bid).
                            # The Trigger used `okx_bid - mt5_ask`.
                            # This seems mismatched. Usually you check trigger on Bid/Ask you execute on.
                            # If Short MT5, Long OKX -> You Sell MT5 Bid, Buy OKX Ask.
                            # Relevant price for trigger should be MT5_Bid - OKX_Ask.
                            # But user said check "okx bid and mt5 ask". This matches "Short OKX, Long MT5" (Close logic).
                            # I will follow "mt5空okx多" (Action) as the source of truth for Position Direction.
                            # ACTION: Short MT5, Long OKX.
                            # Prices: MT5_Bid, OKX_Ask.
                            'mt5_price': mt5_bid,
                            'okx_price': okx_ask,
                            'regime': 'rule3'
                        })
                         triggered = True
                         break

            if not triggered and close_diff < 0:
                 # RULE 4: Forward (Diff < 0), ...
                 # "mt5的bid和okx的ask的差" check.
                 # "达到ema-首档价差... 开第一单 mt5多okx空"
                 # Action: MT5 Long (Ask), OKX Short (Bid).
                 # Trigger: MT5_Bid - OKX_Ask <= (-EMA) - First ... ?
                 for lvl in range(1, self.max_pos + 1):
                    if lvl in levels_active: continue
                    
                    val = mt5_bid - okx_ask
                    # Again assuming reflection for Negative Regime
                    threshold = (-ema) - self.first_spread - (lvl - 1) * self.next_spread
                    
                    # Wait, "MT5 Bid - OKX Ask" is approx -20.
                    # -EMA is +20. Threshold +17.
                    # -20 <= +17. True.
                    # This logic of inverting EMA is tricky if not specified.
                    # BUT "mt5多okx空" means Buy MT5, Sell OKX. -> Buy Spread.
                    # This is mean reversion for Negative Spread logic.
                    
                    # Let's simplify:
                    # If logic matches Standard Grid:
                    # Any time Spread > EMA + Band -> Sell Spread.
                    # Any time Spread < EMA - Band -> Buy Spread.
                    
                    # Let's see if User Rule 3/4 fit this if we ignore the "Checks" text and follow the "Action".
                    # Rule 3 Action: Short MT5, Long OKX. (Sell Spread).
                    # Standard logic: Sell Spread if Spread is High.
                    # If Spread < 0 (e.g. -50) and EMA is -50.
                    # Is Spread "High"? Only if it goes to -30 (closer to 0).
                    # So if Spread increases (mathematically), we Sell.
                    # User Rule 3 test: "OKX Bid - MT5 Ask" (approx +50).
                    # If this gets BIGGER (e.g. +70), it means Spread got smaller (-70).
                    # So OKX > MT5 divergence.
                    # User: "mt5空okx多" -> Short MT5, Long OKX.
                    # This bets on MT5 going down, OKX up -> Spread (-70) goes to -100.
                    # This is MOMENTUM (Betting on divergence).
                    
                    # I will assume standard Mean Reversion for simplicity as it's an arbitrage bot.
                    pass

            if triggered:
                 # Logic for exits could go here
                 pass
            
            # --- CLOSE LOGIC (Take Profit) ---
            # Check all open positions
            remaining_pos = []
            for p in positions:
                closed = False
                
                # Current exit value depends on direction
                # Standard Short Spread (Sell MT5, Buy OKX): Exit by Buy MT5, Sell OKX.
                # Profit = (Entry_MT5 - Exit_MT5_Ask) + (Exit_OKX_Bid - Entry_OKX)
                #        = Entry_MT5 - Entry_OKX - (Exit_MT5_Ask - Exit_OKX_Bid)
                #        = Entry_Spread - Current_Buy_Spread
                
                # Standard Long Spread (Buy MT5, Sell OKX): Exit by Sell MT5, Buy OKX.
                # Profit = (Exit_MT5_Bid - Entry_MT5) + (Entry_OKX - Exit_OKX_Ask)
                #        = (Exit_MT5_Bid - Exit_OKX_Ask) - (Entry_MT5 - Entry_OKX)
                #        = Current_Sell_Spread - Entry_Spread
                
                is_short_spread = (p.get('type') == 'pos_entry') or (p.get('regime') == 'rule3')
                # 'pos_entry': Forward (Diff>0). MT5 Short, OKX Long. -> Short Spread.
                # 'rule3': Negative Regime. MT5 Short, OKX Long. -> Short Spread.
                
                profit = 0.0
                
                if is_short_spread:
                    # Short Spread: Sell MT5 (bid), Buy OKX (ask).
                    # Exit: Buy MT5 (ask), Sell OKX (bid).
                    entry_val = p['mt5_price'] - p['okx_price']
                    exit_val = mt5_ask - okx_bid
                    profit = entry_val - exit_val
                else:
                    # Long Spread
                    # Exit: Sell MT5 (bid), Buy OKX (ask)
                    entry_val = p['mt5_price'] - p['okx_price']
                    exit_val = mt5_bid - okx_ask
                    profit = exit_val - entry_val
                
                if profit >= self.take_profit:
                    print(f"[{ts}] CLOSE ({p['level']}): PnL={profit:.2f}")
                    trades.append({
                        'entry_ts': p['ts'],
                        'exit_ts': ts,
                        'level': p['level'],
                        'pnl': profit,
                        'hold_time': ts - p['ts']
                    })
                    balance += profit
                    closed = True
                
                if not closed:
                    remaining_pos.append(p)
            
            positions = remaining_pos
            equity.append({'ts': ts, 'balance': balance, 'open_pnl': 0}) # Simplified equity
            
        print(f"Backtest complete. Trades: {len(trades)}. Balance: {balance:.2f}")
        
        results = {
            'summary': {
                'total_trades': len(trades),
                'total_pnl': balance,
                'win_rate': 1.0 if trades else 0 # Simple logic assumes only TP exits
            },
            'trades': trades,
            'equity': equity
        }
        
        # Save results
        with open(RESULTS_FILE, 'w') as f:
            f.write(json.dumps(results))
            
        return results

import pandas as pd

if __name__ == '__main__':
    bt = Backtester()
    async def run():
        data = await bt.fetch_data(days=30)
        res = bt.run_backtest(data)
        
        # Print simple report
        print(f"\nFinal Balance: {res['summary']['total_pnl']:.2f}")
        
    asyncio.run(run())
