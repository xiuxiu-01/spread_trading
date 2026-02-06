"""
Data Service - Handles data persistence and retrieval.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

from ..config.logging_config import get_logger

logger = get_logger("services.data")


class DataService:
    """
    Data persistence and retrieval service.
    
    Handles:
    - Daily log files
    - Historical data queries
    - Data aggregation
    """
    
    def __init__(self, data_dir: Path):
        """
        Initialize data service.
        
        Args:
            data_dir: Base data directory
        """
        self.data_dir = data_dir
        self.daily_logs_dir = data_dir / "daily_logs"
        self.daily_logs_dir.mkdir(parents=True, exist_ok=True)
    
    def get_daily_file(self, date: Optional[str] = None) -> Path:
        """Get daily log file path."""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        return self.daily_logs_dir / f"spread_data_{date}.jsonl"
    
    def save_minute_data(self, data: Dict[str, Any]):
        """Save minute-level data to daily log."""
        try:
            daily_file = self.get_daily_file()
            
            with open(daily_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(data) + "\n")
                
        except Exception as e:
            logger.error(f"Error saving minute data: {e}")
    
    def load_daily_data(self, date: str) -> List[Dict]:
        """Load all data for a specific date."""
        daily_file = self.get_daily_file(date)
        data = []
        
        if not daily_file.exists():
            return data
        
        try:
            with open(daily_file, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        data.append(json.loads(line.strip()))
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logger.error(f"Error loading daily data: {e}")
        
        return data
    
    def load_recent_days(self, days: int = 7) -> List[Dict]:
        """Load data from recent days."""
        all_data = []
        today = datetime.now()
        
        for i in range(days):
            date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            daily_data = self.load_daily_data(date)
            all_data.extend(daily_data)
        
        # Sort by timestamp
        all_data.sort(key=lambda x: x.get("ts", x.get("local_time", "")))
        
        return all_data
    
    def get_pnl_summary(self, days: int = 1) -> Dict[str, Any]:
        """Calculate PnL summary for recent days."""
        data = self.load_recent_days(days)
        
        if not data:
            return {
                "total_pnl": 0,
                "trade_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "avg_pnl": 0,
                "days": days
            }
        
        # Extract trades with PnL
        trades = [d for d in data if "pnl" in d and d.get("pnl") != 0]
        
        if not trades:
            return {
                "total_pnl": 0,
                "trade_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "avg_pnl": 0,
                "days": days
            }
        
        total_pnl = sum(t.get("pnl", 0) for t in trades)
        win_count = sum(1 for t in trades if t.get("pnl", 0) > 0)
        loss_count = sum(1 for t in trades if t.get("pnl", 0) < 0)
        
        return {
            "total_pnl": round(total_pnl, 2),
            "trade_count": len(trades),
            "win_count": win_count,
            "loss_count": loss_count,
            "avg_pnl": round(total_pnl / len(trades), 2) if trades else 0,
            "win_rate": round(win_count / len(trades) * 100, 1) if trades else 0,
            "days": days
        }
    
    def get_spread_statistics(self, days: int = 1) -> Dict[str, Any]:
        """Calculate spread statistics."""
        data = self.load_recent_days(days)
        
        if not data:
            return {}
        
        spreads = [d.get("spread_mid", d.get("spread", 0)) for d in data if "spread" in d or "spread_mid" in d]
        
        if not spreads:
            return {}
        
        return {
            "count": len(spreads),
            "mean": round(sum(spreads) / len(spreads), 2),
            "min": round(min(spreads), 2),
            "max": round(max(spreads), 2),
            "range": round(max(spreads) - min(spreads), 2),
            "std": round(self._std(spreads), 2)
        }
    
    def _std(self, values: List[float]) -> float:
        """Calculate standard deviation."""
        if len(values) < 2:
            return 0
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5
    
    def cleanup_old_files(self, keep_days: int = 30):
        """Remove log files older than keep_days."""
        cutoff = datetime.now() - timedelta(days=keep_days)
        
        for file in self.daily_logs_dir.glob("spread_data_*.jsonl"):
            try:
                # Extract date from filename
                date_str = file.stem.replace("spread_data_", "")
                file_date = datetime.strptime(date_str, "%Y-%m-%d")
                
                if file_date < cutoff:
                    file.unlink()
                    logger.info(f"Removed old log file: {file.name}")
                    
            except (ValueError, OSError) as e:
                logger.warning(f"Error processing {file}: {e}")
