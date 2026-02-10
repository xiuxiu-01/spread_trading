"""
Time utility functions.
"""

from datetime import datetime, timezone, timedelta
from typing import Optional


def utc_now() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


def local_now() -> datetime:
    """Get current local datetime."""
    return datetime.now()


def timestamp_to_iso(ts: float) -> str:
    """Convert Unix timestamp to ISO format string."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def iso_to_timestamp(iso_str: str) -> float:
    """Convert ISO format string to Unix timestamp."""
    try:
        # Handle various ISO formats
        iso_str = iso_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso_str)
        return dt.timestamp()
    except Exception:
        return 0.0


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Examples:
        - 30 -> "30s"
        - 90 -> "1m 30s"
        - 3661 -> "1h 1m 1s"
        - 86400 -> "1d 0h"
    """
    if seconds < 60:
        return f"{int(seconds)}s"
    
    minutes, secs = divmod(int(seconds), 60)
    
    if minutes < 60:
        return f"{minutes}m {secs}s"
    
    hours, mins = divmod(minutes, 60)
    
    if hours < 24:
        return f"{hours}h {mins}m"
    
    days, hrs = divmod(hours, 24)
    return f"{days}d {hrs}h"


def is_market_open(
    now: Optional[datetime] = None,
) -> bool:
    """
    Check if forex market is open.
    
    IC Markets XAUUSD trading hours:
    - Daily maintenance: Beijing 05:55 - 07:01 = UTC 21:55 - 23:01
    - Weekend: Friday UTC 22:00 close, Monday UTC 23:01 open
    
    Args:
        now: Current time (default: UTC now)
    
    Returns:
        True if market is open
    """
    if now is None:
        now = utc_now()
    
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
        
    # Sunday: Closed before 23:01 UTC (Beijing Monday 07:01)
    if weekday == 6:
        return False
    
    # Friday after 22:00 UTC: Closed for weekend
    if weekday == 4 and hour >= 22:
        return False
        
    return True


def get_next_bar_time(timeframe: str = "1m", now: Optional[datetime] = None) -> datetime:
    """
    Get the start time of the next bar.
    
    Args:
        timeframe: Bar timeframe (1m, 5m, 15m, 1h, etc.)
        now: Current time (default: UTC now)
    
    Returns:
        Datetime of next bar start
    """
    if now is None:
        now = utc_now()
    
    # Parse timeframe
    tf_map = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
    }
    
    interval_seconds = tf_map.get(timeframe, 60)
    
    # Calculate next bar time
    ts = now.timestamp()
    next_bar_ts = (int(ts / interval_seconds) + 1) * interval_seconds
    
    return datetime.fromtimestamp(next_bar_ts, tz=timezone.utc)


def seconds_until_next_bar(timeframe: str = "1m", now: Optional[datetime] = None) -> float:
    """
    Get seconds until next bar starts.
    
    Args:
        timeframe: Bar timeframe
        now: Current time (default: UTC now)
    
    Returns:
        Seconds until next bar
    """
    if now is None:
        now = utc_now()
    
    next_bar = get_next_bar_time(timeframe, now)
    return (next_bar - now).total_seconds()
