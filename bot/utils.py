import os
import json
import hashlib
import time
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable
from functools import wraps
from collections import OrderedDict
import pytz
from bot.logger import setup_logger

logger = setup_logger('Utils')

def retry(max_retries: int = 3, backoff_factor: float = 2.0, exceptions: tuple = (Exception,)):
    """
    Decorator for retrying functions with exponential backoff
    
    Args:
        max_retries: Maximum number of retry attempts
        backoff_factor: Exponential backoff multiplier
        exceptions: Tuple of exceptions to catch and retry
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception: Optional[Exception] = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        wait_time = backoff_factor ** attempt
                        logger.warning(f"Retry {attempt + 1}/{max_retries} for {func.__name__} after {wait_time}s due to: {e}")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"All {max_retries} retries failed for {func.__name__}: {e}")
            
            if last_exception:
                raise last_exception
            else:
                raise RuntimeError(f"Function {func.__name__} failed without raising an exception")
        return wrapper
    return decorator

def format_currency(amount: float, symbol: str = '$') -> str:
    return f"{symbol}{amount:,.2f}"

def format_percentage(value: float, decimals: int = 2) -> str:
    return f"{value:.{decimals}f}%"

def format_pips(pips: float, decimals: int = 1) -> str:
    return f"{pips:.{decimals}f} pips"

def format_datetime(dt: datetime, timezone_str: str = 'Asia/Jakarta', 
                   format_str: str = '%Y-%m-%d %H:%M:%S') -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=pytz.UTC)
    
    tz = pytz.timezone(timezone_str)
    local_dt = dt.astimezone(tz)
    return local_dt.strftime(format_str)

def get_today_start(timezone_str: str = 'Asia/Jakarta') -> datetime:
    tz = pytz.timezone(timezone_str)
    now = datetime.now(tz)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return today_start.astimezone(pytz.UTC)

def get_datetime_range(days: int, timezone_str: str = 'Asia/Jakarta') -> tuple:
    tz = pytz.timezone(timezone_str)
    end = datetime.now(tz)
    start = end - timedelta(days=days)
    return start.astimezone(pytz.UTC), end.astimezone(pytz.UTC)

def calculate_percentage_change(old_value: float, new_value: float) -> float:
    if old_value == 0:
        return 0.0
    return ((new_value - old_value) / old_value) * 100

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    try:
        if denominator == 0:
            logger.debug("safe_divide: Division by zero, returning default")
            return default
        
        result = numerator / denominator
        
        if math.isnan(result):
            logger.debug("safe_divide: Result is NaN, returning default")
            return default
        if math.isinf(result):
            logger.debug("safe_divide: Result is Inf, returning default")
            return default
        
        return result
    except ZeroDivisionError:
        logger.debug("safe_divide: ZeroDivisionError caught, returning default")
        return default
    except TypeError as e:
        logger.warning(f"safe_divide: TypeError - invalid operand types: {e}")
        return default
    except Exception as e:
        logger.error(f"safe_divide: Unexpected error ({type(e).__name__}): {e}")
        return default

def truncate_string(text: str, max_length: int = 100, suffix: str = '...') -> str:
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix

def hash_string(text: str, algorithm: str = 'md5') -> str:
    if algorithm == 'md5':
        return hashlib.md5(text.encode()).hexdigest()
    elif algorithm == 'sha256':
        return hashlib.sha256(text.encode()).hexdigest()
    else:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")

@retry(max_retries=3, backoff_factor=2.0, exceptions=(IOError, OSError))
def save_json(data: Dict, filepath: str) -> bool:
    try:
        dir_path = os.path.dirname(filepath)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4, default=str)
        logger.info(f"JSON saved to {filepath}")
        return True
    except PermissionError as e:
        logger.error(f"PermissionError saving JSON to {filepath}: {e}")
        return False
    except TypeError as e:
        logger.error(f"TypeError serializing JSON data: {e}")
        return False
    except OSError as e:
        logger.error(f"OSError saving JSON to {filepath}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error ({type(e).__name__}) saving JSON to {filepath}: {e}")
        return False

@retry(max_retries=3, backoff_factor=2.0, exceptions=(IOError, OSError))
def load_json(filepath: str) -> Optional[Dict]:
    try:
        if not os.path.exists(filepath):
            logger.warning(f"FileNotFoundError: File not found: {filepath}")
            return None
        
        with open(filepath, 'r') as f:
            data = json.load(f)
        logger.info(f"JSON loaded from {filepath}")
        return data
    except FileNotFoundError as e:
        logger.error(f"FileNotFoundError loading JSON from {filepath}: {e}")
        return None
    except PermissionError as e:
        logger.error(f"PermissionError loading JSON from {filepath}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"JSONDecodeError parsing JSON from {filepath}: {e}")
        return None
    except OSError as e:
        logger.error(f"OSError loading JSON from {filepath}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error ({type(e).__name__}) loading JSON from {filepath}: {e}")
        return None

@retry(max_retries=3, backoff_factor=2.0, exceptions=(IOError, OSError))
def ensure_directory_exists(directory: str) -> bool:
    try:
        os.makedirs(directory, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"Error creating directory {directory}: {e}")
        return False

@retry(max_retries=3, backoff_factor=2.0, exceptions=(IOError, OSError))
def cleanup_files(directory: str, pattern: str = '*', days_old: int = 7, 
                  recursive: bool = False, max_depth: int = 10) -> int:
    import glob
    deleted_count = 0
    
    try:
        if not os.path.exists(directory):
            logger.warning(f"Directory not found for cleanup: {directory}")
            return 0
        
        now = datetime.now()
        
        if recursive:
            dirs_to_process = [(directory, 0)]
            
            while dirs_to_process:
                current_dir, current_depth = dirs_to_process.pop(0)
                
                if current_depth > max_depth:
                    logger.warning(f"Max depth {max_depth} reached at {current_dir}, skipping deeper directories")
                    continue
                
                pattern_path = os.path.join(current_dir, pattern)
                items = glob.glob(pattern_path)
                
                for item_path in items:
                    if os.path.isfile(item_path):
                        try:
                            file_time = datetime.fromtimestamp(os.path.getmtime(item_path))
                            if (now - file_time).days > days_old:
                                os.remove(item_path)
                                deleted_count += 1
                                logger.info(f"Deleted old file: {item_path}")
                        except PermissionError as e:
                            logger.warning(f"PermissionError deleting {item_path}: {e}")
                        except FileNotFoundError:
                            logger.debug(f"File already deleted: {item_path}")
                        except OSError as e:
                            logger.warning(f"OSError deleting {item_path}: {e}")
                    elif os.path.isdir(item_path):
                        dirs_to_process.append((item_path, current_depth + 1))
        else:
            pattern_path = os.path.join(directory, pattern)
            files = glob.glob(pattern_path)
            
            for filepath in files:
                if os.path.isfile(filepath):
                    try:
                        file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                        if (now - file_time).days > days_old:
                            os.remove(filepath)
                            deleted_count += 1
                            logger.info(f"Deleted old file: {filepath}")
                    except PermissionError as e:
                        logger.warning(f"PermissionError deleting {filepath}: {e}")
                    except FileNotFoundError:
                        logger.debug(f"File already deleted: {filepath}")
                    except OSError as e:
                        logger.warning(f"OSError deleting {filepath}: {e}")
        
        logger.info(f"Cleanup completed. Deleted {deleted_count} files from {directory}")
        return deleted_count
        
    except PermissionError as e:
        logger.error(f"PermissionError during cleanup of {directory}: {e}")
        return deleted_count
    except OSError as e:
        logger.error(f"OSError during cleanup of {directory}: {e}")
        return deleted_count
    except Exception as e:
        logger.error(f"Unexpected error ({type(e).__name__}) during cleanup: {e}")
        return deleted_count

def validate_price(price: float) -> bool:
    return price is not None and price > 0

def validate_percentage(percentage: float, min_val: float = 0, max_val: float = 100) -> bool:
    return percentage is not None and min_val <= percentage <= max_val

def clamp(value: float, min_val: float, max_val: float) -> float:
    return max(min_val, min(value, max_val))

def round_to_pips(value: float, pip_value: float = 10.0) -> float:
    return round(value * pip_value) / pip_value

def calculate_lot_size(risk_amount: float, pips_at_risk: float, 
                      min_lot: float = 0.01, max_lot: float = 10.0) -> float:
    if pips_at_risk <= 0:
        return min_lot
    
    lot_size = risk_amount / pips_at_risk
    return clamp(lot_size, min_lot, max_lot)

def is_market_open(check_time: Optional[datetime] = None, 
                  timezone_str: str = 'America/New_York') -> bool:
    if check_time is None:
        check_time = datetime.now(pytz.UTC)
    
    tz = pytz.timezone(timezone_str)
    local_time = check_time.astimezone(tz)
    
    weekday = local_time.weekday()
    hour = local_time.hour
    
    if weekday == 4 and hour >= 17:
        return False
    
    if weekday == 5:
        return False
    
    if weekday == 6 and hour < 17:
        return False
    
    return True

def get_emoji_for_result(result: str) -> str:
    emoji_map = {
        'WIN': '✅',
        'LOSS': '❌',
        'OPEN': '🔄',
        'CLOSED': '🔒',
        'BUY': '📈',
        'SELL': '📉',
        'PENDING': '⏳',
        'CANCELLED': '🚫'
    }
    return emoji_map.get(result.upper(), '❓')

def parse_timeframe(timeframe: str) -> int:
    timeframe_map = {
        'M1': 1,
        'M5': 5,
        'M15': 15,
        'M30': 30,
        'H1': 60,
        'H4': 240,
        'D1': 1440
    }
    return timeframe_map.get(timeframe.upper(), 1)

def format_trade_summary(trade_data: Dict) -> str:
    summary = f"{get_emoji_for_result(trade_data.get('signal_type', ''))} *{trade_data.get('signal_type', 'N/A')}*\n"
    summary += f"Entry: {format_currency(trade_data.get('entry_price', 0))}\n"
    
    if trade_data.get('exit_price'):
        summary += f"Exit: {format_currency(trade_data.get('exit_price', 0))}\n"
    
    if trade_data.get('actual_pl') is not None:
        pl_emoji = '💰' if trade_data.get('actual_pl', 0) > 0 else '💸'
        summary += f"P/L: {format_currency(trade_data.get('actual_pl', 0))} {pl_emoji}\n"
    
    return summary

class RateLimiter:
    def __init__(self, max_calls: int, time_window: int):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = []
    
    def can_proceed(self) -> bool:
        now = datetime.now()
        cutoff_time = now - timedelta(seconds=self.time_window)
        
        self.calls = [call_time for call_time in self.calls if call_time > cutoff_time]
        
        if len(self.calls) < self.max_calls:
            self.calls.append(now)
            return True
        
        return False
    
    def get_wait_time(self) -> float:
        if not self.calls:
            return 0.0
        
        oldest_call = min(self.calls)
        cutoff_time = oldest_call + timedelta(seconds=self.time_window)
        now = datetime.now()
        
        if now >= cutoff_time:
            return 0.0
        
        return (cutoff_time - now).total_seconds()

class Cache:
    def __init__(self, ttl_seconds: int = 60, max_size: int = 1000):
        self.cache: OrderedDict = OrderedDict()
        self.ttl = ttl_seconds
        self.max_size = max_size
    
    def get(self, key: str) -> Optional[Any]:
        if key in self.cache:
            value, timestamp = self.cache[key]
            if (datetime.now() - timestamp).total_seconds() < self.ttl:
                self.cache.move_to_end(key)
                return value
            else:
                del self.cache[key]
        return None
    
    def set(self, key: str, value: Any):
        if key in self.cache:
            del self.cache[key]
        
        if len(self.cache) >= self.max_size:
            self._evict_oldest()
        
        self.cache[key] = (value, datetime.now())
    
    def _evict_oldest(self):
        if self.cache:
            oldest_key = next(iter(self.cache))
            del self.cache[oldest_key]
            logger.debug(f"Cache evicted oldest entry: {oldest_key}")
    
    def cleanup_expired(self) -> int:
        now = datetime.now()
        expired_keys = []
        
        for key, (value, timestamp) in self.cache.items():
            if (now - timestamp).total_seconds() >= self.ttl:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.cache[key]
        
        if expired_keys:
            logger.debug(f"Cache cleanup removed {len(expired_keys)} expired entries")
        
        return len(expired_keys)
    
    def clear(self):
        self.cache.clear()
    
    def delete(self, key: str):
        if key in self.cache:
            del self.cache[key]
    
    def size(self) -> int:
        return len(self.cache)
    
    def is_full(self) -> bool:
        return len(self.cache) >= self.max_size
