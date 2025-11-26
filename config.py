import os
import re
from typing import List, Any, Optional
from dotenv import load_dotenv

load_dotenv()


class ConfigError(Exception):
    """
    Raised when configuration validation fails.
    
    Attributes:
        errors: List of validation error messages
        warnings: List of validation warning messages
    """
    
    def __init__(self, message: str, errors: Optional[List[str]] = None, warnings: Optional[List[str]] = None):
        self.errors = errors if errors is not None else []
        self.warnings = warnings if warnings is not None else []
        self.message = message
        super().__init__(self._format_message())
    
    def _format_message(self) -> str:
        """Format error message with all details."""
        parts = [self.message]
        
        if self.errors:
            parts.append("\nErrors:")
            for i, err in enumerate(self.errors, 1):
                parts.append(f"  {i}. {err}")
        
        if self.warnings:
            parts.append("\nWarnings:")
            for i, warn in enumerate(self.warnings, 1):
                parts.append(f"  {i}. {warn}")
        
        return "\n".join(parts)
    
    def get_error_count(self) -> int:
        """Return total number of errors."""
        return len(self.errors)
    
    def get_warning_count(self) -> int:
        """Return total number of warnings."""
        return len(self.warnings)


ConfigValidationError = ConfigError


def _validate_type(value: Any, expected_type: type, field_name: str) -> Optional[str]:
    """
    Validate that a value is of the expected type.
    
    Returns:
        Error message if validation fails, None otherwise
    """
    if not isinstance(value, expected_type):
        return f"{field_name} must be {expected_type.__name__}, got {type(value).__name__}"
    return None


def _validate_range(value: float, min_val: float, max_val: float, 
                    field_name: str, inclusive: bool = True) -> Optional[str]:
    """
    Validate that a numeric value is within range.
    
    Returns:
        Error message if validation fails, None otherwise
    """
    if inclusive:
        if value < min_val or value > max_val:
            return f"{field_name} must be between {min_val} and {max_val}, got {value}"
    else:
        if value <= min_val or value >= max_val:
            return f"{field_name} must be between {min_val} and {max_val} (exclusive), got {value}"
    return None


def _validate_positive(value: float, field_name: str, allow_zero: bool = False) -> Optional[str]:
    """
    Validate that a value is positive.
    
    Returns:
        Error message if validation fails, None otherwise
    """
    if allow_zero:
        if value < 0:
            return f"{field_name} must be non-negative, got {value}"
    else:
        if value <= 0:
            return f"{field_name} must be positive, got {value}"
    return None


def _validate_non_empty_string(value: str, field_name: str) -> Optional[str]:
    """
    Validate that a string is non-empty.
    
    Returns:
        Error message if validation fails, None otherwise
    """
    if not value or not isinstance(value, str) or not value.strip():
        return f"{field_name} is required but not set or empty"
    return None


def _validate_list_not_empty(value: list, field_name: str) -> Optional[str]:
    """
    Validate that a list is non-empty.
    
    Returns:
        Error message if validation fails, None otherwise
    """
    if not value or not isinstance(value, list) or len(value) == 0:
        return f"{field_name} must contain at least one item"
    return None


def _validate_list_items_type(value: list, item_type: type, field_name: str) -> Optional[str]:
    """
    Validate that all items in a list are of expected type.
    
    Returns:
        Error message if validation fails, None otherwise
    """
    if not isinstance(value, list):
        return f"{field_name} must be a list"
    
    for i, item in enumerate(value):
        if not isinstance(item, item_type):
            return f"{field_name}[{i}] must be {item_type.__name__}, got {type(item).__name__}"
    return None


def _validate_url_format(value: str, field_name: str) -> Optional[str]:
    """
    Validate URL format.
    
    Returns:
        Error message if validation fails, None otherwise
    """
    if not value:
        return None
    
    value = value.strip()
    if not (value.startswith('http://') or value.startswith('https://')):
        return f"{field_name} must start with http:// or https://, got: {value[:50]}..."
    return None


def _get_float_env(key: str, default: str) -> float:
    """Safely get float environment variable with validation"""
    value = os.getenv(key, default)
    
    if isinstance(value, (int, float)):
        return float(value)
    
    if isinstance(value, str):
        value = value.strip()
        if not value:
            value = default
    
    try:
        return float(value)
    except (ValueError, TypeError):
        return float(default)


def _get_int_env(key: str, default: str) -> int:
    """Safely get integer environment variable with validation"""
    value = os.getenv(key, default)
    
    if isinstance(value, int):
        return value
    
    if isinstance(value, str):
        value = value.strip()
        if not value:
            value = default
    
    try:
        return int(value)
    except (ValueError, TypeError):
        return int(default)


def _parse_user_ids(env_value: str) -> list:
    """Parse comma-separated user IDs, returning empty list on error"""
    if not env_value:
        return []
    try:
        result = [int(uid.strip()) for uid in env_value.split(',') if uid.strip()]
        for uid in result:
            if uid <= 0:
                return []
        return result
    except (ValueError, AttributeError):
        return []


def _parse_int_list(env_value: str, default_list: list) -> list:
    """Parse comma-separated integers, returning default on error"""
    if not env_value:
        return default_list
    try:
        result = [int(p.strip()) for p in env_value.split(',') if p.strip()]
        if not result:
            return default_list
        for val in result:
            if val <= 0:
                return default_list
        return result
    except (ValueError, AttributeError):
        return default_list


class Config:
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
    TELEGRAM_WEBHOOK_MODE = os.getenv('TELEGRAM_WEBHOOK_MODE', 'false').lower() == 'true'
    WEBHOOK_URL = os.getenv('WEBHOOK_URL', '')
    FREE_TIER_MODE = os.getenv('FREE_TIER_MODE', 'true').lower() == 'true'
    TICK_LOG_SAMPLE_RATE = _get_int_env('TICK_LOG_SAMPLE_RATE', '30')
    AUTHORIZED_USER_IDS = _parse_user_ids(os.getenv('AUTHORIZED_USER_IDS', ''))
    ID_USER_PUBLIC = _parse_user_ids(os.getenv('ID_USER_PUBLIC', ''))
    EMA_PERIODS = _parse_int_list(os.getenv('EMA_PERIODS', '5,10,20'), [5, 10, 20])
    EMA_PERIODS_LONG = _parse_int_list(os.getenv('EMA_PERIODS_LONG', '50'), [50])
    
    MEMORY_WARNING_THRESHOLD_MB = _get_int_env('MEMORY_WARNING_THRESHOLD_MB', '400')
    MEMORY_CRITICAL_THRESHOLD_MB = _get_int_env('MEMORY_CRITICAL_THRESHOLD_MB', '450')
    OOM_GRACEFUL_DEGRADATION = os.getenv('OOM_GRACEFUL_DEGRADATION', 'true').lower() == 'true'
    MAX_CANDLE_HISTORY = _get_int_env('MAX_CANDLE_HISTORY', '200') if FREE_TIER_MODE else _get_int_env('MAX_CANDLE_HISTORY', '500')
    
    @classmethod
    def get_masked_token(cls) -> str:
        from bot.logger import mask_token
        return mask_token(cls.TELEGRAM_BOT_TOKEN)
    
    @classmethod
    def validate(cls, strict: bool = True) -> bool:
        """
        Validate all configuration settings with comprehensive checks.
        
        Args:
            strict: If True, raises exception on errors. If False, returns False.
        
        Raises:
            ConfigError: If strict=True and critical configuration is missing or invalid
        
        Returns:
            True if validation passes, False otherwise (when strict=False)
        """
        errors = []
        warnings = []
        
        err = _validate_non_empty_string(cls.TELEGRAM_BOT_TOKEN, "TELEGRAM_BOT_TOKEN")
        if err:
            errors.append(err)
        else:
            token_pattern = r'^\d+:[A-Za-z0-9_-]{35,}$'
            if not re.match(token_pattern, cls.TELEGRAM_BOT_TOKEN.strip()):
                warnings.append("TELEGRAM_BOT_TOKEN format may be invalid (expected format: numbers:alphanumeric)")
        
        err = _validate_list_not_empty(cls.AUTHORIZED_USER_IDS, "AUTHORIZED_USER_IDS")
        if err:
            errors.append(err)
        else:
            err = _validate_list_items_type(cls.AUTHORIZED_USER_IDS, int, "AUTHORIZED_USER_IDS")
            if err:
                errors.append(err)
        
        err = _validate_list_items_type(cls.ID_USER_PUBLIC, int, "ID_USER_PUBLIC")
        if err:
            errors.append(err)
        
        err = _validate_list_not_empty(cls.EMA_PERIODS, "EMA_PERIODS")
        if err:
            errors.append(err)
        else:
            err = _validate_list_items_type(cls.EMA_PERIODS, int, "EMA_PERIODS")
            if err:
                errors.append(err)
        
        err = _validate_list_not_empty(cls.EMA_PERIODS_LONG, "EMA_PERIODS_LONG")
        if err:
            errors.append(err)
        else:
            err = _validate_list_items_type(cls.EMA_PERIODS_LONG, int, "EMA_PERIODS_LONG")
            if err:
                errors.append(err)
        
        if cls.TELEGRAM_WEBHOOK_MODE:
            if not cls.WEBHOOK_URL or cls.WEBHOOK_URL.strip() == '':
                warnings.append("TELEGRAM_WEBHOOK_MODE is enabled but WEBHOOK_URL is not set - will attempt auto-detection")
            else:
                err = _validate_url_format(cls.WEBHOOK_URL, "WEBHOOK_URL")
                if err:
                    errors.append(err)
                elif '/bot' not in cls.WEBHOOK_URL:
                    warnings.append("WEBHOOK_URL should typically contain '/bot<token>' endpoint for Telegram webhooks")
        
        err = _validate_range(cls.RISK_PER_TRADE_PERCENT, 0.01, 100, "RISK_PER_TRADE_PERCENT")
        if err:
            errors.append(err)
        
        err = _validate_range(cls.DAILY_LOSS_PERCENT, 0.01, 100, "DAILY_LOSS_PERCENT")
        if err:
            errors.append(err)
        
        err = _validate_positive(cls.FIXED_RISK_AMOUNT, "FIXED_RISK_AMOUNT")
        if err:
            errors.append(err)
        
        err = _validate_positive(cls.TP_RR_RATIO, "TP_RR_RATIO")
        if err:
            errors.append(err)
        
        err = _validate_positive(cls.SL_ATR_MULTIPLIER, "SL_ATR_MULTIPLIER")
        if err:
            errors.append(err)
        
        err = _validate_positive(cls.DEFAULT_SL_PIPS, "DEFAULT_SL_PIPS")
        if err:
            errors.append(err)
        
        err = _validate_positive(cls.DEFAULT_TP_PIPS, "DEFAULT_TP_PIPS")
        if err:
            errors.append(err)
        
        err = _validate_positive(cls.RSI_PERIOD, "RSI_PERIOD")
        if err:
            errors.append(err)
        
        err = _validate_range(cls.RSI_OVERSOLD_LEVEL, 0, 100, "RSI_OVERSOLD_LEVEL")
        if err:
            errors.append(err)
        
        err = _validate_range(cls.RSI_OVERBOUGHT_LEVEL, 0, 100, "RSI_OVERBOUGHT_LEVEL")
        if err:
            errors.append(err)
        
        if cls.RSI_OVERSOLD_LEVEL >= cls.RSI_OVERBOUGHT_LEVEL:
            errors.append(f"RSI_OVERSOLD_LEVEL ({cls.RSI_OVERSOLD_LEVEL}) must be less than RSI_OVERBOUGHT_LEVEL ({cls.RSI_OVERBOUGHT_LEVEL})")
        
        err = _validate_positive(cls.ATR_PERIOD, "ATR_PERIOD")
        if err:
            errors.append(err)
        
        err = _validate_positive(cls.MACD_FAST, "MACD_FAST")
        if err:
            errors.append(err)
        
        err = _validate_positive(cls.MACD_SLOW, "MACD_SLOW")
        if err:
            errors.append(err)
        
        if cls.MACD_FAST >= cls.MACD_SLOW:
            errors.append(f"MACD_FAST ({cls.MACD_FAST}) must be less than MACD_SLOW ({cls.MACD_SLOW})")
        
        err = _validate_positive(cls.SIGNAL_COOLDOWN_SECONDS, "SIGNAL_COOLDOWN_SECONDS", allow_zero=True)
        if err:
            errors.append(err)
        
        err = _validate_positive(cls.MAX_TRADES_PER_DAY, "MAX_TRADES_PER_DAY")
        if err:
            errors.append(err)
        
        err = _validate_positive(cls.MEMORY_WARNING_THRESHOLD_MB, "MEMORY_WARNING_THRESHOLD_MB")
        if err:
            errors.append(err)
        
        err = _validate_positive(cls.MEMORY_CRITICAL_THRESHOLD_MB, "MEMORY_CRITICAL_THRESHOLD_MB")
        if err:
            errors.append(err)
        
        if cls.MEMORY_WARNING_THRESHOLD_MB >= cls.MEMORY_CRITICAL_THRESHOLD_MB:
            warnings.append(f"MEMORY_WARNING_THRESHOLD_MB ({cls.MEMORY_WARNING_THRESHOLD_MB}) should be less than MEMORY_CRITICAL_THRESHOLD_MB ({cls.MEMORY_CRITICAL_THRESHOLD_MB})")
        
        if warnings:
            try:
                from bot.logger import setup_logger
                logger = setup_logger('Config')
                for warning in warnings:
                    logger.warning(f"Configuration warning: {warning}")
            except Exception:
                pass
        
        if errors:
            if strict:
                raise ConfigError(
                    f"Configuration validation failed with {len(errors)} error(s)",
                    errors=errors,
                    warnings=warnings
                )
            return False
        
        return True
    
    @classmethod
    def validate_runtime(cls) -> dict:
        """
        Perform runtime validation and return status.
        
        Returns:
            dict with 'valid' boolean and 'issues' list
        """
        issues = []
        
        if not cls.TELEGRAM_BOT_TOKEN:
            issues.append("TELEGRAM_BOT_TOKEN not configured")
        
        if not cls.AUTHORIZED_USER_IDS:
            issues.append("AUTHORIZED_USER_IDS not configured")
        
        if cls.RISK_PER_TRADE_PERCENT > 5:
            issues.append(f"RISK_PER_TRADE_PERCENT ({cls.RISK_PER_TRADE_PERCENT}%) is unusually high")
        
        if cls.DAILY_LOSS_PERCENT > 10:
            issues.append(f"DAILY_LOSS_PERCENT ({cls.DAILY_LOSS_PERCENT}%) is unusually high")
        
        return {
            'valid': len(issues) == 0,
            'issues': issues
        }
    
    RSI_PERIOD = _get_int_env('RSI_PERIOD', '14')
    RSI_OVERSOLD_LEVEL = _get_int_env('RSI_OVERSOLD_LEVEL', '30')
    RSI_OVERBOUGHT_LEVEL = _get_int_env('RSI_OVERBOUGHT_LEVEL', '70')
    STOCH_K_PERIOD = _get_int_env('STOCH_K_PERIOD', '14')
    STOCH_D_PERIOD = _get_int_env('STOCH_D_PERIOD', '3')
    STOCH_SMOOTH_K = _get_int_env('STOCH_SMOOTH_K', '3')
    STOCH_OVERSOLD_LEVEL = _get_int_env('STOCH_OVERSOLD_LEVEL', '20')
    STOCH_OVERBOUGHT_LEVEL = _get_int_env('STOCH_OVERBOUGHT_LEVEL', '80')
    ATR_PERIOD = _get_int_env('ATR_PERIOD', '14')
    MACD_FAST = _get_int_env('MACD_FAST', '12')
    MACD_SLOW = _get_int_env('MACD_SLOW', '26')
    MACD_SIGNAL = _get_int_env('MACD_SIGNAL', '9')
    VOLUME_THRESHOLD_MULTIPLIER = _get_float_env('VOLUME_THRESHOLD_MULTIPLIER', '0.5')
    MAX_SPREAD_PIPS = _get_float_env('MAX_SPREAD_PIPS', '10.0')
    
    SL_ATR_MULTIPLIER = _get_float_env('SL_ATR_MULTIPLIER', '1.0')
    DEFAULT_SL_PIPS = _get_float_env('DEFAULT_SL_PIPS', '20.0')
    TP_RR_RATIO = _get_float_env('TP_RR_RATIO', '1.5')
    DEFAULT_TP_PIPS = _get_float_env('DEFAULT_TP_PIPS', '30.0')
    
    SIGNAL_COOLDOWN_SECONDS = _get_int_env('SIGNAL_COOLDOWN_SECONDS', '30')
    MAX_TRADES_PER_DAY = _get_int_env('MAX_TRADES_PER_DAY', '999999')
    DAILY_LOSS_PERCENT = _get_float_env('DAILY_LOSS_PERCENT', '3.0')
    RISK_PER_TRADE_PERCENT = _get_float_env('RISK_PER_TRADE_PERCENT', '0.5')
    FIXED_RISK_AMOUNT = _get_float_env('FIXED_RISK_AMOUNT', '1.0')
    
    DYNAMIC_SL_LOSS_THRESHOLD = _get_float_env('DYNAMIC_SL_LOSS_THRESHOLD', '1.0')
    DYNAMIC_SL_TIGHTENING_MULTIPLIER = _get_float_env('DYNAMIC_SL_TIGHTENING_MULTIPLIER', '0.5')
    BREAKEVEN_PROFIT_THRESHOLD = _get_float_env('BREAKEVEN_PROFIT_THRESHOLD', '0.5')
    TRAILING_STOP_PROFIT_THRESHOLD = _get_float_env('TRAILING_STOP_PROFIT_THRESHOLD', '1.0')
    TRAILING_STOP_DISTANCE_PIPS = _get_float_env('TRAILING_STOP_DISTANCE_PIPS', '5.0')
    
    CHART_AUTO_DELETE = os.getenv('CHART_AUTO_DELETE', 'true').lower() == 'true'
    CHART_EXPIRY_MINUTES = _get_int_env('CHART_EXPIRY_MINUTES', '60')
    
    WS_DISCONNECT_ALERT_SECONDS = _get_int_env('WS_DISCONNECT_ALERT_SECONDS', '30')
    
    DATABASE_URL = os.getenv('DATABASE_URL', '')
    DATABASE_PATH = os.getenv('DATABASE_PATH', 'data/bot.db')
    
    DRY_RUN = os.getenv('DRY_RUN', 'false').lower() == 'true'
    
    HEALTH_CHECK_PORT = _get_int_env('PORT', '8080')
    
    XAUUSD_PIP_VALUE = 10.0
    LOT_SIZE = 0.01
    
    @classmethod
    def check_memory_status(cls) -> dict:
        """Check current memory usage and return status
        
        Returns:
            dict: Memory status with level ('normal', 'warning', 'critical')
        """
        try:
            import psutil
            process = psutil.Process()
            mem_info = process.memory_info()
            mem_mb = mem_info.rss / (1024 * 1024)
            
            status = 'normal'
            if mem_mb >= cls.MEMORY_CRITICAL_THRESHOLD_MB:
                status = 'critical'
            elif mem_mb >= cls.MEMORY_WARNING_THRESHOLD_MB:
                status = 'warning'
            
            return {
                'memory_mb': round(mem_mb, 2),
                'warning_threshold': cls.MEMORY_WARNING_THRESHOLD_MB,
                'critical_threshold': cls.MEMORY_CRITICAL_THRESHOLD_MB,
                'status': status,
                'graceful_degradation_enabled': cls.OOM_GRACEFUL_DEGRADATION
            }
        except ImportError:
            try:
                import resource
                mem_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                mem_mb = mem_usage / 1024
                
                status = 'normal'
                if mem_mb >= cls.MEMORY_CRITICAL_THRESHOLD_MB:
                    status = 'critical'
                elif mem_mb >= cls.MEMORY_WARNING_THRESHOLD_MB:
                    status = 'warning'
                
                return {
                    'memory_mb': round(mem_mb, 2),
                    'warning_threshold': cls.MEMORY_WARNING_THRESHOLD_MB,
                    'critical_threshold': cls.MEMORY_CRITICAL_THRESHOLD_MB,
                    'status': status,
                    'source': 'resource',
                    'graceful_degradation_enabled': cls.OOM_GRACEFUL_DEGRADATION
                }
            except Exception:
                return {'status': 'unknown', 'error': 'psutil/resource not available'}
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    @classmethod
    def should_degrade_gracefully(cls) -> bool:
        """Check if bot should enter graceful degradation mode
        
        Returns:
            bool: True if memory is critical and degradation is enabled
        """
        if not cls.OOM_GRACEFUL_DEGRADATION:
            return False
        
        status = cls.check_memory_status()
        return status.get('status') == 'critical'
    
    @classmethod
    def get_adjusted_settings(cls) -> dict:
        """Get adjusted settings based on memory status
        
        Returns settings that should be reduced when memory is critical
        """
        if not cls.should_degrade_gracefully():
            return {}
        
        return {
            'max_candle_history': 100,
            'chart_generation': False,
            'signal_cache_expiry': 60,
            'position_update_interval': 5
        }
