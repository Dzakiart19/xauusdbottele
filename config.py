import os
from dotenv import load_dotenv

load_dotenv()

class ConfigValidationError(Exception):
    """Raised when critical configuration is invalid or missing"""
    pass

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
    try:
        return [int(uid.strip()) for uid in env_value.split(',') if uid.strip()]
    except (ValueError, AttributeError):
        return []

def _parse_int_list(env_value: str, default_list: list) -> list:
    """Parse comma-separated integers, returning default on error"""
    try:
        return [int(p.strip()) for p in env_value.split(',') if p.strip()]
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
    
    @classmethod
    def get_masked_token(cls) -> str:
        from bot.logger import mask_token
        return mask_token(cls.TELEGRAM_BOT_TOKEN)
    
    @classmethod
    def validate(cls):
        """Validate critical configuration settings
        
        Raises:
            ConfigValidationError: If critical configuration is missing or invalid
        """
        errors = []
        warnings = []
        
        if not cls.TELEGRAM_BOT_TOKEN or cls.TELEGRAM_BOT_TOKEN.strip() == '':
            errors.append("TELEGRAM_BOT_TOKEN is required but not set")
        
        if not cls.AUTHORIZED_USER_IDS or len(cls.AUTHORIZED_USER_IDS) == 0:
            errors.append("AUTHORIZED_USER_IDS must contain at least one user ID")
        
        if cls.TELEGRAM_WEBHOOK_MODE:
            if not cls.WEBHOOK_URL or cls.WEBHOOK_URL.strip() == '':
                warnings.append("TELEGRAM_WEBHOOK_MODE is enabled but WEBHOOK_URL is not set - will attempt auto-detection")
            else:
                webhook_url = cls.WEBHOOK_URL.strip()
                if not (webhook_url.startswith('http://') or webhook_url.startswith('https://')):
                    errors.append(f"WEBHOOK_URL must start with http:// or https://, got: {webhook_url[:30]}...")
                if not ('/bot' in webhook_url):
                    warnings.append("WEBHOOK_URL should typically contain '/bot<token>' endpoint for Telegram webhooks")
        
        if cls.RISK_PER_TRADE_PERCENT <= 0 or cls.RISK_PER_TRADE_PERCENT > 100:
            errors.append(f"RISK_PER_TRADE_PERCENT must be between 0 and 100, got {cls.RISK_PER_TRADE_PERCENT}")
        
        if cls.DAILY_LOSS_PERCENT <= 0 or cls.DAILY_LOSS_PERCENT > 100:
            errors.append(f"DAILY_LOSS_PERCENT must be between 0 and 100, got {cls.DAILY_LOSS_PERCENT}")
        
        if cls.FIXED_RISK_AMOUNT <= 0:
            errors.append(f"FIXED_RISK_AMOUNT must be positive, got {cls.FIXED_RISK_AMOUNT}")
        
        if cls.TP_RR_RATIO <= 0:
            errors.append(f"TP_RR_RATIO must be positive, got {cls.TP_RR_RATIO}")
        
        if warnings:
            from bot.logger import setup_logger
            logger = setup_logger('Config')
            for warning in warnings:
                logger.warning(f"Configuration warning: {warning}")
        
        if errors:
            raise ConfigValidationError("Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors))
        
        return True
    
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
