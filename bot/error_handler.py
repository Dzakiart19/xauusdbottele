import sys
import traceback
from typing import Optional, Callable, Any, Type, Tuple, Union
from functools import wraps
from datetime import datetime
import asyncio
from bot.logger import setup_logger

logger = setup_logger('ErrorHandler')

CRITICAL_EXCEPTIONS: Tuple[Type[BaseException], ...] = (
    KeyboardInterrupt,
    SystemExit,
    GeneratorExit,
)

ASYNC_CRITICAL_EXCEPTIONS: Tuple[Type[BaseException], ...] = (
    asyncio.CancelledError,
    KeyboardInterrupt,
    SystemExit,
    GeneratorExit,
)

class TradingBotException(Exception):
    pass

class DatabaseException(TradingBotException):
    pass

class APIException(TradingBotException):
    pass

class ValidationException(TradingBotException):
    pass

class ConfigurationException(TradingBotException):
    pass

class MarketDataException(TradingBotException):
    pass

class SignalException(TradingBotException):
    pass

class CircuitBreakerOpenException(TradingBotException):
    pass

def categorize_exception(exception: Exception) -> str:
    if isinstance(exception, DatabaseException):
        return "DATABASE"
    elif isinstance(exception, APIException):
        return "API"
    elif isinstance(exception, ValidationException):
        return "VALIDATION"
    elif isinstance(exception, ConfigurationException):
        return "CONFIGURATION"
    elif isinstance(exception, MarketDataException):
        return "MARKET_DATA"
    elif isinstance(exception, SignalException):
        return "SIGNAL"
    elif isinstance(exception, TradingBotException):
        return "TRADING_BOT"
    elif isinstance(exception, ConnectionError):
        return "CONNECTION"
    elif isinstance(exception, TimeoutError):
        return "TIMEOUT"
    elif isinstance(exception, ValueError):
        return "VALUE"
    elif isinstance(exception, TypeError):
        return "TYPE"
    elif isinstance(exception, KeyError):
        return "KEY"
    elif isinstance(exception, AttributeError):
        return "ATTRIBUTE"
    elif isinstance(exception, IOError):
        return "IO"
    else:
        return "UNKNOWN"

class ErrorHandler:
    def __init__(self, config):
        self.config = config
        self.error_count = {}
        self.last_error_time = {}
        self.max_retries = 3
        self.retry_delay = 5
    
    def log_exception(self, exception: Exception, context: str = ""):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        
        category = categorize_exception(exception)
        
        error_info = {
            'timestamp': datetime.now().isoformat(),
            'exception_type': type(exception).__name__,
            'exception_category': category,
            'exception_message': str(exception),
            'context': context,
            'traceback': ''.join(traceback.format_tb(exc_traceback)) if exc_traceback else 'N/A'
        }
        
        logger.error(
            f"[{category}] Exception in {context}: "
            f"{type(exception).__name__}: {str(exception)}"
        )
        logger.debug(f"Full traceback:\n{error_info['traceback']}")
        
        error_key = f"{context}_{type(exception).__name__}"
        self.error_count[error_key] = self.error_count.get(error_key, 0) + 1
        self.last_error_time[error_key] = datetime.now()
        
        return error_info
    
    def get_error_stats(self) -> dict:
        return {
            'error_count': dict(self.error_count),
            'last_error_time': {k: v.isoformat() for k, v in self.last_error_time.items()}
        }
    
    def should_retry(self, error_key: str) -> bool:
        count = self.error_count.get(error_key, 0)
        return count < self.max_retries
    
    def reset_error_count(self, error_key: str):
        if error_key in self.error_count:
            del self.error_count[error_key]
        if error_key in self.last_error_time:
            del self.last_error_time[error_key]

def handle_exceptions(context: str = "", reraise: bool = False, 
                     default_return: Any = None,
                     propagate_types: Tuple[Type[Exception], ...] = ()):
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except CRITICAL_EXCEPTIONS:
                raise
            except propagate_types:
                raise
            except Exception as e:
                category = categorize_exception(e)
                
                error_handler = None
                for arg in args:
                    if hasattr(arg, 'error_handler'):
                        error_handler = arg.error_handler
                        break
                
                if error_handler:
                    error_handler.log_exception(e, context or func.__name__)
                else:
                    logger.error(
                        f"[{category}] Exception in {context or func.__name__}: "
                        f"{type(e).__name__}: {e}"
                    )
                
                if reraise:
                    raise
                return default_return
        
        return wrapper
    return decorator

def handle_async_exceptions(context: str = "", reraise: bool = False, 
                           default_return: Any = None,
                           propagate_types: Tuple[Type[Exception], ...] = ()):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except ASYNC_CRITICAL_EXCEPTIONS:
                raise
            except propagate_types:
                raise
            except Exception as e:
                category = categorize_exception(e)
                
                error_handler = None
                for arg in args:
                    if hasattr(arg, 'error_handler'):
                        error_handler = arg.error_handler
                        break
                
                if error_handler:
                    error_handler.log_exception(e, context or func.__name__)
                else:
                    logger.error(
                        f"[{category}] Exception in {context or func.__name__}: "
                        f"{type(e).__name__}: {e}"
                    )
                
                if reraise:
                    raise
                return default_return
        
        return wrapper
    return decorator

def retry_on_exception(max_retries: int = 3, delay: float = 5.0, 
                      exceptions: tuple = (Exception,)):
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except CRITICAL_EXCEPTIONS:
                    raise
                except exceptions as e:
                    retries += 1
                    category = categorize_exception(e)
                    
                    if retries >= max_retries:
                        logger.error(
                            f"[{category}] Max retries ({max_retries}) reached for "
                            f"{func.__name__}: {type(e).__name__}: {e}"
                        )
                        raise
                    
                    logger.warning(
                        f"[{category}] Retry {retries}/{max_retries} for "
                        f"{func.__name__} after {type(e).__name__}: {e}"
                    )
                    import time
                    time.sleep(delay)
            
            return None
        
        return wrapper
    return decorator

def retry_on_async_exception(max_retries: int = 3, delay: float = 5.0, 
                            exceptions: tuple = (Exception,)):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except ASYNC_CRITICAL_EXCEPTIONS:
                    raise
                except exceptions as e:
                    retries += 1
                    category = categorize_exception(e)
                    
                    if retries >= max_retries:
                        logger.error(
                            f"[{category}] Max retries ({max_retries}) reached for "
                            f"{func.__name__}: {type(e).__name__}: {e}"
                        )
                        raise
                    
                    logger.warning(
                        f"[{category}] Retry {retries}/{max_retries} for "
                        f"{func.__name__} after {type(e).__name__}: {e}"
                    )
                    await asyncio.sleep(delay)
            
            return None
        
        return wrapper
    return decorator

class CircuitBreaker:
    CLOSED = 'CLOSED'
    OPEN = 'OPEN'
    HALF_OPEN = 'HALF_OPEN'
    
    def __init__(self, failure_threshold: int = 5, timeout: float = 60.0, 
                 name: str = "default"):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.name = name
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self._state = self.CLOSED
        logger.info(f"CircuitBreaker[{self.name}]: Initialized in CLOSED state")
    
    @property
    def state(self) -> str:
        return self._state
    
    @state.setter
    def state(self, new_state: str):
        if new_state != self._state:
            old_state = self._state
            self._state = new_state
            logger.info(
                f"CircuitBreaker[{self.name}]: State transition {old_state} -> {new_state}"
            )
    
    def _check_state_transition(self) -> bool:
        if self._state == self.OPEN:
            if self.last_failure_time and \
               (datetime.now() - self.last_failure_time).total_seconds() > self.timeout:
                self.state = self.HALF_OPEN
                return True
            raise CircuitBreakerOpenException(
                f"CircuitBreaker[{self.name}] is OPEN. Service temporarily unavailable."
            )
        return True
    
    def _handle_success(self):
        if self._state == self.HALF_OPEN:
            self.state = self.CLOSED
            self.failure_count = 0
    
    def _handle_failure(self, exception: Exception):
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        logger.warning(
            f"CircuitBreaker[{self.name}]: Failure {self.failure_count}/{self.failure_threshold} "
            f"- {type(exception).__name__}: {exception}"
        )
        
        if self.failure_count >= self.failure_threshold:
            self.state = self.OPEN
    
    def call(self, func: Callable, *args, **kwargs):
        self._check_state_transition()
        
        try:
            result = func(*args, **kwargs)
            self._handle_success()
            return result
        except CRITICAL_EXCEPTIONS:
            raise
        except Exception as e:
            self._handle_failure(e)
            raise
    
    async def call_async(self, func: Callable, *args, **kwargs):
        self._check_state_transition()
        
        try:
            result = await func(*args, **kwargs)
            self._handle_success()
            return result
        except ASYNC_CRITICAL_EXCEPTIONS:
            raise
        except Exception as e:
            self._handle_failure(e)
            raise
    
    def __enter__(self):
        self._check_state_transition()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._handle_success()
            return False
        
        if exc_type in CRITICAL_EXCEPTIONS or issubclass(exc_type, CRITICAL_EXCEPTIONS):
            return False
        
        if exc_val is not None:
            self._handle_failure(exc_val)
        
        return False
    
    async def __aenter__(self):
        self._check_state_transition()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._handle_success()
            return False
        
        if exc_type in ASYNC_CRITICAL_EXCEPTIONS or \
           any(issubclass(exc_type, exc) for exc in ASYNC_CRITICAL_EXCEPTIONS):
            return False
        
        if exc_val is not None:
            self._handle_failure(exc_val)
        
        return False
    
    def reset(self):
        old_state = self._state
        self._state = self.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        logger.info(
            f"CircuitBreaker[{self.name}]: Reset from {old_state} to CLOSED state"
        )
    
    def get_stats(self) -> dict:
        return {
            'name': self.name,
            'state': self._state,
            'failure_count': self.failure_count,
            'failure_threshold': self.failure_threshold,
            'timeout': self.timeout,
            'last_failure_time': self.last_failure_time.isoformat() if self.last_failure_time else None
        }
