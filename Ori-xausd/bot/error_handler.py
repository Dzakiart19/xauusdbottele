import sys
import traceback
from typing import Optional, Callable, Any
from functools import wraps
from datetime import datetime
import asyncio
from bot.logger import setup_logger

logger = setup_logger('ErrorHandler')

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

class ErrorHandler:
    def __init__(self, config):
        self.config = config
        self.error_count = {}
        self.last_error_time = {}
        self.max_retries = 3
        self.retry_delay = 5
    
    def log_exception(self, exception: Exception, context: str = ""):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        
        error_info = {
            'timestamp': datetime.now().isoformat(),
            'exception_type': type(exception).__name__,
            'exception_message': str(exception),
            'context': context,
            'traceback': ''.join(traceback.format_tb(exc_traceback)) if exc_traceback else 'N/A'
        }
        
        logger.error(f"Exception in {context}: {type(exception).__name__}: {str(exception)}")
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
                     default_return: Any = None):
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_handler = None
                for arg in args:
                    if hasattr(arg, 'error_handler'):
                        error_handler = arg.error_handler
                        break
                
                if error_handler:
                    error_handler.log_exception(e, context or func.__name__)
                else:
                    logger.error(f"Exception in {context or func.__name__}: {e}")
                
                if reraise:
                    raise
                return default_return
        
        return wrapper
    return decorator

def handle_async_exceptions(context: str = "", reraise: bool = False, 
                           default_return: Any = None):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                error_handler = None
                for arg in args:
                    if hasattr(arg, 'error_handler'):
                        error_handler = arg.error_handler
                        break
                
                if error_handler:
                    error_handler.log_exception(e, context or func.__name__)
                else:
                    logger.error(f"Exception in {context or func.__name__}: {e}")
                
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
                except exceptions as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(f"Max retries ({max_retries}) reached for {func.__name__}: {e}")
                        raise
                    
                    logger.warning(f"Retry {retries}/{max_retries} for {func.__name__} after error: {e}")
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
                except exceptions as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(f"Max retries ({max_retries}) reached for {func.__name__}: {e}")
                        raise
                    
                    logger.warning(f"Retry {retries}/{max_retries} for {func.__name__} after error: {e}")
                    await asyncio.sleep(delay)
            
            return None
        
        return wrapper
    return decorator

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'
    
    def call(self, func: Callable, *args, **kwargs):
        if self.state == 'OPEN':
            if self.last_failure_time and (datetime.now() - self.last_failure_time).total_seconds() > self.timeout:
                logger.info("Circuit breaker attempting to close")
                self.state = 'HALF_OPEN'
            else:
                raise Exception("Circuit breaker is OPEN. Service temporarily unavailable.")
        
        try:
            result = func(*args, **kwargs)
            
            if self.state == 'HALF_OPEN':
                logger.info("Circuit breaker closing after successful call")
                self.state = 'CLOSED'
                self.failure_count = 0
            
            return result
            
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = datetime.now()
            
            if self.failure_count >= self.failure_threshold:
                logger.error(f"Circuit breaker opening after {self.failure_count} failures")
                self.state = 'OPEN'
            
            raise
    
    async def call_async(self, func: Callable, *args, **kwargs):
        if self.state == 'OPEN':
            if self.last_failure_time and (datetime.now() - self.last_failure_time).total_seconds() > self.timeout:
                logger.info("Circuit breaker attempting to close")
                self.state = 'HALF_OPEN'
            else:
                raise Exception("Circuit breaker is OPEN. Service temporarily unavailable.")
        
        try:
            result = await func(*args, **kwargs)
            
            if self.state == 'HALF_OPEN':
                logger.info("Circuit breaker closing after successful call")
                self.state = 'CLOSED'
                self.failure_count = 0
            
            return result
            
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = datetime.now()
            
            if self.failure_count >= self.failure_threshold:
                logger.error(f"Circuit breaker opening after {self.failure_count} failures")
                self.state = 'OPEN'
            
            raise
    
    def reset(self):
        self.state = 'CLOSED'
        self.failure_count = 0
        self.last_failure_time = None
        logger.info("Circuit breaker reset")
