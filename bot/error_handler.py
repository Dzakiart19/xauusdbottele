import sys
import traceback
from typing import Optional, Callable, Any, Type, Tuple, Union, Dict, List
from functools import wraps
from datetime import datetime
from dataclasses import dataclass, field
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


class ExceptionCategory:
    """Enum-like class for exception categories"""
    DATABASE = "DATABASE"
    API = "API"
    HTTP = "HTTP"
    WEBSOCKET = "WEBSOCKET"
    TELEGRAM = "TELEGRAM"
    ASYNCIO = "ASYNCIO"
    SQLALCHEMY = "SQLALCHEMY"
    VALIDATION = "VALIDATION"
    CONFIGURATION = "CONFIGURATION"
    MARKET_DATA = "MARKET_DATA"
    SIGNAL = "SIGNAL"
    TRADING_BOT = "TRADING_BOT"
    CONNECTION = "CONNECTION"
    TIMEOUT = "TIMEOUT"
    VALUE = "VALUE"
    TYPE = "TYPE"
    KEY = "KEY"
    ATTRIBUTE = "ATTRIBUTE"
    IO = "IO"
    NETWORK = "NETWORK"
    UNKNOWN = "UNKNOWN"


@dataclass
class ErrorContext:
    """Rich context information for errors
    
    Captures comprehensive error context including traceback, 
    local variables, and structured metadata for debugging.
    """
    exception_type: str
    exception_message: str
    category: str
    context_name: str
    timestamp: datetime = field(default_factory=datetime.now)
    traceback_str: str = ""
    traceback_frames: List[Dict[str, Any]] = field(default_factory=list)
    local_vars: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    is_recoverable: bool = True
    severity: str = "ERROR"
    
    @classmethod
    def from_exception(cls, exception: Exception, context: str = "", 
                       capture_locals: bool = False,
                       extra_metadata: Optional[Dict[str, Any]] = None) -> 'ErrorContext':
        """Create ErrorContext from an exception
        
        Args:
            exception: The exception to capture context from
            context: Name/description of the context where error occurred
            capture_locals: Whether to capture local variables from frames
            extra_metadata: Additional metadata to include
            
        Returns:
            ErrorContext instance with captured information
        """
        exc_type, exc_value, exc_traceback = sys.exc_info()
        
        category = categorize_exception(exception)
        
        traceback_str = ""
        traceback_frames = []
        local_vars = {}
        
        if exc_traceback:
            traceback_str = ''.join(traceback.format_tb(exc_traceback))
            
            frame = exc_traceback
            while frame is not None:
                frame_info = {
                    'filename': frame.tb_frame.f_code.co_filename,
                    'function': frame.tb_frame.f_code.co_name,
                    'lineno': frame.tb_lineno,
                }
                
                if capture_locals and frame.tb_frame.f_locals:
                    frame_locals = {}
                    for key, value in frame.tb_frame.f_locals.items():
                        if not key.startswith('_'):
                            try:
                                frame_locals[key] = repr(value)[:200]
                            except Exception:
                                frame_locals[key] = "<unrepresentable>"
                    frame_info['locals'] = frame_locals
                    
                    if frame.tb_next is None:
                        local_vars = frame_locals
                
                traceback_frames.append(frame_info)
                frame = frame.tb_next
        
        severity = cls._determine_severity(exception, category)
        is_recoverable = cls._is_recoverable(exception, category)
        
        metadata = extra_metadata or {}
        metadata.update({
            'exception_class': type(exception).__name__,
            'exception_module': type(exception).__module__,
        })
        
        if hasattr(exception, '__cause__') and exception.__cause__:
            metadata['cause'] = str(exception.__cause__)
        if hasattr(exception, '__context__') and exception.__context__:
            metadata['context_exception'] = str(exception.__context__)
        
        return cls(
            exception_type=type(exception).__name__,
            exception_message=str(exception),
            category=category,
            context_name=context,
            traceback_str=traceback_str,
            traceback_frames=traceback_frames,
            local_vars=local_vars,
            metadata=metadata,
            severity=severity,
            is_recoverable=is_recoverable
        )
    
    @staticmethod
    def _determine_severity(exception: Exception, category: str) -> str:
        """Determine severity level based on exception type and category"""
        critical_categories = {
            ExceptionCategory.DATABASE, 
            ExceptionCategory.CONFIGURATION,
            ExceptionCategory.SQLALCHEMY
        }
        warning_categories = {
            ExceptionCategory.TIMEOUT,
            ExceptionCategory.NETWORK,
            ExceptionCategory.CONNECTION
        }
        
        if category in critical_categories:
            return "CRITICAL"
        elif category in warning_categories:
            return "WARNING"
        else:
            return "ERROR"
    
    @staticmethod
    def _is_recoverable(exception: Exception, category: str) -> bool:
        """Determine if exception is recoverable"""
        non_recoverable = {
            ExceptionCategory.CONFIGURATION,
            ExceptionCategory.DATABASE
        }
        
        if category in non_recoverable:
            return False
        
        if category in {ExceptionCategory.TIMEOUT, ExceptionCategory.CONNECTION, 
                       ExceptionCategory.NETWORK}:
            return True
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'exception_type': self.exception_type,
            'exception_message': self.exception_message,
            'category': self.category,
            'context_name': self.context_name,
            'timestamp': self.timestamp.isoformat(),
            'traceback': self.traceback_str,
            'traceback_frames': self.traceback_frames,
            'local_vars': self.local_vars,
            'metadata': self.metadata,
            'retry_count': self.retry_count,
            'is_recoverable': self.is_recoverable,
            'severity': self.severity
        }
    
    def to_structured_payload(self) -> Dict[str, Any]:
        """Create structured error payload for external systems"""
        return {
            'error': {
                'type': self.exception_type,
                'message': self.exception_message,
                'category': self.category
            },
            'context': {
                'name': self.context_name,
                'timestamp': self.timestamp.isoformat(),
                'severity': self.severity,
                'recoverable': self.is_recoverable
            },
            'debug': {
                'traceback': self.traceback_str,
                'frames_count': len(self.traceback_frames),
                'metadata': self.metadata
            }
        }


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

class HTTPException(APIException):
    """HTTP-specific API exception"""
    def __init__(self, message: str, status_code: Optional[int] = None, 
                 response_body: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body

class WebSocketException(APIException):
    """WebSocket-specific exception"""
    def __init__(self, message: str, close_code: Optional[int] = None):
        super().__init__(message)
        self.close_code = close_code

class TelegramException(APIException):
    """Telegram API exception"""
    def __init__(self, message: str, error_code: Optional[int] = None,
                 retry_after: Optional[int] = None):
        super().__init__(message)
        self.error_code = error_code
        self.retry_after = retry_after

class SQLAlchemyException(DatabaseException):
    """SQLAlchemy-specific exception"""
    pass


HTTP_EXCEPTION_PATTERNS = {
    'aiohttp.ClientError': ExceptionCategory.HTTP,
    'aiohttp.ClientResponseError': ExceptionCategory.HTTP,
    'aiohttp.ServerTimeoutError': ExceptionCategory.HTTP,
    'requests.exceptions.RequestException': ExceptionCategory.HTTP,
    'requests.exceptions.HTTPError': ExceptionCategory.HTTP,
    'requests.exceptions.ConnectionError': ExceptionCategory.HTTP,
    'requests.exceptions.Timeout': ExceptionCategory.HTTP,
    'urllib3.exceptions.HTTPError': ExceptionCategory.HTTP,
    'httpx.HTTPError': ExceptionCategory.HTTP,
    'httpx.RequestError': ExceptionCategory.HTTP,
}

WEBSOCKET_EXCEPTION_PATTERNS = {
    'websockets.exceptions.WebSocketException': ExceptionCategory.WEBSOCKET,
    'websockets.exceptions.ConnectionClosed': ExceptionCategory.WEBSOCKET,
    'websockets.exceptions.ConnectionClosedError': ExceptionCategory.WEBSOCKET,
    'aiohttp.WSServerHandshakeError': ExceptionCategory.WEBSOCKET,
}

TELEGRAM_EXCEPTION_PATTERNS = {
    'telegram.error.TelegramError': ExceptionCategory.TELEGRAM,
    'telegram.error.NetworkError': ExceptionCategory.TELEGRAM,
    'telegram.error.TimedOut': ExceptionCategory.TELEGRAM,
    'telegram.error.RetryAfter': ExceptionCategory.TELEGRAM,
    'telegram.error.Conflict': ExceptionCategory.TELEGRAM,
    'telegram.error.BadRequest': ExceptionCategory.TELEGRAM,
    'telegram.error.Forbidden': ExceptionCategory.TELEGRAM,
}

ASYNCIO_EXCEPTION_PATTERNS = {
    'asyncio.TimeoutError': ExceptionCategory.ASYNCIO,
    'asyncio.CancelledError': ExceptionCategory.ASYNCIO,
    'asyncio.InvalidStateError': ExceptionCategory.ASYNCIO,
    'concurrent.futures.TimeoutError': ExceptionCategory.ASYNCIO,
}

SQLALCHEMY_EXCEPTION_PATTERNS = {
    'sqlalchemy.exc.SQLAlchemyError': ExceptionCategory.SQLALCHEMY,
    'sqlalchemy.exc.OperationalError': ExceptionCategory.SQLALCHEMY,
    'sqlalchemy.exc.IntegrityError': ExceptionCategory.SQLALCHEMY,
    'sqlalchemy.exc.ProgrammingError': ExceptionCategory.SQLALCHEMY,
    'sqlalchemy.exc.DatabaseError': ExceptionCategory.SQLALCHEMY,
    'sqlalchemy.exc.InterfaceError': ExceptionCategory.SQLALCHEMY,
    'sqlalchemy.orm.exc.NoResultFound': ExceptionCategory.SQLALCHEMY,
    'sqlalchemy.orm.exc.MultipleResultsFound': ExceptionCategory.SQLALCHEMY,
}


def _get_exception_full_name(exception: Exception) -> str:
    """Get fully qualified name of exception class"""
    exc_type = type(exception)
    return f"{exc_type.__module__}.{exc_type.__name__}"


def categorize_exception(exception: Exception) -> str:
    """Categorize exception with enhanced HTTP/WS/SQLAlchemy/Telegram/asyncio mapping"""
    if isinstance(exception, DatabaseException):
        return ExceptionCategory.DATABASE
    elif isinstance(exception, SQLAlchemyException):
        return ExceptionCategory.SQLALCHEMY
    elif isinstance(exception, HTTPException):
        return ExceptionCategory.HTTP
    elif isinstance(exception, WebSocketException):
        return ExceptionCategory.WEBSOCKET
    elif isinstance(exception, TelegramException):
        return ExceptionCategory.TELEGRAM
    elif isinstance(exception, APIException):
        return ExceptionCategory.API
    elif isinstance(exception, ValidationException):
        return ExceptionCategory.VALIDATION
    elif isinstance(exception, ConfigurationException):
        return ExceptionCategory.CONFIGURATION
    elif isinstance(exception, MarketDataException):
        return ExceptionCategory.MARKET_DATA
    elif isinstance(exception, SignalException):
        return ExceptionCategory.SIGNAL
    elif isinstance(exception, TradingBotException):
        return ExceptionCategory.TRADING_BOT
    
    exc_full_name = _get_exception_full_name(exception)
    exc_class_name = type(exception).__name__
    
    for pattern, category in HTTP_EXCEPTION_PATTERNS.items():
        if pattern in exc_full_name or exc_class_name in pattern:
            return category
    
    for pattern, category in WEBSOCKET_EXCEPTION_PATTERNS.items():
        if pattern in exc_full_name or exc_class_name in pattern:
            return category
    
    for pattern, category in TELEGRAM_EXCEPTION_PATTERNS.items():
        if pattern in exc_full_name or exc_class_name in pattern:
            return category
    
    for pattern, category in ASYNCIO_EXCEPTION_PATTERNS.items():
        if pattern in exc_full_name or exc_class_name in pattern:
            return category
    
    for pattern, category in SQLALCHEMY_EXCEPTION_PATTERNS.items():
        if pattern in exc_full_name or exc_class_name in pattern:
            return category
    
    if isinstance(exception, ConnectionError):
        return ExceptionCategory.CONNECTION
    elif isinstance(exception, (TimeoutError, asyncio.TimeoutError)):
        return ExceptionCategory.TIMEOUT
    elif isinstance(exception, ValueError):
        return ExceptionCategory.VALUE
    elif isinstance(exception, TypeError):
        return ExceptionCategory.TYPE
    elif isinstance(exception, KeyError):
        return ExceptionCategory.KEY
    elif isinstance(exception, AttributeError):
        return ExceptionCategory.ATTRIBUTE
    elif isinstance(exception, (IOError, OSError)):
        return ExceptionCategory.IO
    elif isinstance(exception, (ConnectionRefusedError, ConnectionResetError, 
                                ConnectionAbortedError, BrokenPipeError)):
        return ExceptionCategory.NETWORK
    else:
        return ExceptionCategory.UNKNOWN

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
