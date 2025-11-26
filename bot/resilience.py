"""
Resilience patterns: Circuit Breaker and Rate Limiter
"""
import asyncio
import time
from typing import Optional, Callable, Any
from enum import Enum
from collections import deque
from datetime import datetime, timedelta
import logging

logger = logging.getLogger('Resilience')


class CircuitBreakerOpenException(Exception):
    """Exception raised when circuit breaker is open"""
    def __init__(self, name: str, retry_in: float):
        self.name = name
        self.retry_in = retry_in
        super().__init__(f"CircuitBreaker '{name}' is OPEN. Retry in {retry_in:.1f}s")


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """Circuit breaker pattern for preventing cascading failures
    
    Tracks failures and opens circuit after threshold is exceeded.
    Automatically attempts recovery after cooldown period.
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type = Exception,
        name: str = "CircuitBreaker"
    ):
        """Initialize circuit breaker
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
            expected_exception: Exception type to catch
            name: Name for logging
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.name = name
        
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = CircuitState.CLOSED
        
        logger.info(
            f"CircuitBreaker '{name}' initialized: "
            f"threshold={failure_threshold}, timeout={recovery_timeout}s"
        )
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection
        
        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            Exception: If circuit is open or function fails
        """
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                logger.info(f"CircuitBreaker '{self.name}': Attempting recovery (HALF_OPEN)")
                self.state = CircuitState.HALF_OPEN
            else:
                remaining = self.recovery_timeout - (time.time() - (self.last_failure_time or 0))
                raise CircuitBreakerOpenException(self.name, remaining)
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except CircuitBreakerOpenException:
            raise
        except Exception as e:
            if isinstance(e, self.expected_exception):
                self._on_failure()
            raise
    
    async def call_async(self, func: Callable, *args, **kwargs) -> Any:
        """Execute async function with circuit breaker protection
        
        Args:
            func: Async function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            Exception: If circuit is open or function fails
        """
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                logger.info(f"CircuitBreaker '{self.name}': Attempting recovery (HALF_OPEN)")
                self.state = CircuitState.HALF_OPEN
            else:
                remaining = self.recovery_timeout - (time.time() - (self.last_failure_time or 0))
                raise CircuitBreakerOpenException(self.name, remaining)
        
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except CircuitBreakerOpenException:
            raise
        except Exception as e:
            if isinstance(e, self.expected_exception):
                self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt recovery"""
        if self.last_failure_time is None:
            return False
        return time.time() - self.last_failure_time >= self.recovery_timeout
    
    def _on_success(self):
        """Handle successful execution"""
        if self.state == CircuitState.HALF_OPEN:
            logger.info(f"CircuitBreaker '{self.name}': Recovery successful (CLOSED)")
            self.state = CircuitState.CLOSED
            self.failure_count = 0
    
    def _on_failure(self):
        """Handle failed execution"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.state == CircuitState.HALF_OPEN:
            logger.warning(
                f"CircuitBreaker '{self.name}': Recovery failed (OPEN again)"
            )
            self.state = CircuitState.OPEN
        elif self.failure_count >= self.failure_threshold:
            logger.error(
                f"CircuitBreaker '{self.name}': Threshold exceeded "
                f"({self.failure_count}/{self.failure_threshold}) - Opening circuit"
            )
            self.state = CircuitState.OPEN
        else:
            logger.warning(
                f"CircuitBreaker '{self.name}': Failure {self.failure_count}/{self.failure_threshold}"
            )
    
    def reset(self):
        """Manually reset circuit breaker"""
        logger.info(f"CircuitBreaker '{self.name}': Manual reset")
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
    
    def get_state(self) -> dict:
        """Get current circuit breaker state"""
        return {
            'name': self.name,
            'state': self.state.value,
            'failure_count': self.failure_count,
            'failure_threshold': self.failure_threshold,
            'last_failure_time': self.last_failure_time
        }


class RateLimiter:
    """Rate limiter using token bucket algorithm
    
    Limits number of operations within a time window.
    """
    
    def __init__(
        self,
        max_calls: int = 30,
        time_window: float = 60.0,
        name: str = "RateLimiter"
    ):
        """Initialize rate limiter
        
        Args:
            max_calls: Maximum number of calls allowed
            time_window: Time window in seconds
            name: Name for logging
        """
        self.max_calls = max_calls
        self.time_window = time_window
        self.name = name
        self.call_times: deque = deque(maxlen=max_calls)
        
        logger.info(
            f"RateLimiter '{name}' initialized: "
            f"{max_calls} calls per {time_window}s"
        )
    
    def acquire(self) -> bool:
        """Try to acquire permission to make a call
        
        Returns:
            True if call is allowed, False if rate limit exceeded
        """
        now = time.time()
        
        while self.call_times and now - self.call_times[0] > self.time_window:
            self.call_times.popleft()
        
        if len(self.call_times) < self.max_calls:
            self.call_times.append(now)
            return True
        else:
            oldest_call = self.call_times[0]
            wait_time = self.time_window - (now - oldest_call)
            logger.warning(
                f"RateLimiter '{self.name}': Rate limit exceeded. "
                f"Wait {wait_time:.1f}s"
            )
            return False
    
    async def acquire_async(self, wait: bool = False) -> bool:
        """Try to acquire permission (async version)
        
        Args:
            wait: If True, wait until permission is available
            
        Returns:
            True if call is allowed, False if rate limit exceeded (when wait=False)
        """
        while True:
            now = time.time()
            
            while self.call_times and now - self.call_times[0] > self.time_window:
                self.call_times.popleft()
            
            if len(self.call_times) < self.max_calls:
                self.call_times.append(now)
                return True
            else:
                if not wait:
                    oldest_call = self.call_times[0]
                    wait_time = self.time_window - (now - oldest_call)
                    logger.warning(
                        f"RateLimiter '{self.name}': Rate limit exceeded. "
                        f"Wait {wait_time:.1f}s"
                    )
                    return False
                
                oldest_call = self.call_times[0]
                wait_time = self.time_window - (now - oldest_call)
                logger.info(
                    f"RateLimiter '{self.name}': Waiting {wait_time:.1f}s for rate limit"
                )
                await asyncio.sleep(wait_time + 0.1)
    
    def get_remaining(self) -> int:
        """Get remaining calls in current window"""
        now = time.time()
        
        while self.call_times and now - self.call_times[0] > self.time_window:
            self.call_times.popleft()
        
        return self.max_calls - len(self.call_times)
    
    def get_wait_time(self) -> float:
        """Get wait time until next call is allowed (seconds)"""
        now = time.time()
        
        while self.call_times and now - self.call_times[0] > self.time_window:
            self.call_times.popleft()
        
        if len(self.call_times) < self.max_calls:
            return 0.0
        
        oldest_call = self.call_times[0]
        return max(0.0, self.time_window - (now - oldest_call))
    
    def reset(self):
        """Reset rate limiter"""
        logger.info(f"RateLimiter '{self.name}': Reset")
        self.call_times.clear()
    
    def get_state(self) -> dict:
        """Get current rate limiter state"""
        return {
            'name': self.name,
            'max_calls': self.max_calls,
            'time_window': self.time_window,
            'current_calls': len(self.call_times),
            'remaining': self.get_remaining(),
            'wait_time': self.get_wait_time()
        }
    
    def get_time_window(self) -> float:
        """Get time window in seconds (public accessor)"""
        return self.time_window
    
    def get_call_times(self) -> list:
        """Get list of call timestamps (public accessor, returns copy)"""
        return list(self.call_times)
    
    def set_call_times(self, call_times: list):
        """Set call times from restored state (public accessor)"""
        self.call_times.clear()
        for ts in call_times:
            self.call_times.append(ts)
