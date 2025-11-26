import asyncio
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.error import (
    TelegramError, NetworkError, TimedOut, RetryAfter, BadRequest,
    Forbidden, Conflict, ChatMigrated, InvalidToken
)
from telegram import error as telegram_error
from datetime import datetime, timedelta
import pytz
import pandas as pd
from typing import Optional, List, Callable, Any, Dict
from functools import wraps
import time
from bot.logger import setup_logger, mask_user_id, mask_token, sanitize_log_message
from bot.database import Trade, Position, Performance
from bot.signal_session_manager import SignalSessionManager
from bot.message_templates import MessageFormatter
from bot.resilience import RateLimiter

logger = setup_logger('TelegramBot')

class TelegramBotError(Exception):
    """Base exception for Telegram bot errors"""
    pass

class RateLimitError(TelegramBotError):
    """Rate limit exceeded"""
    pass

class ValidationError(TelegramBotError):
    """Input validation error"""
    pass

def validate_user_id(user_id_str: str) -> tuple[bool, Optional[int], Optional[str]]:
    """Validate and sanitize user ID input
    
    Returns:
        tuple: (is_valid, sanitized_user_id, error_message)
    """
    try:
        if not user_id_str or not isinstance(user_id_str, str):
            return False, None, "User ID tidak boleh kosong"
        
        user_id_str = user_id_str.strip()
        
        if not user_id_str.isdigit():
            return False, None, "User ID harus berupa angka"
        
        user_id = int(user_id_str)
        
        if user_id <= 0:
            return False, None, "User ID harus positif"
        
        if user_id > 9999999999:
            return False, None, "User ID tidak valid (terlalu besar)"
        
        return True, user_id, None
        
    except ValueError:
        return False, None, "Format user ID tidak valid"
    except Exception as e:
        return False, None, f"Error validasi user ID: {str(e)}"

def validate_duration_days(duration_str: str) -> tuple[bool, Optional[int], Optional[str]]:
    """Validate and sanitize duration days input
    
    Returns:
        tuple: (is_valid, sanitized_duration, error_message)
    """
    try:
        if not duration_str or not isinstance(duration_str, str):
            return False, None, "Durasi tidak boleh kosong"
        
        duration_str = duration_str.strip()
        
        if not duration_str.isdigit():
            return False, None, "Durasi harus berupa angka"
        
        duration = int(duration_str)
        
        if duration <= 0:
            return False, None, "Durasi harus lebih dari 0 hari"
        
        if duration > 365:
            return False, None, "Durasi maksimal 365 hari"
        
        return True, duration, None
        
    except ValueError:
        return False, None, "Format durasi tidak valid"
    except Exception as e:
        return False, None, f"Error validasi durasi: {str(e)}"

def sanitize_command_argument(arg: str, max_length: int = 100) -> str:
    """Sanitize command argument to prevent injection
    
    Args:
        arg: Argument string to sanitize
        max_length: Maximum allowed length
        
    Returns:
        Sanitized argument string
    """
    if not arg or not isinstance(arg, str):
        return ""
    
    sanitized = arg.strip()
    
    sanitized = ''.join(c for c in sanitized if c.isprintable())
    
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length]
    
    dangerous_patterns = ['<script>', 'javascript:', 'onerror=', 'onclick=', '${', '`']
    for pattern in dangerous_patterns:
        if pattern.lower() in sanitized.lower():
            logger.warning(f"Dangerous pattern detected in input: {pattern}")
            sanitized = sanitized.replace(pattern, '')
    
    return sanitized

def retry_on_telegram_error(max_retries: int = 3, initial_delay: float = 1.0):
    """Decorator to retry Telegram API calls with exponential backoff.
    
    Error handling strategy:
    - Transient errors (NetworkError, TimedOut): Retry with exponential backoff
    - Rate limit (RetryAfter): Wait for specified duration then retry
    - Permanent errors (BadRequest, Forbidden): No retry, log and raise
    - Auth errors (InvalidToken, Conflict): Critical alert, no retry
    - Migration (ChatMigrated): Extract new chat ID and raise with info
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            delay = initial_delay
            
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                    
                except RetryAfter as e:
                    retry_after = e.retry_after if hasattr(e, 'retry_after') else 5
                    logger.warning(f"Rate limit hit in {func.__name__}, retrying after {retry_after}s")
                    await asyncio.sleep(retry_after + 1)
                    last_exception = e
                    
                except TimedOut as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Timeout in {func.__name__} (attempt {attempt + 1}/{max_retries}), retrying in {delay}s")
                        await asyncio.sleep(delay)
                        delay *= 2
                        last_exception = e
                    else:
                        logger.error(f"Max retries reached for {func.__name__} due to timeout")
                        raise
                        
                except NetworkError as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Network error in {func.__name__} (attempt {attempt + 1}/{max_retries}): {e}")
                        await asyncio.sleep(delay)
                        delay *= 2
                        last_exception = e
                    else:
                        logger.error(f"Max retries reached for {func.__name__} due to network error")
                        raise
                
                except BadRequest as e:
                    logger.error(f"BadRequest in {func.__name__}: {e} - Invalid request, tidak akan retry")
                    raise
                
                except Forbidden as e:
                    logger.warning(f"Forbidden in {func.__name__}: {e} - User mungkin memblokir bot atau chat tidak dapat diakses")
                    raise
                
                except ChatMigrated as e:
                    new_chat_id = e.new_chat_id if hasattr(e, 'new_chat_id') else None
                    logger.warning(f"ChatMigrated in {func.__name__}: Chat migrated to new ID: {new_chat_id}")
                    raise
                
                except Conflict as e:
                    logger.critical(f"🔴 CONFLICT in {func.__name__}: {e} - Multiple bot instances detected!")
                    raise
                
                except InvalidToken as e:
                    logger.critical(f"🔴 UNAUTHORIZED in {func.__name__}: {e} - Token tidak valid atau bot di-revoke!")
                    raise
                        
                except TelegramError as e:
                    logger.error(f"Telegram API error in {func.__name__}: {e}")
                    raise
                    
                except Exception as e:
                    logger.error(f"Unexpected error in {func.__name__}: {type(e).__name__}: {e}")
                    raise
            
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator

def validate_chat_id(chat_id: Any) -> bool:
    """Validate chat ID"""
    try:
        if chat_id is None:
            return False
        if isinstance(chat_id, int):
            return chat_id != 0
        if isinstance(chat_id, str):
            return chat_id.lstrip('-').isdigit()
        return False
    except Exception:
        return False

class TradingBot:
    MAX_CACHE_SIZE = 1000
    MAX_DASHBOARDS = 100
    MAX_MONITORING_CHATS = 50
    
    def __init__(self, config, db_manager, strategy, risk_manager, 
                 market_data, position_tracker, chart_generator,
                 alert_system=None, error_handler=None, user_manager=None, signal_session_manager=None, task_scheduler=None):
        self.config = config
        self.db = db_manager
        self.strategy = strategy
        self.risk_manager = risk_manager
        self.market_data = market_data
        self.position_tracker = position_tracker
        self.chart_generator = chart_generator
        self.alert_system = alert_system
        self.error_handler = error_handler
        self.user_manager = user_manager
        self.signal_session_manager = signal_session_manager
        self.task_scheduler = task_scheduler
        self.app = None
        self.monitoring = False
        self.monitoring_chats = []
        self.signal_lock = asyncio.Lock()
        self.monitoring_tasks: Dict[int, asyncio.Task] = {}
        self.active_dashboards: Dict[int, Dict] = {}
        self._is_shutting_down: bool = False
        
        self.rate_limiter = RateLimiter(
            max_calls=30,
            time_window=60.0,
            name="TelegramAPI"
        )
        logger.info("✅ Rate limiter initialized for Telegram API")
        
        self.global_last_signal_time = datetime.now() - timedelta(seconds=60)
        self.signal_detection_interval = 0  # INSTANT - 0 delay, check on every tick
        self.global_signal_cooldown = 3.0  # Global cooldown to prevent Telegram API flood
        self.tick_throttle_seconds = 3.0  # Minimum interval between signal checks (match global cooldown)
        logger.info(f"✅ Optimized signal detection: global_cooldown={self.global_signal_cooldown}s, tick_throttle={self.tick_throttle_seconds}s")
        
        self.sent_signals_cache: Dict[str, Dict[str, any]] = {}
        self.signal_cache_expiry_seconds = 120
        self.signal_price_tolerance_pips = 10.0
        self._cache_lock = asyncio.Lock()
        self._dashboard_lock = asyncio.Lock()
        self._chart_cleanup_lock = asyncio.Lock()
        self._cache_cleanup_task: Optional[asyncio.Task] = None
        self._dashboard_cleanup_task: Optional[asyncio.Task] = None
        self._chart_cleanup_task: Optional[asyncio.Task] = None
        self._cleanup_tasks_running: bool = False
        
        self._cache_telemetry = {
            'hits': 0,
            'misses': 0,
            'pending_set': 0,
            'confirmed': 0,
            'rollbacks': 0,
            'expired_cleanups': 0,
            'size_enforcements': 0,
            'last_cleanup_time': None,
            'last_cleanup_count': 0,
        }
        self._pending_charts: Dict[int, Dict[str, any]] = {}
        self._chart_eviction_callbacks: List[Callable] = []
        logger.info("✅ Two-phase anti-duplicate signal cache initialized (pending→confirmed) with telemetry")
        
        import os
        self.instance_lock_file = os.path.join('data', '.bot_instance.lock')
        os.makedirs('data', exist_ok=True)
        
    def is_authorized(self, user_id: int) -> bool:
        if self.user_manager:
            return self.user_manager.has_access(user_id)
        
        if not self.config.AUTHORIZED_USER_IDS:
            return True
        return user_id in self.config.AUTHORIZED_USER_IDS
    
    def is_admin(self, user_id: int) -> bool:
        if self.user_manager:
            user = self.user_manager.get_user(user_id)
            return user.is_admin if user else False
        return user_id in self.config.AUTHORIZED_USER_IDS
    
    def _generate_signal_hash(self, user_id: int, signal_type: str, entry_price: float) -> str:
        price_bucket = round(entry_price / (self.signal_price_tolerance_pips / self.config.XAUUSD_PIP_VALUE))
        return f"{user_id}_{signal_type}_{price_bucket}"
    
    async def _check_and_set_pending(self, user_id: int, signal_type: str, entry_price: float) -> bool:
        """Check for duplicate and set pending status atomically. Returns True if signal can proceed.
        
        Uses TTL-backed cache with time decay for automatic expiry.
        Tracks telemetry for cache hits/misses.
        """
        async with self._cache_lock:
            now = datetime.now()
            cache_copy = dict(self.sent_signals_cache)
            
            expired_keys = [
                k for k, v in cache_copy.items() 
                if (now - v['timestamp']).total_seconds() > self.signal_cache_expiry_seconds
            ]
            if expired_keys:
                for k in expired_keys:
                    self.sent_signals_cache.pop(k, None)
                self._cache_telemetry['expired_cleanups'] += len(expired_keys)
            
            if len(self.sent_signals_cache) >= self.MAX_CACHE_SIZE:
                sorted_entries = sorted(
                    self.sent_signals_cache.items(),
                    key=lambda x: x[1]['timestamp']
                )
                entries_to_remove = len(self.sent_signals_cache) - self.MAX_CACHE_SIZE + 1
                for i in range(min(entries_to_remove, len(sorted_entries))):
                    key_to_remove = sorted_entries[i][0]
                    self.sent_signals_cache.pop(key_to_remove, None)
                self._cache_telemetry['size_enforcements'] += 1
                logger.debug(f"Cache limit enforcement: removed {entries_to_remove} oldest entries")
            
            signal_hash = self._generate_signal_hash(user_id, signal_type, entry_price)
            
            cached = self.sent_signals_cache.get(signal_hash)
            if cached:
                status = cached.get('status', 'confirmed')
                time_since = (now - cached['timestamp']).total_seconds()
                self._cache_telemetry['hits'] += 1
                logger.debug(f"Cache HIT - Duplicate signal blocked: {signal_hash}, status={status}, {time_since:.1f}s ago")
                return False
            
            self._cache_telemetry['misses'] += 1
            self._cache_telemetry['pending_set'] += 1
            self.sent_signals_cache[signal_hash] = {
                'status': 'pending',
                'timestamp': now,
                'user_id': user_id,
                'signal_type': signal_type,
                'entry_price': entry_price
            }
            logger.debug(f"Cache MISS - Signal marked as pending: {signal_hash}")
            return True
    
    async def _confirm_signal_sent(self, user_id: int, signal_type: str, entry_price: float):
        """Confirm signal was sent successfully - upgrade from pending to confirmed.
        
        Layered cache transition: pending → confirmed
        """
        async with self._cache_lock:
            signal_hash = self._generate_signal_hash(user_id, signal_type, entry_price)
            if signal_hash in self.sent_signals_cache:
                self.sent_signals_cache[signal_hash]['status'] = 'confirmed'
                self.sent_signals_cache[signal_hash]['timestamp'] = datetime.now()
                self.sent_signals_cache[signal_hash]['confirmed_at'] = datetime.now()
                self._cache_telemetry['confirmed'] += 1
                logger.debug(f"Signal confirmed (pending→confirmed): {signal_hash}")
    
    async def _rollback_signal_cache(self, user_id: int, signal_type: str, entry_price: float):
        """Remove pending signal entry on failure.
        
        Cleans up pending entries when signal send fails.
        """
        async with self._cache_lock:
            signal_hash = self._generate_signal_hash(user_id, signal_type, entry_price)
            removed = self.sent_signals_cache.pop(signal_hash, None)
            if removed:
                self._cache_telemetry['rollbacks'] += 1
                logger.debug(f"Signal cache rolled back: {signal_hash}")
    
    async def _clear_signal_cache(self, user_id: int = None):
        """Clear signal cache for user or all users."""
        async with self._cache_lock:
            if user_id:
                cache_keys = list(self.sent_signals_cache.keys())
                for k in cache_keys:
                    if k.startswith(f"{user_id}_"):
                        self.sent_signals_cache.pop(k, None)
                logger.debug(f"Cleared signal cache for user {user_id}")
            else:
                self.sent_signals_cache.clear()
                logger.debug("Cleared all signal cache")
    
    async def _handle_forbidden_error(self, chat_id: int, error: Forbidden):
        """Handle Forbidden error - user blocked bot or chat inaccessible.
        
        Actions:
        - Remove chat from monitoring list
        - Cancel monitoring task for this chat
        - Clear any pending signals for this user
        - Log the event for analytics
        
        Args:
            chat_id: The chat ID that generated the error
            error: The Forbidden exception
        """
        logger.warning(f"🚫 Forbidden error for chat {mask_user_id(chat_id)}: {error}")
        
        if chat_id in self.monitoring_chats:
            self.monitoring_chats.remove(chat_id)
            logger.info(f"Removed chat {mask_user_id(chat_id)} from monitoring list (user blocked bot)")
        
        task = self.monitoring_tasks.pop(chat_id, None)
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            logger.info(f"Cancelled monitoring task for blocked chat {mask_user_id(chat_id)}")
        
        await self._clear_signal_cache(chat_id)
        
        if self.signal_session_manager:
            try:
                await self.signal_session_manager.end_session(
                    chat_id, 
                    reason='user_blocked', 
                    notify=False
                )
            except Exception as e:
                logger.debug(f"Could not end session for blocked user: {e}")
        
        if self.alert_system:
            try:
                await self.alert_system.send_alert(
                    level='warning',
                    title='User Blocked Bot',
                    message=f"User {mask_user_id(chat_id)} telah memblokir bot atau chat tidak dapat diakses",
                    context={'chat_id': chat_id, 'error': str(error)}
                )
            except Exception as e:
                logger.debug(f"Could not send alert for blocked user: {e}")
    
    async def _handle_chat_migrated(self, old_chat_id: int, error: ChatMigrated) -> Optional[int]:
        """Handle ChatMigrated error - group migrated to supergroup.
        
        Actions:
        - Extract new chat ID from error
        - Update monitoring list with new chat ID
        - Update any active sessions/positions with new chat ID
        - Return new chat ID for caller to retry operation
        
        Args:
            old_chat_id: The old chat ID that triggered migration
            error: The ChatMigrated exception containing new_chat_id
            
        Returns:
            int or None: The new chat ID if available
        """
        new_chat_id = getattr(error, 'new_chat_id', None)
        logger.warning(f"📤 Chat migrated: {mask_user_id(old_chat_id)} -> {mask_user_id(new_chat_id) if new_chat_id else 'unknown'}")
        
        if not new_chat_id:
            logger.error(f"ChatMigrated error without new_chat_id for {mask_user_id(old_chat_id)}")
            return None
        
        if old_chat_id in self.monitoring_chats:
            idx = self.monitoring_chats.index(old_chat_id)
            self.monitoring_chats[idx] = new_chat_id
            logger.info(f"Updated monitoring list: {mask_user_id(old_chat_id)} -> {mask_user_id(new_chat_id)}")
        
        old_task = self.monitoring_tasks.pop(old_chat_id, None)
        if old_task:
            if not old_task.done():
                old_task.cancel()
                try:
                    await asyncio.wait_for(old_task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            new_task = asyncio.create_task(self._monitoring_loop(new_chat_id))
            self.monitoring_tasks[new_chat_id] = new_task
            logger.info(f"Created new monitoring task for migrated chat {mask_user_id(new_chat_id)}")
        
        if self.signal_session_manager:
            try:
                await self.signal_session_manager.migrate_user_sessions(old_chat_id, new_chat_id)
            except Exception as e:
                logger.warning(f"Could not migrate sessions for chat migration: {e}")
        
        if self.alert_system:
            try:
                await self.alert_system.send_alert(
                    level='info',
                    title='Chat Migrated',
                    message=f"Chat {mask_user_id(old_chat_id)} migrated to {mask_user_id(new_chat_id)}",
                    context={'old_chat_id': old_chat_id, 'new_chat_id': new_chat_id}
                )
            except Exception as e:
                logger.debug(f"Could not send migration alert: {e}")
        
        return new_chat_id
    
    async def _handle_unauthorized_error(self, error: InvalidToken):
        """Handle InvalidToken error - invalid or revoked bot token.
        
        This is a CRITICAL error that requires immediate attention.
        
        Actions:
        - Log critical error
        - Send critical alert (if possible through alternative channels)
        - Stop all monitoring and cleanup
        - Set shutdown flag
        
        Args:
            error: The InvalidToken exception
        """
        logger.critical(f"🔴 CRITICAL: InvalidToken error - Bot token invalid or revoked: {error}")
        
        self._is_shutting_down = True
        
        if self.alert_system:
            try:
                await self.alert_system.send_critical_alert(
                    title='🔴 BOT TOKEN INVALID',
                    message=f"Bot token tidak valid atau telah di-revoke. Bot akan berhenti.\nError: {error}",
                    context={'error': str(error), 'timestamp': datetime.now().isoformat()}
                )
            except Exception as e:
                logger.error(f"Could not send critical alert for unauthorized error: {e}")
        
        if self.error_handler:
            try:
                await self.error_handler.handle_critical_error(
                    error_type='UNAUTHORIZED',
                    error=error,
                    context={'action': 'bot_shutdown_required'}
                )
            except Exception as e:
                logger.error(f"Error handler failed for unauthorized error: {e}")
        
        logger.critical("🛑 Initiating emergency shutdown due to unauthorized error...")
        await self.stop_background_cleanup_tasks()
    
    async def _handle_conflict_error(self, error: Conflict):
        """Handle Conflict error - multiple bot instances detected.
        
        This is a CRITICAL error indicating duplicate bot instances.
        
        Actions:
        - Log critical error with instance info
        - Send alert to admins
        - Gracefully stop this instance to avoid conflicts
        
        Args:
            error: The Conflict exception
        """
        logger.critical(f"🔴 CRITICAL: Conflict error - Multiple bot instances detected: {error}")
        
        if self.alert_system:
            try:
                await self.alert_system.send_critical_alert(
                    title='🔴 BOT INSTANCE CONFLICT',
                    message=f"Multiple bot instances detected! Ini dapat menyebabkan behavior yang tidak konsisten.\nError: {error}",
                    context={
                        'error': str(error),
                        'timestamp': datetime.now().isoformat(),
                        'instance_lock_file': self.instance_lock_file
                    }
                )
            except Exception as e:
                logger.error(f"Could not send critical alert for conflict error: {e}")
        
        if self.error_handler:
            try:
                await self.error_handler.handle_critical_error(
                    error_type='CONFLICT',
                    error=error,
                    context={'action': 'check_other_instances'}
                )
            except Exception as e:
                logger.error(f"Error handler failed for conflict error: {e}")
        
        logger.critical("🛑 This instance will stop to avoid conflicts...")
        self._is_shutting_down = True
        await self.stop_background_cleanup_tasks()
    
    async def _handle_bad_request(self, chat_id: int, error: BadRequest, context: str = ""):
        """Handle BadRequest error - invalid request parameters.
        
        This error should NOT be retried as it indicates a programming error
        or invalid input that won't change on retry.
        
        Actions:
        - Log detailed error with context
        - Track for debugging
        - Optionally notify developers
        
        Args:
            chat_id: The chat ID involved (if any)
            error: The BadRequest exception
            context: Additional context about what operation failed
        """
        error_message = str(error)
        logger.error(f"❌ BadRequest for chat {mask_user_id(chat_id)}: {error_message} (context: {context})")
        
        known_bad_requests = {
            'message is not modified': 'info',
            'message to edit not found': 'warning',
            'chat not found': 'warning',
            'user not found': 'warning',
            'message text is empty': 'error',
            'can\'t parse entities': 'error',
            'wrong file identifier': 'error',
        }
        
        log_level = 'error'
        for pattern, level in known_bad_requests.items():
            if pattern.lower() in error_message.lower():
                log_level = level
                break
        
        if log_level == 'info':
            logger.info(f"BadRequest (expected): {error_message}")
        elif log_level == 'warning':
            logger.warning(f"BadRequest (expected): {error_message}")
        else:
            if self.error_handler:
                try:
                    await self.error_handler.track_error(
                        error_type='BAD_REQUEST',
                        error=error,
                        context={'chat_id': chat_id, 'operation': context}
                    )
                except Exception as e:
                    logger.debug(f"Could not track bad request error: {e}")
    
    async def start_background_cleanup_tasks(self):
        """Mulai background tasks untuk cleanup cache, dashboards, dan pending charts"""
        self._cleanup_tasks_running = True
        
        if self._cache_cleanup_task is None or self._cache_cleanup_task.done():
            self._cache_cleanup_task = asyncio.create_task(self._signal_cache_cleanup_loop())
            logger.info("✅ Signal cache cleanup background task started")
        
        if self._dashboard_cleanup_task is None or self._dashboard_cleanup_task.done():
            self._dashboard_cleanup_task = asyncio.create_task(self._dashboard_cleanup_loop())
            logger.info("✅ Dashboard cleanup background task started")
        
        if self._chart_cleanup_task is None or self._chart_cleanup_task.done():
            self._chart_cleanup_task = asyncio.create_task(self._pending_chart_cleanup_loop())
            logger.info("✅ Pending chart cleanup background task started")
    
    async def stop_background_cleanup_tasks(self):
        """Stop background cleanup tasks and all monitoring tasks"""
        self._cleanup_tasks_running = False
        self._is_shutting_down = True
        
        if self._cache_cleanup_task and not self._cache_cleanup_task.done():
            self._cache_cleanup_task.cancel()
            try:
                await asyncio.wait_for(self._cache_cleanup_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            self._cache_cleanup_task = None
            logger.info("Signal cache cleanup task stopped")
        
        if self._dashboard_cleanup_task and not self._dashboard_cleanup_task.done():
            self._dashboard_cleanup_task.cancel()
            try:
                await asyncio.wait_for(self._dashboard_cleanup_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            self._dashboard_cleanup_task = None
            logger.info("Dashboard cleanup task stopped")
        
        if self._chart_cleanup_task and not self._chart_cleanup_task.done():
            self._chart_cleanup_task.cancel()
            try:
                await asyncio.wait_for(self._chart_cleanup_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            self._chart_cleanup_task = None
            logger.info("Pending chart cleanup task stopped")
        
        await self._cleanup_all_pending_charts()
        
        await self._cancel_all_monitoring_tasks()
        
        await self._cancel_all_dashboard_tasks()
        
        logger.info("All background and monitoring tasks stopped")
    
    async def _cancel_all_monitoring_tasks(self, timeout: float = 10.0):
        """Cancel all monitoring tasks with proper cleanup"""
        if not self.monitoring_tasks:
            logger.debug("No monitoring tasks to cancel")
            return
        
        self.monitoring = False
        task_count = len(self.monitoring_tasks)
        logger.info(f"Cancelling {task_count} monitoring tasks...")
        
        tasks_to_cancel = []
        for chat_id, task in list(self.monitoring_tasks.items()):
            if task and not task.done():
                task.cancel()
                tasks_to_cancel.append(task)
                logger.debug(f"Cancelled monitoring task for chat {mask_user_id(chat_id)}")
        
        if tasks_to_cancel:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks_to_cancel, return_exceptions=True),
                    timeout=timeout
                )
                logger.info(f"✅ All {len(tasks_to_cancel)} monitoring tasks cancelled successfully")
            except asyncio.TimeoutError:
                logger.warning(f"Some monitoring tasks did not complete within {timeout}s timeout")
            except asyncio.CancelledError:
                logger.debug("Monitoring task cancellation was itself cancelled")
        
        self.monitoring_tasks.clear()
        self.monitoring_chats.clear()
    
    async def _cancel_all_dashboard_tasks(self, timeout: float = 5.0):
        """Cancel all dashboard update tasks"""
        if not self.active_dashboards:
            logger.debug("No dashboard tasks to cancel")
            return
        
        dashboard_count = len(self.active_dashboards)
        logger.info(f"Cancelling {dashboard_count} dashboard tasks...")
        
        tasks_to_cancel = []
        for user_id, dash_info in list(self.active_dashboards.items()):
            task = dash_info.get('task')
            if task and not task.done():
                task.cancel()
                tasks_to_cancel.append(task)
        
        if tasks_to_cancel:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks_to_cancel, return_exceptions=True),
                    timeout=timeout
                )
                logger.info(f"✅ All {len(tasks_to_cancel)} dashboard tasks cancelled successfully")
            except asyncio.TimeoutError:
                logger.warning(f"Some dashboard tasks did not complete within {timeout}s timeout")
            except asyncio.CancelledError:
                pass
        
        self.active_dashboards.clear()
    
    async def _signal_cache_cleanup_loop(self):
        """Background task untuk cleanup expired signal cache entries"""
        cleanup_interval = 60
        
        try:
            while self._cleanup_tasks_running:
                await asyncio.sleep(cleanup_interval)
                
                if not self._cleanup_tasks_running:
                    break
                
                try:
                    cleaned = await self._cleanup_expired_cache_entries()
                    if cleaned > 0:
                        logger.debug(f"Signal cache cleanup: removed {cleaned} expired entries")
                except Exception as e:
                    logger.error(f"Error in signal cache cleanup: {e}")
                    
        except asyncio.CancelledError:
            logger.info("Signal cache cleanup loop cancelled")
    
    async def _cleanup_expired_cache_entries(self) -> int:
        """Cleanup expired entries dari signal cache dengan time decay.
        
        Implements TTL-backed cache with time decay:
        - Pending entries expire faster (60s) to prevent stuck entries
        - Confirmed entries use full TTL (120s) for proper duplicate prevention
        
        Returns:
            int: Number of entries cleaned up
        """
        async with self._cache_lock:
            now = datetime.now()
            pending_ttl = 60
            confirmed_ttl = self.signal_cache_expiry_seconds
            
            expired_keys = []
            for k, v in self.sent_signals_cache.items():
                age_seconds = (now - v['timestamp']).total_seconds()
                status = v.get('status', 'confirmed')
                
                if status == 'pending' and age_seconds > pending_ttl:
                    expired_keys.append(k)
                    logger.debug(f"Time decay: pending entry {k} expired after {age_seconds:.1f}s")
                elif status == 'confirmed' and age_seconds > confirmed_ttl:
                    expired_keys.append(k)
            
            for k in expired_keys:
                self.sent_signals_cache.pop(k, None)
            
            if expired_keys:
                self._cache_telemetry['expired_cleanups'] += len(expired_keys)
                self._cache_telemetry['last_cleanup_time'] = now
                self._cache_telemetry['last_cleanup_count'] = len(expired_keys)
            
            return len(expired_keys)
    
    async def _dashboard_cleanup_loop(self):
        """Background task untuk cleanup dead/stale dashboards"""
        cleanup_interval = 30
        max_dashboard_age_seconds = 3600
        
        try:
            while self._cleanup_tasks_running:
                await asyncio.sleep(cleanup_interval)
                
                if not self._cleanup_tasks_running:
                    break
                
                try:
                    cleaned = await self._cleanup_stale_dashboards(max_dashboard_age_seconds)
                    if cleaned > 0:
                        logger.info(f"Dashboard cleanup: removed {cleaned} stale entries")
                except Exception as e:
                    logger.error(f"Error in dashboard cleanup: {e}")
                    
        except asyncio.CancelledError:
            logger.info("Dashboard cleanup loop cancelled")
    
    async def _cleanup_stale_dashboards(self, max_age_seconds: int = 3600) -> int:
        """Cleanup stale dashboard entries"""
        cleaned = 0
        stale_users = []
        
        async with self._dashboard_lock:
            now = datetime.now()
            
            for user_id, dashboard_info in list(self.active_dashboards.items()):
                try:
                    task = dashboard_info.get('task')
                    created_at = dashboard_info.get('created_at', now)
                    
                    age_seconds = (now - created_at).total_seconds() if isinstance(created_at, datetime) else 0
                    
                    is_stale = False
                    if task is None:
                        is_stale = True
                    elif task.done():
                        is_stale = True
                    elif age_seconds > max_age_seconds:
                        is_stale = True
                        if not task.done():
                            task.cancel()
                    
                    if is_stale:
                        stale_users.append(user_id)
                        
                except Exception as e:
                    logger.error(f"Error checking dashboard for user {user_id}: {e}")
                    stale_users.append(user_id)
            
            for user_id in stale_users:
                self.active_dashboards.pop(user_id, None)
                cleaned += 1
        
        return cleaned
    
    def get_cache_stats(self) -> Dict:
        """Dapatkan statistik cache untuk monitoring dengan telemetry data.
        
        Returns comprehensive cache statistics including:
        - Cache size and usage
        - Hit/miss ratios
        - Pending vs confirmed entries breakdown
        - Dashboard and monitoring stats
        - Pending charts info
        """
        try:
            pending_count = sum(1 for v in self.sent_signals_cache.values() if v.get('status') == 'pending')
            confirmed_count = sum(1 for v in self.sent_signals_cache.values() if v.get('status') == 'confirmed')
            
            total_lookups = self._cache_telemetry['hits'] + self._cache_telemetry['misses']
            hit_rate = (self._cache_telemetry['hits'] / total_lookups * 100) if total_lookups > 0 else 0.0
            
            return {
                'signal_cache_size': len(self.sent_signals_cache),
                'signal_cache_max': self.MAX_CACHE_SIZE,
                'signal_cache_usage_pct': (len(self.sent_signals_cache) / self.MAX_CACHE_SIZE * 100) if self.MAX_CACHE_SIZE > 0 else 0,
                'pending_entries': pending_count,
                'confirmed_entries': confirmed_count,
                'active_dashboards': len(self.active_dashboards),
                'dashboards_max': self.MAX_DASHBOARDS,
                'dashboards_usage_pct': (len(self.active_dashboards) / self.MAX_DASHBOARDS * 100) if self.MAX_DASHBOARDS > 0 else 0,
                'monitoring_chats': len(self.monitoring_chats),
                'monitoring_chats_max': self.MAX_MONITORING_CHATS,
                'monitoring_tasks': len(self.monitoring_tasks),
                'pending_charts': len(self._pending_charts),
                'cache_expiry_seconds': self.signal_cache_expiry_seconds,
                'is_shutting_down': self._is_shutting_down,
                'cleanup_tasks_running': self._cleanup_tasks_running,
                'telemetry': {
                    'cache_hits': self._cache_telemetry['hits'],
                    'cache_misses': self._cache_telemetry['misses'],
                    'hit_rate_pct': round(hit_rate, 2),
                    'total_lookups': total_lookups,
                    'pending_set': self._cache_telemetry['pending_set'],
                    'confirmed': self._cache_telemetry['confirmed'],
                    'rollbacks': self._cache_telemetry['rollbacks'],
                    'expired_cleanups': self._cache_telemetry['expired_cleanups'],
                    'size_enforcements': self._cache_telemetry['size_enforcements'],
                    'last_cleanup_time': self._cache_telemetry['last_cleanup_time'].isoformat() if self._cache_telemetry['last_cleanup_time'] else None,
                    'last_cleanup_count': self._cache_telemetry['last_cleanup_count'],
                }
            }
        except AttributeError as e:
            logger.warning(f"Attribute error getting cache stats (bot may not be fully initialized): {e}")
            return {'error': str(e), 'initialized': False}
        except Exception as e:
            logger.error(f"Unexpected error getting cache stats: {type(e).__name__}: {e}")
            return {'error': str(e)}
    
    def _get_cache_stats(self) -> Dict:
        """Alias untuk get_cache_stats() untuk backward compatibility."""
        return self.get_cache_stats()
    
    async def register_pending_chart(self, user_id: int, chart_path: str, signal_type: str = None):
        """Register a pending chart for cleanup tracking.
        
        Args:
            user_id: User ID associated with the chart
            chart_path: Path to the chart file
            signal_type: Type of signal (BUY/SELL)
        """
        async with self._chart_cleanup_lock:
            self._pending_charts[user_id] = {
                'chart_path': chart_path,
                'signal_type': signal_type,
                'created_at': datetime.now(),
                'status': 'pending'
            }
            logger.debug(f"Registered pending chart for user {mask_user_id(user_id)}: {chart_path}")
    
    async def confirm_chart_sent(self, user_id: int):
        """Confirm chart was sent successfully, update status.
        
        Args:
            user_id: User ID whose chart was sent
        """
        async with self._chart_cleanup_lock:
            if user_id in self._pending_charts:
                self._pending_charts[user_id]['status'] = 'sent'
                self._pending_charts[user_id]['sent_at'] = datetime.now()
                logger.debug(f"Chart confirmed sent for user {mask_user_id(user_id)}")
    
    async def evict_pending_chart(self, user_id: int, reason: str = "manual"):
        """Evict and cleanup a pending chart for a user.
        
        Args:
            user_id: User ID whose chart should be evicted
            reason: Reason for eviction
            
        Returns:
            bool: True if chart was evicted, False otherwise
        """
        import os
        
        chart_info = None
        async with self._chart_cleanup_lock:
            chart_info = self._pending_charts.pop(user_id, None)
        
        if chart_info:
            chart_path = chart_info.get('chart_path')
            if chart_path and os.path.exists(chart_path):
                try:
                    os.remove(chart_path)
                    logger.info(f"🗑️ Evicted pending chart for user {mask_user_id(user_id)}: {chart_path} (reason: {reason})")
                    
                    for callback in self._chart_eviction_callbacks:
                        try:
                            if asyncio.iscoroutinefunction(callback):
                                await callback(user_id, chart_path, reason)
                            else:
                                callback(user_id, chart_path, reason)
                        except Exception as e:
                            logger.error(f"Error in chart eviction callback: {e}")
                    
                    return True
                except FileNotFoundError:
                    logger.debug(f"Chart already deleted: {chart_path}")
                except Exception as e:
                    logger.warning(f"Failed to evict chart {chart_path}: {e}")
            return True
        return False
    
    def register_chart_eviction_callback(self, callback: Callable):
        """Register a callback to be called when a chart is evicted.
        
        Args:
            callback: Callback function (can be async) with signature (user_id, chart_path, reason)
        """
        self._chart_eviction_callbacks.append(callback)
        logger.debug(f"Registered chart eviction callback: {callback.__name__}")
    
    async def _pending_chart_cleanup_loop(self):
        """Background task for cleaning up stale pending charts.
        
        Runs periodically to cleanup charts that were never sent or confirmed.
        Uses time decay: charts older than TTL are automatically evicted.
        """
        cleanup_interval = 30
        pending_chart_ttl_seconds = 120
        
        try:
            while self._cleanup_tasks_running:
                await asyncio.sleep(cleanup_interval)
                
                if not self._cleanup_tasks_running:
                    break
                
                try:
                    cleaned = await self._cleanup_stale_pending_charts(pending_chart_ttl_seconds)
                    if cleaned > 0:
                        logger.info(f"Pending chart cleanup: evicted {cleaned} stale charts")
                except Exception as e:
                    logger.error(f"Error in pending chart cleanup: {e}")
                    
        except asyncio.CancelledError:
            logger.info("Pending chart cleanup loop cancelled")
    
    async def _cleanup_stale_pending_charts(self, max_age_seconds: int = 120) -> int:
        """Cleanup stale pending charts with time decay.
        
        Args:
            max_age_seconds: Maximum age in seconds before a chart is considered stale
            
        Returns:
            int: Number of charts cleaned up
        """
        import os
        
        stale_users = []
        charts_to_cleanup = []
        
        async with self._chart_cleanup_lock:
            now = datetime.now()
            
            for user_id, chart_info in list(self._pending_charts.items()):
                created_at = chart_info.get('created_at', now)
                age_seconds = (now - created_at).total_seconds()
                status = chart_info.get('status', 'pending')
                
                is_stale = False
                if status == 'pending' and age_seconds > max_age_seconds:
                    is_stale = True
                    logger.debug(f"Stale pending chart for user {mask_user_id(user_id)}: {age_seconds:.1f}s old")
                elif status == 'sent' and age_seconds > (max_age_seconds * 2):
                    is_stale = True
                
                if is_stale:
                    stale_users.append(user_id)
                    charts_to_cleanup.append((user_id, chart_info.get('chart_path')))
            
            for user_id in stale_users:
                self._pending_charts.pop(user_id, None)
        
        cleaned = 0
        for user_id, chart_path in charts_to_cleanup:
            if chart_path and os.path.exists(chart_path):
                try:
                    os.remove(chart_path)
                    logger.info(f"🗑️ Cleaned stale pending chart: {chart_path}")
                    cleaned += 1
                except FileNotFoundError:
                    pass
                except Exception as e:
                    logger.warning(f"Failed to cleanup stale chart {chart_path}: {e}")
        
        return cleaned
    
    async def _cleanup_all_pending_charts(self):
        """Cleanup all pending charts - called during shutdown.
        
        Evicts all registered pending charts for a clean shutdown.
        """
        import os
        
        async with self._chart_cleanup_lock:
            chart_count = len(self._pending_charts)
            
            if chart_count == 0:
                logger.debug("No pending charts to cleanup")
                return
            
            logger.info(f"Cleaning up {chart_count} pending charts during shutdown...")
            
            for user_id, chart_info in list(self._pending_charts.items()):
                chart_path = chart_info.get('chart_path')
                if chart_path and os.path.exists(chart_path):
                    try:
                        os.remove(chart_path)
                        logger.debug(f"Cleaned up pending chart: {chart_path}")
                    except Exception as e:
                        logger.warning(f"Failed to cleanup chart {chart_path}: {e}")
            
            self._pending_charts.clear()
            logger.info(f"✅ All {chart_count} pending charts cleaned up")
    
    async def _integrate_chart_with_session_manager(self):
        """Integrate chart cleanup with SignalSessionManager.
        
        Registers event handlers for session events to cleanup charts
        when sessions end.
        """
        if self.signal_session_manager:
            async def on_session_end_chart_cleanup(session):
                """Handler untuk cleanup chart saat session berakhir."""
                try:
                    user_id = session.user_id
                    chart_path = session.chart_path
                    
                    if user_id in self._pending_charts:
                        await self.evict_pending_chart(user_id, reason="session_end")
                    elif chart_path:
                        import os
                        if os.path.exists(chart_path):
                            try:
                                os.remove(chart_path)
                                logger.info(f"🗑️ Cleaned session chart on end: {chart_path}")
                            except Exception as e:
                                logger.warning(f"Failed to cleanup session chart: {e}")
                except Exception as e:
                    logger.error(f"Error in session end chart cleanup: {e}")
            
            self.signal_session_manager.register_event_handler('on_session_end', on_session_end_chart_cleanup)
            logger.info("✅ Chart cleanup integrated with SignalSessionManager")
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            if self.user_manager:
                self.user_manager.create_user(
                    telegram_id=update.effective_user.id,
                    username=update.effective_user.username,
                    first_name=update.effective_user.first_name,
                    last_name=update.effective_user.last_name
                )
                self.user_manager.update_user_activity(update.effective_user.id)
            
            if not self.is_authorized(update.effective_user.id):
                access_denied_msg = (
                    "⛔ *Akses Ditolak*\n\n"
                    "Maaf, Anda tidak terdaftar sebagai user yang diizinkan menggunakan bot ini.\n\n"
                    "🔒 *Bot ini bersifat privat*\n"
                    "Hanya user yang terdaftar yang dapat mengakses fitur bot.\n\n"
                    "Jika Anda merasa ini adalah kesalahan, silakan hubungi pemilik bot."
                )
                await update.message.reply_text(access_denied_msg, parse_mode='Markdown')
                return
            
            user_status = "Admin" if self.is_admin(update.effective_user.id) else "User Terdaftar"
            mode = "LIVE" if not self.config.DRY_RUN else "DRY RUN"
            
            welcome_msg = (
                "🤖 *XAUUSD Trading Bot Pro*\n\n"
                "Bot trading otomatis untuk XAUUSD dengan analisis teknikal canggih.\n\n"
                f"👑 Status: {user_status}\n\n"
                "*Commands:*\n"
                "/start - Tampilkan pesan ini\n"
                "/help - Bantuan lengkap\n"
                "/monitor - Mulai monitoring sinyal\n"
                "/stopmonitor - Stop monitoring\n"
                "/getsignal - Dapatkan sinyal manual\n"
                "/status - Cek posisi aktif\n"
                "/riwayat - Lihat riwayat trading\n"
                "/performa - Statistik performa\n"
                "/analytics - Comprehensive analytics\n"
                "/systemhealth - System health status\n"
                "/tasks - Lihat scheduled tasks\n"
                "/settings - Lihat konfigurasi\n"
            )
            
            if self.is_admin(update.effective_user.id):
                welcome_msg += (
                    "\n*Admin Commands:*\n"
                    "/riset - Reset database trading\n"
                )
            
            welcome_msg += f"\n*Mode:* {mode}\n"
            
            await update.message.reply_text(welcome_msg, parse_mode='Markdown')
            logger.info(f"Start command executed successfully for user {mask_user_id(update.effective_user.id)}")
            
        except asyncio.CancelledError:
            logger.info(f"Start command cancelled for user {mask_user_id(update.effective_user.id)}")
            raise
        except RetryAfter as e:
            logger.warning(f"Rate limit in start command: retry after {e.retry_after}s")
            await asyncio.sleep(e.retry_after)
        except (TimedOut, NetworkError) as e:
            logger.warning(f"Network/timeout error in start command: {e}")
            try:
                await update.message.reply_text("⏳ Koneksi timeout, silakan coba lagi.")
            except (TelegramError, asyncio.CancelledError):
                pass
        except BadRequest as e:
            await self._handle_bad_request(update.effective_chat.id, e, context="start_command")
        except Forbidden as e:
            await self._handle_forbidden_error(update.effective_chat.id, e)
        except ChatMigrated as e:
            new_chat_id = await self._handle_chat_migrated(update.effective_chat.id, e)
            if new_chat_id:
                logger.info(f"Chat migrated in start command, new ID: {new_chat_id}")
        except Conflict as e:
            await self._handle_conflict_error(e)
        except InvalidToken as e:
            await self._handle_unauthorized_error(e)
        except TelegramError as e:
            logger.error(f"Telegram error in start command: {e}")
            try:
                await update.message.reply_text("❌ Terjadi error Telegram. Silakan coba lagi.")
            except (TelegramError, asyncio.CancelledError):
                pass
        except Exception as e:
            logger.error(f"Unexpected error in start command: {type(e).__name__}: {e}", exc_info=True)
            try:
                await update.message.reply_text("❌ Terjadi error saat memproses command. Silakan coba lagi.")
            except (TelegramError, asyncio.CancelledError):
                pass
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            if not self.is_authorized(update.effective_user.id):
                return
            
            help_msg = (
                "📖 *Bantuan XAUUSD Trading Bot*\n\n"
                "*Cara Kerja:*\n"
                "1. Gunakan /monitor untuk mulai monitoring\n"
                "2. Bot akan menganalisis chart XAUUSD M1 dan M5\n"
                "3. Sinyal BUY/SELL akan dikirim jika kondisi terpenuhi\n"
                "4. Posisi akan dimonitor hingga TP/SL tercapai\n\n"
                "*Indikator:*\n"
                f"- EMA: {', '.join(map(str, self.config.EMA_PERIODS))}\n"
                f"- RSI: {self.config.RSI_PERIOD} (OB/OS: {self.config.RSI_OVERBOUGHT_LEVEL}/{self.config.RSI_OVERSOLD_LEVEL})\n"
                f"- Stochastic: K={self.config.STOCH_K_PERIOD}, D={self.config.STOCH_D_PERIOD}\n"
                f"- ATR: {self.config.ATR_PERIOD}\n\n"
                "*Risk Management:*\n"
                f"- Max trades per day: Unlimited (24/7)\n"
                f"- Daily loss limit: {self.config.DAILY_LOSS_PERCENT}%\n"
                f"- Signal cooldown: {self.config.SIGNAL_COOLDOWN_SECONDS}s\n"
                f"- Risk per trade: ${self.config.FIXED_RISK_AMOUNT:.2f} (Fixed)\n"
            )
            
            await update.message.reply_text(help_msg, parse_mode='Markdown')
            logger.info(f"Help command executed for user {mask_user_id(update.effective_user.id)}")
            
        except asyncio.CancelledError:
            logger.info(f"Help command cancelled for user {mask_user_id(update.effective_user.id)}")
            raise
        except RetryAfter as e:
            logger.warning(f"Rate limit in help command: retry after {e.retry_after}s")
        except (TimedOut, NetworkError) as e:
            logger.warning(f"Network/timeout error in help command: {e}")
        except BadRequest as e:
            await self._handle_bad_request(update.effective_chat.id, e, context="help_command")
        except Forbidden as e:
            await self._handle_forbidden_error(update.effective_chat.id, e)
        except ChatMigrated as e:
            await self._handle_chat_migrated(update.effective_chat.id, e)
        except Conflict as e:
            await self._handle_conflict_error(e)
        except InvalidToken as e:
            await self._handle_unauthorized_error(e)
        except TelegramError as e:
            logger.error(f"Telegram error in help command: {e}")
            try:
                await update.message.reply_text("❌ Error menampilkan bantuan.")
            except (TelegramError, asyncio.CancelledError):
                pass
        except Exception as e:
            logger.error(f"Unexpected error in help command: {type(e).__name__}: {e}", exc_info=True)
            try:
                await update.message.reply_text("❌ Error menampilkan bantuan.")
            except (TelegramError, asyncio.CancelledError):
                pass
    
    async def monitor_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            if not self.is_authorized(update.effective_user.id):
                return
            
            chat_id = update.effective_chat.id
            
            if self.monitoring and chat_id in self.monitoring_chats:
                await update.message.reply_text("⚠️ Monitoring sudah berjalan untuk Anda!")
                return
            
            if len(self.monitoring_chats) >= self.MAX_MONITORING_CHATS:
                await update.message.reply_text("⚠️ Batas maksimum monitoring tercapai. Silakan coba lagi nanti.")
                logger.warning(f"Monitoring limit reached ({self.MAX_MONITORING_CHATS})")
                return
            
            if not self.monitoring:
                self.monitoring = True
            
            if chat_id not in self.monitoring_chats:
                self.monitoring_chats.append(chat_id)
                await update.message.reply_text("✅ Monitoring dimulai! Bot akan mendeteksi sinyal secara real-time...")
                task = asyncio.create_task(self._monitoring_loop(chat_id))
                self.monitoring_tasks[chat_id] = task
                logger.info(f"✅ Monitoring task created for chat {mask_user_id(chat_id)}")
                
        except asyncio.CancelledError:
            logger.info(f"Monitor command cancelled for user {mask_user_id(update.effective_user.id)}")
            raise
        except RetryAfter as e:
            logger.warning(f"Rate limit in monitor command: retry after {e.retry_after}s")
        except (TimedOut, NetworkError) as e:
            logger.warning(f"Network/timeout error in monitor command: {e}")
            try:
                await update.message.reply_text("⏳ Koneksi timeout, silakan coba lagi.")
            except (TelegramError, asyncio.CancelledError):
                pass
        except BadRequest as e:
            await self._handle_bad_request(update.effective_chat.id, e, context="monitor_command")
        except Forbidden as e:
            await self._handle_forbidden_error(update.effective_chat.id, e)
        except ChatMigrated as e:
            new_chat_id = await self._handle_chat_migrated(update.effective_chat.id, e)
            if new_chat_id:
                logger.info(f"Chat migrated in monitor command, monitoring new chat: {new_chat_id}")
        except Conflict as e:
            await self._handle_conflict_error(e)
        except InvalidToken as e:
            await self._handle_unauthorized_error(e)
        except TelegramError as e:
            logger.error(f"Telegram error in monitor command: {e}")
            try:
                await update.message.reply_text("❌ Error memulai monitoring. Silakan coba lagi.")
            except (TelegramError, asyncio.CancelledError):
                pass
        except Exception as e:
            logger.error(f"Unexpected error in monitor command: {type(e).__name__}: {e}", exc_info=True)
            try:
                await update.message.reply_text("❌ Error memulai monitoring. Silakan coba lagi.")
            except (TelegramError, asyncio.CancelledError):
                pass
    
    async def auto_start_monitoring(self, chat_ids: List[int]):
        if not self.monitoring:
            self.monitoring = True
        
        for chat_id in chat_ids:
            if chat_id not in self.monitoring_chats:
                self.monitoring_chats.append(chat_id)
                logger.info(f"Auto-starting monitoring for chat {mask_user_id(chat_id)}")
                task = asyncio.create_task(self._monitoring_loop(chat_id))
                self.monitoring_tasks[chat_id] = task
                logger.info(f"✅ Monitoring task created for chat {mask_user_id(chat_id)}")
    
    async def stopmonitor_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            if not self.is_authorized(update.effective_user.id):
                return
            
            chat_id = update.effective_chat.id
            
            if chat_id in self.monitoring_chats:
                self.monitoring_chats.remove(chat_id)
                
                task = self.monitoring_tasks.pop(chat_id, None)
                if task:
                    if not task.done():
                        task.cancel()
                        try:
                            await asyncio.wait_for(task, timeout=3.0)
                        except (asyncio.CancelledError, asyncio.TimeoutError):
                            pass
                    logger.info(f"✅ Monitoring task cancelled for chat {mask_user_id(chat_id)}")
                
                await update.message.reply_text("🛑 Monitoring dihentikan untuk Anda.")
                
                if len(self.monitoring_chats) == 0:
                    self.monitoring = False
                    logger.info("All monitoring stopped")
            else:
                await update.message.reply_text("⚠️ Monitoring tidak sedang berjalan untuk Anda.")
                
            logger.info(f"Stop monitor command executed for user {mask_user_id(update.effective_user.id)}")
            
        except asyncio.CancelledError:
            logger.info(f"Stopmonitor command cancelled for user {mask_user_id(update.effective_user.id)}")
            raise
        except RetryAfter as e:
            logger.warning(f"Rate limit in stopmonitor command: retry after {e.retry_after}s")
        except (TimedOut, NetworkError) as e:
            logger.warning(f"Network/timeout error in stopmonitor command: {e}")
            try:
                await update.message.reply_text("⏳ Koneksi timeout, silakan coba lagi.")
            except (TelegramError, asyncio.CancelledError):
                pass
        except BadRequest as e:
            await self._handle_bad_request(update.effective_chat.id, e, context="stopmonitor_command")
        except Forbidden as e:
            await self._handle_forbidden_error(update.effective_chat.id, e)
        except ChatMigrated as e:
            await self._handle_chat_migrated(update.effective_chat.id, e)
        except Conflict as e:
            await self._handle_conflict_error(e)
        except InvalidToken as e:
            await self._handle_unauthorized_error(e)
        except TelegramError as e:
            logger.error(f"Telegram error in stopmonitor command: {e}")
            try:
                await update.message.reply_text("❌ Error menghentikan monitoring. Silakan coba lagi.")
            except (TelegramError, asyncio.CancelledError):
                pass
        except Exception as e:
            logger.error(f"Unexpected error in stopmonitor command: {type(e).__name__}: {e}", exc_info=True)
            try:
                await update.message.reply_text("❌ Error menghentikan monitoring. Silakan coba lagi.")
            except (TelegramError, asyncio.CancelledError):
                pass
    
    async def _monitoring_loop(self, chat_id: int):
        tick_queue = await self.market_data.subscribe_ticks(f'telegram_bot_{chat_id}')
        logger.debug(f"Monitoring started for user {mask_user_id(chat_id)}")
        
        last_signal_check = datetime.now() - timedelta(seconds=self.config.SIGNAL_COOLDOWN_SECONDS)
        last_tick_process_time = datetime.now() - timedelta(seconds=self.tick_throttle_seconds)
        last_sent_signal = None
        last_sent_signal_price = None
        last_sent_signal_time = datetime.now() - timedelta(seconds=5)
        retry_delay = 1.0
        max_retry_delay = 30.0
        
        try:
            while self.monitoring and chat_id in self.monitoring_chats and not self._is_shutting_down:
                try:
                    tick = await asyncio.wait_for(tick_queue.get(), timeout=30.0)
                    
                    now = datetime.now()
                    
                    # Tick throttling - jangan process setiap tick untuk hemat CPU
                    time_since_last_tick = (now - last_tick_process_time).total_seconds()
                    if time_since_last_tick < self.tick_throttle_seconds:
                        continue
                    
                    last_tick_process_time = now
                    
                    df_m1 = await self.market_data.get_historical_data('M1', 100)
                    
                    if df_m1 is None:
                        continue
                    
                    candle_count = len(df_m1)
                    
                    if candle_count >= 30:
                        # EARLY CHECK: Skip signal detection if user already has active position
                        if self.signal_session_manager:
                            can_create, block_reason = await self.signal_session_manager.can_create_signal(chat_id, 'auto')
                            if not can_create:
                                logger.debug(f"Skipping signal detection - {block_reason}")
                                # Update both global and per-user cooldown bookkeeping to prevent tight loop
                                self.global_last_signal_time = datetime.now()
                                last_signal_check = datetime.now()
                                # Sleep with global cooldown value to prevent tight loop
                                await asyncio.sleep(self.global_signal_cooldown)
                                continue
                        elif self.position_tracker.has_active_position(chat_id):
                            logger.debug(f"Skipping signal detection - user already has active position")
                            # Update both global and per-user cooldown bookkeeping to prevent tight loop
                            self.global_last_signal_time = datetime.now()
                            last_signal_check = datetime.now()
                            # Sleep with global cooldown value to prevent tight loop
                            await asyncio.sleep(self.global_signal_cooldown)
                            continue
                        
                        current_price = await self.market_data.get_current_price()
                        spread_value = await self.market_data.get_spread()
                        spread = spread_value if spread_value else 0.5
                        
                        if spread > self.config.MAX_SPREAD_PIPS:
                            logger.debug(f"Spread terlalu lebar ({spread:.2f} pips), skip signal detection")
                            continue
                        
                        # Signal detection - hanya jika tidak ada posisi aktif
                        from bot.indicators import IndicatorEngine
                        indicator_engine = IndicatorEngine(self.config)
                        indicators = indicator_engine.get_indicators(df_m1)
                        
                        if indicators:
                            signal = self.strategy.detect_signal(indicators, 'M1', signal_source='auto')
                            
                            # Avoid duplicate signals - check if this is a new signal (different price or direction)
                            signal_direction = signal['signal'] if signal else None
                            signal_price = signal['entry_price'] if signal else None
                            
                            is_duplicate = False
                            if signal_direction and last_sent_signal:
                                same_direction = (signal_direction == last_sent_signal)
                                time_too_soon = (now - last_sent_signal_time).total_seconds() < 5
                                
                                # Check if price is almost same (within 5 pips tolerance)
                                same_price = False
                                price_diff_pips = 0.0
                                if signal_price is not None and last_sent_signal_price is not None:
                                    price_diff_pips = abs(signal_price - last_sent_signal_price) * self.config.XAUUSD_PIP_VALUE
                                    same_price = price_diff_pips < 5.0
                                
                                is_duplicate = same_direction and time_too_soon and same_price
                                
                                if is_duplicate and signal_price is not None and last_sent_signal_price is not None:
                                    logger.debug(f"Duplicate signal detected: {signal_direction} @{signal_price:.2f} (last: {last_sent_signal_price:.2f}, diff: {price_diff_pips:.1f} pips)")
                            
                            if signal and not is_duplicate:
                                time_since_last_check = (now - last_signal_check).total_seconds()
                                
                                if time_since_last_check < self.config.SIGNAL_COOLDOWN_SECONDS:
                                    logger.debug(f"Per-user cooldown aktif, tunggu {self.config.SIGNAL_COOLDOWN_SECONDS - time_since_last_check:.1f}s lagi")
                                    continue
                                
                                can_trade, rejection_reason = self.risk_manager.can_trade(chat_id, signal['signal'])
                                
                                if can_trade:
                                    is_valid, validation_msg = self.strategy.validate_signal(signal, spread)
                                    
                                    if is_valid:
                                        async with self.signal_lock:
                                            global_time_since_signal = (datetime.now() - self.global_last_signal_time).total_seconds()
                                            
                                            if global_time_since_signal < self.global_signal_cooldown:
                                                wait_time = self.global_signal_cooldown - global_time_since_signal
                                                logger.info(f"Global cooldown aktif, menunda sinyal {wait_time:.1f}s untuk user {mask_user_id(chat_id)}")
                                                await asyncio.sleep(wait_time)
                                            
                                            # Double check sebelum create session (untuk race condition)
                                            if self.signal_session_manager:
                                                can_create, block_reason = await self.signal_session_manager.can_create_signal(chat_id, 'auto')
                                                if not can_create:
                                                    logger.info(f"Signal creation blocked for user {mask_user_id(chat_id)}: {block_reason}")
                                                    continue
                                                
                                                await self.signal_session_manager.create_session(
                                                    chat_id,
                                                    f"auto_{int(time.time())}",
                                                    'auto',
                                                    signal['signal'],
                                                    signal['entry_price'],
                                                    signal['stop_loss'],
                                                    signal['take_profit']
                                                )
                                            elif self.position_tracker.has_active_position(chat_id):
                                                logger.debug(f"Skipping - user has active position (race condition check)")
                                                continue
                                            
                                            await self._send_signal(chat_id, chat_id, signal, df_m1)
                                            
                                            # Track sent signal to prevent duplicates
                                            last_sent_signal = signal_direction
                                            last_sent_signal_price = signal_price
                                            last_sent_signal_time = now
                                            self.global_last_signal_time = now
                                        
                                        self.risk_manager.record_signal(chat_id)
                                        last_signal_check = now
                                        
                                        if self.user_manager:
                                            self.user_manager.update_user_activity(chat_id)
                                        
                                        retry_delay = 1.0
                    
                except asyncio.TimeoutError:
                    logger.debug(f"Tick queue timeout untuk user {mask_user_id(chat_id)}, mencoba lagi...")
                    continue
                except asyncio.CancelledError:
                    logger.info(f"Monitoring loop cancelled for user {mask_user_id(chat_id)}")
                    break
                except ConnectionError as e:
                    logger.warning(f"Connection error dalam monitoring loop: {e}, retry in {retry_delay}s")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, max_retry_delay)
                except Forbidden as e:
                    logger.warning(f"Forbidden error in monitoring loop for {mask_user_id(chat_id)}: {e}")
                    await self._handle_forbidden_error(chat_id, e)
                    break
                except ChatMigrated as e:
                    new_chat_id = await self._handle_chat_migrated(chat_id, e)
                    if new_chat_id:
                        logger.info(f"Monitoring loop: chat migrated {mask_user_id(chat_id)} -> {mask_user_id(new_chat_id)}")
                    break
                except Conflict as e:
                    await self._handle_conflict_error(e)
                    break
                except InvalidToken as e:
                    await self._handle_unauthorized_error(e)
                    break
                except BadRequest as e:
                    await self._handle_bad_request(chat_id, e, context="monitoring_loop")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, max_retry_delay)
                except (TimedOut, NetworkError) as e:
                    logger.warning(f"Network/Timeout error in monitoring loop for {mask_user_id(chat_id)}: {e}")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, max_retry_delay)
                except Exception as e:
                    logger.error(f"Error processing tick dalam monitoring loop: {type(e).__name__}: {e}")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, max_retry_delay)
                    
        finally:
            await self.market_data.unsubscribe_ticks(f'telegram_bot_{chat_id}')
            
            if self.monitoring_tasks.pop(chat_id, None):
                logger.debug(f"Monitoring task removed from tracking for chat {mask_user_id(chat_id)}")
            
            logger.debug(f"Monitoring stopped for user {mask_user_id(chat_id)}")
    
    @retry_on_telegram_error(max_retries=3, initial_delay=1.0)
    async def _send_telegram_message(self, chat_id: int, text: str, parse_mode: str = 'Markdown', timeout: float = 30.0):
        """Send Telegram message with retry logic and validation"""
        if not validate_chat_id(chat_id):
            raise ValidationError(f"Invalid chat_id: {chat_id}")
        
        if not text or not text.strip():
            raise ValidationError("Empty message text")
        
        if len(text) > 4096:
            logger.warning(f"Message too long ({len(text)} chars), truncating to 4096")
            text = text[:4090] + "..."
        
        await self.rate_limiter.acquire_async(wait=True)
        
        try:
            return await asyncio.wait_for(
                self.app.bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            # Fallback ke plain text jika Markdown timeout
            logger.warning(f"Markdown message timeout, trying plain text fallback")
            try:
                # Strip Markdown formatting untuk plain text
                plain_text = text.replace('*', '').replace('_', '').replace('`', '')
                return await asyncio.wait_for(
                    self.app.bot.send_message(chat_id=chat_id, text=plain_text, parse_mode=None),
                    timeout=15.0
                )
            except asyncio.TimeoutError:
                logger.error(f"Plain text fallback also timeout for chat {mask_user_id(chat_id)}")
                raise TimedOut("Message send timeout (fallback failed)")
            except Exception as fallback_error:
                logger.error(f"Fallback error: {fallback_error}")
                raise TimedOut("Message send timeout")
    
    @retry_on_telegram_error(max_retries=3, initial_delay=1.0)
    async def _send_telegram_photo(self, chat_id: int, photo_path: str, caption: str = None, timeout: float = 90.0):
        """Send Telegram photo with retry logic and validation"""
        if not validate_chat_id(chat_id):
            raise ValidationError(f"Invalid chat_id: {chat_id}")
        
        if not photo_path or not photo_path.strip():
            raise ValidationError("Empty photo path")
        
        import os
        if not os.path.exists(photo_path):
            raise ValidationError(f"Photo file not found: {photo_path}")
        
        if caption and len(caption) > 1024:
            logger.warning(f"Caption too long ({len(caption)} chars), truncating")
            caption = caption[:1020] + "..."
        
        await self.rate_limiter.acquire_async(wait=True)
        
        try:
            with open(photo_path, 'rb') as photo:
                return await asyncio.wait_for(
                    self.app.bot.send_photo(chat_id=chat_id, photo=photo, caption=caption),
                    timeout=timeout
                )
        except asyncio.TimeoutError:
            # Fallback: coba kirim tanpa caption untuk reduce payload
            logger.warning(f"Photo with caption timeout, trying without caption")
            try:
                with open(photo_path, 'rb') as photo:
                    return await asyncio.wait_for(
                        self.app.bot.send_photo(chat_id=chat_id, photo=photo, caption=None),
                        timeout=45.0
                    )
            except asyncio.TimeoutError:
                logger.error(f"Photo send timeout (fallback also failed) for chat {mask_user_id(chat_id)}")
                raise TimedOut("Photo send timeout (fallback failed)")
            except Exception as fallback_error:
                logger.error(f"Photo fallback error: {fallback_error}")
                raise TimedOut("Photo send timeout")
    
    async def _send_signal(self, user_id: int, chat_id: int, signal: dict, df: Optional[pd.DataFrame] = None):
        """Send trading signal with enhanced error handling and validation"""
        try:
            if not validate_chat_id(user_id):
                logger.error(f"Invalid user_id: {user_id}")
                return
            
            if not validate_chat_id(chat_id):
                logger.error(f"Invalid chat_id: {chat_id}")
                return
            
            if not signal or not isinstance(signal, dict):
                logger.error(f"Invalid signal data: {type(signal)}")
                return
            
            required_fields = ['signal', 'entry_price', 'stop_loss', 'take_profit', 'timeframe']
            missing_fields = [f for f in required_fields if f not in signal]
            if missing_fields:
                logger.error(f"Signal missing required fields: {missing_fields}")
                return
            
            can_proceed = await self._check_and_set_pending(user_id, signal['signal'], signal['entry_price'])
            if not can_proceed:
                logger.warning(f"🚫 Duplicate signal blocked for user {mask_user_id(user_id)}: {signal['signal']} @${signal['entry_price']:.2f}")
                return
            
            signal_sent_successfully = False
            signal_type = signal['signal']
            entry_price = signal['entry_price']
            
            session = self.db.get_session()
            trade_id = None
            position_id = None
            
            try:
                signal_source = signal.get('signal_source', 'auto')
                
                trade = Trade(
                    user_id=user_id,
                    ticker='XAUUSD',
                    signal_type=signal_type,
                    signal_source=signal_source,
                    entry_price=entry_price,
                    stop_loss=signal['stop_loss'],
                    take_profit=signal['take_profit'],
                    timeframe=signal['timeframe'],
                    status='OPEN'
                )
                session.add(trade)
                session.flush()
                trade_id = trade.id
                
                logger.debug(f"Trade created in DB with ID {trade_id}, preparing to add position...")
                
                position_id = await self.position_tracker.add_position(
                    user_id,
                    trade_id,
                    signal_type,
                    entry_price,
                    signal['stop_loss'],
                    signal['take_profit']
                )
                
                if not position_id:
                    raise ValueError("Failed to create position in position tracker")
                
                session.commit()
                logger.debug(f"✅ Database committed - Trade:{trade_id} Position:{position_id}")
                
            except Exception as e:
                logger.error(f"DB/Position error: {type(e).__name__}: {e}")
                session.rollback()
                raise
            finally:
                session.close()
            
            try:
                sl_pips = signal.get('sl_pips', abs(signal['entry_price'] - signal['stop_loss']) * self.config.XAUUSD_PIP_VALUE)
                tp_pips = signal.get('tp_pips', abs(signal['entry_price'] - signal['take_profit']) * self.config.XAUUSD_PIP_VALUE)
                lot_size = signal.get('lot_size', self.config.LOT_SIZE)
                
                source_icon = "🤖" if signal_source == 'auto' else "👤"
                source_text = "OTOMATIS" if signal_source == 'auto' else "MANUAL"
                
                msg = MessageFormatter.signal_alert(signal, signal_source)
                
                signal_message = None
                if self.app and self.app.bot:
                    try:
                        signal_message = await self._send_telegram_message(chat_id, msg, parse_mode='Markdown', timeout=30.0)
                    except Forbidden as e:
                        logger.warning(f"User blocked bot, cannot send signal: {e}")
                        await self._handle_forbidden_error(chat_id, e)
                        return
                    except ChatMigrated as e:
                        new_chat_id = await self._handle_chat_migrated(chat_id, e)
                        if new_chat_id:
                            try:
                                signal_message = await self._send_telegram_message(new_chat_id, msg, parse_mode='Markdown', timeout=30.0)
                                chat_id = new_chat_id
                            except Exception as retry_error:
                                logger.error(f"Failed to send signal to migrated chat: {retry_error}")
                    except BadRequest as e:
                        await self._handle_bad_request(chat_id, e, context="send_signal_message")
                        try:
                            fallback_msg = f"🚨 SINYAL {signal['signal']} @${signal['entry_price']:.2f} | SL: ${signal['stop_loss']:.2f} | TP: ${signal['take_profit']:.2f}"
                            signal_message = await self._send_telegram_message(chat_id, fallback_msg, parse_mode=None, timeout=15.0)
                        except Exception as fallback_error:
                            logger.error(f"Fallback message also failed: {fallback_error}")
                    except Conflict as e:
                        await self._handle_conflict_error(e)
                        return
                    except InvalidToken as e:
                        await self._handle_unauthorized_error(e)
                        return
                    except (TimedOut, NetworkError, TelegramError) as e:
                        logger.error(f"Failed to send signal message: {e}")
                        try:
                            fallback_msg = f"🚨 SINYAL {signal['signal']} @${signal['entry_price']:.2f} | SL: ${signal['stop_loss']:.2f} | TP: ${signal['take_profit']:.2f}"
                            await self._send_telegram_message(chat_id, fallback_msg, parse_mode=None, timeout=15.0)
                        except Exception as fallback_error:
                            logger.error(f"Fallback message also failed: {fallback_error}")
                    
                    if df is not None and len(df) >= 30:
                        # Check if photo already sent for this session (prevent duplicates)
                        photo_already_sent = False
                        if self.signal_session_manager:
                            session_data = self.signal_session_manager.get_active_session(user_id)
                            if session_data and session_data.photo_sent:
                                photo_already_sent = True
                                logger.debug(f"Photo already sent for user {mask_user_id(user_id)}, skipping duplicate")
                        
                        if not photo_already_sent:
                            try:
                                chart_path = await asyncio.wait_for(
                                    self.chart_generator.generate_chart_async(df, signal, signal['timeframe']),
                                    timeout=45.0
                                )
                                
                                if chart_path:
                                    try:
                                        await self._send_telegram_photo(chat_id, chart_path, timeout=60.0)
                                        if self.signal_session_manager:
                                            await self.signal_session_manager.update_session(
                                                user_id, 
                                                photo_sent=True,
                                                chart_path=chart_path
                                            )
                                        logger.info(f"📸 Chart sent successfully for user {mask_user_id(user_id)}")
                                    except Forbidden as e:
                                        logger.warning(f"User blocked bot, cannot send chart: {e}")
                                        if self.signal_session_manager:
                                            await self.signal_session_manager.update_session(user_id, photo_sent=True)
                                    except ChatMigrated as e:
                                        new_chat_id = await self._handle_chat_migrated(chat_id, e)
                                        if new_chat_id:
                                            try:
                                                await self._send_telegram_photo(new_chat_id, chart_path, timeout=60.0)
                                                if self.signal_session_manager:
                                                    await self.signal_session_manager.update_session(user_id, photo_sent=True, chart_path=chart_path)
                                            except Exception:
                                                pass
                                    except BadRequest as e:
                                        await self._handle_bad_request(chat_id, e, context="send_chart_photo")
                                        if self.signal_session_manager:
                                            await self.signal_session_manager.update_session(user_id, photo_sent=True)
                                    except (TimedOut, NetworkError, TelegramError) as e:
                                        logger.warning(f"Failed to send chart: {e}. Signal sent successfully.")
                                        if self.signal_session_manager:
                                            await self.signal_session_manager.update_session(user_id, photo_sent=True)
                                    finally:
                                        # Chart cleanup handled by session end, but also auto-delete if enabled
                                        if self.config.CHART_AUTO_DELETE and not self.signal_session_manager:
                                            await asyncio.sleep(2)
                                            self.chart_generator.delete_chart(chart_path)
                                            logger.debug(f"Auto-deleted chart: {chart_path}")
                                else:
                                    logger.warning(f"Chart generation returned None for {signal['signal']} signal")
                            except asyncio.TimeoutError:
                                logger.warning("Chart generation timeout - signal sent without chart")
                            except Exception as e:
                                logger.warning(f"Chart generation/send failed: {e}. Signal sent successfully.")
                    else:
                        logger.debug(f"Skipping chart - insufficient candles ({len(df) if df is not None else 0}/30)")
                
                signal_sent_successfully = True
                await self._confirm_signal_sent(user_id, signal_type, entry_price)
                logger.info(f"✅ Signal sent - Trade:{trade_id} Position:{position_id} User:{mask_user_id(user_id)} {signal_type} @${entry_price:.2f}")
                
                if signal_message and signal_message.message_id:
                    await self.start_dashboard(user_id, chat_id, position_id, signal_message.message_id)
                    
            except (ValidationError, ValueError) as e:
                logger.error(f"Validation error in signal processing: {e}")
            except Exception as e:
                logger.error(f"Error in signal processing: {type(e).__name__}: {e}", exc_info=True)
            finally:
                if not signal_sent_successfully:
                    await self._rollback_signal_cache(user_id, signal_type, entry_price)
                    logger.debug(f"Signal cache rolled back after failure for user {mask_user_id(user_id)}")
            
        except Exception as e:
            logger.error(f"Critical error sending signal: {type(e).__name__}: {e}", exc_info=True)
            await self._rollback_signal_cache(user_id, signal['signal'], signal['entry_price'])
            if self.error_handler:
                self.error_handler.log_exception(e, "send_signal")
            if self.alert_system:
                try:
                    await asyncio.wait_for(
                        self.alert_system.send_system_error(f"Error sending signal: {str(e)}"),
                        timeout=10.0
                    )
                except Exception as alert_error:
                    logger.error(f"Failed to send error alert: {alert_error}")
    
    async def _on_session_end_handler(self, session):
        """Handler untuk event on_session_end dari SignalSessionManager"""
        try:
            user_id = session.user_id
            logger.info(f"Session ended for user {mask_user_id(user_id)}, stopping dashboard and cleaning up cache")
            
            await self.stop_dashboard(user_id)
            
            await self._clear_signal_cache(user_id)
            logger.info(f"✅ Signal cache cleared for user {mask_user_id(user_id)} after session end")
            
        except Exception as e:
            logger.error(f"Error in session end handler: {e}")
    
    async def start_dashboard(self, user_id: int, chat_id: int, position_id: int, message_id: int):
        """Start real-time dashboard monitoring untuk posisi aktif"""
        try:
            if user_id in self.active_dashboards:
                logger.debug(f"Dashboard already running for user {mask_user_id(user_id)}, stopping old one first")
                await self.stop_dashboard(user_id)
            
            async with self._dashboard_lock:
                if len(self.active_dashboards) >= self.MAX_DASHBOARDS:
                    now = datetime.now(pytz.UTC)
                    stale_users = []
                    
                    for uid, dash_info in list(self.active_dashboards.items()):
                        task = dash_info.get('task')
                        if task is None or task.done():
                            stale_users.append(uid)
                    
                    for uid in stale_users:
                        self.active_dashboards.pop(uid, None)
                    
                    if len(self.active_dashboards) >= self.MAX_DASHBOARDS:
                        sorted_dashboards = sorted(
                            self.active_dashboards.items(),
                            key=lambda x: x[1].get('started_at', now)
                        )
                        oldest_user_id = sorted_dashboards[0][0]
                        oldest_task = sorted_dashboards[0][1].get('task')
                        if oldest_task and not oldest_task.done():
                            oldest_task.cancel()
                        self.active_dashboards.pop(oldest_user_id, None)
                        logger.warning(f"Dashboard limit reached, removed oldest dashboard for user {mask_user_id(oldest_user_id)}")
            
            dashboard_task = asyncio.create_task(
                self._dashboard_update_loop(user_id, chat_id, position_id, message_id)
            )
            
            self.active_dashboards[user_id] = {
                'task': dashboard_task,
                'chat_id': chat_id,
                'position_id': position_id,
                'message_id': message_id,
                'started_at': datetime.now(pytz.UTC),
                'created_at': datetime.now()
            }
            
            logger.info(f"📊 Dashboard started - User:{mask_user_id(user_id)} Position:{position_id} Message:{message_id}")
            
        except asyncio.CancelledError:
            logger.info(f"Dashboard start cancelled for user {mask_user_id(user_id)}")
            raise
        except Forbidden as e:
            logger.warning(f"User blocked bot, cannot start dashboard: {e}")
            await self._handle_forbidden_error(chat_id, e)
        except ChatMigrated as e:
            await self._handle_chat_migrated(chat_id, e)
        except BadRequest as e:
            await self._handle_bad_request(chat_id, e, context="start_dashboard")
        except Conflict as e:
            await self._handle_conflict_error(e)
        except InvalidToken as e:
            await self._handle_unauthorized_error(e)
        except (TelegramError, NetworkError, TimedOut) as e:
            logger.error(f"Telegram error starting dashboard for user {mask_user_id(user_id)}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error starting dashboard for user {mask_user_id(user_id)}: {type(e).__name__}: {e}")
    
    async def stop_dashboard(self, user_id: int):
        """Stop dashboard monitoring dan cleanup task"""
        try:
            if user_id not in self.active_dashboards:
                logger.debug(f"No active dashboard for user {mask_user_id(user_id)}")
                return
            
            dashboard = self.active_dashboards[user_id]
            task = dashboard.get('task')
            
            if task and not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            
            del self.active_dashboards[user_id]
            
            duration = (datetime.now(pytz.UTC) - dashboard['started_at']).total_seconds()
            logger.info(f"🛑 Dashboard stopped - User:{mask_user_id(user_id)} Duration:{duration:.1f}s")
            
        except Exception as e:
            logger.error(f"Error stopping dashboard for user {mask_user_id(user_id)}: {e}")
    
    async def _dashboard_update_loop(self, user_id: int, chat_id: int, position_id: int, message_id: int):
        """Loop update dashboard INSTANT dengan progress TP/SL real-time"""
        update_count = 0
        last_message_text = None
        dashboard_update_interval = 5  # Optimal - 5 detik update
        
        try:
            while True:
                try:
                    await asyncio.sleep(dashboard_update_interval)
                    
                    if user_id not in self.active_dashboards:
                        logger.debug(f"Dashboard removed for user {mask_user_id(user_id)}, stopping loop")
                        break
                    
                    if not self.position_tracker:
                        logger.warning("Position tracker not available, stopping dashboard")
                        break
                    
                    session = self.db.get_session()
                    try:
                        position_db = session.query(Position).filter(
                            Position.id == position_id,
                            Position.user_id == user_id
                        ).first()
                        
                        if not position_db:
                            logger.info(f"Position {position_id} not found in DB, stopping dashboard")
                            break
                        
                        if position_db.status != 'ACTIVE':
                            logger.info(f"Position {position_id} is {position_db.status}, sending EXPIRED message")
                            
                            try:
                                expired_msg = (
                                    f"⏱️ *DASHBOARD EXPIRED*\n"
                                    f"{'━' * 32}\n\n"
                                    f"✅ Posisi sudah ditutup\n"
                                    f"📊 Status: {position_db.status}\n\n"
                                    f"💡 Cek hasil:\n"
                                    f"  • /riwayat - Riwayat trading\n"
                                    f"  • /performa - Statistik lengkap\n\n"
                                    f"⏰ {datetime.now(pytz.timezone('Asia/Jakarta')).strftime('%H:%M:%S WIB')}"
                                )
                                
                                await self.app.bot.edit_message_text(
                                    chat_id=chat_id,
                                    message_id=message_id,
                                    text=expired_msg,
                                    parse_mode='Markdown'
                                )
                                logger.info(f"✅ EXPIRED message sent to user {mask_user_id(user_id)}")
                            except Exception as e:
                                logger.error(f"Error sending EXPIRED message: {e}")
                            
                            break
                        
                        current_price = await self.market_data.get_current_price()
                        
                        if current_price is None:
                            logger.warning("Failed to get current price, skipping update")
                            continue
                        
                        signal_type = position_db.signal_type
                        entry_price = position_db.entry_price
                        stop_loss = position_db.stop_loss
                        take_profit = position_db.take_profit
                        
                        unrealized_pl = self.risk_manager.calculate_pl(entry_price, current_price, signal_type)
                        
                        position_data = {
                            'signal_type': signal_type,
                            'entry_price': entry_price,
                            'current_price': current_price,
                            'stop_loss': stop_loss,
                            'take_profit': take_profit,
                            'unrealized_pl': unrealized_pl
                        }
                        
                        message_text = MessageFormatter.position_update(position_data)
                        
                        if message_text == last_message_text:
                            continue
                        
                        try:
                            await self.app.bot.edit_message_text(
                                chat_id=chat_id,
                                message_id=message_id,
                                text=message_text,
                                parse_mode='Markdown'
                            )
                            
                            update_count += 1
                            last_message_text = message_text
                            logger.debug(f"Dashboard updated #{update_count} for user {mask_user_id(user_id)}")
                            
                        except BadRequest as e:
                            if "message is not modified" in str(e).lower():
                                logger.debug("Message content unchanged, skipping edit")
                                continue
                            elif "message to edit not found" in str(e).lower() or "message can't be edited" in str(e).lower():
                                logger.warning(f"Message {message_id} too old or deleted, stopping dashboard")
                                break
                            else:
                                logger.error(f"BadRequest editing message: {e}")
                                continue
                        
                    finally:
                        session.close()
                    
                except asyncio.CancelledError:
                    logger.info(f"Dashboard update loop cancelled for user {mask_user_id(user_id)}")
                    break
                except Forbidden as e:
                    logger.warning(f"User blocked bot in dashboard loop: {e}")
                    await self._handle_forbidden_error(chat_id, e)
                    break
                except ChatMigrated as e:
                    new_chat_id = await self._handle_chat_migrated(chat_id, e)
                    if new_chat_id:
                        chat_id = new_chat_id
                        logger.info(f"Dashboard: Updated chat ID to {mask_user_id(new_chat_id)}")
                    else:
                        break
                except Conflict as e:
                    await self._handle_conflict_error(e)
                    break
                except InvalidToken as e:
                    await self._handle_unauthorized_error(e)
                    break
                except (TimedOut, NetworkError) as e:
                    logger.warning(f"Network/Timeout error in dashboard update: {e}")
                    await asyncio.sleep(5)
                except Exception as e:
                    logger.error(f"Error in dashboard update loop: {type(e).__name__}: {e}")
                    await asyncio.sleep(5)
                    
        except Exception as e:
            logger.error(f"Critical error in dashboard loop: {type(e).__name__}: {e}")
        
        finally:
            if user_id in self.active_dashboards:
                await self.stop_dashboard(user_id)
    
    async def riwayat_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.is_authorized(update.effective_user.id):
            return
        
        user_id = update.effective_user.id
        session = None
        
        try:
            session = self.db.get_session()
            trades = session.query(Trade).filter(Trade.user_id == user_id).order_by(Trade.signal_time.desc()).limit(10).all()
            
            if not trades:
                await update.message.reply_text("📊 Belum ada riwayat trading.")
                return
            
            msg = "📊 *Riwayat Trading (10 Terakhir):*\n\n"
            
            jakarta_tz = pytz.timezone('Asia/Jakarta')
            
            for trade in trades:
                signal_time = trade.signal_time.replace(tzinfo=pytz.UTC).astimezone(jakarta_tz)
                
                msg += f"*{trade.signal_type}* - {signal_time.strftime('%d/%m %H:%M')}\n"
                msg += f"Entry: ${trade.entry_price:.2f}\n"
                
                if trade.status == 'CLOSED':
                    result_emoji = "✅" if trade.result == 'WIN' else "❌"
                    msg += f"Exit: ${trade.exit_price:.2f}\n"
                    msg += f"P/L: ${trade.actual_pl:.2f} {result_emoji}\n"
                else:
                    msg += f"Status: {trade.status}\n"
                
                msg += "\n"
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except asyncio.CancelledError:
            logger.info(f"Riwayat command cancelled for user {mask_user_id(user_id)}")
            raise
        except RetryAfter as e:
            logger.warning(f"Rate limit in riwayat command: retry after {e.retry_after}s")
        except (TimedOut, NetworkError) as e:
            logger.warning(f"Network/timeout error in riwayat command: {e}")
        except TelegramError as e:
            logger.error(f"Telegram error in riwayat command: {e}")
            try:
                await update.message.reply_text("❌ Error mengambil riwayat.")
            except (TelegramError, asyncio.CancelledError):
                pass
        except Exception as e:
            logger.error(f"Unexpected error fetching history: {type(e).__name__}: {e}")
            try:
                await update.message.reply_text("❌ Error mengambil riwayat.")
            except (TelegramError, asyncio.CancelledError):
                pass
        finally:
            if session:
                session.close()
    
    async def performa_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.is_authorized(update.effective_user.id):
            return
        
        user_id = update.effective_user.id
        session = None
        
        try:
            session = self.db.get_session()
            
            all_trades = session.query(Trade).filter(Trade.user_id == user_id, Trade.status == 'CLOSED').all()
            
            if not all_trades:
                await update.message.reply_text("📊 Belum ada data performa.")
                return
            
            total_trades = len(all_trades)
            wins = len([t for t in all_trades if t.result == 'WIN'])
            losses = len([t for t in all_trades if t.result == 'LOSS'])
            total_pl = sum([t.actual_pl for t in all_trades if t.actual_pl])
            
            win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
            
            today = datetime.now(pytz.timezone('Asia/Jakarta')).replace(hour=0, minute=0, second=0, microsecond=0)
            today_utc = today.astimezone(pytz.UTC)
            
            today_trades = session.query(Trade).filter(
                Trade.user_id == user_id,
                Trade.signal_time >= today_utc,
                Trade.status == 'CLOSED'
            ).all()
            
            today_pl = sum([t.actual_pl for t in today_trades if t.actual_pl])
            
            msg = (
                "📊 *Statistik Performa*\n\n"
                f"*Total Trades:* {total_trades}\n"
                f"*Wins:* {wins} ✅\n"
                f"*Losses:* {losses} ❌\n"
                f"*Win Rate:* {win_rate:.1f}%\n"
                f"*Total P/L:* ${total_pl:.2f}\n"
                f"*Avg P/L per Trade:* ${total_pl/total_trades:.2f}\n\n"
                f"*Hari Ini:*\n"
                f"Trades: {len(today_trades)}\n"
                f"P/L: ${today_pl:.2f}\n"
            )
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except asyncio.CancelledError:
            logger.info(f"Performa command cancelled for user {mask_user_id(user_id)}")
            raise
        except RetryAfter as e:
            logger.warning(f"Rate limit in performa command: retry after {e.retry_after}s")
        except (TimedOut, NetworkError) as e:
            logger.warning(f"Network/timeout error in performa command: {e}")
        except TelegramError as e:
            logger.error(f"Telegram error in performa command: {e}")
            try:
                await update.message.reply_text("❌ Error menghitung performa.")
            except (TelegramError, asyncio.CancelledError):
                pass
        except Exception as e:
            logger.error(f"Unexpected error calculating performance: {type(e).__name__}: {e}")
            try:
                await update.message.reply_text("❌ Error menghitung performa.")
            except (TelegramError, asyncio.CancelledError):
                pass
        finally:
            if session:
                session.close()
    
    async def analytics_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.is_authorized(update.effective_user.id):
            return
        
        user_id = update.effective_user.id
        
        try:
            from bot.analytics import TradingAnalytics
            
            analytics = TradingAnalytics(self.db, self.config)
            
            days = 30
            if context.args and len(context.args) > 0:
                try:
                    days = int(context.args[0])
                    days = max(1, min(days, 365))
                except ValueError:
                    days = 30
            
            await update.message.reply_text(f"📊 Mengambil analytics {days} hari terakhir...")
            
            performance = analytics.get_trading_performance(user_id, days)
            hourly = analytics.get_hourly_stats(user_id, days)
            source_perf = analytics.get_signal_source_performance(user_id, days)
            position_stats = analytics.get_position_tracking_stats(user_id, days)
            risk_metrics = analytics.get_risk_metrics(user_id, days)
            
            if 'error' in performance:
                await update.message.reply_text(f"❌ Error: {performance['error']}")
                return
            
            msg = f"📊 *COMPREHENSIVE ANALYTICS* ({days} hari)\n\n"
            
            msg += "*📈 Trading Performance:*\n"
            msg += f"• Total Trades: {performance['total_trades']}\n"
            msg += f"• Wins: {performance['wins']} | Losses: {performance['losses']}\n"
            msg += f"• Win Rate: {performance['winrate']}%\n"
            msg += f"• Total P/L: ${performance['total_pl']:.2f}\n"
            msg += f"• Avg P/L: ${performance['avg_pl']:.2f}\n"
            msg += f"• Avg Win: ${performance['avg_win']:.2f}\n"
            msg += f"• Avg Loss: ${performance['avg_loss']:.2f}\n"
            msg += f"• Profit Factor: {performance['profit_factor']}\n\n"
            
            msg += "*🎯 Signal Source Performance:*\n"
            auto_stats = source_perf.get('auto', {})
            manual_stats = source_perf.get('manual', {})
            msg += f"Auto: {auto_stats.get('total_trades', 0)} trades | WR: {auto_stats.get('winrate', 0)}% | P/L: ${auto_stats.get('total_pl', 0):.2f}\n"
            msg += f"Manual: {manual_stats.get('total_trades', 0)} trades | WR: {manual_stats.get('winrate', 0)}% | P/L: ${manual_stats.get('total_pl', 0):.2f}\n\n"
            
            msg += "*⏱️ Position Tracking:*\n"
            msg += f"• Avg Hold Time: {position_stats.get('avg_hold_time_hours', 0):.1f} hours\n"
            msg += f"• Avg Max Profit: ${position_stats.get('avg_max_profit', 0):.2f}\n"
            msg += f"• SL Adjusted: {position_stats.get('positions_with_sl_adjusted', 0)} ({position_stats.get('sl_adjustment_rate', 0):.1f}%)\n"
            msg += f"• Avg Profit Captured: {position_stats.get('avg_profit_captured', 0):.1f}%\n\n"
            
            msg += "*🛡️ Risk Metrics:*\n"
            msg += f"• TP Hit Rate: {risk_metrics.get('tp_hit_rate', 0):.1f}%\n"
            msg += f"• SL Hit Rate: {risk_metrics.get('sl_hit_rate', 0):.1f}%\n"
            msg += f"• Avg Planned R:R: 1:{risk_metrics.get('avg_planned_rr', 0):.2f}\n"
            msg += f"• Avg Actual R:R: 1:{risk_metrics.get('avg_actual_rr', 0):.2f}\n"
            msg += f"• R:R Efficiency: {risk_metrics.get('rr_efficiency', 0):.1f}%\n\n"
            
            best_hour = hourly.get('best_hour', {})
            worst_hour = hourly.get('worst_hour', {})
            if best_hour.get('hour') is not None:
                msg += f"*⏰ Best Hour:* {best_hour['hour']}:00 (P/L: ${best_hour.get('stats', {}).get('total_pl', 0):.2f})\n"
            if worst_hour.get('hour') is not None:
                msg += f"*⏰ Worst Hour:* {worst_hour['hour']}:00 (P/L: ${worst_hour.get('stats', {}).get('total_pl', 0):.2f})\n"
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Error in analytics command: {e}", exc_info=True)
            await update.message.reply_text("❌ Error mengambil analytics.")
    
    async def systemhealth_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.is_authorized(update.effective_user.id):
            return
        
        try:
            from bot.performance_monitor import SystemMonitor
            
            system_monitor = SystemMonitor(self.config)
            
            health = system_monitor.get_comprehensive_health()
            
            process_info = health.get('process', {})
            system_info = health.get('system', {})
            ws_info = health.get('websocket', {})
            
            cpu = process_info.get('cpu_percent', 0)
            mem = process_info.get('memory', {})
            uptime_seconds = process_info.get('uptime_seconds', 0)
            
            uptime_hours = uptime_seconds / 3600
            uptime_str = f"{uptime_hours:.1f}h" if uptime_hours < 24 else f"{uptime_hours/24:.1f}d"
            
            sys_cpu = system_info.get('system_cpu_percent', 0)
            sys_mem = system_info.get('system_memory_percent', 0)
            disk_usage = system_info.get('disk_usage_percent', 0)
            
            ws_status = ws_info.get('status', 'unknown')
            ws_health = ws_info.get('health_status', 'unknown')
            ws_reconnects = ws_info.get('reconnection_count', 0)
            
            health_emoji = "🟢" if ws_health == 'healthy' else "🟡" if ws_health == 'warning' else "🔴"
            
            msg = (
                f"🏥 *SYSTEM HEALTH*\n\n"
                f"*Process Status:*\n"
                f"• CPU: {cpu:.1f}%\n"
                f"• Memory: {mem.get('percent', 0):.1f}% ({mem.get('rss_mb', 0):.1f} MB)\n"
                f"• Threads: {process_info.get('num_threads', 0)}\n"
                f"• Uptime: {uptime_str}\n\n"
                f"*System Resources:*\n"
                f"• System CPU: {sys_cpu:.1f}%\n"
                f"• System Memory: {sys_mem:.1f}%\n"
                f"• Disk Usage: {disk_usage:.1f}%\n"
                f"• Disk Free: {system_info.get('disk_free_gb', 0):.1f} GB\n\n"
                f"*WebSocket Status:* {health_emoji}\n"
                f"• Status: {ws_status}\n"
                f"• Health: {ws_health}\n"
                f"• Reconnections: {ws_reconnects}\n"
            )
            
            if ws_info.get('seconds_since_heartbeat'):
                msg += f"• Last Heartbeat: {ws_info['seconds_since_heartbeat']:.0f}s ago\n"
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Error in systemhealth command: {e}", exc_info=True)
            await update.message.reply_text("❌ Error mengambil system health.")
    
    async def tasks_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show scheduled tasks status"""
        if not self.is_authorized(update.effective_user.id):
            return
        
        try:
            if not self.task_scheduler:
                await update.message.reply_text(
                    "❌ Task scheduler tidak tersedia.\n"
                    "Bot mungkin running dalam limited mode.",
                    parse_mode='Markdown'
                )
                return
            
            status = self.task_scheduler.get_status()
            
            msg = f"📅 *SCHEDULED TASKS*\n\n"
            msg += f"Scheduler: {'✅ Running' if status['running'] else '⛔ Stopped'}\n"
            msg += f"Total Tasks: {status['total_tasks']}\n"
            msg += f"Enabled: {status['enabled_tasks']}\n"
            msg += f"Active Executions: {status['active_executions']}\n\n"
            
            tasks = status.get('tasks', {})
            
            if not tasks:
                msg += "Tidak ada task yang dijadwalkan."
            else:
                for task_name, task_info in tasks.items():
                    status_icon = '✅' if task_info.get('enabled') else '⛔'
                    
                    msg += f"{status_icon} *{task_name}*\n"
                    
                    if task_info.get('interval'):
                        interval_seconds = task_info['interval']
                        if interval_seconds < 60:
                            interval_str = f"{interval_seconds:.0f}s"
                        elif interval_seconds < 3600:
                            interval_str = f"{interval_seconds/60:.0f}m"
                        else:
                            interval_str = f"{interval_seconds/3600:.1f}h"
                        msg += f"Interval: {interval_str}\n"
                    elif task_info.get('schedule_time'):
                        msg += f"Scheduled: {task_info['schedule_time']}\n"
                    
                    if task_info.get('last_run'):
                        msg += f"Last Run: {task_info['last_run']}\n"
                    
                    if task_info.get('next_run'):
                        msg += f"Next Run: {task_info['next_run']}\n"
                    
                    run_count = task_info.get('run_count', 0)
                    error_count = task_info.get('error_count', 0)
                    msg += f"Runs: {run_count} | Errors: {error_count}\n\n"
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Error in tasks command: {e}", exc_info=True)
            await update.message.reply_text("❌ Error mengambil task status.")
    
    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show active positions with dynamic SL/TP tracking info"""
        if not self.is_authorized(update.effective_user.id):
            return
        
        user_id = update.effective_user.id
        
        try:
            if not self.position_tracker:
                await update.message.reply_text("❌ Position tracker tidak tersedia.")
                return
            
            active_positions = self.position_tracker.get_active_positions(user_id)
            
            if not active_positions:
                await update.message.reply_text(
                    "📊 *Position Status*\n\n"
                    "Tidak ada posisi aktif saat ini.\n"
                    "Gunakan /getsignal untuk membuat sinyal baru.",
                    parse_mode='Markdown'
                )
                return
            
            session = self.db.get_session()
            
            msg = f"📊 *Active Positions* ({len(active_positions)})\n\n"
            
            for pos_id, pos_data in active_positions.items():
                position_db = session.query(Position).filter(
                    Position.id == pos_id,
                    Position.user_id == user_id
                ).first()
                
                if not position_db:
                    continue
                
                signal_type = pos_data['signal_type']
                entry_price = pos_data['entry_price']
                current_sl = pos_data['stop_loss']
                original_sl = pos_data.get('original_sl', current_sl)
                take_profit = pos_data['take_profit']
                sl_count = pos_data.get('sl_adjustment_count', 0)
                max_profit = pos_data.get('max_profit_reached', 0.0)
                
                unrealized_pl = position_db.unrealized_pl or 0.0
                current_price = position_db.current_price or entry_price
                
                pl_emoji = "🟢" if unrealized_pl > 0 else "🔴" if unrealized_pl < 0 else "⚪"
                
                msg += f"*Position #{pos_id}* - {signal_type} {pl_emoji}\n"
                msg += f"Entry: ${entry_price:.2f}\n"
                msg += f"Current: ${current_price:.2f}\n"
                msg += f"P/L: ${unrealized_pl:.2f}\n\n"
                
                msg += f"*Take Profit:* ${take_profit:.2f}\n"
                
                if sl_count > 0:
                    msg += f"*Original SL:* ${original_sl:.2f}\n"
                    msg += f"*Current SL:* ${current_sl:.2f} ✅\n"
                    msg += f"*SL Adjusted:* {sl_count}x\n"
                else:
                    msg += f"*Stop Loss:* ${current_sl:.2f}\n"
                
                if max_profit > 0:
                    msg += f"*Max Profit:* ${max_profit:.2f}\n"
                    if unrealized_pl >= self.config.TRAILING_STOP_PROFIT_THRESHOLD:
                        msg += f"*Trailing Stop:* Active 💎\n"
                
                if position_db.last_price_update:
                    jakarta_tz = pytz.timezone('Asia/Jakarta')
                    last_update = position_db.last_price_update.replace(tzinfo=pytz.UTC).astimezone(jakarta_tz)
                    msg += f"Last Update: {last_update.strftime('%H:%M:%S')}\n"
                
                msg += "\n"
            
            session.close()
            await update.message.reply_text(msg, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Error fetching position status: {e}")
            await update.message.reply_text("❌ Error mengambil status posisi.")
    
    async def getsignal_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.is_authorized(update.effective_user.id):
            return
        
        user_id = update.effective_user.id
        
        try:
            if self.signal_session_manager:
                can_create, block_reason = await self.signal_session_manager.can_create_signal(user_id, 'manual')
                if not can_create:
                    await update.message.reply_text(
                        block_reason if block_reason else MessageFormatter.session_blocked('auto', 'manual'),
                        parse_mode='Markdown'
                    )
                    return
            elif self.position_tracker and self.position_tracker.has_active_position(user_id):
                await update.message.reply_text(
                    "⏳ *Tidak Dapat Membuat Sinyal Baru*\n\n"
                    "Saat ini Anda memiliki posisi aktif yang sedang berjalan.\n"
                    "Bot akan tracking hingga TP/SL tercapai.\n\n"
                    "Tunggu hasil posisi Anda saat ini sebelum request sinyal baru.",
                    parse_mode='Markdown'
                )
                return
            
            can_trade, rejection_reason = self.risk_manager.can_trade(user_id, 'ANY')
            
            if not can_trade:
                await update.message.reply_text(
                    f"⛔ *Tidak Bisa Trading*\n\n{rejection_reason}",
                    parse_mode='Markdown'
                )
                return
            
            df_m1 = await self.market_data.get_historical_data('M1', 100)
            
            if df_m1 is None or len(df_m1) < 30:
                await update.message.reply_text(
                    "⚠️ *Data Tidak Cukup*\n\n"
                    "Belum cukup data candle untuk analisis.\n"
                    f"Candles: {len(df_m1) if df_m1 is not None else 0}/30\n\n"
                    "Tunggu beberapa saat dan coba lagi.",
                    parse_mode='Markdown'
                )
                return
            
            from bot.indicators import IndicatorEngine
            indicator_engine = IndicatorEngine(self.config)
            indicators = indicator_engine.get_indicators(df_m1)
            
            if not indicators:
                await update.message.reply_text(
                    "⚠️ *Analisis Gagal*\n\n"
                    "Tidak dapat menghitung indikator.\n"
                    "Coba lagi nanti.",
                    parse_mode='Markdown'
                )
                return
            
            signal = self.strategy.detect_signal(indicators, 'M1', signal_source='manual')
            
            if not signal:
                trend_strength = indicators.get('trend_strength', 'UNKNOWN')
                current_price = await self.market_data.get_current_price()
                
                msg = (
                    "⚠️ *Tidak Ada Sinyal*\n\n"
                    "Kondisi market saat ini tidak memenuhi kriteria trading.\n\n"
                    f"*Market Info:*\n"
                    f"Price: ${current_price:.2f}\n"
                    f"Trend: {trend_strength}\n\n"
                    "Gunakan /monitor untuk auto-detect sinyal."
                )
                await update.message.reply_text(msg, parse_mode='Markdown')
                return
            
            current_price = await self.market_data.get_current_price()
            spread_value = await self.market_data.get_spread()
            spread = spread_value if spread_value else 0.5
            
            is_valid, validation_msg = self.strategy.validate_signal(signal, spread)
            
            if not is_valid:
                await update.message.reply_text(
                    f"⚠️ *Sinyal Tidak Valid*\n\n{validation_msg}",
                    parse_mode='Markdown'
                )
                return
            
            if self.signal_session_manager:
                await self.signal_session_manager.create_session(
                    user_id,
                    f"manual_{int(time.time())}",
                    'manual',
                    signal['signal'],
                    signal['entry_price'],
                    signal['stop_loss'],
                    signal['take_profit']
                )
            
            await self._send_signal(user_id, update.effective_chat.id, signal, df_m1)
            self.risk_manager.record_signal(user_id)
            
            if self.user_manager:
                self.user_manager.update_user_activity(user_id)
            
        except Exception as e:
            logger.error(f"Error generating manual signal: {e}")
            await update.message.reply_text("❌ Error membuat sinyal. Coba lagi nanti.")
    
    async def settings_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            if not self.is_authorized(update.effective_user.id):
                return
            
            msg = (
                "⚙️ *Bot Configuration*\n\n"
                f"*Mode:* {'DRY RUN' if self.config.DRY_RUN else 'LIVE'}\n"
                f"*Lot Size:* {self.config.LOT_SIZE:.2f}\n"
                f"*Fixed Risk:* ${self.config.FIXED_RISK_AMOUNT:.2f}\n"
                f"*Daily Loss Limit:* {self.config.DAILY_LOSS_PERCENT}%\n"
                f"*Signal Cooldown:* {self.config.SIGNAL_COOLDOWN_SECONDS}s\n"
                f"*Trailing Stop Threshold:* ${self.config.TRAILING_STOP_PROFIT_THRESHOLD:.2f}\n"
                f"*Breakeven Threshold:* ${self.config.BREAKEVEN_PROFIT_THRESHOLD:.2f}\n\n"
                f"*EMA Periods:* {', '.join(map(str, self.config.EMA_PERIODS))}\n"
                f"*RSI Period:* {self.config.RSI_PERIOD}\n"
                f"*ATR Period:* {self.config.ATR_PERIOD}\n"
            )
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            logger.info(f"Settings command executed for user {mask_user_id(update.effective_user.id)}")
            
        except Exception as e:
            logger.error(f"Error in settings command: {e}", exc_info=True)
            try:
                await update.message.reply_text("❌ Error menampilkan konfigurasi.")
            except Exception:
                pass
    
    async def riset_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.is_admin(update.effective_user.id):
            await update.message.reply_text("⛔ Perintah ini hanya untuk admin.")
            return
        
        try:
            logger.info("=" * 60)
            logger.info("STARTING COMPLETE SYSTEM RESET")
            logger.info("=" * 60)
            
            monitoring_count = len(self.monitoring_chats)
            active_tasks = len(self.monitoring_tasks)
            
            logger.info("Stopping all monitoring...")
            self.monitoring = False
            self.monitoring_chats.clear()
            
            logger.info("Stopping all active dashboards...")
            dashboard_count = len(self.active_dashboards)
            dashboard_users = list(self.active_dashboards.keys())
            for user_id in dashboard_users:
                await self.stop_dashboard(user_id)
            logger.info(f"Stopped {dashboard_count} dashboards")
            
            logger.info(f"Cancelling {active_tasks} monitoring tasks...")
            for chat_id, task in list(self.monitoring_tasks.items()):
                if not task.done():
                    task.cancel()
                    logger.debug(f"Cancelled monitoring task for chat {mask_user_id(chat_id)}")
            
            if self.monitoring_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*self.monitoring_tasks.values(), return_exceptions=True),
                        timeout=5
                    )
                    logger.info("All monitoring tasks cancelled")
                except asyncio.TimeoutError:
                    logger.warning("Some monitoring tasks did not complete within timeout")
                except Exception as e:
                    logger.error(f"Error during task cleanup: {e}")
            
            self.monitoring_tasks.clear()
            
            if self.position_tracker:
                logger.info("Clearing active positions from memory...")
                active_pos_count = sum(len(positions) for positions in self.position_tracker.active_positions.values())
                self.position_tracker.active_positions.clear()
                self.position_tracker.stop_monitoring()
                logger.info(f"Cleared {active_pos_count} positions from tracker")
            else:
                active_pos_count = 0
            
            if self.signal_session_manager:
                logger.info("Clearing all active signal sessions...")
                cleared_sessions = await self.signal_session_manager.clear_all_sessions(reason="system_reset")
                logger.info(f"Cleared {cleared_sessions} signal sessions")
            else:
                cleared_sessions = 0
            
            logger.info("Cleaning up pending charts...")
            pending_charts_count = len(self._pending_charts)
            await self._cleanup_all_pending_charts()
            logger.info(f"Cleaned up {pending_charts_count} pending charts")
            
            logger.info("Clearing signal cache...")
            signal_cache_count = len(self.sent_signals_cache)
            await self._clear_signal_cache()
            logger.info(f"Cleared {signal_cache_count} signal cache entries")
            
            logger.info("Clearing database records...")
            session = self.db.get_session()
            
            deleted_trades = session.query(Trade).delete()
            deleted_positions = session.query(Position).delete()
            deleted_performance = session.query(Performance).delete()
            
            session.commit()
            session.close()
            
            logger.info("=" * 60)
            logger.info("SYSTEM RESET COMPLETE")
            logger.info("=" * 60)
            
            msg = (
                "✅ *Reset Sistem Berhasil - Semua Dibersihkan!*\n\n"
                "*Database:*\n"
                f"• Trades dihapus: {deleted_trades}\n"
                f"• Positions dihapus: {deleted_positions}\n"
                f"• Performance dihapus: {deleted_performance}\n\n"
                "*Monitoring & Sinyal:*\n"
                f"• Monitoring dihentikan: {monitoring_count} chat\n"
                f"• Task dibatalkan: {active_tasks}\n"
                f"• Posisi aktif dihapus: {active_pos_count}\n"
                f"• Sesi sinyal dibersihkan: {cleared_sessions}\n\n"
                "*Cache & Charts:*\n"
                f"• Pending charts dibersihkan: {pending_charts_count}\n"
                f"• Signal cache dibersihkan: {signal_cache_count}\n\n"
                "✨ *Sistem sekarang bersih dan siap digunakan lagi!*\n"
                "Gunakan /monitor untuk mulai monitoring baru."
            )
            
            await update.message.reply_text(msg, parse_mode='Markdown')
            logger.info(f"Complete system reset by admin {mask_user_id(update.effective_user.id)}")
            
        except Exception as e:
            logger.error(f"Error resetting system: {e}")
            await update.message.reply_text("❌ Error reset sistem. Cek logs untuk detail.")
    
    async def initialize(self):
        if not self.config.TELEGRAM_BOT_TOKEN:
            logger.error("Telegram bot token not configured!")
            return False
        
        self.app = Application.builder().token(self.config.TELEGRAM_BOT_TOKEN).build()
        
        if self.signal_session_manager:
            self.signal_session_manager.register_event_handler('on_session_end', self._on_session_end_handler)
            logger.info("Registered dashboard cleanup handler for session end events")
        
        await self._integrate_chart_with_session_manager()
        
        if self.chart_generator:
            def on_chart_eviction_notify(user_id: int, chart_path: str, reason: str):
                """Notify chart generator when chart is evicted."""
                try:
                    if hasattr(self.chart_generator, '_pending_charts'):
                        self.chart_generator._pending_charts.discard(chart_path)
                except Exception as e:
                    logger.debug(f"Chart generator notification skipped: {e}")
            
            self.register_chart_eviction_callback(on_chart_eviction_notify)
            logger.info("Registered chart eviction callback for ChartGenerator integration")
        
        self.app.add_handler(CommandHandler("start", self.start_command))
        self.app.add_handler(CommandHandler("help", self.help_command))
        self.app.add_handler(CommandHandler("monitor", self.monitor_command))
        self.app.add_handler(CommandHandler("stopmonitor", self.stopmonitor_command))
        self.app.add_handler(CommandHandler("getsignal", self.getsignal_command))
        self.app.add_handler(CommandHandler("status", self.status_command))
        self.app.add_handler(CommandHandler("riwayat", self.riwayat_command))
        self.app.add_handler(CommandHandler("performa", self.performa_command))
        self.app.add_handler(CommandHandler("analytics", self.analytics_command))
        self.app.add_handler(CommandHandler("systemhealth", self.systemhealth_command))
        self.app.add_handler(CommandHandler("tasks", self.tasks_command))
        self.app.add_handler(CommandHandler("settings", self.settings_command))
        self.app.add_handler(CommandHandler("riset", self.riset_command))
        
        logger.info("Initializing Telegram bot...")
        await self.app.initialize()
        await self.app.start()
        logger.info("Telegram bot initialized and ready!")
        return True
    
    async def setup_webhook(self, webhook_url: str, max_retries: int = 3):
        if not self.app:
            logger.error("Bot not initialized! Call initialize() first.")
            return False
        
        if not webhook_url or not webhook_url.strip():
            logger.error("Invalid webhook URL provided - empty or None")
            return False
        
        webhook_url = webhook_url.strip()
        
        if not (webhook_url.startswith('http://') or webhook_url.startswith('https://')):
            logger.error(f"Invalid webhook URL format: {webhook_url[:50]}... (must start with http:// or https://)")
            return False
        
        is_https = webhook_url.startswith('https://')
        if not is_https:
            logger.warning("⚠️ Webhook URL uses HTTP instead of HTTPS - this may cause issues with Telegram")
        
        retry_delay = 2.0
        max_delay = 30.0
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Setting up webhook (attempt {attempt}/{max_retries}): {webhook_url}")
                
                await self.app.bot.set_webhook(
                    url=webhook_url,
                    allowed_updates=['message', 'callback_query', 'edited_message'],
                    drop_pending_updates=True
                )
                
                webhook_info = await self.app.bot.get_webhook_info()
                
                if webhook_info.url == webhook_url:
                    logger.info(f"✅ Webhook configured successfully!")
                    logger.info(f"Webhook URL: {webhook_info.url}")
                    logger.info(f"Pending updates: {webhook_info.pending_update_count}")
                    if webhook_info.last_error_message:
                        logger.warning(f"Previous webhook error: {webhook_info.last_error_message}")
                    return True
                else:
                    logger.warning(f"Webhook URL mismatch - Expected: {webhook_url}, Got: {webhook_info.url}")
                    if attempt < max_retries:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, max_delay)
                        continue
                    return False
            
            except asyncio.TimeoutError as e:
                logger.error(f"Timeout setting webhook (attempt {attempt}/{max_retries})")
                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, max_delay)
                else:
                    logger.error("❌ Webhook setup timed out. Check network connectivity.")
                    
            except ConnectionError as e:
                logger.error(f"Connection error setting webhook (attempt {attempt}/{max_retries}): {e}")
                logger.error("Possible causes:")
                logger.error("  - Server not reachable from internet")
                logger.error("  - Firewall blocking incoming connections")
                logger.error("  - DNS resolution failed")
                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, max_delay)
                    
            except TelegramError as e:
                error_msg = str(e).lower()
                logger.error(f"Telegram API error (attempt {attempt}/{max_retries}): {e}")
                
                if 'ssl' in error_msg or 'certificate' in error_msg:
                    logger.error("❌ SSL Certificate Error!")
                    logger.error("Possible solutions:")
                    logger.error("  - Ensure HTTPS URL has valid SSL certificate")
                    logger.error("  - Check if certificate is from trusted CA")
                    logger.error("  - Verify certificate chain is complete")
                    logger.error("  - Use a service like Let's Encrypt for free SSL")
                elif 'not found' in error_msg or 'resolve' in error_msg:
                    logger.error("❌ DNS Resolution Error!")
                    logger.error("Possible solutions:")
                    logger.error("  - Check if domain name is correct")
                    logger.error("  - Verify DNS records are propagated")
                    logger.error("  - Wait 5-10 minutes for DNS propagation")
                elif 'refused' in error_msg or 'connection' in error_msg:
                    logger.error("❌ Connection Refused!")
                    logger.error("Possible solutions:")
                    logger.error("  - Check if server is running and accessible")
                    logger.error("  - Verify firewall allows incoming HTTPS (port 443)")
                    logger.error("  - Ensure webhook endpoint is listening")
                
                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, max_delay)
                    
            except Exception as e:
                error_type = type(e).__name__
                logger.error(f"Failed to setup webhook (attempt {attempt}/{max_retries}): [{error_type}] {e}")
                
                if self.error_handler:
                    self.error_handler.log_exception(e, f"setup_webhook_attempt_{attempt}")
                
                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, max_delay)
                else:
                    logger.error("❌ All webhook setup attempts failed!")
                    logger.error("General troubleshooting:")
                    logger.error("  1. Webhook URL is publicly accessible")
                    logger.error("  2. SSL certificate is valid (for HTTPS)")
                    logger.error("  3. Telegram Bot API can reach your server")
                    logger.error("  4. No firewall blocking incoming connections")
                    logger.error("  5. Webhook endpoint is properly configured")
                    return False
        
        return False
    
    async def run_webhook(self):
        if not self.app:
            logger.error("Bot not initialized! Call initialize() first.")
            return
        
        logger.info("Telegram bot running in webhook mode...")
        logger.info("Bot is ready to receive webhook updates")
    
    async def process_update(self, update_data):
        if not self.app:
            logger.error("❌ Bot not initialized! Cannot process update.")
            logger.error("This usually means bot is running in limited mode")
            logger.error("Set TELEGRAM_BOT_TOKEN and AUTHORIZED_USER_IDS and restart")
            return
        
        if not update_data:
            logger.error("❌ Received empty update data")
            return
        
        try:
            from telegram import Update
            import json
            
            if isinstance(update_data, Update):
                update = update_data
                logger.info(f"📥 Received native telegram.Update object: {update.update_id}")
            else:
                parsed_data = update_data
                
                if isinstance(update_data, str):
                    try:
                        parsed_data = json.loads(update_data)
                        logger.debug("Parsed webhook update from JSON string")
                    except json.JSONDecodeError as e:
                        logger.error(f"❌ Failed to parse JSON string update: {e}")
                        return
                elif hasattr(update_data, 'to_dict') and callable(update_data.to_dict):
                    try:
                        parsed_data = update_data.to_dict()
                        logger.debug(f"Converted update data via to_dict(): {type(update_data)}")
                    except Exception as e:
                        logger.warning(f"Failed to convert via to_dict: {e}")
                elif not hasattr(update_data, '__getitem__'):
                    logger.warning(f"Update data is not dict-like: {type(update_data)}")
                    logger.debug(f"Attempting to use as-is: {str(update_data)[:200]}")
                
                update = Update.de_json(parsed_data, self.app.bot)
            
            if update:
                update_id = update.update_id
                
                message_info = ""
                if update.message:
                    message_info = f" from user {update.message.from_user.id}"
                    if update.message.text:
                        message_info += f": '{update.message.text}'"
                
                logger.info(f"🔄 Processing webhook update {update_id}{message_info}")
                
                await self.app.process_update(update)
                
                logger.info(f"✅ Successfully processed update {update_id}")
            else:
                logger.warning("⚠️ Received invalid or malformed update data")
                from collections.abc import Mapping
                if isinstance(parsed_data, Mapping):
                    logger.debug(f"Update data keys: {list(parsed_data.keys())}")
                
        except ValueError as e:
            logger.error(f"ValueError parsing update data: {e}")
            logger.debug(f"Problematic update data: {str(update_data)[:200]}...")
        except AttributeError as e:
            logger.error(f"AttributeError processing update: {e}")
            if self.error_handler:
                self.error_handler.log_exception(e, "process_webhook_update_attribute")
        except Exception as e:
            error_type = type(e).__name__
            logger.error(f"Unexpected error processing webhook update: [{error_type}] {e}")
            if self.error_handler:
                self.error_handler.log_exception(e, "process_webhook_update")
            
            if hasattr(e, '__traceback__'):
                import traceback
                tb_str = ''.join(traceback.format_tb(e.__traceback__)[:3])
                logger.debug(f"Traceback: {tb_str}")
    
    async def run(self):
        if not self.app:
            logger.error("Bot not initialized! Call initialize() first.")
            return
        
        if self.config.TELEGRAM_WEBHOOK_MODE:
            if not self.config.WEBHOOK_URL:
                logger.error("WEBHOOK_URL not configured! Cannot use webhook mode.")
                logger.error("Please set WEBHOOK_URL environment variable or disable webhook mode.")
                return
            
            webhook_set = await self.setup_webhook(self.config.WEBHOOK_URL)
            if not webhook_set:
                logger.error("Failed to setup webhook! Bot cannot start in webhook mode.")
                return
            
            await self.run_webhook()
        else:
            import os
            import fcntl
            
            if os.path.exists(self.instance_lock_file):
                try:
                    with open(self.instance_lock_file, 'r') as f:
                        pid_str = f.read().strip()
                        if pid_str.isdigit():
                            old_pid = int(pid_str)
                            
                            # Check if process is still running
                            try:
                                os.kill(old_pid, 0)
                                # Process exists
                                logger.error(f"🔴 CRITICAL: Another bot instance is RUNNING (PID: {old_pid})!")
                                logger.error("Multiple bot instances will cause 'Conflict: terminated by other getUpdates' errors!")
                                logger.error(f"Solutions:")
                                logger.error(f"  1. Kill the other instance: kill {old_pid}")
                                logger.error(f"  2. Use webhook mode instead: TELEGRAM_WEBHOOK_MODE=true")
                                logger.error(f"  3. Delete lock file if you're sure: rm {self.instance_lock_file}")
                                logger.error("Bot will continue but may not work properly!")
                            except OSError:
                                # Process doesn't exist (stale lock)
                                logger.warning(f"⚠️ Stale lock file detected (PID {old_pid} not running)")
                                logger.info(f"Removing stale lock file: {self.instance_lock_file}")
                                try:
                                    os.remove(self.instance_lock_file)
                                    logger.info("✅ Stale lock file removed successfully")
                                except Exception as remove_error:
                                    logger.error(f"Failed to remove stale lock: {remove_error}")
                        else:
                            logger.warning(f"Invalid PID in lock file: {pid_str}")
                            logger.info("Removing invalid lock file")
                            try:
                                os.remove(self.instance_lock_file)
                            except Exception:
                                pass
                except Exception as e:
                    logger.error(f"Error reading lock file: {e}")
                    logger.info("Attempting to remove potentially corrupted lock file")
                    try:
                        os.remove(self.instance_lock_file)
                    except Exception:
                        pass
            
            try:
                with open(self.instance_lock_file, 'w') as f:
                    f.write(str(os.getpid()))
                logger.info(f"✅ Bot instance lock created: PID {os.getpid()}")
            except Exception as e:
                logger.warning(f"Could not create instance lock: {e}")
            
            logger.info("Starting Telegram bot polling...")
            await self.app.updater.start_polling()
            logger.info("Telegram bot is running!")
    
    async def stop(self):
        logger.info("=" * 50)
        logger.info("STOPPING TELEGRAM BOT")
        logger.info("=" * 50)
        
        self._is_shutting_down = True
        
        import os
        if os.path.exists(self.instance_lock_file):
            try:
                os.remove(self.instance_lock_file)
                logger.info("✅ Bot instance lock removed")
            except OSError as e:
                logger.warning(f"Could not remove instance lock (OS error): {e}")
            except Exception as e:
                logger.warning(f"Could not remove instance lock: {type(e).__name__}: {e}")
        
        if not self.app:
            logger.warning("Bot app not initialized, nothing to stop")
            return
        
        await self.stop_background_cleanup_tasks()
        logger.info("✅ All background tasks and monitoring stopped")
        
        if self.config.TELEGRAM_WEBHOOK_MODE:
            logger.info("Deleting Telegram webhook...")
            try:
                await asyncio.wait_for(
                    self.app.bot.delete_webhook(drop_pending_updates=True),
                    timeout=5
                )
                logger.info("✅ Webhook deleted successfully")
            except asyncio.TimeoutError:
                logger.warning("Webhook deletion timed out after 5s")
            except Exception as e:
                logger.error(f"Error deleting webhook: {e}")
        else:
            logger.info("Stopping Telegram bot polling...")
            try:
                if self.app.updater and self.app.updater.running:
                    await asyncio.wait_for(
                        self.app.updater.stop(),
                        timeout=5
                    )
                    logger.info("✅ Telegram bot polling stopped")
            except asyncio.TimeoutError:
                logger.warning("Updater stop timed out after 5s")
            except Exception as e:
                logger.error(f"Error stopping updater: {e}")
        
        logger.info("Stopping Telegram application...")
        try:
            await asyncio.wait_for(self.app.stop(), timeout=5)
            logger.info("✅ Telegram application stopped")
        except asyncio.TimeoutError:
            logger.warning("App stop timed out after 5s")
        except Exception as e:
            logger.error(f"Error stopping app: {e}")
        
        logger.info("Shutting down Telegram application...")
        try:
            await asyncio.wait_for(self.app.shutdown(), timeout=5)
            logger.info("✅ Telegram application shutdown complete")
        except asyncio.TimeoutError:
            logger.warning("App shutdown timed out after 5s")
        except Exception as e:
            logger.error(f"Error shutting down app: {e}")
        
        logger.info("=" * 50)
        logger.info("TELEGRAM BOT STOPPED")
        logger.info("=" * 50)
