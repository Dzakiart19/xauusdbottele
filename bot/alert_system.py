import asyncio
import json
import os
import pickle
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from collections import deque
import time
import pytz
from bot.logger import setup_logger
from bot.database import Trade, Position
from bot.resilience import RateLimiter, CircuitBreaker

logger = setup_logger('AlertSystem')

MAX_QUEUE_SIZE = 100
MAX_HISTORY_SIZE = 100
SEND_TIMEOUT_SECONDS = 30
MAX_RETRY_ATTEMPTS = 3
BASE_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 60.0
HISTORY_CLEANUP_INTERVAL_SECONDS = 300
QUEUE_PERSISTENCE_PATH = "data/alert_queue_state.json"
RATE_LIMITER_STATE_PATH = "data/rate_limiter_state.json"
HISTORY_CLEANUP_BACKOFF_BASE = 60.0
HISTORY_CLEANUP_BACKOFF_MAX = 3600.0


class AlertPriority:
    """Alert priority levels for queue management"""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


CRITICAL_ALERT_TYPES = frozenset({
    "STOP_LOSS_HIT",
    "TAKE_PROFIT_HIT", 
    "SYSTEM_ERROR",
    "RISK_WARNING"
})


class AlertType:
    TRADE_ENTRY = "TRADE_ENTRY"
    TRADE_EXIT = "TRADE_EXIT"
    PRICE_ALERT = "PRICE_ALERT"
    STOP_LOSS_HIT = "STOP_LOSS_HIT"
    TAKE_PROFIT_HIT = "TAKE_PROFIT_HIT"
    DAILY_SUMMARY = "DAILY_SUMMARY"
    RISK_WARNING = "RISK_WARNING"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    MARKET_CLOSE = "MARKET_CLOSE"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"


class Alert:
    def __init__(self, alert_type: str, message: str, priority: str = "NORMAL",
                 data: Optional[Dict] = None):
        self.alert_type = alert_type
        self.message = message
        self.priority = priority
        self.data = data or {}
        self.timestamp = datetime.now(pytz.UTC)
        self.sent = False
        self.retry_count = 0
        self.last_error: Optional[str] = None
    
    def to_dict(self):
        return {
            'alert_type': self.alert_type,
            'message': self.message,
            'priority': self.priority,
            'data': self.data,
            'timestamp': self.timestamp.isoformat(),
            'sent': self.sent,
            'retry_count': self.retry_count,
            'last_error': self.last_error
        }


class AlertSystem:
    def __init__(self, config, db_manager):
        self.config = config
        self.db = db_manager
        self.alert_queue: list = []
        self.alert_history: deque = deque(maxlen=MAX_HISTORY_SIZE)
        self.telegram_app = None
        self.chat_ids = []
        self.enabled = True
        self.max_history = MAX_HISTORY_SIZE
        self.max_queue_size = MAX_QUEUE_SIZE
        self.send_message_callback = None
        
        self.rate_limiter = RateLimiter(
            max_calls=30,
            time_window=60.0,
            name="AlertRateLimiter"
        )
        
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60.0,
            expected_exception=Exception,
            name="AlertCircuitBreaker"
        )
        
        self._current_backoff = BASE_BACKOFF_SECONDS
        self._failed_sends = 0
        self._total_dropped = 0
        self._total_rejected = 0
        self._critical_dropped = 0
        self._last_history_cleanup = time.time()
        
        logger.info(f"Alert system initialized (max_queue={MAX_QUEUE_SIZE}, max_history={MAX_HISTORY_SIZE})")
    
    def set_telegram_app(self, app, chat_ids: List[int], send_message_callback=None):
        self.telegram_app = app
        self.chat_ids = chat_ids
        self.send_message_callback = send_message_callback
        logger.info(f"Telegram app set with {len(chat_ids)} chat IDs")
    
    def _get_priority_value(self, alert: Alert) -> int:
        """Get numeric priority for an alert (lower = higher priority)."""
        if alert.alert_type in CRITICAL_ALERT_TYPES:
            return AlertPriority.CRITICAL
        priority_map = {
            "HIGH": AlertPriority.HIGH,
            "NORMAL": AlertPriority.NORMAL,
            "LOW": AlertPriority.LOW,
            "CRITICAL": AlertPriority.CRITICAL
        }
        return priority_map.get(alert.priority, AlertPriority.NORMAL)
    
    def _is_critical_alert(self, alert: Alert) -> bool:
        """Check if an alert is critical and should never be dropped."""
        return (alert.alert_type in CRITICAL_ALERT_TYPES or 
                alert.priority == "CRITICAL" or
                self._get_priority_value(alert) == AlertPriority.CRITICAL)
    
    def _try_enqueue_alert(self, alert: Alert) -> tuple[bool, str]:
        """Try to enqueue an alert with priority-aware overflow handling.
        
        Returns:
            Tuple of (success, reason_if_failed)
        """
        is_critical = self._is_critical_alert(alert)
        
        if len(self.alert_queue) < self.max_queue_size:
            self.alert_queue.append(alert)
            return True, ""
        
        if is_critical:
            non_critical_idx = None
            for i in range(len(self.alert_queue) - 1, -1, -1):
                if not self._is_critical_alert(self.alert_queue[i]):
                    non_critical_idx = i
                    break
            
            if non_critical_idx is not None:
                dropped_alert = self.alert_queue.pop(non_critical_idx)
                self._total_dropped += 1
                logger.warning(
                    f"Queue full: Dropped LOW priority alert '{dropped_alert.alert_type}' "
                    f"to make room for CRITICAL alert '{alert.alert_type}'"
                )
                self.alert_queue.append(alert)
                return True, ""
            else:
                self._critical_dropped += 1
                logger.error(
                    f"CRITICAL ALERT DROPPED: Queue full of critical alerts. "
                    f"Alert type: {alert.alert_type}, Message: {alert.message[:100]}..."
                )
                return False, "Queue full of critical alerts"
        else:
            self._total_rejected += 1
            logger.warning(
                f"Alert REJECTED due to queue overflow: {alert.alert_type} "
                f"(queue_size={len(self.alert_queue)}, rejected_total={self._total_rejected})"
            )
            return False, "Queue at capacity, non-critical alert rejected"
    
    def _calculate_backoff(self) -> float:
        """Calculate exponential backoff delay."""
        backoff = min(
            BASE_BACKOFF_SECONDS * (2 ** self._failed_sends),
            MAX_BACKOFF_SECONDS
        )
        return backoff
    
    def _reset_backoff(self):
        """Reset backoff after successful send."""
        self._current_backoff = BASE_BACKOFF_SECONDS
        self._failed_sends = 0
    
    def _increment_backoff(self):
        """Increment backoff after failed send."""
        self._failed_sends += 1
        self._current_backoff = self._calculate_backoff()
        logger.warning(
            f"Send failed. Backoff increased to {self._current_backoff:.1f}s "
            f"(failures: {self._failed_sends})"
        )
    
    async def _cleanup_old_history(self):
        """Periodically cleanup old history entries."""
        current_time = time.time()
        if current_time - self._last_history_cleanup >= HISTORY_CLEANUP_INTERVAL_SECONDS:
            cutoff_time = datetime.now(pytz.UTC) - timedelta(hours=24)
            
            old_count = len(self.alert_history)
            new_history = deque(
                [a for a in self.alert_history if a.timestamp > cutoff_time],
                maxlen=MAX_HISTORY_SIZE
            )
            removed_count = old_count - len(new_history)
            
            if removed_count > 0:
                self.alert_history = new_history
                logger.info(f"Cleaned up {removed_count} old history entries")
            
            self._last_history_cleanup = current_time
    
    async def send_alert(self, alert: Alert) -> bool:
        """Send an alert. Returns True if alert was successfully queued.
        
        Args:
            alert: The alert to send
            
        Returns:
            True if alert was queued and processed, False if rejected
        """
        if not self.enabled:
            logger.info(f"Alert system disabled, skipping: {alert.alert_type}")
            return False
        
        success, reason = self._try_enqueue_alert(alert)
        if not success:
            logger.warning(f"Alert not queued: {reason}")
            return False
        
        logger.info(
            f"Alert queued: {alert.alert_type} - Queue size: {len(self.alert_queue)} "
            f"(critical={self._is_critical_alert(alert)})"
        )
        
        await self._cleanup_old_history()
        
        await self._process_alert(alert)
        return True
    
    async def _send_with_timeout(self, chat_id: int, formatted_msg: str) -> bool:
        """Send message with timeout and proper error handling."""
        try:
            if self.send_message_callback:
                await asyncio.wait_for(
                    self.send_message_callback(
                        chat_id=chat_id,
                        text=formatted_msg,
                        parse_mode='Markdown'
                    ),
                    timeout=SEND_TIMEOUT_SECONDS
                )
            elif self.telegram_app and self.telegram_app.bot:
                await asyncio.wait_for(
                    self.telegram_app.bot.send_message(
                        chat_id=chat_id,
                        text=formatted_msg,
                        parse_mode='Markdown'
                    ),
                    timeout=SEND_TIMEOUT_SECONDS
                )
            return True
        except asyncio.TimeoutError:
            logger.error(f"Timeout sending alert to chat {chat_id} after {SEND_TIMEOUT_SECONDS}s")
            return False
        except Exception as e:
            error_name = type(e).__name__
            if 'RetryAfter' in error_name or 'Flood' in error_name.lower():
                retry_after = getattr(e, 'retry_after', 30)
                logger.warning(
                    f"Telegram rate limit hit for chat {chat_id}. "
                    f"Retry after {retry_after}s"
                )
                await asyncio.sleep(retry_after)
                return await self._send_with_timeout(chat_id, formatted_msg)
            else:
                logger.error(f"Failed to send alert to chat {chat_id}: {e}")
                return False
    
    async def _process_alert(self, alert: Alert):
        try:
            if not await self.rate_limiter.acquire_async(wait=True):
                logger.warning("Rate limit exceeded, waiting before processing alert")
                await asyncio.sleep(self.rate_limiter.get_wait_time())
            
            if self._failed_sends > 0:
                backoff_time = self._calculate_backoff()
                logger.info(f"Applying backoff delay: {backoff_time:.1f}s")
                await asyncio.sleep(backoff_time)
            
            formatted_msg = self._format_alert_message(alert)
            
            all_sent = True
            
            if self.send_message_callback or (self.telegram_app and self.telegram_app.bot):
                for chat_id in self.chat_ids:
                    success = False
                    for attempt in range(MAX_RETRY_ATTEMPTS):
                        try:
                            success = await self._send_with_timeout(chat_id, formatted_msg)
                            if success:
                                logger.info(f"Alert sent to chat {chat_id}: {alert.alert_type}")
                                self._reset_backoff()
                                break
                            else:
                                alert.retry_count += 1
                                if attempt < MAX_RETRY_ATTEMPTS - 1:
                                    retry_delay = self._calculate_backoff()
                                    logger.info(
                                        f"Retrying send to chat {chat_id} "
                                        f"(attempt {attempt + 2}/{MAX_RETRY_ATTEMPTS}) "
                                        f"after {retry_delay:.1f}s"
                                    )
                                    await asyncio.sleep(retry_delay)
                                    self._increment_backoff()
                        except Exception as e:
                            alert.last_error = str(e)
                            alert.retry_count += 1
                            self._increment_backoff()
                            if attempt < MAX_RETRY_ATTEMPTS - 1:
                                retry_delay = self._calculate_backoff()
                                logger.warning(
                                    f"Send attempt {attempt + 1} failed for chat {chat_id}: {e}. "
                                    f"Retrying in {retry_delay:.1f}s"
                                )
                                await asyncio.sleep(retry_delay)
                            else:
                                logger.error(
                                    f"All {MAX_RETRY_ATTEMPTS} send attempts failed "
                                    f"for chat {chat_id}: {e}"
                                )
                                all_sent = False
            
            alert.sent = all_sent
            self.alert_history.append(alert)
            
            self._remove_processed_alert(alert)
            
        except Exception as e:
            logger.error(f"Error processing alert: {e}")
            alert.last_error = str(e)
            self._increment_backoff()
    
    def _remove_processed_alert(self, alert: Alert):
        """Remove processed alert from queue."""
        try:
            new_queue = [a for a in self.alert_queue if a is not alert]
            self.alert_queue = new_queue
        except Exception as e:
            logger.error(f"Error removing processed alert from queue: {e}")
    
    def _format_alert_message(self, alert: Alert) -> str:
        priority_emoji = {
            'LOW': '📘',
            'NORMAL': '📗',
            'HIGH': '📙',
            'CRITICAL': '📕'
        }
        
        emoji = priority_emoji.get(alert.priority, '📗')
        
        jakarta_tz = pytz.timezone('Asia/Jakarta')
        local_time = alert.timestamp.astimezone(jakarta_tz)
        time_str = local_time.strftime('%H:%M:%S WIB')
        
        msg = f"{emoji} *{alert.alert_type}*\n\n"
        msg += f"{alert.message}\n\n"
        msg += f"🕐 {time_str}"
        
        return msg
    
    async def send_trade_entry_alert(self, trade_data: Dict):
        message = (
            f"🚨 *SINYAL {trade_data['signal_type']}*\n\n"
            f"Entry: ${trade_data['entry_price']:.2f}\n"
            f"SL: ${trade_data['stop_loss']:.2f}\n"
            f"TP: ${trade_data['take_profit']:.2f}\n"
            f"Timeframe: {trade_data.get('timeframe', 'M1')}"
        )
        
        alert = Alert(
            alert_type=AlertType.TRADE_ENTRY,
            message=message,
            priority='HIGH',
            data=trade_data
        )
        
        await self.send_alert(alert)
    
    async def send_trade_exit_alert(self, trade_data: Dict, result: str):
        result_emoji = '✅' if result == 'WIN' else '❌'
        pl_value = trade_data.get('actual_pl', 0)
        
        message = (
            f"{result_emoji} *Trade {result}*\n\n"
            f"Type: {trade_data['signal_type']}\n"
            f"Entry: ${trade_data['entry_price']:.2f}\n"
            f"Exit: ${trade_data.get('exit_price', 0):.2f}\n"
            f"P/L: ${pl_value:.2f}"
        )
        
        priority = 'HIGH' if result == 'WIN' else 'NORMAL'
        
        alert = Alert(
            alert_type=AlertType.TRADE_EXIT,
            message=message,
            priority=priority,
            data=trade_data
        )
        
        await self.send_alert(alert)
    
    async def send_daily_summary(self):
        try:
            session = self.db.get_session()
            
            jakarta_tz = pytz.timezone('Asia/Jakarta')
            today = datetime.now(jakarta_tz).replace(hour=0, minute=0, second=0, microsecond=0)
            today_utc = today.astimezone(pytz.UTC)
            
            today_trades = session.query(Trade).filter(
                Trade.signal_time >= today_utc
            ).all()
            
            total_trades = len(today_trades)
            closed_trades = [t for t in today_trades if t.status == 'CLOSED']
            wins = len([t for t in closed_trades if t.result == 'WIN'])
            losses = len([t for t in closed_trades if t.result == 'LOSS'])
            
            total_pl = sum([t.actual_pl for t in closed_trades if t.actual_pl])
            win_rate = (wins / len(closed_trades) * 100) if closed_trades else 0
            
            message = (
                "📊 *Ringkasan Harian*\n\n"
                f"Total Sinyal: {total_trades}\n"
                f"Closed: {len(closed_trades)}\n"
                f"Wins: {wins} ✅\n"
                f"Losses: {losses} ❌\n"
                f"Win Rate: {win_rate:.1f}%\n"
                f"Total P/L: ${total_pl:.2f}"
            )
            
            session.close()
            
            alert = Alert(
                alert_type=AlertType.DAILY_SUMMARY,
                message=message,
                priority='NORMAL',
                data={'trades': total_trades, 'pl': total_pl}
            )
            
            await self.send_alert(alert)
            
        except Exception as e:
            logger.error(f"Error sending daily summary: {e}")
    
    async def send_risk_warning(self, warning_type: str, details: str):
        message = (
            f"⚠️ *Risk Warning: {warning_type}*\n\n"
            f"{details}"
        )
        
        alert = Alert(
            alert_type=AlertType.RISK_WARNING,
            message=message,
            priority='CRITICAL',
            data={'warning_type': warning_type, 'details': details}
        )
        
        await self.send_alert(alert)
    
    async def send_system_error(self, error_msg: str):
        message = f"❌ *System Error*\n\n{error_msg}"
        
        alert = Alert(
            alert_type=AlertType.SYSTEM_ERROR,
            message=message,
            priority='CRITICAL',
            data={'error': error_msg}
        )
        
        await self.send_alert(alert)
    
    async def send_price_alert(self, symbol: str, current_price: float, 
                              target_price: float, condition: str):
        message = (
            f"💰 *Price Alert: {symbol}*\n\n"
            f"Current: ${current_price:.2f}\n"
            f"Target: ${target_price:.2f}\n"
            f"Condition: {condition}"
        )
        
        alert = Alert(
            alert_type=AlertType.PRICE_ALERT,
            message=message,
            priority='NORMAL',
            data={'symbol': symbol, 'price': current_price}
        )
        
        await self.send_alert(alert)
    
    async def send_high_volatility_alert(self, symbol: str, volatility_level: float):
        message = (
            f"⚡ *High Volatility Detected*\n\n"
            f"Symbol: {symbol}\n"
            f"Volatility: {volatility_level:.2f}%\n"
            f"⚠️ Consider reducing position sizes"
        )
        
        alert = Alert(
            alert_type=AlertType.HIGH_VOLATILITY,
            message=message,
            priority='HIGH',
            data={'symbol': symbol, 'volatility': volatility_level}
        )
        
        await self.send_alert(alert)
    
    def get_alert_history(self, limit: int = 10) -> List[Dict]:
        recent_alerts = list(self.alert_history)[-limit:]
        return [alert.to_dict() for alert in recent_alerts]
    
    def clear_alert_queue(self):
        self.alert_queue.clear()
        logger.info("Alert queue cleared")
    
    def enable(self):
        self.enabled = True
        logger.info("Alert system enabled")
    
    def disable(self):
        self.enabled = False
        logger.info("Alert system disabled")
    
    def reset_circuit_breaker(self):
        """Reset circuit breaker manually."""
        self.circuit_breaker.reset()
        self._reset_backoff()
        logger.info("Circuit breaker and backoff reset")
    
    def get_stats(self) -> Dict:
        return {
            'total_alerts': len(self.alert_history),
            'queued_alerts': len(self.alert_queue),
            'max_queue_size': self.max_queue_size,
            'max_history_size': self.max_history,
            'enabled': self.enabled,
            'chat_ids_count': len(self.chat_ids),
            'failed_sends': self._failed_sends,
            'total_dropped': self._total_dropped,
            'current_backoff': self._current_backoff,
            'rate_limiter': self.rate_limiter.get_state(),
            'circuit_breaker': self.circuit_breaker.get_state()
        }
    
    def save_queue_state(self, filepath: Optional[str] = None) -> bool:
        """Persist queue and rate limiter state to file
        
        Args:
            filepath: Optional custom path, defaults to QUEUE_PERSISTENCE_PATH
            
        Returns:
            True if save successful, False otherwise
        """
        filepath = filepath or QUEUE_PERSISTENCE_PATH
        
        try:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            queue_data = []
            for alert in self.alert_queue:
                try:
                    queue_data.append(alert.to_dict())
                except Exception as e:
                    logger.warning(f"Could not serialize alert: {e}")
            
            state = {
                'version': 1,
                'timestamp': datetime.now(pytz.UTC).isoformat(),
                'queue': queue_data,
                'stats': {
                    'current_backoff': self._current_backoff,
                    'failed_sends': self._failed_sends,
                    'total_dropped': self._total_dropped,
                    'total_rejected': self._total_rejected,
                    'critical_dropped': self._critical_dropped,
                    'last_history_cleanup': self._last_history_cleanup,
                    'history_cleanup_failures': getattr(self, '_history_cleanup_failures', 0),
                },
                'rate_limiter_state': self._serialize_rate_limiter_state()
            }
            
            with open(filepath, 'w') as f:
                json.dump(state, f, indent=2, default=str)
            
            logger.info(
                f"Queue state saved: {len(queue_data)} alerts, "
                f"backoff={self._current_backoff:.1f}s"
            )
            return True
            
        except PermissionError as e:
            logger.error(f"Permission denied saving queue state to {filepath}: {e}")
            return False
        except OSError as e:
            logger.error(f"OS error saving queue state to {filepath}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error saving queue state: {type(e).__name__}: {e}")
            return False
    
    def restore_queue_state(self, filepath: Optional[str] = None) -> bool:
        """Restore queue and rate limiter state from file
        
        Args:
            filepath: Optional custom path, defaults to QUEUE_PERSISTENCE_PATH
            
        Returns:
            True if restore successful, False otherwise
        """
        filepath = filepath or QUEUE_PERSISTENCE_PATH
        
        try:
            if not os.path.exists(filepath):
                logger.info(f"No queue state file found at {filepath}")
                return False
            
            with open(filepath, 'r') as f:
                state = json.load(f)
            
            version = state.get('version', 0)
            if version != 1:
                logger.warning(f"Unknown queue state version: {version}")
            
            saved_time_str = state.get('timestamp')
            if saved_time_str:
                try:
                    saved_time = datetime.fromisoformat(saved_time_str.replace('Z', '+00:00'))
                    age_seconds = (datetime.now(pytz.UTC) - saved_time).total_seconds()
                    if age_seconds > 3600:
                        logger.warning(
                            f"Queue state is {age_seconds/3600:.1f} hours old, "
                            "some alerts may be stale"
                        )
                except Exception:
                    pass
            
            restored_count = 0
            queue_data = state.get('queue', [])
            for alert_dict in queue_data:
                try:
                    alert = Alert(
                        alert_type=alert_dict.get('alert_type', 'UNKNOWN'),
                        message=alert_dict.get('message', ''),
                        priority=alert_dict.get('priority', 'NORMAL'),
                        data=alert_dict.get('data', {})
                    )
                    
                    timestamp_str = alert_dict.get('timestamp')
                    if timestamp_str:
                        try:
                            alert.timestamp = datetime.fromisoformat(
                                timestamp_str.replace('Z', '+00:00')
                            )
                        except Exception:
                            pass
                    
                    alert.sent = alert_dict.get('sent', False)
                    alert.retry_count = alert_dict.get('retry_count', 0)
                    alert.last_error = alert_dict.get('last_error')
                    
                    if not alert.sent:
                        self.alert_queue.append(alert)
                        restored_count += 1
                        
                except Exception as e:
                    logger.warning(f"Could not restore alert: {e}")
            
            stats = state.get('stats', {})
            self._current_backoff = stats.get('current_backoff', BASE_BACKOFF_SECONDS)
            self._failed_sends = stats.get('failed_sends', 0)
            self._total_dropped = stats.get('total_dropped', 0)
            self._total_rejected = stats.get('total_rejected', 0)
            self._critical_dropped = stats.get('critical_dropped', 0)
            self._last_history_cleanup = stats.get('last_history_cleanup', time.time())
            self._history_cleanup_failures = stats.get('history_cleanup_failures', 0)
            
            rate_limiter_state = state.get('rate_limiter_state', {})
            self._restore_rate_limiter_state(rate_limiter_state)
            
            logger.info(
                f"Queue state restored: {restored_count} alerts, "
                f"backoff={self._current_backoff:.1f}s"
            )
            return True
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in queue state file: {e}")
            return False
        except PermissionError as e:
            logger.error(f"Permission denied reading queue state from {filepath}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error restoring queue state: {type(e).__name__}: {e}")
            return False
    
    def _serialize_rate_limiter_state(self) -> Dict[str, Any]:
        """Serialize rate limiter state for persistence"""
        try:
            state = self.rate_limiter.get_state()
            call_times = self.rate_limiter.get_call_times()
            state['calls'] = [
                ts.isoformat() if isinstance(ts, datetime) else str(ts)
                for ts in call_times
            ]
            return state
        except Exception as e:
            logger.warning(f"Could not serialize rate limiter state: {e}")
            return {}
    
    def _restore_rate_limiter_state(self, state: Dict[str, Any]):
        """Restore rate limiter state from persisted data"""
        try:
            if not state:
                return
            
            if 'calls' in state:
                restored_calls = []
                time_window = self.rate_limiter.get_time_window()
                for ts_str in state.get('calls', []):
                    try:
                        dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                        if (datetime.now(pytz.UTC) - dt).total_seconds() < time_window:
                            restored_calls.append(dt)
                    except Exception:
                        pass
                self.rate_limiter.set_call_times(restored_calls)
                logger.debug(f"Restored {len(restored_calls)} rate limiter calls")
                
        except Exception as e:
            logger.warning(f"Could not restore rate limiter state: {e}")
    
    async def periodic_history_cleanup_with_backoff(self):
        """Run periodic history cleanup with exponential backoff on failures"""
        cleanup_failures = getattr(self, '_history_cleanup_failures', 0)
        
        while True:
            try:
                cleanup_interval = HISTORY_CLEANUP_INTERVAL_SECONDS
                if cleanup_failures > 0:
                    backoff = min(
                        HISTORY_CLEANUP_BACKOFF_BASE * (2 ** cleanup_failures),
                        HISTORY_CLEANUP_BACKOFF_MAX
                    )
                    cleanup_interval = max(cleanup_interval, backoff)
                    logger.info(
                        f"History cleanup using backoff: {cleanup_interval:.0f}s "
                        f"(failures: {cleanup_failures})"
                    )
                
                await asyncio.sleep(cleanup_interval)
                
                await self._cleanup_old_history()
                
                self.save_queue_state()
                
                cleanup_failures = 0
                self._history_cleanup_failures = 0
                
            except asyncio.CancelledError:
                logger.info("History cleanup task cancelled")
                self.save_queue_state()
                raise
            except Exception as e:
                cleanup_failures += 1
                self._history_cleanup_failures = cleanup_failures
                logger.error(
                    f"History cleanup failed (attempt {cleanup_failures}): "
                    f"{type(e).__name__}: {e}"
                )
    
    def start_periodic_cleanup(self) -> asyncio.Task:
        """Start the periodic history cleanup background task
        
        Returns:
            The created asyncio Task
        """
        task = asyncio.create_task(self.periodic_history_cleanup_with_backoff())
        logger.info("Started periodic history cleanup task")
        return task
