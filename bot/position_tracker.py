import asyncio
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, Tuple, Set, Callable, Any, List
import pytz
from telegram.error import TimedOut
from bot.logger import setup_logger
from bot.database import Position, Trade
from bot.signal_session_manager import SignalSessionManager

logger = setup_logger('PositionTracker')

DEFAULT_OPERATION_TIMEOUT = 30.0
NOTIFICATION_TIMEOUT = 10.0
FALLBACK_NOTIFICATION_TIMEOUT = 5.0
TICK_QUEUE_TIMEOUT = 5.0
TASK_CLEANUP_TIMEOUT = 10.0
LONG_RUNNING_TASK_THRESHOLD = 60.0
TASK_AUTO_CANCEL_THRESHOLD = 300.0
SLOW_TASK_ALERT_COUNT = 3
TASK_MONITOR_INTERVAL = 30.0
MAX_EXCEPTION_HISTORY = 100


@dataclass
class TaskExceptionRecord:
    """Record of a task exception for debugging"""
    task_name: str
    exception_type: str
    exception_message: str
    traceback_str: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(pytz.UTC))


@dataclass
class TaskCompletionResult:
    """Result of wait_for_completion with detailed information"""
    completed: bool
    total_tasks: int
    completed_tasks: int
    pending_tasks: List[str]
    timed_out_tasks: List[str]
    cancelled_tasks: List[str]
    error_tasks: List[str]
    elapsed_time: float = 0.0

class PositionError(Exception):
    """Base exception for position tracking errors"""
    pass

class ValidationError(PositionError):
    """Position data validation error"""
    pass

def validate_position_data(user_id: int, trade_id: int, signal_type: str,
                          entry_price: float, stop_loss: float, take_profit: float) -> Tuple[bool, Optional[str]]:
    """Validate position data before processing"""
    try:
        if user_id is None or user_id <= 0:
            return False, f"Invalid user_id: {user_id}"
        
        if trade_id is None or trade_id <= 0:
            return False, f"Invalid trade_id: {trade_id}"
        
        if signal_type not in ['BUY', 'SELL']:
            return False, f"Invalid signal_type: {signal_type}. Must be 'BUY' or 'SELL'"
        
        if entry_price is None or entry_price <= 0:
            return False, f"Invalid entry_price: {entry_price}"
        
        if stop_loss is None or stop_loss <= 0:
            return False, f"Invalid stop_loss: {stop_loss}"
        
        if take_profit is None or take_profit <= 0:
            return False, f"Invalid take_profit: {take_profit}"
        
        if signal_type == 'BUY':
            if stop_loss >= entry_price:
                return False, f"BUY: stop_loss ({stop_loss}) must be < entry_price ({entry_price})"
            if take_profit <= entry_price:
                return False, f"BUY: take_profit ({take_profit}) must be > entry_price ({entry_price})"
        else:
            if stop_loss <= entry_price:
                return False, f"SELL: stop_loss ({stop_loss}) must be > entry_price ({entry_price})"
            if take_profit >= entry_price:
                return False, f"SELL: take_profit ({take_profit}) must be < entry_price ({entry_price})"
        
        sl_distance = abs(entry_price - stop_loss)
        tp_distance = abs(entry_price - take_profit)
        
        if sl_distance < 0.10:
            return False, f"SL distance too small: ${sl_distance:.2f}"
        
        if tp_distance < 0.10:
            return False, f"TP distance too small: ${tp_distance:.2f}"
        
        return True, None
        
    except Exception as e:
        return False, f"Validation error: {str(e)}"

class PositionTracker:
    """Position tracker with async task lifecycle management.
    
    Task Lifecycle Tracking:
    - All spawned tasks are registered in _pending_tasks set
    - Done callbacks automatically remove completed tasks and drain exceptions
    - wait_for_completion() allows waiting for all tasks to finish
    - SignalSessionManager states are resolved on task completion
    """
    MAX_SLIPPAGE_PIPS = 5.0
    
    def __init__(self, config, db_manager, risk_manager, alert_system=None, user_manager=None, 
                 chart_generator=None, market_data=None, telegram_app=None, signal_session_manager=None):
        self.config = config
        self.db = db_manager
        self.risk_manager = risk_manager
        self.alert_system = alert_system
        self.user_manager = user_manager
        self.chart_generator = chart_generator
        self.market_data = market_data
        self.telegram_app = telegram_app
        self.signal_session_manager = signal_session_manager
        self.active_positions = {}
        self.monitoring = False
        
        self._position_lock = asyncio.Lock()
        self._pending_tasks: Set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        self._completion_event = asyncio.Event()
        self._monitoring_task: Optional[asyncio.Task] = None
        self._task_monitor_task: Optional[asyncio.Task] = None
        self._task_callbacks: Dict[str, Callable] = {}
        self._task_results: Dict[str, Any] = {}
        
        self._task_start_times: Dict[str, datetime] = {}
        self._exception_history: List[TaskExceptionRecord] = []
        self._slow_task_counts: Dict[str, int] = defaultdict(int)
        self._cancelled_task_names: Set[str] = set()
        
        self.long_running_threshold = LONG_RUNNING_TASK_THRESHOLD
        self.auto_cancel_threshold = TASK_AUTO_CANCEL_THRESHOLD
        self.slow_task_alert_count = SLOW_TASK_ALERT_COUNT
    
    def _on_task_done(self, task: asyncio.Task, task_name: str = None) -> None:
        """Callback invoked when a tracked task completes.
        
        Handles:
        - Exception draining and logging with full traceback
        - Task removal from pending set
        - Exception history tracking for debugging
        - InvalidStateError graceful handling
        - SignalSessionManager state resolution if applicable
        - Custom callback execution
        """
        effective_name = task_name or task.get_name()
        
        try:
            self._pending_tasks.discard(task)
            
            if effective_name in self._task_start_times:
                start_time = self._task_start_times.pop(effective_name)
                elapsed = (datetime.now(pytz.UTC) - start_time).total_seconds()
                if elapsed > self.long_running_threshold:
                    logger.warning(f"⏱️ Task {effective_name} took {elapsed:.2f}s to complete (threshold: {self.long_running_threshold}s)")
                    self._slow_task_counts[effective_name] += 1
        except Exception as cleanup_err:
            logger.debug(f"Error during task cleanup: {cleanup_err}")
        
        try:
            if task.cancelled():
                self._cancelled_task_names.add(effective_name)
                logger.debug(f"Task {effective_name} was cancelled")
                self._cleanup_task_state(effective_name)
                return
        except asyncio.InvalidStateError as e:
            logger.debug(f"InvalidStateError checking cancelled state for {effective_name}: {e}")
            self._cleanup_task_state(effective_name)
            return
        
        exception = None
        try:
            exception = task.exception()
        except asyncio.CancelledError:
            self._cancelled_task_names.add(effective_name)
            logger.debug(f"Task {effective_name} was cancelled (detected via CancelledError)")
            self._cleanup_task_state(effective_name)
            return
        except asyncio.InvalidStateError as e:
            logger.debug(f"InvalidStateError getting exception for {effective_name}: {e}")
            self._cleanup_task_state(effective_name)
            return
        
        if exception:
            self._record_exception(effective_name, exception)
            
            tb_str = "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
            logger.error(
                f"Task {effective_name} raised exception: {type(exception).__name__}: {exception}\n"
                f"Traceback:\n{tb_str}"
            )
            
            if self.alert_system:
                try:
                    asyncio.create_task(
                        self.alert_system.send_system_error(
                            f"⚠️ Task Error: {effective_name}\n"
                            f"Exception: {type(exception).__name__}: {str(exception)}\n"
                            f"Check logs for full traceback"
                        )
                    )
                except Exception as alert_err:
                    logger.error(f"Failed to send task error alert: {alert_err}")
        else:
            try:
                result = task.result()
                if task_name:
                    self._task_results[task_name] = result
                logger.debug(f"Task {effective_name} completed successfully")
            except asyncio.CancelledError:
                self._cancelled_task_names.add(effective_name)
                logger.debug(f"Task {effective_name} was cancelled (detected via result())")
            except asyncio.InvalidStateError as e:
                logger.debug(f"InvalidStateError getting result for {effective_name}: {e}")
            except Exception as result_err:
                logger.error(f"Error getting result for {effective_name}: {result_err}")
        
        if task_name and task_name in self._task_callbacks:
            try:
                callback = self._task_callbacks.pop(task_name)
                callback(task)
            except Exception as cb_err:
                logger.error(f"Task callback error for {task_name}: {cb_err}")
        
        self._cleanup_task_state(effective_name)
        
        if len(self._pending_tasks) == 0:
            self._completion_event.set()
    
    def _record_exception(self, task_name: str, exception: Exception) -> None:
        """Record exception in history for debugging"""
        try:
            tb_str = "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
            record = TaskExceptionRecord(
                task_name=task_name,
                exception_type=type(exception).__name__,
                exception_message=str(exception),
                traceback_str=tb_str
            )
            self._exception_history.append(record)
            
            if len(self._exception_history) > MAX_EXCEPTION_HISTORY:
                self._exception_history = self._exception_history[-MAX_EXCEPTION_HISTORY:]
        except Exception as e:
            logger.debug(f"Error recording exception: {e}")
    
    def _cleanup_task_state(self, task_name: str) -> None:
        """Clean up task-related state"""
        self._task_start_times.pop(task_name, None)
        self._task_callbacks.pop(task_name, None)
    
    def get_exception_history(self, limit: int = 20) -> List[TaskExceptionRecord]:
        """Get recent exception history for debugging"""
        return self._exception_history[-limit:]
    
    def get_slow_task_stats(self) -> Dict[str, int]:
        """Get statistics on slow tasks"""
        return dict(self._slow_task_counts)
    
    def _create_tracked_task(self, coro, name: str = None, 
                             on_complete: Callable = None,
                             resolve_session_user_id: int = None,
                             timeout: float = None) -> asyncio.Task:
        """Create a task and track it for proper cleanup on shutdown.
        
        Args:
            coro: Coroutine to execute
            name: Optional task name for identification
            on_complete: Optional callback when task completes
            resolve_session_user_id: If set, resolve SignalSessionManager for this user on completion
            timeout: Optional task timeout in seconds (auto-cancels if exceeded)
            
        Returns:
            asyncio.Task: The created and tracked task
        """
        effective_name = name or f"task_{id(coro)}"
        
        if timeout:
            async def wrapped_coro():
                try:
                    return await asyncio.wait_for(coro, timeout=timeout)
                except asyncio.TimeoutError:
                    logger.warning(f"⏱️ Task {effective_name} timed out after {timeout}s")
                    raise
            task = asyncio.create_task(wrapped_coro(), name=effective_name)
        else:
            task = asyncio.create_task(coro, name=effective_name)
        
        self._pending_tasks.add(task)
        self._completion_event.clear()
        
        self._task_start_times[effective_name] = datetime.now(pytz.UTC)
        
        if on_complete:
            self._task_callbacks[effective_name] = on_complete
        
        def done_callback(t: asyncio.Task):
            self._on_task_done(t, effective_name)
            
            if resolve_session_user_id and self.signal_session_manager:
                try:
                    if not t.cancelled():
                        exc = None
                        try:
                            exc = t.exception()
                        except (asyncio.CancelledError, asyncio.InvalidStateError):
                            pass
                        if exc is None:
                            asyncio.create_task(
                                self._resolve_session_state(resolve_session_user_id)
                            )
                except Exception as e:
                    logger.error(f"Error resolving session state: {e}")
        
        task.add_done_callback(done_callback)
        return task
    
    async def _resolve_session_state(self, user_id: int) -> None:
        """Resolve SignalSessionManager state after task completion."""
        if not self.signal_session_manager:
            return
        
        try:
            async with self._position_lock:
                has_active = user_id in self.active_positions and len(self.active_positions[user_id]) > 0
            
            if not has_active:
                session = self.signal_session_manager.get_session(user_id)
                if session and session.get('status') == 'monitoring':
                    logger.info(f"Resolving orphaned session state for user {user_id}")
                    await self.signal_session_manager.end_session(user_id, 'RESOLVED')
        except Exception as e:
            logger.error(f"Error resolving session state for user {user_id}: {e}")
    
    async def wait_for_completion(self, timeout: float = None, 
                                   progress_interval: float = 5.0) -> TaskCompletionResult:
        """Wait for all pending tasks to complete with detailed progress tracking.
        
        Args:
            timeout: Maximum time to wait in seconds. None means wait indefinitely.
            progress_interval: Interval for logging progress updates.
            
        Returns:
            TaskCompletionResult: Detailed result with completion status and task information
        """
        start_time = datetime.now(pytz.UTC)
        initial_count = len(self._pending_tasks)
        
        if not self._pending_tasks:
            return TaskCompletionResult(
                completed=True,
                total_tasks=0,
                completed_tasks=0,
                pending_tasks=[],
                timed_out_tasks=[],
                cancelled_tasks=[],
                error_tasks=[],
                elapsed_time=0.0
            )
        
        logger.info(f"📊 Waiting for {initial_count} pending tasks to complete...")
        pending_names_initial = self.get_pending_task_names()
        
        async def progress_reporter():
            """Report progress at intervals"""
            last_count = initial_count
            while True:
                await asyncio.sleep(progress_interval)
                current_count = len(self._pending_tasks)
                if current_count != last_count:
                    completed = initial_count - current_count
                    logger.info(f"📊 Progress: {completed}/{initial_count} tasks completed, {current_count} remaining")
                    if current_count > 0:
                        remaining = self.get_pending_task_names()[:5]
                        logger.debug(f"   Remaining tasks: {remaining}{'...' if len(self._pending_tasks) > 5 else ''}")
                    last_count = current_count
        
        progress_task = asyncio.create_task(progress_reporter())
        
        completed_ok = False
        timed_out_tasks = []
        error_tasks = []
        
        try:
            if timeout:
                try:
                    await asyncio.wait_for(self._completion_event.wait(), timeout=timeout)
                    completed_ok = True
                except asyncio.TimeoutError:
                    timed_out_tasks = self.get_pending_task_names()
                    logger.warning(
                        f"⏰ Timeout after {timeout}s waiting for task completion. "
                        f"{len(self._pending_tasks)} tasks still pending: {timed_out_tasks[:10]}{'...' if len(timed_out_tasks) > 10 else ''}"
                    )
            else:
                await self._completion_event.wait()
                completed_ok = True
        finally:
            progress_task.cancel()
            try:
                await progress_task
            except asyncio.CancelledError:
                pass
        
        elapsed = (datetime.now(pytz.UTC) - start_time).total_seconds()
        pending_final = self.get_pending_task_names()
        cancelled = list(self._cancelled_task_names)
        
        error_tasks = [record.task_name for record in self._exception_history 
                      if record.timestamp >= start_time]
        
        completed_count = initial_count - len(pending_final)
        
        result = TaskCompletionResult(
            completed=completed_ok and len(pending_final) == 0,
            total_tasks=initial_count,
            completed_tasks=completed_count,
            pending_tasks=pending_final,
            timed_out_tasks=timed_out_tasks,
            cancelled_tasks=cancelled,
            error_tasks=error_tasks,
            elapsed_time=elapsed
        )
        
        if result.completed:
            logger.info(f"✅ All {initial_count} tasks completed in {elapsed:.2f}s")
        else:
            logger.warning(
                f"⚠️ Partial completion: {completed_count}/{initial_count} tasks completed in {elapsed:.2f}s. "
                f"Pending: {len(pending_final)}, Errors: {len(error_tasks)}"
            )
        
        return result
    
    def get_pending_task_count(self) -> int:
        """Get the number of pending tasks."""
        return len(self._pending_tasks)
    
    def get_pending_task_names(self) -> list:
        """Get names of all pending tasks."""
        return [t.get_name() for t in self._pending_tasks]
    
    def get_task_ages(self) -> Dict[str, float]:
        """Get age in seconds for each running task"""
        now = datetime.now(pytz.UTC)
        ages = {}
        for task_name, start_time in self._task_start_times.items():
            ages[task_name] = (now - start_time).total_seconds()
        return ages
    
    async def _monitor_task_timeouts(self) -> None:
        """Background task to monitor for long-running and stuck tasks.
        
        - Logs warnings for tasks exceeding long_running_threshold
        - Auto-cancels tasks exceeding auto_cancel_threshold
        - Sends alerts for recurring slow tasks
        """
        logger.info(f"⏱️ Task timeout monitor started (warning: {self.long_running_threshold}s, auto-cancel: {self.auto_cancel_threshold}s)")
        
        try:
            while not self._shutdown_event.is_set():
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=TASK_MONITOR_INTERVAL
                    )
                    break
                except asyncio.TimeoutError:
                    pass
                
                if not self._pending_tasks:
                    continue
                
                now = datetime.now(pytz.UTC)
                tasks_to_cancel = []
                
                for task in list(self._pending_tasks):
                    if task.done():
                        continue
                    
                    task_name = task.get_name()
                    start_time = self._task_start_times.get(task_name)
                    
                    if not start_time:
                        continue
                    
                    age = (now - start_time).total_seconds()
                    
                    if age >= self.auto_cancel_threshold:
                        logger.warning(
                            f"🚨 Task {task_name} exceeded auto-cancel threshold "
                            f"({age:.1f}s > {self.auto_cancel_threshold}s) - scheduling cancellation"
                        )
                        tasks_to_cancel.append((task, task_name, age))
                        
                    elif age >= self.long_running_threshold:
                        slow_count = self._slow_task_counts.get(task_name, 0)
                        
                        if slow_count == 0:
                            logger.warning(
                                f"⏱️ Task {task_name} is running long ({age:.1f}s > {self.long_running_threshold}s)"
                            )
                        
                        if slow_count >= self.slow_task_alert_count:
                            if slow_count == self.slow_task_alert_count:
                                logger.error(
                                    f"🔥 Recurring slow task detected: {task_name} "
                                    f"(slow {slow_count} times)"
                                )
                                if self.alert_system:
                                    try:
                                        asyncio.create_task(
                                            self.alert_system.send_system_error(
                                                f"🔥 Recurring Slow Task Alert\n\n"
                                                f"Task: {task_name}\n"
                                                f"Slow count: {slow_count} times\n"
                                                f"Current age: {age:.1f}s\n"
                                                f"Threshold: {self.long_running_threshold}s\n\n"
                                                f"Consider investigating this task for performance issues"
                                            )
                                        )
                                    except Exception as e:
                                        logger.error(f"Failed to send slow task alert: {e}")
                
                for task, task_name, age in tasks_to_cancel:
                    try:
                        logger.warning(f"🛑 Auto-cancelling stuck task: {task_name} (age: {age:.1f}s)")
                        task.cancel()
                        
                        try:
                            await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
                        except asyncio.TimeoutError:
                            logger.error(f"❌ Task {task_name} did not respond to auto-cancel")
                        except asyncio.CancelledError:
                            logger.info(f"✓ Task {task_name} auto-cancelled successfully")
                            
                    except Exception as e:
                        logger.error(f"Error auto-cancelling task {task_name}: {e}")
                        
        except asyncio.CancelledError:
            logger.info("Task timeout monitor cancelled")
        except Exception as e:
            logger.error(f"Error in task timeout monitor: {e}")
        finally:
            logger.info("Task timeout monitor stopped")
    
    def start_task_monitor(self) -> asyncio.Task:
        """Start the background task timeout monitor.
        
        Returns:
            asyncio.Task: The monitor task
        """
        if self._task_monitor_task and not self._task_monitor_task.done():
            logger.warning("Task monitor already running")
            return self._task_monitor_task
        
        self._task_monitor_task = asyncio.create_task(
            self._monitor_task_timeouts(),
            name="task_timeout_monitor"
        )
        return self._task_monitor_task
    
    async def stop_task_monitor(self) -> None:
        """Stop the task timeout monitor"""
        if self._task_monitor_task and not self._task_monitor_task.done():
            self._task_monitor_task.cancel()
            try:
                await self._task_monitor_task
            except asyncio.CancelledError:
                pass
            self._task_monitor_task = None
            logger.info("Task timeout monitor stopped")
    
    def configure_timeouts(self, long_running_threshold: float = None,
                           auto_cancel_threshold: float = None,
                           slow_task_alert_count: int = None) -> None:
        """Configure task timeout thresholds.
        
        Args:
            long_running_threshold: Seconds before logging warning for slow tasks
            auto_cancel_threshold: Seconds before auto-cancelling stuck tasks
            slow_task_alert_count: Number of slow occurrences before alerting
        """
        if long_running_threshold is not None:
            self.long_running_threshold = long_running_threshold
            logger.info(f"Long running threshold set to {long_running_threshold}s")
        
        if auto_cancel_threshold is not None:
            self.auto_cancel_threshold = auto_cancel_threshold
            logger.info(f"Auto-cancel threshold set to {auto_cancel_threshold}s")
        
        if slow_task_alert_count is not None:
            self.slow_task_alert_count = slow_task_alert_count
            logger.info(f"Slow task alert count set to {slow_task_alert_count}")
    
    def get_task_monitoring_stats(self) -> Dict[str, Any]:
        """Get current task monitoring statistics.
        
        Returns:
            Dict with current monitoring statistics
        """
        ages = self.get_task_ages()
        
        return {
            "pending_count": len(self._pending_tasks),
            "pending_tasks": self.get_pending_task_names(),
            "task_ages": ages,
            "slow_task_counts": dict(self._slow_task_counts),
            "exception_count": len(self._exception_history),
            "recent_exceptions": [
                {"task": r.task_name, "type": r.exception_type, "time": r.timestamp.isoformat()}
                for r in self._exception_history[-5:]
            ],
            "cancelled_tasks": list(self._cancelled_task_names),
            "thresholds": {
                "long_running": self.long_running_threshold,
                "auto_cancel": self.auto_cancel_threshold,
                "slow_alert_count": self.slow_task_alert_count
            },
            "monitor_running": self._task_monitor_task is not None and not self._task_monitor_task.done()
        }
    
    async def _cancel_pending_tasks(self, timeout: float = TASK_CLEANUP_TIMEOUT,
                                     per_task_timeout: float = 2.0,
                                     force_cancel_on_timeout: bool = True) -> Dict[str, str]:
        """Cancel all pending tasks with proper cleanup and detailed logging.
        
        Args:
            timeout: Maximum total time to wait for all task cancellations
            per_task_timeout: Timeout for individual task cancellation attempts
            force_cancel_on_timeout: Whether to force cancel tasks that don't respond
            
        Returns:
            Dict mapping task names to their final status (cancelled, timeout, error, etc.)
        """
        cancellation_results: Dict[str, str] = {}
        
        if not self._pending_tasks:
            logger.info("No pending tasks to cancel")
            return cancellation_results
        
        task_count = len(self._pending_tasks)
        logger.info(f"🛑 Initiating cancellation of {task_count} pending tasks (timeout: {timeout}s)...")
        
        tasks_to_cancel = list(self._pending_tasks)
        task_names = {task: task.get_name() for task in tasks_to_cancel}
        
        uncancellable_tasks: List[str] = []
        
        for task in tasks_to_cancel:
            task_name = task_names[task]
            if not task.done():
                try:
                    task.cancel()
                    logger.debug(f"  Cancellation requested for: {task_name}")
                except Exception as e:
                    logger.warning(f"  ⚠️ Failed to request cancellation for {task_name}: {e}")
                    uncancellable_tasks.append(task_name)
                    cancellation_results[task_name] = f"cancel_request_failed: {e}"
        
        if uncancellable_tasks:
            logger.warning(f"⚠️ {len(uncancellable_tasks)} tasks could not be cancelled: {uncancellable_tasks}")
        
        if tasks_to_cancel:
            try:
                done, pending = await asyncio.wait(
                    tasks_to_cancel,
                    timeout=timeout,
                    return_when=asyncio.ALL_COMPLETED
                )
                
                for task in done:
                    task_name = task_names[task]
                    try:
                        if task.cancelled():
                            cancellation_results[task_name] = "cancelled"
                            logger.debug(f"  ✓ {task_name}: cancelled successfully")
                        else:
                            try:
                                exc = task.exception()
                                if exc:
                                    cancellation_results[task_name] = f"error: {type(exc).__name__}"
                                    logger.warning(f"  ⚠️ {task_name}: raised {type(exc).__name__} during cancel")
                                else:
                                    cancellation_results[task_name] = "completed"
                                    logger.debug(f"  ✓ {task_name}: completed normally")
                            except asyncio.CancelledError:
                                cancellation_results[task_name] = "cancelled"
                            except asyncio.InvalidStateError:
                                cancellation_results[task_name] = "invalid_state"
                                logger.debug(f"  ? {task_name}: invalid state")
                    except Exception as e:
                        cancellation_results[task_name] = f"check_failed: {e}"
                        logger.error(f"  ❌ Error checking {task_name} status: {e}")
                
                if pending:
                    logger.warning(f"⏰ {len(pending)} tasks did not complete within {timeout}s timeout")
                    
                    for task in pending:
                        task_name = task_names[task]
                        cancellation_results[task_name] = "timeout"
                        
                        age = "unknown"
                        if task_name in self._task_start_times:
                            age_seconds = (datetime.now(pytz.UTC) - self._task_start_times[task_name]).total_seconds()
                            age = f"{age_seconds:.1f}s"
                        
                        logger.warning(f"  ⏰ {task_name}: did not respond to cancel (age: {age})")
                        
                        if force_cancel_on_timeout and not task.done():
                            logger.info(f"  🔨 Force cancelling {task_name}...")
                            task.cancel()
                            try:
                                await asyncio.wait_for(asyncio.shield(task), timeout=per_task_timeout)
                            except asyncio.TimeoutError:
                                cancellation_results[task_name] = "force_timeout"
                                logger.error(f"  ❌ {task_name}: STUCK - did not respond to force cancel")
                            except asyncio.CancelledError:
                                cancellation_results[task_name] = "force_cancelled"
                                logger.info(f"  ✓ {task_name}: force cancelled")
                            except Exception as e:
                                cancellation_results[task_name] = f"force_error: {e}"
                                logger.error(f"  ❌ {task_name}: error during force cancel: {e}")
                else:
                    logger.info(f"✅ All {len(done)} tasks completed cancellation gracefully")
                    
            except Exception as e:
                logger.error(f"❌ Error during task cancellation phase: {e}")
        
        self._pending_tasks.clear()
        self._task_callbacks.clear()
        self._task_results.clear()
        self._task_start_times.clear()
        self._completion_event.set()
        
        cancelled_count = sum(1 for s in cancellation_results.values() if 'cancelled' in s)
        timeout_count = sum(1 for s in cancellation_results.values() if 'timeout' in s)
        error_count = sum(1 for s in cancellation_results.values() if 'error' in s)
        
        logger.info(
            f"🧹 Task cleanup complete: {cancelled_count} cancelled, "
            f"{timeout_count} timed out, {error_count} errors"
        )
        
        return cancellation_results
    
    def set_signal_session_manager(self, signal_session_manager: SignalSessionManager):
        """Set signal session manager for dependency injection"""
        self.signal_session_manager = signal_session_manager
        logger.info("Signal session manager injected to position tracker")
    
    def check_slippage(self, expected_price: float, actual_price: float, operation: str, user_id: int, position_id: Optional[int] = None):
        """Check for excessive slippage and send alert if threshold is exceeded
        
        Args:
            expected_price: Expected price (signal price or TP/SL price)
            actual_price: Actual execution price
            operation: Operation type ('OPEN' or 'CLOSE')
            user_id: User ID
            position_id: Position ID (optional for OPEN)
        """
        try:
            slippage_dollars = abs(actual_price - expected_price)
            slippage_pips = slippage_dollars * self.config.XAUUSD_PIP_VALUE
            
            if slippage_pips > self.MAX_SLIPPAGE_PIPS:
                logger.warning(f"⚠️ High slippage detected on {operation} - Expected: ${expected_price:.2f}, Actual: ${actual_price:.2f}, Slippage: {slippage_pips:.1f} pips")
                
                if self.alert_system:
                    try:
                        self._create_tracked_task(
                            self.alert_system.send_system_error(
                                f"⚠️ High Slippage Alert\n\n"
                                f"Operation: {operation}\n"
                                f"Position ID: {position_id if position_id else 'N/A'}\n"
                                f"User: {user_id}\n"
                                f"Expected Price: ${expected_price:.2f}\n"
                                f"Actual Price: ${actual_price:.2f}\n"
                                f"Slippage: {slippage_pips:.1f} pips (>${self.MAX_SLIPPAGE_PIPS} pips threshold)\n\n"
                                f"⚠️ Check market conditions and broker execution quality"
                            ),
                            name=f"slippage_alert_{user_id}_{position_id}"
                        )
                    except Exception as e:
                        logger.error(f"Failed to send slippage alert: {e}")
            else:
                logger.debug(f"Slippage within threshold on {operation}: {slippage_pips:.1f} pips")
                
        except Exception as e:
            logger.error(f"Error checking slippage: {e}")
    
    def _normalize_position_dict(self, pos: Dict) -> Dict:
        """Ensure all required keys exist in position dict with safe defaults"""
        if 'original_sl' not in pos or pos['original_sl'] is None:
            pos['original_sl'] = pos.get('stop_loss', 0.0)
        if 'sl_adjustment_count' not in pos or pos['sl_adjustment_count'] is None:
            pos['sl_adjustment_count'] = 0
        if 'max_profit_reached' not in pos or pos['max_profit_reached'] is None:
            pos['max_profit_reached'] = 0.0
        if 'last_price_update' not in pos:
            pos['last_price_update'] = datetime.now(pytz.UTC)
        return pos
        
    async def add_position(self, user_id: int, trade_id: int, signal_type: str, entry_price: float,
                          stop_loss: float, take_profit: float):
        """Add position with comprehensive validation and error handling
        
        Thread-safe with asyncio.Lock for active_positions access
        """
        is_valid, error_msg = validate_position_data(user_id, trade_id, signal_type, entry_price, stop_loss, take_profit)
        if not is_valid:
            logger.error(f"Position validation failed: {error_msg}")
            logger.debug(f"Invalid data: user_id={user_id}, trade_id={trade_id}, signal={signal_type}, entry={entry_price}, sl={stop_loss}, tp={take_profit}")
            return None
        
        session = self.db.get_session()
        position_id = None
        try:
            position = Position(
                user_id=user_id,
                trade_id=trade_id,
                ticker='XAUUSD',
                signal_type=signal_type,
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                current_price=entry_price,
                unrealized_pl=0.0,
                status='ACTIVE',
                original_sl=stop_loss,
                sl_adjustment_count=0,
                max_profit_reached=0.0,
                last_price_update=datetime.now(pytz.UTC)
            )
            session.add(position)
            session.flush()
            position_id = position.id
            session.commit()
            
            async with self._position_lock:
                if user_id not in self.active_positions:
                    self.active_positions[user_id] = {}
                
                self.active_positions[user_id][position_id] = {
                    'trade_id': trade_id,
                    'signal_type': signal_type,
                    'entry_price': entry_price,
                    'stop_loss': stop_loss,
                    'take_profit': take_profit,
                    'original_sl': stop_loss,
                    'sl_adjustment_count': 0,
                    'max_profit_reached': 0.0
                }
            
            logger.info(f"✅ Position added - User:{user_id} ID:{position_id} {signal_type} @${entry_price:.2f} SL:${stop_loss:.2f} TP:${take_profit:.2f}")
            
            if self.signal_session_manager:
                try:
                    await asyncio.wait_for(
                        self.signal_session_manager.update_session(
                            user_id,
                            position_id=position_id,
                            trade_id=trade_id
                        ),
                        timeout=DEFAULT_OPERATION_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout updating signal session for position {position_id}")
            
            return position_id
            
        except Exception as e:
            logger.error(f"Database error adding position for User:{user_id} Trade:{trade_id}: {type(e).__name__}: {e}", exc_info=True)
            try:
                session.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback: {rollback_error}")
            return None
        finally:
            try:
                session.close()
            except Exception as close_error:
                logger.error(f"Error closing session: {close_error}")
    
    async def _apply_dynamic_sl_internal(self, user_id: int, position_id: int, pos: Dict, 
                                          current_price: float, unrealized_pl: float) -> tuple[bool, Optional[float], Dict]:
        """Internal method: Apply dynamic SL tightening when loss >= threshold
        
        Args:
            user_id: User ID
            position_id: Position ID
            pos: Position dict (will be modified in place)
            current_price: Current market price
            unrealized_pl: Unrealized P/L
            
        Returns:
            tuple[bool, Optional[float], Dict]: (sl_adjusted, new_stop_loss, updated_pos)
        """
        pos = self._normalize_position_dict(pos)
        signal_type = pos['signal_type']
        entry_price = pos['entry_price']
        stop_loss = pos['stop_loss']
        original_sl = pos.get('original_sl')
        
        if original_sl is None:
            original_sl = stop_loss
            pos['original_sl'] = stop_loss
            logger.warning(f"original_sl was None for position {position_id}, using current stop_loss")
        
        if unrealized_pl >= 0 or abs(unrealized_pl) < self.config.DYNAMIC_SL_LOSS_THRESHOLD:
            return False, None, pos
        
        original_sl_distance = abs(entry_price - original_sl)
        new_sl_distance = original_sl_distance * self.config.DYNAMIC_SL_TIGHTENING_MULTIPLIER
        
        new_stop_loss = None
        sl_adjusted = False
        
        if signal_type == 'BUY':
            new_stop_loss = entry_price - new_sl_distance
            if new_stop_loss > stop_loss:
                pos['stop_loss'] = new_stop_loss
                pos['sl_adjustment_count'] = pos.get('sl_adjustment_count', 0) + 1
                sl_adjusted = True
                logger.info(f"🛡️ Dynamic SL activated! Loss ${abs(unrealized_pl):.2f} >= ${self.config.DYNAMIC_SL_LOSS_THRESHOLD}. SL tightened from ${stop_loss:.2f} → ${new_stop_loss:.2f} (protect capital)")
                
                if self.telegram_app:
                    try:
                        msg = (
                            f"🛡️ *Dynamic SL Activated*\n\n"
                            f"Position ID: {position_id}\n"
                            f"Type: {signal_type}\n"
                            f"Current Loss: ${abs(unrealized_pl):.2f}\n"
                            f"SL Updated: ${stop_loss:.2f} → ${new_stop_loss:.2f}\n"
                            f"Protection: Capital preservation mode"
                        )
                        await asyncio.wait_for(
                            self.telegram_app.bot.send_message(chat_id=user_id, text=msg, parse_mode='Markdown'),
                            timeout=NOTIFICATION_TIMEOUT
                        )
                    except asyncio.TimeoutError:
                        logger.error(f"Timeout sending dynamic SL notification to user {user_id}")
                    except Exception as e:
                        logger.error(f"Failed to send dynamic SL notification: {e}")
        else:
            new_stop_loss = entry_price + new_sl_distance
            if new_stop_loss < stop_loss:
                pos['stop_loss'] = new_stop_loss
                pos['sl_adjustment_count'] = pos.get('sl_adjustment_count', 0) + 1
                sl_adjusted = True
                logger.info(f"🛡️ Dynamic SL activated! Loss ${abs(unrealized_pl):.2f} >= ${self.config.DYNAMIC_SL_LOSS_THRESHOLD}. SL tightened from ${stop_loss:.2f} → ${new_stop_loss:.2f} (protect capital)")
                
                if self.telegram_app:
                    try:
                        msg = (
                            f"🛡️ *Dynamic SL Activated*\n\n"
                            f"Position ID: {position_id}\n"
                            f"Type: {signal_type}\n"
                            f"Current Loss: ${abs(unrealized_pl):.2f}\n"
                            f"SL Updated: ${stop_loss:.2f} → ${new_stop_loss:.2f}\n"
                            f"Protection: Capital preservation mode"
                        )
                        await asyncio.wait_for(
                            self.telegram_app.bot.send_message(chat_id=user_id, text=msg, parse_mode='Markdown'),
                            timeout=NOTIFICATION_TIMEOUT
                        )
                    except asyncio.TimeoutError:
                        logger.error(f"Timeout sending dynamic SL notification to user {user_id}")
                    except Exception as e:
                        logger.error(f"Failed to send dynamic SL notification: {e}")
        
        return sl_adjusted, new_stop_loss if sl_adjusted else None, pos
    
    async def apply_dynamic_sl(self, user_id: int, position_id: int, current_price: float, unrealized_pl: float) -> tuple[bool, Optional[float]]:
        """Apply dynamic SL tightening when loss >= threshold
        
        Thread-safe wrapper for _apply_dynamic_sl_internal
        
        Returns:
            tuple[bool, Optional[float]]: (sl_adjusted, new_stop_loss)
        """
        async with self._position_lock:
            if user_id not in self.active_positions or position_id not in self.active_positions[user_id]:
                return False, None
            
            pos = self.active_positions[user_id][position_id].copy()
        
        sl_adjusted, new_sl, updated_pos = await self._apply_dynamic_sl_internal(
            user_id, position_id, pos, current_price, unrealized_pl
        )
        
        if sl_adjusted:
            async with self._position_lock:
                if user_id in self.active_positions and position_id in self.active_positions[user_id]:
                    self.active_positions[user_id][position_id].update(updated_pos)
        
        return sl_adjusted, new_sl
    
    async def _apply_trailing_stop_internal(self, user_id: int, position_id: int, pos: Dict,
                                             current_price: float, unrealized_pl: float) -> tuple[bool, Optional[float], Dict]:
        """Internal method: Apply trailing stop when profit >= threshold
        
        Args:
            user_id: User ID
            position_id: Position ID
            pos: Position dict (will be modified)
            current_price: Current market price
            unrealized_pl: Unrealized P/L
            
        Returns:
            tuple[bool, Optional[float], Dict]: (sl_adjusted, new_stop_loss, updated_pos)
        """
        pos = self._normalize_position_dict(pos)
        signal_type = pos['signal_type']
        stop_loss = pos['stop_loss']
        
        if unrealized_pl <= 0 or unrealized_pl < self.config.TRAILING_STOP_PROFIT_THRESHOLD:
            return False, None, pos
        
        max_profit = pos.get('max_profit_reached', 0.0)
        if max_profit is None:
            max_profit = 0.0
        
        if unrealized_pl > max_profit:
            pos['max_profit_reached'] = unrealized_pl
        
        trailing_distance = self.config.TRAILING_STOP_DISTANCE_PIPS / self.config.XAUUSD_PIP_VALUE
        
        new_trailing_sl = None
        sl_adjusted = False
        
        if signal_type == 'BUY':
            new_trailing_sl = current_price - trailing_distance
            if new_trailing_sl > stop_loss:
                pos['stop_loss'] = new_trailing_sl
                pos['sl_adjustment_count'] = pos.get('sl_adjustment_count', 0) + 1
                sl_adjusted = True
                logger.info(f"💎 Trailing stop activated! Profit ${unrealized_pl:.2f}. SL moved to ${new_trailing_sl:.2f} (lock-in profit)")
                
                if self.telegram_app:
                    try:
                        msg = (
                            f"💎 *Trailing Stop Active*\n\n"
                            f"Position ID: {position_id}\n"
                            f"Type: {signal_type}\n"
                            f"Current Profit: ${unrealized_pl:.2f}\n"
                            f"Max Profit: ${pos['max_profit_reached']:.2f}\n"
                            f"SL Updated: ${stop_loss:.2f} → ${new_trailing_sl:.2f}\n"
                            f"Status: Profit locked-in!"
                        )
                        await asyncio.wait_for(
                            self.telegram_app.bot.send_message(chat_id=user_id, text=msg, parse_mode='Markdown'),
                            timeout=NOTIFICATION_TIMEOUT
                        )
                    except asyncio.TimeoutError:
                        logger.error(f"Timeout sending trailing stop notification to user {user_id}")
                    except Exception as e:
                        logger.error(f"Failed to send trailing stop notification: {e}")
        else:
            new_trailing_sl = current_price + trailing_distance
            if new_trailing_sl < stop_loss:
                pos['stop_loss'] = new_trailing_sl
                pos['sl_adjustment_count'] = pos.get('sl_adjustment_count', 0) + 1
                sl_adjusted = True
                logger.info(f"💎 Trailing stop activated! Profit ${unrealized_pl:.2f}. SL moved to ${new_trailing_sl:.2f} (lock-in profit)")
                
                if self.telegram_app:
                    try:
                        msg = (
                            f"💎 *Trailing Stop Active*\n\n"
                            f"Position ID: {position_id}\n"
                            f"Type: {signal_type}\n"
                            f"Current Profit: ${unrealized_pl:.2f}\n"
                            f"Max Profit: ${pos['max_profit_reached']:.2f}\n"
                            f"SL Updated: ${stop_loss:.2f} → ${new_trailing_sl:.2f}\n"
                            f"Status: Profit locked-in!"
                        )
                        await asyncio.wait_for(
                            self.telegram_app.bot.send_message(chat_id=user_id, text=msg, parse_mode='Markdown'),
                            timeout=NOTIFICATION_TIMEOUT
                        )
                    except asyncio.TimeoutError:
                        logger.error(f"Timeout sending trailing stop notification to user {user_id}")
                    except Exception as e:
                        logger.error(f"Failed to send trailing stop notification: {e}")
        
        return sl_adjusted, new_trailing_sl if sl_adjusted else None, pos
    
    async def apply_trailing_stop(self, user_id: int, position_id: int, current_price: float, unrealized_pl: float) -> tuple[bool, Optional[float]]:
        """Apply trailing stop when profit >= threshold
        
        Thread-safe wrapper for _apply_trailing_stop_internal
        
        Returns:
            tuple[bool, Optional[float]]: (sl_adjusted, new_stop_loss)
        """
        async with self._position_lock:
            if user_id not in self.active_positions or position_id not in self.active_positions[user_id]:
                return False, None
            
            pos = self.active_positions[user_id][position_id].copy()
        
        sl_adjusted, new_sl, updated_pos = await self._apply_trailing_stop_internal(
            user_id, position_id, pos, current_price, unrealized_pl
        )
        
        if sl_adjusted:
            async with self._position_lock:
                if user_id in self.active_positions and position_id in self.active_positions[user_id]:
                    self.active_positions[user_id][position_id].update(updated_pos)
        
        return sl_adjusted, new_sl
    
    async def update_position(self, user_id: int, position_id: int, current_price: float) -> Optional[str]:
        """Update position with current price and apply dynamic SL/TP logic with validation
        
        Thread-safe position update with lock protection
        """
        try:
            if current_price is None or current_price <= 0:
                logger.warning(f"Invalid current_price for position {position_id}: {current_price}")
                return None
            
            async with self._position_lock:
                if user_id not in self.active_positions or position_id not in self.active_positions[user_id]:
                    logger.debug(f"Position {position_id} for User:{user_id} not found in active positions")
                    return None
                
                pos = self.active_positions[user_id][position_id].copy()
            
            try:
                signal_type = pos['signal_type']
                entry_price = pos['entry_price']
                stop_loss = pos['stop_loss']
                take_profit = pos['take_profit']
                
                if None in [signal_type, entry_price, stop_loss, take_profit]:
                    logger.error(f"Position {position_id} has None values: signal={signal_type}, entry={entry_price}, sl={stop_loss}, tp={take_profit}")
                    return None
                    
            except KeyError as e:
                logger.error(f"Missing required key in position {position_id}: {e}")
                return None
            
            unrealized_pl = self.risk_manager.calculate_pl(entry_price, current_price, signal_type)
        
            sl_adjusted = False
            dynamic_sl_applied = False
            
            try:
                dynamic_sl_applied, new_sl = await self.apply_dynamic_sl(user_id, position_id, current_price, unrealized_pl)
                if dynamic_sl_applied:
                    sl_adjusted = True
                    stop_loss = new_sl
            except Exception as e:
                logger.error(f"Error applying dynamic SL for position {position_id}: {e}")
            
            if not dynamic_sl_applied:
                try:
                    trailing_applied, new_sl = await self.apply_trailing_stop(user_id, position_id, current_price, unrealized_pl)
                    if trailing_applied:
                        sl_adjusted = True
                        stop_loss = new_sl
                except Exception as e:
                    logger.error(f"Error applying trailing stop for position {position_id}: {e}")
            
            async with self._position_lock:
                if user_id in self.active_positions and position_id in self.active_positions[user_id]:
                    stop_loss = self.active_positions[user_id][position_id]['stop_loss']
                    sl_adjustment_count = self.active_positions[user_id][position_id].get('sl_adjustment_count', 0)
                else:
                    return None
            
            session = self.db.get_session()
            try:
                position = session.query(Position).filter(Position.id == position_id, Position.user_id == user_id).first()
                if not position:
                    logger.warning(f"Position {position_id} not found in database for User:{user_id}")
                    return None
                
                position.current_price = current_price
                position.unrealized_pl = unrealized_pl
                position.last_price_update = datetime.now(pytz.UTC)
                
                current_max_profit = position.max_profit_reached if position.max_profit_reached is not None else 0.0
                if unrealized_pl > 0 and unrealized_pl > current_max_profit:
                    position.max_profit_reached = unrealized_pl
                
                if sl_adjusted:
                    position.stop_loss = stop_loss
                    position.sl_adjustment_count = sl_adjustment_count
                
                session.commit()
            except Exception as e:
                logger.error(f"Database error updating position {position_id}: {type(e).__name__}: {e}", exc_info=True)
                try:
                    session.rollback()
                except Exception as rollback_error:
                    logger.error(f"Error during rollback: {rollback_error}")
            finally:
                try:
                    session.close()
                except Exception as close_error:
                    logger.error(f"Error closing session: {close_error}")
            
            hit_tp = False
            hit_sl = False
            
            try:
                if signal_type == 'BUY':
                    hit_tp = current_price >= take_profit
                    hit_sl = current_price <= stop_loss
                else:
                    hit_tp = current_price <= take_profit
                    hit_sl = current_price >= stop_loss
            except Exception as e:
                logger.error(f"Error checking TP/SL conditions: {e}")
                return None
            
            if hit_tp:
                await self.close_position(user_id, position_id, current_price, 'TP_HIT')
                return 'TP_HIT'
            elif hit_sl:
                reason = 'DYNAMIC_SL_HIT' if sl_adjusted else 'SL_HIT'
                await self.close_position(user_id, position_id, current_price, reason)
                return reason
            
            return None
            
        except Exception as e:
            logger.error(f"Critical error updating position {position_id} for User:{user_id}: {type(e).__name__}: {e}", exc_info=True)
            return None
    
    async def close_position(self, user_id: int, position_id: int, exit_price: float, reason: str):
        """Close a position with thread-safe access and proper cleanup
        
        Thread-safe position closure with lock protection
        """
        async with self._position_lock:
            if user_id not in self.active_positions or position_id not in self.active_positions[user_id]:
                return
            
            pos = self.active_positions[user_id][position_id].copy()
        
        trade_id = pos['trade_id']
        signal_type = pos['signal_type']
        entry_price = pos['entry_price']
        
        actual_pl = self.risk_manager.calculate_pl(entry_price, exit_price, signal_type)
        
        trade_result = None
        
        session = self.db.get_session()
        try:
            position = session.query(Position).filter(Position.id == position_id, Position.user_id == user_id).first()
            if position:
                position.status = 'CLOSED'
                position.current_price = exit_price
                position.unrealized_pl = actual_pl
                position.closed_at = datetime.now(pytz.UTC)
                
            trade = session.query(Trade).filter(Trade.id == trade_id, Trade.user_id == user_id).first()
            if trade:
                trade.status = 'CLOSED'
                trade.exit_price = exit_price
                trade.actual_pl = actual_pl
                trade.close_time = datetime.now(pytz.UTC)
                trade.result = 'WIN' if actual_pl > 0 else 'LOSS'
                trade_result = trade.result
                
            session.commit()
            
            async with self._position_lock:
                if user_id in self.active_positions and position_id in self.active_positions[user_id]:
                    del self.active_positions[user_id][position_id]
                    if not self.active_positions[user_id]:
                        del self.active_positions[user_id]
            
            logger.info(f"Position closed - User:{user_id} ID:{position_id} {reason} P/L:${actual_pl:.2f}")
            
            if self.telegram_app and self.chart_generator and self.market_data:
                try:
                    df_m1 = await self.market_data.get_historical_data('M1', 100)
                    
                    if df_m1 is not None and len(df_m1) >= 30:
                        exit_signal = {
                            'signal': signal_type,
                            'entry_price': entry_price,
                            'stop_loss': pos['stop_loss'],
                            'take_profit': pos['take_profit'],
                            'timeframe': 'M1'
                        }
                        
                        chart_path = await self.chart_generator.generate_chart_async(df_m1, exit_signal, 'M1')
                        
                        if chart_path and self.signal_session_manager:
                            await self.signal_session_manager.update_session(user_id, chart_path=chart_path)
                        
                        result_emoji = '✅' if trade_result == 'WIN' else '❌'
                        exit_label = "TRADE_EXIT" if reason == "TP_HIT" else "Trade LOSS"
                        
                        exit_msg = (
                            f"{result_emoji} *{exit_label}*\n\n"
                            f"Type: {signal_type}\n"
                            f"Entry: ${entry_price:.2f}\n"
                            f"Exit: ${exit_price:.2f}\n"
                            f"P/L: ${actual_pl:.2f}"
                        )
                        
                        try:
                            await asyncio.wait_for(
                                self.telegram_app.bot.send_message(
                                    chat_id=user_id,
                                    text=exit_msg,
                                    parse_mode='Markdown'
                                ),
                                timeout=10.0
                            )
                            logger.info(f"Exit notification sent to user {user_id} - TEXT ONLY (no photo)")
                            
                            if chart_path and self.config.CHART_AUTO_DELETE:
                                await asyncio.sleep(1)
                                self.chart_generator.delete_chart(chart_path)
                                logger.info(f"Auto-deleted unused exit chart: {chart_path}")
                        except asyncio.TimeoutError:
                            logger.error(f"Failed to send exit notification to user {user_id}: asyncio.TimeoutError after 10s")
                            try:
                                simple_msg = f"{result_emoji} {exit_label}\nType: {signal_type}\nEntry: ${entry_price:.2f}\nExit: ${exit_price:.2f}\nP/L: ${actual_pl:.2f}"
                                await asyncio.wait_for(
                                    self.telegram_app.bot.send_message(chat_id=user_id, text=simple_msg),
                                    timeout=5.0
                                )
                                logger.info(f"Fallback notification sent to user {user_id}")
                            except Exception as fallback_error:
                                logger.error(f"Fallback notification also failed: {fallback_error}")
                        except TimedOut as telegram_err:
                            logger.error(f"Failed to send exit notification to user {user_id}: telegram.error.TimedOut")
                            try:
                                simple_msg = f"{result_emoji} {exit_label}\nType: {signal_type}\nEntry: ${entry_price:.2f}\nExit: ${exit_price:.2f}\nP/L: ${actual_pl:.2f}"
                                await asyncio.wait_for(
                                    self.telegram_app.bot.send_message(chat_id=user_id, text=simple_msg),
                                    timeout=5.0
                                )
                                logger.info(f"Fallback notification sent to user {user_id}")
                            except Exception as fallback_error:
                                logger.error(f"Fallback notification also failed: {fallback_error}")
                        except Exception as telegram_err:
                            logger.error(f"Failed to send exit notification to user {user_id}: {telegram_err}")
                    else:
                        logger.warning(f"Not enough candles for exit chart: {len(df_m1) if df_m1 else 0}")
                        
                        if self.alert_system and trade_result:
                            await self.alert_system.send_trade_exit_alert({
                                'signal_type': signal_type,
                                'entry_price': entry_price,
                                'exit_price': exit_price,
                                'actual_pl': actual_pl
                            }, trade_result)
                except Exception as e:
                    logger.error(f"Error sending exit chart: {e}")
                    
                    if self.alert_system and trade_result:
                        await self.alert_system.send_trade_exit_alert({
                            'signal_type': signal_type,
                            'entry_price': entry_price,
                            'exit_price': exit_price,
                            'actual_pl': actual_pl
                        }, trade_result)
            elif self.alert_system and trade_result:
                await self.alert_system.send_trade_exit_alert({
                    'signal_type': signal_type,
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'actual_pl': actual_pl
                }, trade_result)
            
            if self.user_manager:
                self.user_manager.update_user_stats(user_id, actual_pl)
            
            if self.signal_session_manager:
                await self.signal_session_manager.end_session(user_id, reason)
            
        except Exception as e:
            logger.error(f"Error closing position {position_id}: {e}", exc_info=True)
            try:
                session.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback: {rollback_error}")
        finally:
            try:
                session.close()
            except Exception as close_error:
                logger.error(f"Error closing session: {close_error}")
    
    async def monitor_active_positions(self):
        """Monitor all active positions and apply dynamic SL/TP
        
        This method is called by the scheduler every 10 seconds.
        Returns a list of updated positions.
        Thread-safe with lock protection for reading active_positions
        """
        if self._shutdown_event.is_set():
            return []
        
        async with self._position_lock:
            if not self.active_positions:
                return []
            positions_snapshot = {
                user_id: list(positions.keys()) 
                for user_id, positions in self.active_positions.items()
            }
        
        if not self.market_data:
            logger.warning("Market data not available for position monitoring")
            return []
        
        updated_positions = []
        
        try:
            current_price = await asyncio.wait_for(
                self.market_data.get_current_price(),
                timeout=DEFAULT_OPERATION_TIMEOUT
            )
            
            if not current_price:
                logger.warning("No current price available for position monitoring")
                return []
            
            for user_id, position_ids in positions_snapshot.items():
                for position_id in position_ids:
                    if self._shutdown_event.is_set():
                        logger.info("Shutdown requested, stopping position monitoring")
                        return updated_positions
                    
                    try:
                        result = await asyncio.wait_for(
                            self.update_position(user_id, position_id, current_price),
                            timeout=DEFAULT_OPERATION_TIMEOUT
                        )
                        if result:
                            updated_positions.append({
                                'user_id': user_id,
                                'position_id': position_id,
                                'result': result,
                                'price': current_price
                            })
                            logger.info(f"Position {position_id} User:{user_id} closed: {result} at ${current_price:.2f}")
                    except asyncio.TimeoutError:
                        logger.error(f"Timeout updating position {position_id} for user {user_id}")
                    except Exception as e:
                        logger.error(f"Error monitoring position {position_id} for user {user_id}: {e}")
            
            return updated_positions
            
        except asyncio.TimeoutError:
            logger.error("Timeout getting current price for position monitoring")
            return []
        except Exception as e:
            logger.error(f"Error in monitor_active_positions: {e}")
            return []
    
    async def monitor_positions(self, market_data_client):
        """Monitor positions with tick data stream
        
        Uses timeout on tick_queue.get() to allow graceful shutdown checking.
        This task is tracked for proper lifecycle management.
        """
        tick_queue = await market_data_client.subscribe_ticks('position_tracker')
        logger.info("Position tracker monitoring started")
        
        self.monitoring = True
        self._shutdown_event.clear()
        
        try:
            while self.monitoring and not self._shutdown_event.is_set():
                try:
                    try:
                        tick = await asyncio.wait_for(
                            tick_queue.get(),
                            timeout=TICK_QUEUE_TIMEOUT
                        )
                    except asyncio.TimeoutError:
                        continue
                    
                    async with self._position_lock:
                        if not self.active_positions:
                            continue
                        positions_snapshot = {
                            user_id: list(positions.keys()) 
                            for user_id, positions in self.active_positions.items()
                        }
                    
                    mid_price = tick['quote']
                    
                    for user_id, position_ids in positions_snapshot.items():
                        if not self.monitoring or self._shutdown_event.is_set():
                            break
                        for position_id in position_ids:
                            if not self.monitoring or self._shutdown_event.is_set():
                                break
                            try:
                                result = await asyncio.wait_for(
                                    self.update_position(user_id, position_id, mid_price),
                                    timeout=DEFAULT_OPERATION_TIMEOUT
                                )
                                if result:
                                    logger.info(f"Position {position_id} User:{user_id} closed: {result}")
                            except asyncio.TimeoutError:
                                logger.error(f"Timeout updating position {position_id}")
                            except Exception as e:
                                logger.error(f"Error updating position {position_id}: {e}")
                    
                except asyncio.CancelledError:
                    logger.info("Position monitoring cancelled")
                    break
                except Exception as e:
                    logger.error(f"Error processing tick dalam position monitoring: {e}")
                    if self.monitoring and not self._shutdown_event.is_set():
                        await asyncio.sleep(1)
                    
        finally:
            try:
                await market_data_client.unsubscribe_ticks('position_tracker')
            except Exception as e:
                logger.error(f"Error unsubscribing from ticks: {e}")
            logger.info("Position tracker monitoring stopped")
    
    def start_monitoring_task(self, market_data_client) -> asyncio.Task:
        """Start the monitoring task and track it for lifecycle management.
        
        Returns:
            asyncio.Task: The monitoring task
        """
        self._monitoring_task = self._create_tracked_task(
            self.monitor_positions(market_data_client),
            name="position_monitoring"
        )
        return self._monitoring_task
    
    def stop_monitoring(self):
        """Stop the monitoring loop"""
        self.monitoring = False
        self._shutdown_event.set()
        logger.info("Position monitoring stop requested")
    
    async def shutdown(self, timeout: float = TASK_CLEANUP_TIMEOUT):
        """Graceful shutdown with proper resource cleanup
        
        Cancels all pending tasks, stops monitoring loop and task monitor.
        
        Args:
            timeout: Maximum time to wait for task cleanup
        """
        logger.info("PositionTracker shutdown initiated...")
        
        self.monitoring = False
        self._shutdown_event.set()
        
        await self.stop_task_monitor()
        
        if self._monitoring_task and not self._monitoring_task.done():
            logger.info("Cancelling monitoring task...")
            self._monitoring_task.cancel()
            try:
                await asyncio.wait_for(self._monitoring_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        
        cancellation_results = await self._cancel_pending_tasks(timeout=timeout)
        
        if cancellation_results:
            timeout_count = sum(1 for s in cancellation_results.values() if 'timeout' in s)
            if timeout_count > 0:
                logger.warning(f"Shutdown completed with {timeout_count} tasks that did not cancel cleanly")
        
        self._slow_task_counts.clear()
        self._cancelled_task_names.clear()
        
        async with self._position_lock:
            active_count = sum(len(positions) for positions in self.active_positions.values())
            if active_count > 0:
                logger.warning(f"Shutting down with {active_count} active positions")
        
        logger.info("PositionTracker shutdown complete")
    
    async def get_active_positions_async(self, user_id: Optional[int] = None) -> Dict:
        """Thread-safe getter for active positions (async version)"""
        async with self._position_lock:
            if user_id is not None:
                return self.active_positions.get(user_id, {}).copy()
            return {uid: pos.copy() for uid, pos in self.active_positions.items()}
    
    def get_active_positions(self, user_id: Optional[int] = None) -> Dict:
        """Get active positions (sync version, returns snapshot)
        
        Note: For thread-safe access in async context, use get_active_positions_async
        """
        if user_id is not None:
            return self.active_positions.get(user_id, {}).copy()
        return {uid: pos.copy() for uid, pos in self.active_positions.items()}
    
    async def has_active_position_async(self, user_id: int) -> bool:
        """Thread-safe check for active position (async version)"""
        if self.signal_session_manager:
            return self.signal_session_manager.has_active_session(user_id)
        async with self._position_lock:
            return user_id in self.active_positions and len(self.active_positions[user_id]) > 0
    
    def has_active_position(self, user_id: int) -> bool:
        """Check if user has active position (sync version)
        
        Note: For thread-safe access in async context, use has_active_position_async
        """
        if self.signal_session_manager:
            return self.signal_session_manager.has_active_session(user_id)
        return user_id in self.active_positions and len(self.active_positions[user_id]) > 0
    
    async def reload_active_positions(self):
        """Reload active positions from the database
        
        This method is used to restore position tracking after a restart.
        Thread-safe with lock protection.
        """
        session = self.db.get_session()
        try:
            active_positions = session.query(Position).filter(Position.status == 'ACTIVE').all()
            
            async with self._position_lock:
                self.active_positions.clear()
                
                for pos in active_positions:
                    if pos.user_id not in self.active_positions:
                        self.active_positions[pos.user_id] = {}
                    
                    self.active_positions[pos.user_id][pos.id] = {
                        'trade_id': pos.trade_id,
                        'signal_type': pos.signal_type,
                        'entry_price': pos.entry_price,
                        'stop_loss': pos.stop_loss,
                        'take_profit': pos.take_profit,
                        'original_sl': pos.original_sl or pos.stop_loss,
                        'sl_adjustment_count': pos.sl_adjustment_count or 0,
                        'max_profit_reached': pos.max_profit_reached or 0.0
                    }
            
            total_positions = sum(len(positions) for positions in self.active_positions.values())
            logger.info(f"Reloaded {total_positions} active positions from database")
            
        except Exception as e:
            logger.error(f"Error reloading active positions: {e}", exc_info=True)
            try:
                session.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback: {rollback_error}")
        finally:
            try:
                session.close()
            except Exception as close_error:
                logger.error(f"Error closing session: {close_error}")
