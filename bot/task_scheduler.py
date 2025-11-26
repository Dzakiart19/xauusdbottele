import asyncio
import gc
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Callable, Optional, Dict, List, Set, Any, Tuple
import pytz
from bot.logger import setup_logger

logger = setup_logger('TaskScheduler')

TASK_EXECUTION_TIMEOUT = 60
TASK_CANCEL_TIMEOUT = 5
SCHEDULER_STOP_TIMEOUT = 10
FLUSH_PENDING_TIMEOUT = 15
STALE_TASK_THRESHOLD_MINUTES = 30
MAX_CONSECUTIVE_FAILURES_AUTO_DISABLE = 10
EXCEPTION_HISTORY_LIMIT = 20
CLEANUP_INTERVAL_SECONDS = 300


@dataclass
class ExceptionRecord:
    """Record of an exception with timestamp."""
    exception: Exception
    timestamp: datetime
    error_type: str = ""
    
    def __post_init__(self):
        self.error_type = type(self.exception).__name__
    
    def to_dict(self) -> Dict:
        return {
            'error_type': self.error_type,
            'message': str(self.exception),
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class TaskHealthMetrics:
    """Health metrics for a scheduled task."""
    total_runs: int = 0
    successful_runs: int = 0
    failed_runs: int = 0
    total_execution_time_ms: float = 0.0
    last_successful_run: Optional[datetime] = None
    last_execution_time_ms: float = 0.0
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_runs == 0:
            return 0.0
        return (self.successful_runs / self.total_runs) * 100.0
    
    @property
    def average_execution_time_ms(self) -> float:
        """Calculate average execution time in milliseconds."""
        if self.successful_runs == 0:
            return 0.0
        return self.total_execution_time_ms / self.successful_runs
    
    def time_since_last_success(self, timezone) -> Optional[timedelta]:
        """Get time elapsed since last successful run."""
        if self.last_successful_run is None:
            return None
        now = datetime.now(timezone)
        if self.last_successful_run.tzinfo is None:
            last_success = timezone.localize(self.last_successful_run)
        else:
            last_success = self.last_successful_run
        return now - last_success
    
    def to_dict(self) -> Dict:
        return {
            'total_runs': self.total_runs,
            'successful_runs': self.successful_runs,
            'failed_runs': self.failed_runs,
            'success_rate': round(self.success_rate, 2),
            'average_execution_time_ms': round(self.average_execution_time_ms, 2),
            'last_execution_time_ms': round(self.last_execution_time_ms, 2),
            'last_successful_run': self.last_successful_run.isoformat() if self.last_successful_run else None
        }


class ScheduledTask:
    """Represents a scheduled task with execution tracking and error handling.
    
    Task Lifecycle:
    - Tasks are created with interval or schedule_time
    - should_run() determines if task is ready to execute
    - execute() runs the task with timeout protection
    - Done callbacks drain exceptions and log completion
    
    Health Monitoring:
    - Tracks success rate, execution times, and exception history
    - Auto-disables tasks with excessive consecutive failures
    - Detects stale tasks that haven't run for extended periods
    """
    def __init__(self, name: str, func: Callable, interval: Optional[int] = None,
                 schedule_time: Optional[time] = None, timezone: str = 'Asia/Jakarta',
                 auto_disable_threshold: int = MAX_CONSECUTIVE_FAILURES_AUTO_DISABLE):
        self.name = name
        self.func = func
        self.interval = interval
        self.schedule_time = schedule_time
        self.timezone = pytz.timezone(timezone)
        self.last_run = None
        self.next_run = None
        self.enabled = True
        self.run_count = 0
        self.error_count = 0
        self.consecutive_failures = 0
        self.current_execution_task = None
        self._is_executing = False
        self._last_exception: Optional[Exception] = None
        
        self._exception_history: deque = deque(maxlen=EXCEPTION_HISTORY_LIMIT)
        self._health_metrics = TaskHealthMetrics()
        self._auto_disable_threshold = auto_disable_threshold
        self._auto_disabled = False
        self._created_at = datetime.now(self.timezone)
        self._execution_start_time: Optional[datetime] = None
        
        self._calculate_next_run()
    
    def _calculate_next_run(self):
        now = datetime.now(self.timezone)
        
        if self.schedule_time:
            next_run = now.replace(
                hour=self.schedule_time.hour,
                minute=self.schedule_time.minute,
                second=self.schedule_time.second,
                microsecond=0
            )
            
            if next_run <= now:
                next_run += timedelta(days=1)
            
            self.next_run = next_run
        
        elif self.interval:
            if self.last_run:
                self.next_run = self.last_run + timedelta(seconds=self.interval)
            else:
                self.next_run = now
    
    def should_run(self) -> bool:
        if not self.enabled:
            return False
        
        if self.next_run is None:
            return False
        
        if self._is_executing:
            return False
        
        now = datetime.now(self.timezone)
        return now >= self.next_run
    
    def is_stale(self, threshold_minutes: int = STALE_TASK_THRESHOLD_MINUTES) -> bool:
        """Check if task is stale (enabled but hasn't run for extended period).
        
        Args:
            threshold_minutes: Minutes without running to be considered stale
            
        Returns:
            bool: True if task is stale
        """
        if not self.enabled:
            return False
        
        if self._is_executing:
            return False
        
        now = datetime.now(self.timezone)
        
        if self.last_run is None:
            time_since_creation = now - self._created_at
            return time_since_creation > timedelta(minutes=threshold_minutes)
        
        if self.last_run.tzinfo is None:
            last_run = self.timezone.localize(self.last_run)
        else:
            last_run = self.last_run
        
        time_since_last_run = now - last_run
        return time_since_last_run > timedelta(minutes=threshold_minutes)
    
    def has_high_failure_rate(self, threshold: int = 5) -> bool:
        """Check if task has high consecutive failures.
        
        Args:
            threshold: Number of consecutive failures to be considered high
            
        Returns:
            bool: True if consecutive failures exceed threshold
        """
        return self.consecutive_failures >= threshold
    
    def should_auto_disable(self) -> bool:
        """Check if task should be auto-disabled due to excessive failures."""
        return self.consecutive_failures >= self._auto_disable_threshold
    
    def _record_exception(self, exception: Exception) -> None:
        """Record exception with timestamp in history."""
        record = ExceptionRecord(
            exception=exception,
            timestamp=datetime.now(self.timezone)
        )
        self._exception_history.append(record)
    
    def get_exception_history(self, limit: Optional[int] = None) -> List[Dict]:
        """Get exception history for this task.
        
        Args:
            limit: Maximum number of recent exceptions to return
            
        Returns:
            List of exception records as dictionaries
        """
        history = list(self._exception_history)
        if limit is not None:
            history = history[-limit:]
        return [record.to_dict() for record in history]
    
    def get_health_metrics(self) -> Dict:
        """Get health metrics for this task."""
        metrics = self._health_metrics.to_dict()
        time_since_success = self._health_metrics.time_since_last_success(self.timezone)
        if time_since_success:
            metrics['time_since_last_success_seconds'] = time_since_success.total_seconds()
        else:
            metrics['time_since_last_success_seconds'] = None
        metrics['is_stale'] = self.is_stale()
        metrics['has_high_failure_rate'] = self.has_high_failure_rate()
        metrics['auto_disabled'] = self._auto_disabled
        return metrics
    
    def clear_exception_history(self) -> None:
        """Clear all stored exception history."""
        self._exception_history.clear()
    
    def reset_health_metrics(self) -> None:
        """Reset health metrics to initial state."""
        self._health_metrics = TaskHealthMetrics()
        self.consecutive_failures = 0
        self._auto_disabled = False
    
    async def execute(self, alert_system=None, shutdown_flag: Optional[asyncio.Event] = None,
                      on_complete: Optional[Callable[[asyncio.Task, str], None]] = None):
        """Execute the scheduled task with proper lifecycle management.
        
        Args:
            alert_system: Optional alert system for error notifications
            shutdown_flag: Event that signals shutdown request
            on_complete: Optional callback invoked when task completes (receives task and task name)
        """
        if self._is_executing:
            logger.warning(f"Task {self.name} is already executing, skipping")
            return
        
        self._is_executing = True
        self._last_exception = None
        self._execution_start_time = datetime.now(self.timezone)
        
        try:
            logger.info(f"Executing scheduled task: {self.name}")
            
            if shutdown_flag and shutdown_flag.is_set():
                logger.info(f"Shutdown requested, skipping task: {self.name}")
                return
            
            if asyncio.iscoroutinefunction(self.func):
                self.current_execution_task = asyncio.create_task(self.func())
                self.current_execution_task.set_name(f"exec_{self.name}")
                
                if on_complete:
                    def _done_callback(t: asyncio.Task):
                        try:
                            on_complete(t, self.name)
                        except Exception as cb_err:
                            logger.error(f"Completion callback error for {self.name}: {cb_err}")
                    self.current_execution_task.add_done_callback(_done_callback)
                
                try:
                    await asyncio.wait_for(self.current_execution_task, timeout=TASK_EXECUTION_TIMEOUT)
                except asyncio.TimeoutError:
                    logger.warning(f"Task {self.name} timed out after {TASK_EXECUTION_TIMEOUT}s")
                    if self.current_execution_task and not self.current_execution_task.done():
                        self.current_execution_task.cancel()
                        try:
                            await asyncio.wait_for(self.current_execution_task, timeout=TASK_CANCEL_TIMEOUT)
                        except (asyncio.CancelledError, asyncio.TimeoutError):
                            pass
                    raise
            else:
                self.func()
            
            execution_end_time = datetime.now(self.timezone)
            execution_time_ms = (execution_end_time - self._execution_start_time).total_seconds() * 1000
            
            self.last_run = execution_end_time
            self.run_count += 1
            self.consecutive_failures = 0
            
            self._health_metrics.total_runs += 1
            self._health_metrics.successful_runs += 1
            self._health_metrics.total_execution_time_ms += execution_time_ms
            self._health_metrics.last_execution_time_ms = execution_time_ms
            self._health_metrics.last_successful_run = execution_end_time
            
            self._calculate_next_run()
            
            logger.info(f"Task completed: {self.name} (Total runs: {self.run_count}, Execution time: {execution_time_ms:.2f}ms)")
            
        except asyncio.CancelledError:
            logger.info(f"Task cancelled: {self.name}")
            raise
        except asyncio.TimeoutError:
            self.error_count += 1
            self.consecutive_failures += 1
            timeout_exception = asyncio.TimeoutError(f"Task {self.name} timed out")
            self._last_exception = timeout_exception
            self._record_exception(timeout_exception)
            self._health_metrics.total_runs += 1
            self._health_metrics.failed_runs += 1
            self._calculate_next_run()
            logger.error(f"Task {self.name} timed out (Consecutive failures: {self.consecutive_failures})")
            
            self._check_auto_disable(alert_system)
            
        except Exception as e:
            self.error_count += 1
            self.consecutive_failures += 1
            self._last_exception = e
            self._record_exception(e)
            self._health_metrics.total_runs += 1
            self._health_metrics.failed_runs += 1
            self._calculate_next_run()
            logger.error(f"Error executing task {self.name}: {e} (Consecutive failures: {self.consecutive_failures})")
            
            if self.consecutive_failures > 3 and alert_system:
                try:
                    alert_task = asyncio.create_task(
                        alert_system.send_system_error(
                            f"Task '{self.name}' failed {self.consecutive_failures}x consecutively\n"
                            f"Last error: {str(e)}\n"
                            f"Total errors: {self.error_count}/{self.run_count} runs\n"
                            f"Success rate: {self._health_metrics.success_rate:.1f}%"
                        )
                    )
                    try:
                        await asyncio.wait_for(alert_task, timeout=10)
                    except asyncio.TimeoutError:
                        logger.warning(f"Alert task for {self.name} timed out")
                    logger.warning(f"Alert sent: Task {self.name} has {self.consecutive_failures} consecutive failures")
                except Exception as alert_error:
                    logger.error(f"Failed to send task failure alert: {alert_error}")
            
            self._check_auto_disable(alert_system)
            
        finally:
            self.current_execution_task = None
            self._is_executing = False
            self._execution_start_time = None
    
    def _check_auto_disable(self, alert_system=None) -> None:
        """Check and apply auto-disable if threshold is reached."""
        if self.should_auto_disable() and not self._auto_disabled:
            self._auto_disabled = True
            self.enabled = False
            logger.warning(f"Task {self.name} auto-disabled after {self.consecutive_failures} consecutive failures")
            
            if alert_system:
                try:
                    import asyncio
                    asyncio.create_task(
                        alert_system.send_system_error(
                            f"⚠️ Task '{self.name}' has been AUTO-DISABLED\n"
                            f"Reason: {self.consecutive_failures} consecutive failures\n"
                            f"Total errors: {self.error_count}\n"
                            f"Success rate: {self._health_metrics.success_rate:.1f}%\n"
                            f"Last error: {self._last_exception}"
                        )
                    )
                except Exception as e:
                    logger.error(f"Failed to send auto-disable alert: {e}")
    
    def get_last_exception(self) -> Optional[Exception]:
        """Get the last exception raised by this task."""
        return self._last_exception
    
    async def cancel_execution(self, timeout: float = TASK_CANCEL_TIMEOUT) -> bool:
        if self.current_execution_task and not self.current_execution_task.done():
            logger.info(f"Cancelling task execution: {self.name}")
            self.current_execution_task.cancel()
            try:
                await asyncio.wait_for(self.current_execution_task, timeout=timeout)
                logger.info(f"Task {self.name} cancelled successfully")
                return True
            except asyncio.TimeoutError:
                logger.warning(f"Task {self.name} cancellation timed out after {timeout}s")
                return False
            except asyncio.CancelledError:
                logger.info(f"Task {self.name} cancelled")
                return True
        return True
    
    def enable(self, reset_failures: bool = False):
        """Enable the task.
        
        Args:
            reset_failures: If True, reset consecutive failures and auto_disabled flag
        """
        self.enabled = True
        if reset_failures:
            self.consecutive_failures = 0
            self._auto_disabled = False
        self._calculate_next_run()
        logger.info(f"Task enabled: {self.name}" + (" (failures reset)" if reset_failures else ""))
    
    def disable(self):
        self.enabled = False
        logger.info(f"Task disabled: {self.name}")
    
    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'interval': self.interval,
            'schedule_time': self.schedule_time.isoformat() if self.schedule_time else None,
            'enabled': self.enabled,
            'last_run': self.last_run.isoformat() if self.last_run else None,
            'next_run': self.next_run.isoformat() if self.next_run else None,
            'run_count': self.run_count,
            'error_count': self.error_count,
            'consecutive_failures': self.consecutive_failures,
            'is_executing': self._is_executing,
            'auto_disabled': self._auto_disabled,
            'is_stale': self.is_stale(),
            'has_high_failure_rate': self.has_high_failure_rate(),
            'last_exception': str(self._last_exception) if self._last_exception else None,
            'health_metrics': self._health_metrics.to_dict(),
            'exception_history_count': len(self._exception_history)
        }

class TaskScheduler:
    """Task scheduler with background task callbacks and graceful shutdown.
    
    Background Task Callbacks:
    - All scheduled task futures are tracked in _active_task_executions
    - Completion callbacks drain exceptions and log task completion
    - flush_pending_tasks() allows waiting for all tasks with timeout
    - Graceful cancellation with bounded wait timeout
    
    Aggressive Cleanup:
    - Periodic cleanup of completed tasks from _active_task_executions
    - Cleanup of orphaned tasks (tasks without scheduler)
    - Detection of stale tasks that haven't run for extended periods
    """
    def __init__(self, config, alert_system=None, cleanup_interval: int = CLEANUP_INTERVAL_SECONDS):
        self.config = config
        self.alert_system = alert_system
        self.tasks: Dict[str, ScheduledTask] = {}
        self.running = False
        self.scheduler_task = None
        self._cleanup_task = None
        self._shutdown_flag = asyncio.Event()
        self._active_task_executions: Set[asyncio.Task] = set()
        self._all_created_tasks: Set[asyncio.Task] = set()
        self._lock = asyncio.Lock()
        self._task_exceptions: Dict[str, Exception] = {}
        self._exception_history: deque = deque(maxlen=100)
        self._completion_event = asyncio.Event()
        self._cleanup_interval = cleanup_interval
        self._last_cleanup = datetime.now(pytz.timezone('Asia/Jakarta'))
        self._cleanup_stats = {
            'completed_tasks_cleaned': 0,
            'orphaned_tasks_cleaned': 0,
            'stale_tasks_detected': 0,
            'auto_disabled_tasks': 0,
            'last_cleanup_time': None
        }
        logger.info(f"Task scheduler initialized with alert system (cleanup interval: {cleanup_interval}s)")
    
    def _on_task_done(self, task: asyncio.Task, task_name: str) -> None:
        """Callback invoked when a tracked task completes.
        
        Handles exception draining and logging for background tasks.
        """
        try:
            if task.cancelled():
                logger.debug(f"Scheduled task {task_name} was cancelled")
                return
            
            try:
                exception = task.exception()
                if exception:
                    self._task_exceptions[task_name] = exception
                    logger.error(f"Scheduled task {task_name} raised exception: {exception}", exc_info=exception)
            except asyncio.InvalidStateError:
                pass
            except asyncio.CancelledError:
                pass
                
        except Exception as e:
            logger.error(f"Error in task done callback for {task_name}: {e}")
    
    def add_task(self, name: str, func: Callable, interval: Optional[int] = None,
                schedule_time: Optional[time] = None, timezone: str = 'Asia/Jakarta'):
        if name in self.tasks:
            logger.warning(f"Task {name} already exists, replacing")
        
        task = ScheduledTask(name, func, interval, schedule_time, timezone)
        self.tasks[name] = task
        
        logger.info(f"Task added: {name} (interval={interval}s, schedule={schedule_time})")
        return task
    
    def add_interval_task(self, name: str, func: Callable, interval_seconds: int,
                         timezone: str = 'Asia/Jakarta'):
        return self.add_task(name, func, interval=interval_seconds, timezone=timezone)
    
    def add_daily_task(self, name: str, func: Callable, hour: int, minute: int = 0,
                      timezone: str = 'Asia/Jakarta'):
        schedule_time = time(hour=hour, minute=minute)
        return self.add_task(name, func, schedule_time=schedule_time, timezone=timezone)
    
    def remove_task(self, name: str) -> bool:
        if name in self.tasks:
            del self.tasks[name]
            logger.info(f"Task removed: {name}")
            return True
        
        logger.warning(f"Task not found: {name}")
        return False
    
    def enable_task(self, name: str) -> bool:
        if name in self.tasks:
            self.tasks[name].enable()
            return True
        return False
    
    def disable_task(self, name: str) -> bool:
        if name in self.tasks:
            self.tasks[name].disable()
            return True
        return False
    
    def get_task(self, name: str) -> Optional[ScheduledTask]:
        return self.tasks.get(name)
    
    def get_all_tasks(self) -> List[ScheduledTask]:
        return list(self.tasks.values())
    
    def get_pending_exceptions(self) -> Dict[str, Exception]:
        """Get all exceptions from completed tasks.
        
        Returns:
            Dict mapping task names to their exceptions
        """
        return self._task_exceptions.copy()
    
    def clear_exceptions(self) -> None:
        """Clear all stored exceptions."""
        self._task_exceptions.clear()
    
    def record_exception(self, task_name: str, exception: Exception) -> None:
        """Record an exception with timestamp in scheduler-level history."""
        record = ExceptionRecord(
            exception=exception,
            timestamp=datetime.now(pytz.timezone('Asia/Jakarta'))
        )
        self._exception_history.append((task_name, record))
        self._task_exceptions[task_name] = exception
    
    def get_all_exception_history(self, limit: Optional[int] = None) -> List[Dict]:
        """Get exception history across all tasks.
        
        Args:
            limit: Maximum number of recent exceptions to return
            
        Returns:
            List of exception records with task names
        """
        history = list(self._exception_history)
        if limit is not None:
            history = history[-limit:]
        return [
            {
                'task_name': task_name,
                **record.to_dict()
            }
            for task_name, record in history
        ]
    
    def get_task_exception_history(self, task_name: str, limit: Optional[int] = None) -> List[Dict]:
        """Get exception history for a specific task.
        
        Args:
            task_name: Name of the task
            limit: Maximum number of recent exceptions to return
            
        Returns:
            List of exception records for the task
        """
        task = self.tasks.get(task_name)
        if task:
            return task.get_exception_history(limit)
        return []
    
    async def cleanup_completed_tasks(self) -> int:
        """Cleanup completed tasks from _active_task_executions.
        
        Returns:
            Number of tasks cleaned up
        """
        cleaned = 0
        async with self._lock:
            completed_tasks = [t for t in self._active_task_executions if t.done()]
            for task in completed_tasks:
                self._active_task_executions.discard(task)
                cleaned += 1
            
            completed_tracked = [t for t in self._all_created_tasks if t.done()]
            for task in completed_tracked:
                self._all_created_tasks.discard(task)
        
        if cleaned > 0:
            logger.debug(f"Cleaned up {cleaned} completed tasks from active executions")
            self._cleanup_stats['completed_tasks_cleaned'] += cleaned
        
        return cleaned
    
    async def cleanup_orphaned_tasks(self) -> int:
        """Cleanup orphaned asyncio tasks that have no corresponding scheduled task.
        
        Returns:
            Number of orphaned tasks cleaned up
        """
        cleaned = 0
        valid_task_names = set(self.tasks.keys())
        
        async with self._lock:
            orphaned = []
            for task in list(self._all_created_tasks):
                task_name_match = None
                if hasattr(task, 'get_name'):
                    name = task.get_name()
                    for vtn in valid_task_names:
                        if vtn in name:
                            task_name_match = vtn
                            break
                
                if task_name_match is None and not task.done():
                    orphaned.append(task)
            
            for task in orphaned:
                task.cancel()
                cleaned += 1
                self._all_created_tasks.discard(task)
        
        if cleaned > 0:
            logger.warning(f"Cleaned up {cleaned} orphaned tasks")
            self._cleanup_stats['orphaned_tasks_cleaned'] += cleaned
        
        return cleaned
    
    def detect_stale_tasks(self, threshold_minutes: int = STALE_TASK_THRESHOLD_MINUTES) -> List[str]:
        """Detect tasks that are stale (enabled but haven't run for extended period).
        
        Args:
            threshold_minutes: Minutes without running to be considered stale
            
        Returns:
            List of stale task names
        """
        stale_tasks = []
        for task_name, task in self.tasks.items():
            if task.is_stale(threshold_minutes):
                stale_tasks.append(task_name)
        
        if stale_tasks:
            logger.warning(f"Detected {len(stale_tasks)} stale tasks: {stale_tasks}")
            self._cleanup_stats['stale_tasks_detected'] = len(stale_tasks)
        
        return stale_tasks
    
    def detect_high_failure_tasks(self, threshold: int = 5) -> List[str]:
        """Detect tasks with high consecutive failure rates.
        
        Args:
            threshold: Number of consecutive failures to be considered high
            
        Returns:
            List of task names with high failure rates
        """
        failing_tasks = []
        for task_name, task in self.tasks.items():
            if task.has_high_failure_rate(threshold):
                failing_tasks.append(task_name)
        
        if failing_tasks:
            logger.warning(f"Detected {len(failing_tasks)} tasks with high failure rates: {failing_tasks}")
        
        return failing_tasks
    
    def get_auto_disabled_tasks(self) -> List[str]:
        """Get list of tasks that have been auto-disabled."""
        return [
            task_name for task_name, task in self.tasks.items()
            if task._auto_disabled
        ]
    
    def get_all_health_metrics(self) -> Dict[str, Dict]:
        """Get health metrics for all tasks.
        
        Returns:
            Dict mapping task names to their health metrics
        """
        return {
            task_name: task.get_health_metrics()
            for task_name, task in self.tasks.items()
        }
    
    def get_scheduler_health_summary(self) -> Dict:
        """Get overall scheduler health summary."""
        tasks = list(self.tasks.values())
        enabled_tasks = [t for t in tasks if t.enabled]
        executing_tasks = [t for t in tasks if t._is_executing]
        stale_tasks = [t for t in tasks if t.is_stale()]
        failing_tasks = [t for t in tasks if t.has_high_failure_rate()]
        auto_disabled = [t for t in tasks if t._auto_disabled]
        
        total_runs = sum(t.run_count for t in tasks)
        total_errors = sum(t.error_count for t in tasks)
        overall_success_rate = ((total_runs - total_errors) / total_runs * 100) if total_runs > 0 else 0
        
        return {
            'running': self.running,
            'total_tasks': len(tasks),
            'enabled_tasks': len(enabled_tasks),
            'executing_tasks': len(executing_tasks),
            'stale_tasks': len(stale_tasks),
            'failing_tasks': len(failing_tasks),
            'auto_disabled_tasks': len(auto_disabled),
            'active_executions': len(self._active_task_executions),
            'tracked_tasks': len(self._all_created_tasks),
            'total_runs': total_runs,
            'total_errors': total_errors,
            'overall_success_rate': round(overall_success_rate, 2),
            'exception_history_count': len(self._exception_history),
            'cleanup_stats': self._cleanup_stats.copy()
        }
    
    async def run_aggressive_cleanup(self) -> Dict:
        """Run aggressive cleanup of all task-related resources.
        
        Returns:
            Dict with cleanup results
        """
        logger.info("Running aggressive cleanup...")
        
        completed_cleaned = await self.cleanup_completed_tasks()
        orphaned_cleaned = await self.cleanup_orphaned_tasks()
        stale_tasks = self.detect_stale_tasks()
        failing_tasks = self.detect_high_failure_tasks()
        
        gc.collect()
        
        now = datetime.now(pytz.timezone('Asia/Jakarta'))
        self._cleanup_stats['last_cleanup_time'] = now.isoformat()
        self._last_cleanup = now
        
        results = {
            'completed_tasks_cleaned': completed_cleaned,
            'orphaned_tasks_cleaned': orphaned_cleaned,
            'stale_tasks': stale_tasks,
            'failing_tasks': failing_tasks,
            'auto_disabled_tasks': self.get_auto_disabled_tasks(),
            'timestamp': now.isoformat()
        }
        
        logger.info(f"Aggressive cleanup completed: {results}")
        return results
    
    async def _cleanup_loop(self):
        """Background loop for periodic cleanup."""
        logger.info(f"Cleanup loop started (interval: {self._cleanup_interval}s)")
        
        try:
            while self.running and not self._shutdown_flag.is_set():
                try:
                    await asyncio.wait_for(
                        self._shutdown_flag.wait(),
                        timeout=float(self._cleanup_interval)
                    )
                    if self._shutdown_flag.is_set():
                        break
                except asyncio.TimeoutError:
                    if not self._shutdown_flag.is_set():
                        try:
                            await self.run_aggressive_cleanup()
                        except Exception as e:
                            logger.error(f"Error during cleanup: {e}")
        except asyncio.CancelledError:
            logger.info("Cleanup loop cancelled")
        finally:
            logger.info("Cleanup loop exiting")
    
    def enable_task_with_reset(self, name: str) -> bool:
        """Enable a task and reset its failure counters.
        
        Args:
            name: Name of the task to enable
            
        Returns:
            bool: True if task was enabled, False if not found
        """
        if name in self.tasks:
            self.tasks[name].enable(reset_failures=True)
            return True
        return False
    
    async def _track_task(self, task: asyncio.Task, task_name: str):
        """Track a task and handle its completion with exception draining."""
        async with self._lock:
            self._active_task_executions.add(task)
            self._all_created_tasks.add(task)
            self._completion_event.clear()
        
        try:
            await task
        except asyncio.CancelledError:
            logger.debug(f"Tracked task {task_name} was cancelled")
        except Exception as e:
            self._task_exceptions[task_name] = e
            logger.error(f"Tracked task {task_name} failed: {e}")
        finally:
            async with self._lock:
                self._active_task_executions.discard(task)
                self._all_created_tasks.discard(task)
                if len(self._active_task_executions) == 0:
                    self._completion_event.set()
    
    async def flush_pending_tasks(self, timeout: float = FLUSH_PENDING_TIMEOUT) -> bool:
        """Wait for all pending task executions to complete.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            bool: True if all tasks completed, False if timeout occurred
        """
        async with self._lock:
            if not self._active_task_executions:
                return True
            pending_count = len(self._active_task_executions)
        
        logger.info(f"Flushing {pending_count} pending task executions (timeout={timeout}s)...")
        
        try:
            await asyncio.wait_for(self._completion_event.wait(), timeout=timeout)
            logger.info("All pending task executions completed")
            return True
        except asyncio.TimeoutError:
            async with self._lock:
                remaining = len(self._active_task_executions)
            logger.warning(f"Flush timeout: {remaining} tasks still pending after {timeout}s")
            return False
    
    async def _scheduler_loop(self):
        logger.info("Scheduler loop started")
        
        try:
            while self.running and not self._shutdown_flag.is_set():
                try:
                    if self._shutdown_flag.is_set():
                        logger.info("Shutdown flag detected in scheduler loop")
                        break
                    
                    for task in list(self.tasks.values()):
                        if self._shutdown_flag.is_set():
                            logger.info("Shutdown flag detected, stopping task scheduling")
                            break
                        
                        if task.should_run():
                            def make_done_callback(task_name: str):
                                def callback(t: asyncio.Task):
                                    self._on_task_done(t, task_name)
                                return callback
                            
                            execution_task = asyncio.create_task(
                                task.execute(
                                    alert_system=self.alert_system,
                                    shutdown_flag=self._shutdown_flag,
                                    on_complete=lambda t, n: None
                                )
                            )
                            execution_task.set_name(f"task_exec_{task.name}")
                            execution_task.add_done_callback(make_done_callback(task.name))
                            
                            tracking_task = asyncio.create_task(
                                self._track_task(execution_task, task.name)
                            )
                            tracking_task.set_name(f"track_{task.name}")
                    
                    try:
                        await asyncio.wait_for(
                            self._shutdown_flag.wait(),
                            timeout=1.0
                        )
                        if self._shutdown_flag.is_set():
                            logger.info("Shutdown flag set, exiting scheduler loop")
                            break
                    except asyncio.TimeoutError:
                        pass
                    
                except asyncio.CancelledError:
                    logger.info("Scheduler loop cancelled")
                    break
                except Exception as e:
                    logger.error(f"Error in scheduler loop: {e}")
                    if not self._shutdown_flag.is_set():
                        await asyncio.sleep(5)
        except asyncio.CancelledError:
            logger.info("Scheduler loop received cancellation at top level")
        finally:
            logger.info("Scheduler loop exiting")
    
    async def start(self):
        if self.running:
            logger.warning("Scheduler already running")
            return
        
        self._shutdown_flag.clear()
        self._completion_event.clear()
        self.running = True
        self.scheduler_task = asyncio.create_task(self._scheduler_loop())
        self.scheduler_task.set_name("scheduler_loop")
        
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        self._cleanup_task.set_name("cleanup_loop")
        
        logger.info("Task scheduler started with cleanup loop")
    
    async def stop(self, graceful_timeout: float = SCHEDULER_STOP_TIMEOUT):
        """Stop the scheduler with graceful shutdown.
        
        Args:
            graceful_timeout: Maximum time to wait for graceful shutdown
        """
        logger.info("=" * 50)
        logger.info("STOPPING TASK SCHEDULER")
        logger.info(f"Active task executions: {len(self._active_task_executions)}")
        logger.info(f"Total tracked tasks: {len(self._all_created_tasks)}")
        logger.info("=" * 50)
        
        if not self.running:
            logger.warning("Scheduler not running")
            return
        
        self._shutdown_flag.set()
        self.running = False
        
        logger.info("Shutdown flag set, waiting for scheduler loop to stop...")
        
        if self._cleanup_task and not self._cleanup_task.done():
            logger.info("Cancelling cleanup loop task...")
            self._cleanup_task.cancel()
            try:
                await asyncio.wait_for(self._cleanup_task, timeout=TASK_CANCEL_TIMEOUT)
                logger.info("✅ Cleanup loop stopped gracefully")
            except asyncio.TimeoutError:
                logger.warning(f"Cleanup loop cancellation timed out after {TASK_CANCEL_TIMEOUT}s")
            except asyncio.CancelledError:
                logger.info("✅ Cleanup loop cancelled")
            except Exception as e:
                logger.error(f"Error stopping cleanup loop: {e}")
        
        if self.scheduler_task and not self.scheduler_task.done():
            logger.info("Cancelling scheduler loop task...")
            self.scheduler_task.cancel()
            try:
                await asyncio.wait_for(self.scheduler_task, timeout=TASK_CANCEL_TIMEOUT)
                logger.info("✅ Scheduler loop stopped gracefully")
            except asyncio.TimeoutError:
                logger.warning(f"Scheduler loop cancellation timed out after {TASK_CANCEL_TIMEOUT}s")
            except asyncio.CancelledError:
                logger.info("✅ Scheduler loop cancelled")
            except Exception as e:
                logger.error(f"Error stopping scheduler loop: {e}")
        
        logger.info("Cancelling individual task executions...")
        cancelled_count = 0
        for task_name, task in self.tasks.items():
            if task._is_executing:
                logger.info(f"Cancelling running task: {task_name}")
                success = await task.cancel_execution(timeout=TASK_CANCEL_TIMEOUT)
                if success:
                    cancelled_count += 1
                else:
                    logger.warning(f"Failed to cancel task: {task_name}")
        logger.info(f"Cancelled {cancelled_count} individual task executions")
        
        async with self._lock:
            active_tasks = list(self._active_task_executions)
        
        active_count = len(active_tasks)
        if active_count > 0:
            logger.info(f"Cancelling {active_count} remaining active task executions...")
            
            for task_exec in active_tasks:
                if not task_exec.done():
                    task_exec.cancel()
            
            if active_tasks:
                try:
                    done, pending = await asyncio.wait(
                        active_tasks,
                        timeout=graceful_timeout,
                        return_when=asyncio.ALL_COMPLETED
                    )
                    
                    completed = len(done)
                    still_pending = len(pending)
                    
                    if still_pending > 0:
                        logger.warning(f"{still_pending} task executions did not complete within timeout")
                        for p in pending:
                            p.cancel()
                    else:
                        logger.info(f"✅ All {completed} task executions completed")
                except Exception as e:
                    logger.error(f"Error waiting for task executions: {e}")
        else:
            logger.info("No active task executions to cancel")
        
        async with self._lock:
            remaining = len(self._all_created_tasks)
            if remaining > 0:
                logger.info(f"Cleaning up {remaining} remaining tracked tasks...")
                for task in list(self._all_created_tasks):
                    if not task.done():
                        task.cancel()
                self._all_created_tasks.clear()
            
            self._active_task_executions.clear()
        
        self._completion_event.set()
        
        exceptions_count = len(self._task_exceptions)
        if exceptions_count > 0:
            logger.warning(f"{exceptions_count} tasks had exceptions during this session")
            for task_name, exc in self._task_exceptions.items():
                logger.debug(f"  - {task_name}: {exc}")
        
        logger.info("=" * 50)
        logger.info("TASK SCHEDULER STOPPED SUCCESSFULLY")
        logger.info(f"Final state - Running: {self.running}, Shutdown flag: {self._shutdown_flag.is_set()}")
        logger.info("=" * 50)
    
    def get_status(self) -> Dict:
        tasks = list(self.tasks.values())
        stale_tasks = [t for t in tasks if t.is_stale()]
        failing_tasks = [t for t in tasks if t.has_high_failure_rate()]
        auto_disabled = [t for t in tasks if t._auto_disabled]
        
        return {
            'running': self.running,
            'shutting_down': self._shutdown_flag.is_set(),
            'total_tasks': len(self.tasks),
            'enabled_tasks': len([t for t in tasks if t.enabled]),
            'executing_tasks': len([t for t in tasks if t._is_executing]),
            'stale_tasks': len(stale_tasks),
            'failing_tasks': len(failing_tasks),
            'auto_disabled_tasks': len(auto_disabled),
            'active_executions': len(self._active_task_executions),
            'total_tracked_tasks': len(self._all_created_tasks),
            'pending_exceptions': len(self._task_exceptions),
            'exception_history_count': len(self._exception_history),
            'cleanup_stats': self._cleanup_stats.copy(),
            'tasks': {name: task.to_dict() for name, task in self.tasks.items()}
        }
    
    def format_task_list(self) -> str:
        if not self.tasks:
            return "Tidak ada task yang dijadwalkan"
        
        msg = "📅 *Scheduled Tasks*\n\n"
        
        for task in self.tasks.values():
            if task._auto_disabled:
                status_icon = '🚫'
            elif task.enabled:
                status_icon = '✅'
            else:
                status_icon = '⛔'
            
            exec_icon = '🔄' if task._is_executing else ''
            stale_icon = '⏰' if task.is_stale() else ''
            fail_icon = '⚠️' if task.has_high_failure_rate() else ''
            
            msg += f"{status_icon}{exec_icon}{stale_icon}{fail_icon} *{task.name}*\n"
            
            if task.interval:
                msg += f"Interval: {task.interval}s\n"
            elif task.schedule_time:
                msg += f"Scheduled: {task.schedule_time.strftime('%H:%M')}\n"
            
            if task.last_run:
                msg += f"Last Run: {task.last_run.strftime('%Y-%m-%d %H:%M')}\n"
            
            if task.next_run:
                msg += f"Next Run: {task.next_run.strftime('%Y-%m-%d %H:%M')}\n"
            
            success_rate = task._health_metrics.success_rate
            msg += f"Runs: {task.run_count} | Errors: {task.error_count} | Success: {success_rate:.1f}%\n"
            
            if task.consecutive_failures > 0:
                msg += f"Consecutive Failures: {task.consecutive_failures}\n"
            
            if task._auto_disabled:
                msg += "⚠️ AUTO-DISABLED due to failures\n"
            
            msg += "\n"
        
        return msg

def setup_default_tasks(scheduler: TaskScheduler, bot_components: Dict):
    logger.info("Setting up default scheduled tasks")
    
    async def cleanup_old_charts():
        chart_generator = bot_components.get('chart_generator')
        if chart_generator:
            chart_generator.cleanup_old_charts(days=7)
    
    async def send_daily_summary():
        alert_system = bot_components.get('alert_system')
        if alert_system:
            await alert_system.send_daily_summary()
    
    async def cleanup_database():
        db_manager = bot_components.get('db_manager')
        if db_manager:
            session = db_manager.get_session()
            try:
                from bot.database import Trade
                from datetime import datetime, timedelta
                
                cutoff = datetime.utcnow() - timedelta(days=90)
                old_trades = session.query(Trade).filter(
                    Trade.signal_time < cutoff,
                    Trade.status == 'CLOSED'
                ).delete()
                
                session.commit()
                logger.info(f"Deleted {old_trades} old trade records")
            except Exception as e:
                logger.error(f"Error cleaning database: {e}")
                try:
                    session.rollback()
                except Exception as rollback_error:
                    logger.error(f"Error during rollback: {rollback_error}")
            finally:
                try:
                    session.close()
                except Exception as close_error:
                    logger.error(f"Error closing session: {close_error}")
    
    async def health_check():
        logger.info("Running health check...")
        market_data = bot_components.get('market_data')
        if market_data:
            price = await market_data.get_current_price()
            if price:
                logger.info(f"Health check OK - Current price: {price}")
            else:
                logger.warning("Health check: No price data available")
    
    async def monitor_positions():
        position_tracker = bot_components.get('position_tracker')
        if position_tracker:
            total_active = sum(len(positions) for positions in position_tracker.active_positions.values())
            if total_active == 0:
                logger.debug("Position monitoring: Skip - tidak ada active positions")
                return
            
            updated = await position_tracker.monitor_active_positions()
            if updated:
                logger.info(f"Position monitoring: {len(updated)} positions updated")
            else:
                logger.debug("Position monitoring: Tidak ada perubahan")
    
    async def periodic_gc():
        gc.collect()
        logger.debug("Periodic garbage collection completed")
    
    async def save_candles_periodic():
        market_data = bot_components.get('market_data')
        db_manager = bot_components.get('db_manager')
        if market_data and db_manager:
            try:
                await market_data.save_candles_to_db(db_manager)
                market_data._prune_old_candles(db_manager, keep_count=150)
            except Exception as e:
                logger.error(f"Error in periodic candle save: {e}")
    
    scheduler.add_interval_task(
        'cleanup_charts',
        cleanup_old_charts,
        interval_seconds=1800
    )
    
    async def aggressive_chart_cleanup():
        chart_generator = bot_components.get('chart_generator')
        if chart_generator:
            import os
            chart_dir = chart_generator.chart_dir
            max_charts = 10
            max_age_minutes = 30
            
            try:
                if os.path.exists(chart_dir):
                    import time
                    current_time = time.time()
                    files = []
                    
                    for f in os.listdir(chart_dir):
                        if f.endswith('.png'):
                            file_path = os.path.join(chart_dir, f)
                            file_age_minutes = (current_time - os.path.getmtime(file_path)) / 60
                            files.append((f, file_path, file_age_minutes))
                    
                    files.sort(key=lambda x: x[2], reverse=True)
                    
                    deleted_count = 0
                    for f, file_path, age in files:
                        if age > max_age_minutes or len(files) - deleted_count > max_charts:
                            try:
                                os.remove(file_path)
                                deleted_count += 1
                                logger.debug(f"Deleted chart: {f} (age: {age:.1f}min)")
                            except Exception as e:
                                logger.warning(f"Failed to delete chart {f}: {e}")
                    
                    if deleted_count > 0:
                        logger.info(f"Aggressive cleanup: removed {deleted_count} old charts")
            except Exception as e:
                logger.error(f"Error in aggressive chart cleanup: {e}")
    
    scheduler.add_interval_task(
        'aggressive_chart_cleanup',
        aggressive_chart_cleanup,
        interval_seconds=300
    )
    
    scheduler.add_daily_task(
        'daily_summary',
        send_daily_summary,
        hour=23,
        minute=55
    )
    
    scheduler.add_daily_task(
        'database_cleanup',
        cleanup_database,
        hour=3,
        minute=0
    )
    
    scheduler.add_interval_task(
        'health_check',
        health_check,
        interval_seconds=300
    )
    
    scheduler.add_interval_task(
        'position_monitoring',
        monitor_positions,
        interval_seconds=10
    )
    
    scheduler.add_interval_task(
        'periodic_gc',
        periodic_gc,
        interval_seconds=300
    )
    
    scheduler.add_interval_task(
        'save_candles',
        save_candles_periodic,
        interval_seconds=60
    )
    
    logger.info(f"Configured {len(scheduler.tasks)} default scheduled tasks")
