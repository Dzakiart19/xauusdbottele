import asyncio
import gc
from datetime import datetime, time, timedelta
from typing import Callable, Optional, Dict, List, Set
import pytz
from bot.logger import setup_logger

logger = setup_logger('TaskScheduler')

TASK_EXECUTION_TIMEOUT = 60
TASK_CANCEL_TIMEOUT = 5
SCHEDULER_STOP_TIMEOUT = 10

class ScheduledTask:
    def __init__(self, name: str, func: Callable, interval: Optional[int] = None,
                 schedule_time: Optional[time] = None, timezone: str = 'Asia/Jakarta'):
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
    
    async def execute(self, alert_system=None, shutdown_flag: Optional[asyncio.Event] = None):
        if self._is_executing:
            logger.warning(f"Task {self.name} is already executing, skipping")
            return
        
        self._is_executing = True
        try:
            logger.info(f"Executing scheduled task: {self.name}")
            
            if shutdown_flag and shutdown_flag.is_set():
                logger.info(f"Shutdown requested, skipping task: {self.name}")
                return
            
            if asyncio.iscoroutinefunction(self.func):
                self.current_execution_task = asyncio.create_task(self.func())
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
            
            self.last_run = datetime.now(self.timezone)
            self.run_count += 1
            self.consecutive_failures = 0
            self._calculate_next_run()
            
            logger.info(f"Task completed: {self.name} (Total runs: {self.run_count})")
            
        except asyncio.CancelledError:
            logger.info(f"Task cancelled: {self.name}")
            raise
        except asyncio.TimeoutError:
            self.error_count += 1
            self.consecutive_failures += 1
            self._calculate_next_run()
            logger.error(f"Task {self.name} timed out (Consecutive failures: {self.consecutive_failures})")
        except Exception as e:
            self.error_count += 1
            self.consecutive_failures += 1
            self._calculate_next_run()
            logger.error(f"Error executing task {self.name}: {e} (Consecutive failures: {self.consecutive_failures})")
            
            if self.consecutive_failures > 3 and alert_system:
                try:
                    alert_task = asyncio.create_task(
                        alert_system.send_system_error(
                            f"Task '{self.name}' failed {self.consecutive_failures}x consecutively\n"
                            f"Last error: {str(e)}\n"
                            f"Total errors: {self.error_count}/{self.run_count} runs"
                        )
                    )
                    try:
                        await asyncio.wait_for(alert_task, timeout=10)
                    except asyncio.TimeoutError:
                        logger.warning(f"Alert task for {self.name} timed out")
                    logger.warning(f"Alert sent: Task {self.name} has {self.consecutive_failures} consecutive failures")
                except Exception as alert_error:
                    logger.error(f"Failed to send task failure alert: {alert_error}")
        finally:
            self.current_execution_task = None
            self._is_executing = False
    
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
    
    def enable(self):
        self.enabled = True
        self._calculate_next_run()
        logger.info(f"Task enabled: {self.name}")
    
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
            'is_executing': self._is_executing
        }

class TaskScheduler:
    def __init__(self, config, alert_system=None):
        self.config = config
        self.alert_system = alert_system
        self.tasks: Dict[str, ScheduledTask] = {}
        self.running = False
        self.scheduler_task = None
        self._shutdown_flag = asyncio.Event()
        self._active_task_executions: Set[asyncio.Task] = set()
        self._all_created_tasks: Set[asyncio.Task] = set()
        self._lock = asyncio.Lock()
        logger.info("Task scheduler initialized with alert system")
    
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
    
    async def _track_task(self, task: asyncio.Task, task_name: str):
        async with self._lock:
            self._active_task_executions.add(task)
            self._all_created_tasks.add(task)
        
        try:
            await task
        except asyncio.CancelledError:
            logger.debug(f"Tracked task {task_name} was cancelled")
        except Exception as e:
            logger.error(f"Tracked task {task_name} failed: {e}")
        finally:
            async with self._lock:
                self._active_task_executions.discard(task)
                self._all_created_tasks.discard(task)
    
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
                            execution_task = asyncio.create_task(
                                task.execute(
                                    alert_system=self.alert_system,
                                    shutdown_flag=self._shutdown_flag
                                )
                            )
                            execution_task.set_name(f"task_exec_{task.name}")
                            
                            asyncio.create_task(self._track_task(execution_task, task.name))
                    
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
        self.running = True
        self.scheduler_task = asyncio.create_task(self._scheduler_loop())
        self.scheduler_task.set_name("scheduler_loop")
        logger.info("Task scheduler started")
    
    async def stop(self):
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
                        timeout=SCHEDULER_STOP_TIMEOUT,
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
        
        logger.info("=" * 50)
        logger.info("TASK SCHEDULER STOPPED SUCCESSFULLY")
        logger.info(f"Final state - Running: {self.running}, Shutdown flag: {self._shutdown_flag.is_set()}")
        logger.info("=" * 50)
    
    def get_status(self) -> Dict:
        return {
            'running': self.running,
            'shutting_down': self._shutdown_flag.is_set(),
            'total_tasks': len(self.tasks),
            'enabled_tasks': len([t for t in self.tasks.values() if t.enabled]),
            'executing_tasks': len([t for t in self.tasks.values() if t._is_executing]),
            'active_executions': len(self._active_task_executions),
            'total_tracked_tasks': len(self._all_created_tasks),
            'tasks': {name: task.to_dict() for name, task in self.tasks.items()}
        }
    
    def format_task_list(self) -> str:
        if not self.tasks:
            return "Tidak ada task yang dijadwalkan"
        
        msg = "📅 *Scheduled Tasks*\n\n"
        
        for task in self.tasks.values():
            status_icon = '✅' if task.enabled else '⛔'
            exec_icon = '🔄' if task._is_executing else ''
            
            msg += f"{status_icon}{exec_icon} *{task.name}*\n"
            
            if task.interval:
                msg += f"Interval: {task.interval}s\n"
            elif task.schedule_time:
                msg += f"Scheduled: {task.schedule_time.strftime('%H:%M')}\n"
            
            if task.last_run:
                msg += f"Last Run: {task.last_run.strftime('%Y-%m-%d %H:%M')}\n"
            
            if task.next_run:
                msg += f"Next Run: {task.next_run.strftime('%Y-%m-%d %H:%M')}\n"
            
            msg += f"Runs: {task.run_count} | Errors: {task.error_count}\n\n"
        
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
                session.rollback()
            finally:
                session.close()
    
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
        hour=2,
        minute=0
    )
    
    scheduler.add_interval_task(
        'health_check',
        health_check,
        interval_seconds=300
    )
    
    scheduler.add_interval_task(
        'monitor_positions',
        monitor_positions,
        interval_seconds=10
    )
    
    scheduler.add_interval_task(
        'garbage_collection',
        periodic_gc,
        interval_seconds=300
    )
    
    scheduler.add_interval_task(
        'save_candles',
        save_candles_periodic,
        interval_seconds=300
    )
    
    logger.info("Default tasks setup completed")
