import asyncio
import psutil
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from collections import defaultdict, deque
import pytz
import json
from bot.logger import setup_logger

logger = setup_logger('PerformanceMonitor')

class SystemMonitor:
    """Monitor system resources (CPU, memory, WebSocket health)"""
    
    def __init__(self, config=None):
        self.config = config
        self.process = psutil.Process()
        self.ws_connection_status = 'unknown'
        self.ws_last_heartbeat = None
        self.ws_reconnection_count = 0
        logger.info("SystemMonitor initialized")
    
    def get_cpu_usage(self) -> float:
        """Get current CPU usage percentage"""
        try:
            return self.process.cpu_percent(interval=0.1)
        except Exception as e:
            logger.error(f"Error getting CPU usage: {e}")
            return 0.0
    
    def get_memory_usage(self) -> Dict[str, Any]:
        """Get memory usage statistics"""
        try:
            mem_info = self.process.memory_info()
            mem_percent = self.process.memory_percent()
            
            return {
                'rss_mb': round(mem_info.rss / (1024 * 1024), 2),
                'vms_mb': round(mem_info.vms / (1024 * 1024), 2),
                'percent': round(mem_percent, 2)
            }
        except Exception as e:
            logger.error(f"Error getting memory usage: {e}")
            return {'rss_mb': 0.0, 'vms_mb': 0.0, 'percent': 0.0}
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Get overall system statistics"""
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                'system_cpu_percent': round(cpu_percent, 2),
                'system_memory_percent': round(memory.percent, 2),
                'system_memory_available_mb': round(memory.available / (1024 * 1024), 2),
                'disk_usage_percent': round(disk.percent, 2),
                'disk_free_gb': round(disk.free / (1024 * 1024 * 1024), 2)
            }
        except Exception as e:
            logger.error(f"Error getting system stats: {e}")
            return {}
    
    def update_ws_status(self, status: str, is_heartbeat: bool = False):
        """Update WebSocket connection status
        
        Args:
            status: 'connected', 'disconnected', 'reconnecting', 'error'
            is_heartbeat: True if this is a heartbeat update
        """
        try:
            self.ws_connection_status = status
            
            if is_heartbeat or status == 'connected':
                self.ws_last_heartbeat = datetime.now(pytz.UTC)
            
            if status == 'reconnecting':
                self.ws_reconnection_count += 1
            
            logger.debug(f"WebSocket status updated: {status}")
        except Exception as e:
            logger.error(f"Error updating WS status: {e}")
    
    def get_ws_health(self) -> Dict[str, Any]:
        """Get WebSocket connection health status"""
        try:
            health = {
                'status': self.ws_connection_status,
                'reconnection_count': self.ws_reconnection_count,
                'last_heartbeat': None,
                'seconds_since_heartbeat': None,
                'health_status': 'unknown'
            }
            
            if self.ws_last_heartbeat:
                health['last_heartbeat'] = self.ws_last_heartbeat.isoformat()
                seconds_since = (datetime.now(pytz.UTC) - self.ws_last_heartbeat).total_seconds()
                health['seconds_since_heartbeat'] = round(seconds_since, 1)
                
                if seconds_since < 30:
                    health['health_status'] = 'healthy'
                elif seconds_since < 60:
                    health['health_status'] = 'warning'
                else:
                    health['health_status'] = 'critical'
            
            return health
        except Exception as e:
            logger.error(f"Error getting WS health: {e}")
            return {'status': 'error', 'health_status': 'unknown'}
    
    def get_process_info(self) -> Dict[str, Any]:
        """Get current process information"""
        try:
            return {
                'pid': self.process.pid,
                'cpu_percent': self.get_cpu_usage(),
                'memory': self.get_memory_usage(),
                'num_threads': self.process.num_threads(),
                'create_time': datetime.fromtimestamp(self.process.create_time(), tz=pytz.UTC).isoformat(),
                'uptime_seconds': round(time.time() - self.process.create_time(), 2)
            }
        except Exception as e:
            logger.error(f"Error getting process info: {e}")
            return {}
    
    def get_comprehensive_health(self) -> Dict[str, Any]:
        """Get comprehensive system health report"""
        try:
            return {
                'timestamp': datetime.now(pytz.UTC).isoformat(),
                'process': self.get_process_info(),
                'system': self.get_system_stats(),
                'websocket': self.get_ws_health()
            }
        except Exception as e:
            logger.error(f"Error getting comprehensive health: {e}")
            return {'error': str(e)}


class TrackingMetrics:
    """Track signal generation rate, execution time, and performance metrics"""
    
    MAX_OPERATION_TYPES = 50
    MAX_TIMES_PER_OPERATION = 100
    CLEANUP_INTERVAL_SECONDS = 300
    
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        
        self.signal_times = deque(maxlen=window_size)
        self.execution_times = defaultdict(lambda: deque(maxlen=window_size))
        
        self.signal_count = 0
        self.signal_accepted_count = 0
        self.signal_rejected_count = 0
        
        self.operation_counts = defaultdict(int)
        self.operation_times: Dict[str, deque] = {}
        self._last_cleanup = datetime.now(pytz.UTC)
        
        logger.info(f"TrackingMetrics initialized with window size {window_size}")
    
    def record_signal(self, accepted: bool = True):
        """Record a signal generation event
        
        Args:
            accepted: Whether the signal was accepted or rejected
        """
        try:
            self.signal_times.append(datetime.now(pytz.UTC))
            self.signal_count += 1
            
            if accepted:
                self.signal_accepted_count += 1
            else:
                self.signal_rejected_count += 1
            
            logger.debug(f"Signal recorded: accepted={accepted}, total={self.signal_count}")
        except Exception as e:
            logger.error(f"Error recording signal: {e}")
    
    def record_execution_time(self, operation: str, execution_time: float):
        """Record execution time for an operation
        
        Args:
            operation: Name of the operation (e.g., 'signal_detection', 'chart_generation')
            execution_time: Time in seconds
        """
        try:
            self._maybe_cleanup()
            
            self.execution_times[operation].append(execution_time)
            self.operation_counts[operation] += 1
            
            if operation not in self.operation_times:
                if len(self.operation_times) >= self.MAX_OPERATION_TYPES:
                    oldest_key = next(iter(self.operation_times))
                    del self.operation_times[oldest_key]
                    logger.debug(f"Removed oldest operation type: {oldest_key}")
                self.operation_times[operation] = deque(maxlen=self.MAX_TIMES_PER_OPERATION)
            
            self.operation_times[operation].append(execution_time)
            
            logger.debug(f"Execution time recorded: {operation} = {execution_time:.3f}s")
        except Exception as e:
            logger.error(f"Error recording execution time: {e}")
    
    def _maybe_cleanup(self):
        """Cleanup old data if interval has passed"""
        try:
            now = datetime.now(pytz.UTC)
            if (now - self._last_cleanup).total_seconds() >= self.CLEANUP_INTERVAL_SECONDS:
                self._cleanup_old_data()
                self._last_cleanup = now
        except Exception as e:
            logger.error(f"Error in cleanup check: {e}")
    
    def _cleanup_old_data(self):
        """Cleanup stale operation data to prevent memory leaks"""
        try:
            initial_op_count = len(self.operation_counts)
            initial_times_count = len(self.operation_times)
            
            if len(self.operation_counts) > self.MAX_OPERATION_TYPES:
                sorted_ops = sorted(self.operation_counts.items(), key=lambda x: x[1], reverse=True)
                self.operation_counts = defaultdict(int, dict(sorted_ops[:self.MAX_OPERATION_TYPES]))
            
            op_count_cleaned = initial_op_count - len(self.operation_counts)
            times_cleaned = initial_times_count - len(self.operation_times)
            
            if op_count_cleaned > 0 or times_cleaned > 0:
                logger.info(f"TrackingMetrics cleanup: removed {op_count_cleaned} op_counts, {times_cleaned} op_times entries")
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
    
    def reset_counters(self):
        """Reset all counters to prevent long-term overflow"""
        try:
            self.signal_count = 0
            self.signal_accepted_count = 0
            self.signal_rejected_count = 0
            self.operation_counts.clear()
            self.operation_times.clear()
            logger.info("TrackingMetrics counters reset")
        except Exception as e:
            logger.error(f"Error resetting counters: {e}")
    
    def get_signal_rate(self, minutes: int = 60) -> Dict[str, Any]:
        """Get signal generation rate over specified time window
        
        Args:
            minutes: Time window in minutes
        
        Returns:
            Dict with signal rate statistics
        """
        try:
            if not self.signal_times:
                return {
                    'signals_per_hour': 0.0,
                    'signals_in_window': 0,
                    'acceptance_rate': 0.0,
                    'rejection_rate': 0.0
                }
            
            cutoff_time = datetime.now(pytz.UTC) - timedelta(minutes=minutes)
            recent_signals = [t for t in self.signal_times if t >= cutoff_time]
            
            signals_count = len(recent_signals)
            signals_per_hour = (signals_count / minutes) * 60 if minutes > 0 else 0.0
            
            acceptance_rate = (self.signal_accepted_count / self.signal_count * 100) if self.signal_count > 0 else 0.0
            rejection_rate = (self.signal_rejected_count / self.signal_count * 100) if self.signal_count > 0 else 0.0
            
            return {
                'signals_per_hour': round(signals_per_hour, 2),
                'signals_in_window': signals_count,
                'total_signals': self.signal_count,
                'accepted_signals': self.signal_accepted_count,
                'rejected_signals': self.signal_rejected_count,
                'acceptance_rate': round(acceptance_rate, 2),
                'rejection_rate': round(rejection_rate, 2),
                'window_minutes': minutes
            }
        except Exception as e:
            logger.error(f"Error getting signal rate: {e}")
            return {'error': str(e)}
    
    def get_execution_stats(self, operation: Optional[str] = None) -> Dict[str, Any]:
        """Get execution time statistics
        
        Args:
            operation: Specific operation name (None for all)
        
        Returns:
            Dict with execution time statistics
        """
        try:
            if operation:
                if operation not in self.execution_times or not self.execution_times[operation]:
                    return {
                        'operation': operation,
                        'count': 0,
                        'avg_time': 0.0,
                        'min_time': 0.0,
                        'max_time': 0.0
                    }
                
                times = list(self.execution_times[operation])
                if not times:
                    return {
                        'operation': operation,
                        'count': 0,
                        'avg_time': 0.0,
                        'min_time': 0.0,
                        'max_time': 0.0
                    }
                return {
                    'operation': operation,
                    'count': len(times),
                    'avg_time': round(sum(times) / len(times), 3),
                    'min_time': round(min(times), 3),
                    'max_time': round(max(times), 3),
                    'recent_time': round(times[-1], 3) if times else 0.0
                }
            else:
                stats = {}
                for op_name, times_deque in list(self.execution_times.items())[:self.MAX_OPERATION_TYPES]:
                    if times_deque:
                        times = list(times_deque)
                        if times:
                            stats[op_name] = {
                                'count': len(times),
                                'avg_time': round(sum(times) / len(times), 3),
                                'min_time': round(min(times), 3),
                                'max_time': round(max(times), 3)
                            }
                return stats
        except Exception as e:
            logger.error(f"Error getting execution stats: {e}")
            return {'error': str(e)}
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """Get memory usage statistics for this tracker"""
        try:
            return {
                'signal_times_size': len(self.signal_times),
                'execution_times_operations': len(self.execution_times),
                'operation_counts_size': len(self.operation_counts),
                'operation_times_size': len(self.operation_times),
                'total_operations': sum(self.operation_counts.values())
            }
        except Exception as e:
            logger.error(f"Error getting memory stats: {e}")
            return {'error': str(e)}
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        try:
            return {
                'timestamp': datetime.now(pytz.UTC).isoformat(),
                'signal_metrics': self.get_signal_rate(60),
                'execution_stats': self.get_execution_stats(),
                'total_operations': sum(self.operation_counts.values())
            }
        except Exception as e:
            logger.error(f"Error getting performance summary: {e}")
            return {'error': str(e)}


class PerformanceLogger:
    """Log performance metrics at regular intervals"""
    
    def __init__(self, system_monitor: SystemMonitor, tracking_metrics: TrackingMetrics,
                 log_interval_minutes: int = 5, log_file: str = 'logs/performance.log'):
        self.system_monitor = system_monitor
        self.tracking_metrics = tracking_metrics
        self.log_interval_minutes = log_interval_minutes
        self.log_file = log_file
        
        self.logging_task = None
        self.is_running = False
        
        logger.info(f"PerformanceLogger initialized (interval: {log_interval_minutes}min, file: {log_file})")
    
    async def start(self):
        """Start periodic performance logging"""
        if self.is_running:
            logger.warning("PerformanceLogger already running")
            return
        
        self.is_running = True
        self.logging_task = asyncio.create_task(self._logging_loop())
        logger.info(f"PerformanceLogger started - logging every {self.log_interval_minutes} minutes")
    
    async def stop(self):
        """Stop periodic performance logging"""
        if not self.is_running:
            return
        
        self.is_running = False
        
        if self.logging_task:
            self.logging_task.cancel()
            try:
                await self.logging_task
            except asyncio.CancelledError:
                pass
        
        logger.info("PerformanceLogger stopped")
    
    async def _logging_loop(self):
        """Main logging loop"""
        try:
            while self.is_running:
                await self._log_performance()
                
                await asyncio.sleep(self.log_interval_minutes * 60)
                
        except asyncio.CancelledError:
            logger.info("Performance logging loop cancelled")
        except Exception as e:
            logger.error(f"Error in performance logging loop: {e}", exc_info=True)
    
    async def _log_performance(self):
        """Log current performance metrics"""
        try:
            metrics = {
                'timestamp': datetime.now(pytz.UTC).isoformat(),
                'system_health': self.system_monitor.get_comprehensive_health(),
                'tracking_metrics': self.tracking_metrics.get_performance_summary()
            }
            
            log_line = json.dumps(metrics, separators=(',', ':'))
            
            try:
                with open(self.log_file, 'a') as f:
                    f.write(log_line + '\n')
            except Exception as e:
                logger.error(f"Error writing to performance log file: {e}")
            
            cpu = metrics['system_health'].get('process', {}).get('cpu_percent', 0)
            mem = metrics['system_health'].get('process', {}).get('memory', {}).get('percent', 0)
            ws_status = metrics['system_health'].get('websocket', {}).get('health_status', 'unknown')
            signal_rate = metrics['tracking_metrics'].get('signal_metrics', {}).get('signals_per_hour', 0)
            
            logger.info(
                f"📊 Performance: CPU={cpu:.1f}% | MEM={mem:.1f}% | "
                f"WS={ws_status} | Signals/hr={signal_rate:.1f}"
            )
            
        except Exception as e:
            logger.error(f"Error logging performance: {e}", exc_info=True)
    
    def log_now(self):
        """Log performance metrics immediately (non-async)"""
        try:
            metrics = {
                'timestamp': datetime.now(pytz.UTC).isoformat(),
                'system_health': self.system_monitor.get_comprehensive_health(),
                'tracking_metrics': self.tracking_metrics.get_performance_summary()
            }
            
            log_line = json.dumps(metrics, separators=(',', ':'))
            
            try:
                with open(self.log_file, 'a') as f:
                    f.write(log_line + '\n')
                logger.info(f"Performance metrics logged to {self.log_file}")
            except Exception as e:
                logger.error(f"Error writing to performance log file: {e}")
                
        except Exception as e:
            logger.error(f"Error logging performance immediately: {e}")


class PerformanceMonitorManager:
    """Unified manager for all performance monitoring components"""
    
    def __init__(self, config=None, log_interval_minutes: int = 5):
        self.config = config
        self.system_monitor = SystemMonitor(config)
        self.tracking_metrics = TrackingMetrics(window_size=100)
        self.performance_logger = PerformanceLogger(
            self.system_monitor,
            self.tracking_metrics,
            log_interval_minutes=log_interval_minutes
        )
        
        logger.info("PerformanceMonitorManager initialized")
    
    async def start(self):
        """Start all monitoring components"""
        await self.performance_logger.start()
        logger.info("✅ Performance monitoring started")
    
    async def stop(self):
        """Stop all monitoring components"""
        await self.performance_logger.stop()
        logger.info("🛑 Performance monitoring stopped")
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get current system health"""
        return self.system_monitor.get_comprehensive_health()
    
    def get_tracking_summary(self) -> Dict[str, Any]:
        """Get tracking metrics summary"""
        return self.tracking_metrics.get_performance_summary()
    
    def record_signal(self, accepted: bool = True):
        """Record a signal event"""
        self.tracking_metrics.record_signal(accepted)
    
    def record_execution(self, operation: str, execution_time: float):
        """Record execution time for an operation"""
        self.tracking_metrics.record_execution_time(operation, execution_time)
    
    def update_ws_status(self, status: str, is_heartbeat: bool = False):
        """Update WebSocket status"""
        self.system_monitor.update_ws_status(status, is_heartbeat)
