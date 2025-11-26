"""
Logging module for the Trading Bot.

Log Retention Policy:
- Each log file is limited to 5MB (LOG_MAX_BYTES_DEFAULT)
- Maximum 5 backup files per module (LOG_BACKUP_COUNT_DEFAULT)
- Total maximum log storage per module: ~30MB (5MB * 6 files)
- Stale log files (older than LOG_RETENTION_DAYS) are automatically cleaned
- Log rotation is handled automatically by RotatingFileHandler
- Manual cleanup can be triggered via cleanup_old_logs()

Module-specific configurations can be set via LOG_CONFIG dictionary.
"""

import logging
import os
import re
import time
import glob
import threading
from collections import defaultdict
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Any

LOG_MAX_BYTES_DEFAULT = 5 * 1024 * 1024
LOG_BACKUP_COUNT_DEFAULT = 5
LOG_RETENTION_DAYS = 7

LOG_CONFIG = {
    'main': {'max_bytes': 10 * 1024 * 1024, 'backup_count': 5},
    'strategy': {'max_bytes': 5 * 1024 * 1024, 'backup_count': 5},
    'marketdata': {'max_bytes': 5 * 1024 * 1024, 'backup_count': 3},
    'telegrambot': {'max_bytes': 5 * 1024 * 1024, 'backup_count': 5},
    'errorhandler': {'max_bytes': 10 * 1024 * 1024, 'backup_count': 10},
    'positiontracker': {'max_bytes': 5 * 1024 * 1024, 'backup_count': 5},
    'riskmanager': {'max_bytes': 5 * 1024 * 1024, 'backup_count': 5},
    'default': {'max_bytes': LOG_MAX_BYTES_DEFAULT, 'backup_count': LOG_BACKUP_COUNT_DEFAULT}
}


def mask_token(token: str) -> str:
    if not token or not isinstance(token, str):
        return "****"
    
    token = token.strip()
    if len(token) <= 8:
        return "****"
    
    return f"{token[:4]}...{token[-4:]}"


def mask_user_id(user_id: int) -> str:
    if not user_id:
        return "***"
    
    user_id_str = str(user_id)
    if len(user_id_str) <= 6:
        return f"{user_id_str[:2]}***{user_id_str[-1:]}"
    
    mid_len = len(user_id_str) - 6
    return f"{user_id_str[:3]}{'*' * min(mid_len, 3)}{user_id_str[-3:]}"


def sanitize_log_message(message: str) -> str:
    if not message or not isinstance(message, str):
        return message
    
    sanitized = message
    
    bot_token_pattern = r'\b\d{8,}:[A-Za-z0-9_-]{35,}\b'
    matches = re.findall(bot_token_pattern, sanitized)
    for token in matches:
        sanitized = sanitized.replace(token, mask_token(token))
    
    sensitive_patterns = [
        r'\b[A-Za-z0-9]{32,}\b',
        r'\bsk-[A-Za-z0-9]{32,}\b',
        r'\bapi[_-]?key[_-]?[A-Za-z0-9]{20,}\b',
        r'\bAKIA[0-9A-Z]{16}\b',
        r'\baws[_-]?access[_-]?key[_-]?id[=:\s]+[A-Z0-9]{20}\b',
        r'\baws[_-]?secret[_-]?access[_-]?key[=:\s]+[A-Za-z0-9/+=]{40}\b',
        r'\bsecret[_-]?key[=:\s]+[A-Za-z0-9]{20,}\b',
        r'\bbearer[_-\s]+[A-Za-z0-9\-._~+/]+=*\b',
        r'\bauthorization[:\s]+bearer[_-\s]+[A-Za-z0-9\-._~+/]+=*\b',
        r'\bpassword[=:\s]+[A-Za-z0-9!@#$%^&*()_+=\-]{8,}\b',
        r'\bprivate[_-]?key[=:\s]+[A-Za-z0-9+/=]{40,}\b',
        r'\bclient[_-]?secret[=:\s]+[A-Za-z0-9\-._~]{20,}\b',
        r'\btoken[=:\s]+[A-Za-z0-9\-._~+/]{20,}\b'
    ]
    
    for pattern in sensitive_patterns:
        matches = re.findall(pattern, sanitized, re.IGNORECASE)
        for key in matches:
            if len(key) >= 20 and not key.isdigit():
                sanitized = sanitized.replace(key, mask_token(key))
    
    return sanitized


def get_log_config(name: str) -> dict:
    """
    Get log configuration for a specific module.
    
    Args:
        name: Logger/module name
    
    Returns:
        dict with 'max_bytes' and 'backup_count'
    """
    name_lower = name.lower()
    if name_lower in LOG_CONFIG:
        return LOG_CONFIG[name_lower]
    return LOG_CONFIG['default']


def cleanup_old_logs(log_dir: str = 'logs', 
                     retention_days: Optional[int] = None,
                     dry_run: bool = False) -> Dict[str, Any]:
    """
    Clean up old and stale log files.
    
    This function removes:
    1. Log files older than retention_days
    2. Rotated backup files beyond the configured backup count
    3. Empty log files
    
    Args:
        log_dir: Directory containing log files
        retention_days: Days to retain logs (default: LOG_RETENTION_DAYS)
        dry_run: If True, only report what would be deleted without actually deleting
    
    Returns:
        dict with cleanup statistics:
            - 'deleted_count': Number of files deleted
            - 'deleted_files': List of deleted file paths
            - 'freed_bytes': Total bytes freed
            - 'errors': List of any errors encountered
    """
    if retention_days is None:
        retention_days = LOG_RETENTION_DAYS
    
    result = {
        'deleted_count': 0,
        'deleted_files': [],
        'freed_bytes': 0,
        'errors': [],
        'dry_run': dry_run
    }
    
    if not os.path.exists(log_dir):
        return result
    
    cutoff_time = datetime.now() - timedelta(days=retention_days)
    cutoff_timestamp = cutoff_time.timestamp()
    
    log_patterns = [
        os.path.join(log_dir, '*.log'),
        os.path.join(log_dir, '*.log.*')
    ]
    
    log_files = []
    for pattern in log_patterns:
        log_files.extend(glob.glob(pattern))
    
    for log_file in log_files:
        try:
            file_stat = os.stat(log_file)
            file_mtime = file_stat.st_mtime
            file_size = file_stat.st_size
            
            should_delete = False
            reason = ""
            
            if file_mtime < cutoff_timestamp:
                should_delete = True
                reason = f"older than {retention_days} days"
            
            if file_size == 0:
                should_delete = True
                reason = "empty file"
            
            base_name = os.path.basename(log_file)
            if '.log.' in base_name:
                parts = base_name.split('.log.')
                if len(parts) == 2:
                    try:
                        backup_num = int(parts[1])
                        module_name = parts[0].lower()
                        config = get_log_config(module_name)
                        if backup_num > config['backup_count']:
                            should_delete = True
                            reason = f"exceeds backup count ({backup_num} > {config['backup_count']})"
                    except ValueError:
                        pass
            
            if should_delete:
                if dry_run:
                    result['deleted_files'].append(f"{log_file} ({reason})")
                    result['freed_bytes'] += file_size
                    result['deleted_count'] += 1
                else:
                    os.remove(log_file)
                    result['deleted_files'].append(log_file)
                    result['freed_bytes'] += file_size
                    result['deleted_count'] += 1
        
        except OSError as e:
            result['errors'].append(f"Error processing {log_file}: {str(e)}")
        except Exception as e:
            result['errors'].append(f"Unexpected error for {log_file}: {str(e)}")
    
    return result


def get_log_statistics(log_dir: str = 'logs') -> Dict[str, Any]:
    """
    Get statistics about current log files.
    
    Args:
        log_dir: Directory containing log files
    
    Returns:
        dict with log statistics:
            - 'total_files': Total number of log files
            - 'total_size_bytes': Total size of all log files
            - 'total_size_mb': Total size in megabytes
            - 'files_by_module': Dict of module -> list of file info
            - 'oldest_file': Oldest log file info
            - 'newest_file': Newest log file info
    """
    result = {
        'total_files': 0,
        'total_size_bytes': 0,
        'total_size_mb': 0.0,
        'files_by_module': defaultdict(list),
        'oldest_file': None,
        'newest_file': None
    }
    
    if not os.path.exists(log_dir):
        return result
    
    log_patterns = [
        os.path.join(log_dir, '*.log'),
        os.path.join(log_dir, '*.log.*')
    ]
    
    log_files = []
    for pattern in log_patterns:
        log_files.extend(glob.glob(pattern))
    
    oldest_mtime = float('inf')
    newest_mtime = 0
    
    for log_file in log_files:
        try:
            file_stat = os.stat(log_file)
            file_info = {
                'path': log_file,
                'name': os.path.basename(log_file),
                'size_bytes': file_stat.st_size,
                'size_mb': round(file_stat.st_size / (1024 * 1024), 2),
                'modified': datetime.fromtimestamp(file_stat.st_mtime).isoformat()
            }
            
            result['total_files'] += 1
            result['total_size_bytes'] += file_stat.st_size
            
            base_name = os.path.basename(log_file).split('.log')[0]
            result['files_by_module'][base_name].append(file_info)
            
            if file_stat.st_mtime < oldest_mtime:
                oldest_mtime = file_stat.st_mtime
                result['oldest_file'] = file_info
            
            if file_stat.st_mtime > newest_mtime:
                newest_mtime = file_stat.st_mtime
                result['newest_file'] = file_info
        
        except (OSError, Exception):
            pass
    
    result['total_size_mb'] = round(result['total_size_bytes'] / (1024 * 1024), 2)
    result['files_by_module'] = dict(result['files_by_module'])
    
    return result


def setup_logger(name='TradingBot', log_dir='logs', level=None):
    """
    Setup a logger with file rotation and console output.
    
    Log Rotation Configuration:
    - File rotation is handled automatically by RotatingFileHandler
    - Each module can have custom max_bytes and backup_count via LOG_CONFIG
    - Default: 5MB per file, 5 backup files
    
    Args:
        name: Logger name (used for file naming and module-specific config)
        log_dir: Directory for log files
        level: Log level (default: from LOG_LEVEL env var or INFO)
    
    Returns:
        logging.Logger instance
    """
    os.makedirs(log_dir, exist_ok=True)
    
    if level is None:
        log_level_str = os.getenv('LOG_LEVEL', 'INFO').upper()
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        level = level_map.get(log_level_str, logging.INFO)
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if logger.handlers:
        return logger
    
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    config = get_log_config(name)
    
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, f'{name.lower()}.log'),
        maxBytes=config['max_bytes'],
        backupCount=config['backup_count'],
        encoding='utf-8'
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(log_format)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(log_format)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


class RateLimitedLogger:
    """
    Rate-limited logger untuk mencegah log spam pada high-frequency signals.
    Membatasi jumlah log message yang sama dalam window waktu tertentu.
    """
    
    def __init__(self, logger: logging.Logger, 
                 max_similar_logs: int = 10,
                 window_seconds: float = 60.0):
        """
        Args:
            logger: Logger instance yang akan di-wrap
            max_similar_logs: Max jumlah log yang sama per window
            window_seconds: Window waktu dalam detik
        """
        self.logger = logger
        self.max_similar_logs = max_similar_logs
        self.window_seconds = window_seconds
        self._log_counts: Dict[str, list] = defaultdict(list)
        self._lock = threading.Lock()
        self._suppressed_counts: Dict[str, int] = defaultdict(int)
    
    def _get_message_key(self, message: str) -> str:
        """Generate key dari message untuk rate limiting"""
        key_parts = message[:100] if len(message) > 100 else message
        key_parts = re.sub(r'\d+\.?\d*', 'NUM', key_parts)
        key_parts = re.sub(r'0x[0-9a-fA-F]+', 'HEX', key_parts)
        return key_parts
    
    def _should_log(self, message: str) -> bool:
        """Check apakah message boleh di-log"""
        with self._lock:
            now = time.time()
            key = self._get_message_key(message)
            
            self._log_counts[key] = [
                t for t in self._log_counts[key] 
                if now - t < self.window_seconds
            ]
            
            if len(self._log_counts) > 1000:
                self._cleanup_old_entries(now)
            
            if len(self._log_counts[key]) >= self.max_similar_logs:
                self._suppressed_counts[key] += 1
                return False
            
            self._log_counts[key].append(now)
            
            if self._suppressed_counts[key] > 0:
                suppressed = self._suppressed_counts[key]
                self._suppressed_counts[key] = 0
                self.logger.info(f"[Rate Limiter] Suppressed {suppressed} similar messages")
            
            return True
    
    def _cleanup_old_entries(self, now: float):
        """Cleanup old entries dari log_counts dan suppressed_counts untuk mencegah memory growth"""
        keys_to_remove = []
        
        for key in list(self._log_counts.keys()):
            timestamps = self._log_counts[key]
            active_timestamps = [t for t in timestamps if now - t < self.window_seconds]
            
            if not active_timestamps:
                keys_to_remove.append(key)
            else:
                self._log_counts[key] = active_timestamps
        
        for key in keys_to_remove:
            del self._log_counts[key]
            self._suppressed_counts.pop(key, None)
    
    def debug(self, message: str, *args, **kwargs):
        if self._should_log(message):
            self.logger.debug(message, *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs):
        if self._should_log(message):
            self.logger.info(message, *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs):
        if self._should_log(message):
            self.logger.warning(message, *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs):
        self.logger.error(message, *args, **kwargs)
    
    def critical(self, message: str, *args, **kwargs):
        self.logger.critical(message, *args, **kwargs)
    
    def exception(self, message: str, *args, **kwargs):
        self.logger.exception(message, *args, **kwargs)
    
    def get_stats(self) -> Dict:
        """Dapatkan statistik rate limiting"""
        with self._lock:
            return {
                'tracked_message_types': len(self._log_counts),
                'total_suppressed': sum(self._suppressed_counts.values()),
                'window_seconds': self.window_seconds,
                'max_similar_logs': self.max_similar_logs
            }
    
    def clear_stats(self):
        """Reset statistik rate limiting"""
        with self._lock:
            self._log_counts.clear()
            self._suppressed_counts.clear()


def setup_rate_limited_logger(name: str = 'TradingBot', 
                              log_dir: str = 'logs',
                              level: Optional[int] = None,
                              max_similar_logs: int = 10,
                              window_seconds: float = 60.0) -> RateLimitedLogger:
    """
    Setup logger dengan rate limiting untuk mencegah log spam.
    
    Args:
        name: Nama logger
        log_dir: Direktori log
        level: Log level
        max_similar_logs: Max log yang sama per window
        window_seconds: Window waktu
        
    Returns:
        RateLimitedLogger instance
    """
    base_logger = setup_logger(name, log_dir, level)
    return RateLimitedLogger(base_logger, max_similar_logs, window_seconds)


def schedule_log_cleanup(log_dir: str = 'logs',
                         interval_hours: int = 24,
                         retention_days: Optional[int] = None) -> threading.Thread:
    """
    Schedule periodic log cleanup in a background thread.
    
    Args:
        log_dir: Directory containing log files
        interval_hours: Hours between cleanup runs
        retention_days: Days to retain logs
    
    Returns:
        The cleanup thread (already started)
    """
    def cleanup_loop():
        while True:
            try:
                result = cleanup_old_logs(log_dir, retention_days)
                if result['deleted_count'] > 0:
                    logger = logging.getLogger('LogCleanup')
                    logger.info(
                        f"Log cleanup: deleted {result['deleted_count']} files, "
                        f"freed {result['freed_bytes'] / 1024 / 1024:.2f}MB"
                    )
            except Exception as e:
                logger = logging.getLogger('LogCleanup')
                logger.error(f"Log cleanup error: {e}")
            
            time.sleep(interval_hours * 3600)
    
    cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True, name='LogCleanupThread')
    cleanup_thread.start()
    return cleanup_thread
