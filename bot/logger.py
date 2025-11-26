import logging
import os
import re
import time
import threading
from collections import defaultdict
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import Dict, Optional

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

def setup_logger(name='TradingBot', log_dir='logs', level=None):
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
    
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, f'{name.lower()}.log'),
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
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
            
            if len(self._log_counts[key]) >= self.max_similar_logs:
                self._suppressed_counts[key] += 1
                return False
            
            self._log_counts[key].append(now)
            
            if self._suppressed_counts[key] > 0:
                suppressed = self._suppressed_counts[key]
                self._suppressed_counts[key] = 0
                self.logger.info(f"[Rate Limiter] Suppressed {suppressed} similar messages")
            
            return True
    
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
