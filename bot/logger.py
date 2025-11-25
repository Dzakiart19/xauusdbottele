import logging
import os
import re
from logging.handlers import RotatingFileHandler
from datetime import datetime

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
