"""Sentry integration for advanced error tracking and monitoring"""

import logging
import os
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger('SentryIntegration')


class SentryIntegrationManager:
    """Manages Sentry error tracking integration with fallback to local logging"""
    
    def __init__(self, sentry_dsn: Optional[str] = None, environment: str = 'production'):
        """Initialize Sentry integration
        
        Args:
            sentry_dsn: Sentry DSN (from environment variable SENTRY_DSN if not provided)
            environment: Environment name (development, staging, production)
        """
        self.sentry_dsn = sentry_dsn or os.getenv('SENTRY_DSN')
        self.environment = environment
        self.sentry_enabled = False
        
        if self.sentry_dsn:
            self._initialize_sentry()
        else:
            logger.info("Sentry DSN not provided - using local logging only")
    
    def _initialize_sentry(self):
        """Initialize Sentry SDK with proper configuration"""
        try:
            import sentry_sdk
            from sentry_sdk.integrations.logging import LoggingIntegration
            from sentry_sdk.integrations.threading import ThreadingIntegration
            
            logging_integration = LoggingIntegration(
                level=logging.INFO,
                event_level=logging.ERROR
            )
            
            sentry_sdk.init(
                dsn=self.sentry_dsn,
                integrations=[logging_integration, ThreadingIntegration()],
                environment=self.environment,
                traces_sample_rate=0.1,
                profiles_sample_rate=0.1,
                debug=self.environment == 'development',
                attach_stacktrace=True,
            )
            
            self.sentry_enabled = True
            logger.info(f"✅ Sentry initialized for {self.environment} environment")
            
        except ImportError:
            logger.warning("sentry-sdk not installed - install with: pip install sentry-sdk")
        except Exception as e:
            logger.error(f"Failed to initialize Sentry: {e}")
    
    def capture_exception(self, exception: Exception, context: Optional[Dict[str, Any]] = None, level: str = 'error') -> str:
        """Capture exception to Sentry with context
        
        Args:
            exception: The exception to capture
            context: Additional context information
            level: Error level (debug, info, warning, error, fatal)
            
        Returns:
            Event ID if sent to Sentry, otherwise empty string
        """
        event_id = ""
        
        try:
            if self.sentry_enabled:
                import sentry_sdk
                
                if context:
                    with sentry_sdk.push_scope() as scope:
                        for key, value in context.items():
                            scope.set_context("custom", {key: str(value)})
                        event_id = sentry_sdk.capture_exception(exception)
                else:
                    event_id = sentry_sdk.capture_exception(exception)
                
                logger.error(f"Exception captured to Sentry (ID: {event_id}): {exception}")
            else:
                logger.error(f"Exception (no Sentry): {type(exception).__name__}: {exception}")
                if context:
                    logger.debug(f"Context: {context}")
                    
        except Exception as e:
            logger.error(f"Failed to capture exception to Sentry: {e}")
        
        return str(event_id)
    
    def capture_message(self, message: str, level: str = 'info', context: Optional[Dict] = None) -> str:
        """Capture custom message to Sentry
        
        Args:
            message: Message to capture
            level: Message level (debug, info, warning, error)
            context: Additional context
            
        Returns:
            Event ID if sent to Sentry
        """
        event_id = ""
        
        try:
            if self.sentry_enabled:
                import sentry_sdk
                
                if context:
                    with sentry_sdk.push_scope() as scope:
                        for key, value in context.items():
                            scope.set_context("custom", {key: str(value)})
                        event_id = sentry_sdk.capture_message(message, level)
                else:
                    event_id = sentry_sdk.capture_message(message, level)
                
                logger.info(f"Message captured to Sentry (ID: {event_id}, level: {level}): {message}")
            else:
                logger.info(f"Message (no Sentry, level: {level}): {message}")
                
        except Exception as e:
            logger.error(f"Failed to capture message to Sentry: {e}")
        
        return str(event_id)
    
    def set_user_context(self, user_id: int, username: Optional[str] = None, email: Optional[str] = None):
        """Set user context for better error tracking
        
        Args:
            user_id: User/Chat ID
            username: Optional username
            email: Optional email
        """
        try:
            if self.sentry_enabled:
                import sentry_sdk
                
                sentry_sdk.set_user({
                    'id': str(user_id),
                    'username': username or f'user_{user_id}',
                    'email': email
                })
                logger.debug(f"Sentry user context set for user {user_id}")
        except Exception as e:
            logger.debug(f"Failed to set Sentry user context: {e}")
    
    def clear_user_context(self):
        """Clear user context from Sentry"""
        try:
            if self.sentry_enabled:
                import sentry_sdk
                sentry_sdk.set_user(None)
                logger.debug("Sentry user context cleared")
        except Exception as e:
            logger.debug(f"Failed to clear Sentry user context: {e}")
    
    def set_tag(self, key: str, value: str):
        """Set a tag for better error filtering
        
        Args:
            key: Tag key
            value: Tag value
        """
        try:
            if self.sentry_enabled:
                import sentry_sdk
                sentry_sdk.set_tag(key, value)
        except Exception as e:
            logger.debug(f"Failed to set Sentry tag: {e}")
    
    def close(self):
        """Close Sentry connection and flush pending events"""
        try:
            if self.sentry_enabled:
                import sentry_sdk
                sentry_sdk.flush(timeout=2)
                logger.info("Sentry connection closed")
        except Exception as e:
            logger.debug(f"Error closing Sentry: {e}")


# Global instance
_sentry_manager: Optional[SentryIntegrationManager] = None


def get_sentry_manager() -> SentryIntegrationManager:
    """Get or create global Sentry manager instance
    
    Returns:
        SentryIntegrationManager instance
    """
    global _sentry_manager
    if _sentry_manager is None:
        _sentry_manager = SentryIntegrationManager()
    return _sentry_manager


def initialize_sentry(sentry_dsn: Optional[str] = None, environment: str = 'production'):
    """Initialize global Sentry manager
    
    Args:
        sentry_dsn: Sentry DSN URL
        environment: Environment name
    """
    global _sentry_manager
    _sentry_manager = SentryIntegrationManager(sentry_dsn, environment)
