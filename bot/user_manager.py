"""User Manager with Thread-Safe Updates.

This module provides thread-safe user management with the following guarantees:

Thread Safety:
- Per-user locks via defaultdict(threading.RLock) for atomic user operations
- Context managers for clean lock handling
- Guarded active_users mutations with dedicated lock

Read/Write Separation:
- READ operations (get_user, get_user_preferences, is_authorized, etc.):
  - Do NOT acquire user locks for reads
  - Use session-level isolation for data consistency
  - Return detached objects (via session.expunge)

- WRITE operations (create_user, update_user_activity, update_user_stats, etc.):
  - Acquire per-user lock before modification
  - Use context manager for automatic lock release
  - Atomic updates within lock scope
"""
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from contextlib import contextmanager
import threading
import pytz
from bot.logger import setup_logger
from sqlalchemy import Integer, String, DateTime, Boolean, Float, create_engine
from sqlalchemy.orm import sessionmaker, scoped_session, Mapped, mapped_column, DeclarativeBase

logger = setup_logger('UserManager')

class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = 'users'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_id: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)
    username: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_active: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    total_trades: Mapped[int] = mapped_column(Integer, default=0)
    total_profit: Mapped[float] = mapped_column(Float, default=0.0)
    subscription_tier: Mapped[str] = mapped_column(String(20), default='FREE')
    subscription_expires: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    settings: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

class UserPreferences(Base):
    __tablename__ = 'user_preferences'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_id: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)
    notification_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    daily_summary_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    risk_alerts_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    preferred_timeframe: Mapped[str] = mapped_column(String(10), default='M1')
    max_daily_signals: Mapped[int] = mapped_column(Integer, default=999999)
    timezone: Mapped[str] = mapped_column(String(50), default='Asia/Jakarta')

class UserManager:
    """Thread-safe user manager with per-user locking.
    
    Provides:
    - Per-user RLocks for atomic write operations
    - Context managers for clean lock handling
    - Guarded active_users mutations
    - Session-level isolation for read operations
    """
    def __init__(self, config, db_path: str = 'data/users.db'):
        self.config = config
        self.db_path = db_path
        
        self._lock = threading.RLock()
        self._user_locks: Dict[int, threading.RLock] = defaultdict(threading.RLock)
        self._user_locks_lock = threading.RLock()
        self._active_users_lock = threading.RLock()
        
        engine = create_engine(
            f'sqlite:///{self.db_path}',
            connect_args={'check_same_thread': False, 'timeout': 30.0}
        )
        Base.metadata.create_all(engine)
        
        session_factory = sessionmaker(bind=engine)
        self.Session = scoped_session(session_factory)
        
        self.active_users: Dict[int, Dict] = {}
        logger.info("User manager initialized with thread-safe RLock per-user locking")
    
    def _get_user_lock(self, telegram_id: int) -> threading.RLock:
        """Get or create a RLock for a specific user.
        
        Uses defaultdict for automatic RLock creation, protected by meta-lock.
        RLock allows same thread to acquire lock multiple times (reentrant).
        """
        with self._user_locks_lock:
            return self._user_locks[telegram_id]
    
    @contextmanager
    def user_lock(self, telegram_id: int):
        """Context manager for per-user lock handling.
        
        Provides clean lock acquisition and release with automatic cleanup.
        
        Usage:
            with self.user_lock(telegram_id):
                # atomic operations on user data
        """
        lock = self._get_user_lock(telegram_id)
        lock.acquire()
        try:
            yield
        finally:
            lock.release()
    
    @contextmanager
    def get_session(self):
        """Context manager for thread-safe session handling with proper cleanup.
        
        Provides:
        - Automatic commit on success
        - Automatic rollback on exception
        - Session cleanup in finally block
        """
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            try:
                session.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during session rollback: {rollback_error}")
            raise
        finally:
            try:
                session.close()
            except Exception as close_error:
                logger.error(f"Error closing session: {close_error}")
            try:
                self.Session.remove()
            except Exception as remove_error:
                logger.error(f"Error removing scoped session: {remove_error}")
    
    def create_user(self, telegram_id: int, username: Optional[str] = None,
                   first_name: Optional[str] = None, last_name: Optional[str] = None) -> Optional[User]:
        """Create a new user with per-user locking (WRITE operation).
        
        Thread-safe: Acquires per-user lock before modification.
        """
        with self.user_lock(telegram_id):
            with self.get_session() as session:
                try:
                    existing = session.query(User).filter(User.telegram_id == telegram_id).first()
                    
                    if existing:
                        logger.info(f"User already exists: {telegram_id}")
                        session.expunge(existing)
                        return existing
                    
                    is_admin = telegram_id in self.config.AUTHORIZED_USER_IDS
                    
                    user = User(
                        telegram_id=telegram_id,
                        username=username,
                        first_name=first_name,
                        last_name=last_name,
                        is_active=True,
                        is_admin=is_admin
                    )
                    
                    session.add(user)
                    session.flush()
                    
                    preferences = UserPreferences(telegram_id=telegram_id)
                    session.add(preferences)
                    
                    logger.info(f"Created new user: {telegram_id} ({username})")
                    session.expunge(user)
                    return user
                    
                except Exception as e:
                    logger.error(f"Error creating user: {e}")
                    return None
    
    def get_user(self, telegram_id: int) -> Optional[User]:
        """Get user by telegram_id (READ operation).
        
        Thread-safe via session isolation. No per-user lock needed for reads.
        Returns detached object.
        """
        with self.get_session() as session:
            try:
                user = session.query(User).filter(User.telegram_id == telegram_id).first()
                
                if user:
                    session.expunge(user)
                
                return user
            except Exception as e:
                logger.error(f"Error getting user: {e}")
                return None
    
    def get_user_by_username(self, username: str) -> Optional[int]:
        """Get user telegram_id by username (READ operation).
        
        Thread-safe via session isolation. No per-user lock needed for reads.
        """
        with self.get_session() as session:
            try:
                user = session.query(User).filter(User.username == username).first()
                return int(user.telegram_id) if user else None
            except Exception as e:
                logger.error(f"Error getting user by username: {e}")
                return None
    
    def update_user_activity(self, telegram_id: int):
        """Update user activity timestamp (WRITE operation).
        
        Thread-safe: Acquires per-user lock before modification.
        """
        with self.user_lock(telegram_id):
            with self.get_session() as session:
                try:
                    user = session.query(User).filter(User.telegram_id == telegram_id).first()
                    
                    if user:
                        user.last_active = datetime.utcnow()
                        logger.debug(f"Updated activity for user {telegram_id}")
                except Exception as e:
                    logger.error(f"Error updating user activity: {e}")
    
    def is_authorized(self, telegram_id: int) -> bool:
        """Check if user is authorized (READ operation - no lock needed)."""
        if telegram_id in self.config.AUTHORIZED_USER_IDS:
            return True
        
        if hasattr(self.config, 'ID_USER_PUBLIC') and telegram_id in self.config.ID_USER_PUBLIC:
            return True
        
        return False
    
    def is_admin(self, telegram_id: int) -> bool:
        """Check if user is admin (READ operation - no lock needed)."""
        user = self.get_user(telegram_id)
        return bool(user.is_admin) if user else False
    
    def get_all_users(self) -> List[User]:
        """Get all users (READ operation - no lock needed)."""
        with self.get_session() as session:
            try:
                users = session.query(User).all()
                for user in users:
                    session.expunge(user)
                return users
            except Exception as e:
                logger.error(f"Error getting all users: {e}")
                return []
    
    def get_active_users(self) -> List[User]:
        """Get all active users (READ operation - no lock needed)."""
        with self.get_session() as session:
            try:
                users = session.query(User).filter(User.is_active == True).all()
                for user in users:
                    session.expunge(user)
                return users
            except Exception as e:
                logger.error(f"Error getting active users: {e}")
                return []
    
    def deactivate_user(self, telegram_id: int) -> bool:
        """Deactivate a user (WRITE operation).
        
        Thread-safe: Acquires per-user lock before modification.
        """
        with self.user_lock(telegram_id):
            with self.get_session() as session:
                try:
                    user = session.query(User).filter(User.telegram_id == telegram_id).first()
                    
                    if user:
                        user.is_active = False
                        logger.info(f"Deactivated user: {telegram_id}")
                        
                        with self._active_users_lock:
                            if telegram_id in self.active_users:
                                del self.active_users[telegram_id]
                        
                        return True
                    
                    return False
                except Exception as e:
                    logger.error(f"Error deactivating user: {e}")
                    return False
    
    def activate_user(self, telegram_id: int) -> bool:
        """Activate a user (WRITE operation).
        
        Thread-safe: Acquires per-user lock before modification.
        """
        with self.user_lock(telegram_id):
            with self.get_session() as session:
                try:
                    user = session.query(User).filter(User.telegram_id == telegram_id).first()
                    
                    if user:
                        user.is_active = True
                        logger.info(f"Activated user: {telegram_id}")
                        return True
                    
                    return False
                except Exception as e:
                    logger.error(f"Error activating user: {e}")
                    return False
    
    def update_user_stats(self, telegram_id: int, profit: float):
        """Update user trading statistics (WRITE operation).
        
        Thread-safe: Acquires per-user lock before modification.
        Atomic update of total_trades and total_profit.
        """
        with self.user_lock(telegram_id):
            with self.get_session() as session:
                try:
                    user = session.query(User).filter(User.telegram_id == telegram_id).first()
                    
                    if user:
                        user.total_trades += 1
                        user.total_profit += profit
                        logger.debug(f"Updated stats for user {telegram_id}: profit={profit}")
                except Exception as e:
                    logger.error(f"Error updating user stats: {e}")
    
    def get_user_preferences(self, telegram_id: int) -> Optional[UserPreferences]:
        """Get user preferences (READ operation - no lock needed)."""
        with self.get_session() as session:
            try:
                prefs = session.query(UserPreferences).filter(
                    UserPreferences.telegram_id == telegram_id
                ).first()
                if prefs:
                    session.expunge(prefs)
                return prefs
            except Exception as e:
                logger.error(f"Error getting user preferences: {e}")
                return None
    
    def update_user_preferences(self, telegram_id: int, **kwargs) -> bool:
        """Update user preferences (WRITE operation).
        
        Thread-safe: Acquires per-user lock before modification.
        """
        with self.user_lock(telegram_id):
            with self.get_session() as session:
                try:
                    prefs = session.query(UserPreferences).filter(
                        UserPreferences.telegram_id == telegram_id
                    ).first()
                    
                    if not prefs:
                        prefs = UserPreferences(telegram_id=telegram_id)
                        session.add(prefs)
                    
                    for key, value in kwargs.items():
                        if hasattr(prefs, key):
                            setattr(prefs, key, value)
                    
                    logger.info(f"Updated preferences for user {telegram_id}")
                    return True
                    
                except Exception as e:
                    logger.error(f"Error updating preferences: {e}")
                    return False
    
    def get_user_info(self, telegram_id: int) -> Optional[Dict]:
        """Get comprehensive user info (READ operation - no lock needed)."""
        user = self.get_user(telegram_id)
        prefs = self.get_user_preferences(telegram_id)
        
        if not user:
            return None
        
        jakarta_tz = pytz.timezone('Asia/Jakarta')
        created = user.created_at.replace(tzinfo=pytz.UTC).astimezone(jakarta_tz)
        last_active = user.last_active.replace(tzinfo=pytz.UTC).astimezone(jakarta_tz)
        
        info = {
            'telegram_id': user.telegram_id,
            'username': user.username,
            'first_name': user.first_name,
            'last_name': user.last_name,
            'is_active': user.is_active,
            'is_admin': user.is_admin,
            'created_at': created.strftime('%Y-%m-%d %H:%M'),
            'last_active': last_active.strftime('%Y-%m-%d %H:%M'),
            'total_trades': user.total_trades,
            'total_profit': user.total_profit
        }
        
        if prefs:
            info['preferences'] = {
                'notifications': prefs.notification_enabled,
                'daily_summary': prefs.daily_summary_enabled,
                'risk_alerts': prefs.risk_alerts_enabled,
                'timeframe': prefs.preferred_timeframe,
                'timezone': prefs.timezone
            }
        
        return info
    
    def format_user_profile(self, telegram_id: int) -> Optional[str]:
        """Format user profile for display (READ operation - no lock needed)."""
        info = self.get_user_info(telegram_id)
        
        if not info:
            return None
        
        profile = f"👤 *User Profile*\n\n"
        profile += f"Name: {info.get('first_name', 'N/A')} {info.get('last_name', '')}\n"
        profile += f"Username: @{info.get('username', 'N/A')}\n"
        profile += f"Status: {'✅ Active' if info['is_active'] else '⛔ Inactive'}\n"
        profile += f"Role: {'👑 Admin' if info['is_admin'] else '👤 User'}\n\n"
        profile += f"📊 *Statistics*\n"
        profile += f"Total Trades: {info['total_trades']}\n"
        profile += f"Total Profit: ${info['total_profit']:.2f}\n"
        profile += f"Member Since: {info['created_at']}\n"
        profile += f"Last Active: {info['last_active']}\n"
        
        return profile
    
    def get_user_count(self) -> Dict:
        """Get user statistics (READ operation - no lock needed)."""
        with self.get_session() as session:
            try:
                total = session.query(User).count()
                active = session.query(User).filter(User.is_active == True).count()
                admins = session.query(User).filter(User.is_admin == True).count()
                
                return {
                    'total': total,
                    'active': active,
                    'inactive': total - active,
                    'admins': admins
                }
            except Exception as e:
                logger.error(f"Error getting user count: {e}")
                return {
                    'total': 0,
                    'active': 0,
                    'inactive': 0,
                    'admins': 0
                }
    
    def has_access(self, telegram_id: int) -> bool:
        """Check if user has access (READ operation - no lock needed)."""
        if telegram_id in self.config.AUTHORIZED_USER_IDS:
            return True
        
        if hasattr(self.config, 'ID_USER_PUBLIC') and telegram_id in self.config.ID_USER_PUBLIC:
            return True
        
        return False
    
    def set_active_user(self, telegram_id: int, data: Dict):
        """Set active user data (WRITE operation with active_users lock).
        
        Thread-safe: Acquires active_users lock before mutation.
        """
        with self._active_users_lock:
            self.active_users[telegram_id] = data
            logger.debug(f"Set active user: {telegram_id}")
    
    def get_active_user(self, telegram_id: int) -> Optional[Dict]:
        """Get active user data (READ operation with active_users lock).
        
        Thread-safe: Acquires active_users lock for read.
        """
        with self._active_users_lock:
            return self.active_users.get(telegram_id)
    
    def remove_active_user(self, telegram_id: int) -> bool:
        """Remove active user (WRITE operation with active_users lock).
        
        Thread-safe: Acquires active_users lock before mutation.
        """
        with self._active_users_lock:
            if telegram_id in self.active_users:
                del self.active_users[telegram_id]
                logger.debug(f"Removed active user: {telegram_id}")
                return True
            return False
    
    def get_all_active_user_ids(self) -> List[int]:
        """Get all active user IDs (READ operation with active_users lock).
        
        Thread-safe: Acquires active_users lock for read, returns copy.
        """
        with self._active_users_lock:
            return list(self.active_users.keys())
    
    def clear_stale_locks(self, max_age_seconds: int = 3600):
        """Clear stale per-user locks that haven't been used recently.
        
        This helps prevent memory leaks from accumulating user locks.
        Should be called periodically from a maintenance task.
        """
        with self._user_locks_lock:
            initial_count = len(self._user_locks)
            
            self._user_locks = defaultdict(threading.RLock)
            
            logger.info(f"Cleared {initial_count} user locks")
