from datetime import datetime, timedelta
from typing import Dict, List, Optional
from contextlib import contextmanager
import threading
import pytz
from bot.logger import setup_logger
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Float, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

logger = setup_logger('UserManager')

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    telegram_id = Column(Integer, unique=True, nullable=False)
    username = Column(String(100))
    first_name = Column(String(100))
    last_name = Column(String(100))
    is_active = Column(Boolean, default=True)
    is_admin = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_active = Column(DateTime, default=datetime.utcnow)
    total_trades = Column(Integer, default=0)
    total_profit = Column(Float, default=0.0)
    subscription_tier = Column(String(20), default='FREE')
    subscription_expires = Column(DateTime)
    settings = Column(String(500))

class UserPreferences(Base):
    __tablename__ = 'user_preferences'
    
    id = Column(Integer, primary_key=True)
    telegram_id = Column(Integer, unique=True, nullable=False)
    notification_enabled = Column(Boolean, default=True)
    daily_summary_enabled = Column(Boolean, default=True)
    risk_alerts_enabled = Column(Boolean, default=True)
    preferred_timeframe = Column(String(10), default='M1')
    max_daily_signals = Column(Integer, default=999999)
    timezone = Column(String(50), default='Asia/Jakarta')

class UserManager:
    def __init__(self, config, db_path: str = 'data/users.db'):
        self.config = config
        self.db_path = db_path
        
        self._lock = threading.Lock()
        self._user_locks = {}
        self._user_locks_lock = threading.Lock()
        
        engine = create_engine(f'sqlite:///{self.db_path}')
        Base.metadata.create_all(engine)
        
        session_factory = sessionmaker(bind=engine)
        self.Session = scoped_session(session_factory)
        
        self.active_users = {}
        logger.info("User manager initialized with thread-safe session")
    
    def _get_user_lock(self, telegram_id: int) -> threading.Lock:
        """Get or create a lock for a specific user to ensure atomic operations"""
        with self._user_locks_lock:
            if telegram_id not in self._user_locks:
                self._user_locks[telegram_id] = threading.Lock()
            return self._user_locks[telegram_id]
    
    @contextmanager
    def get_session(self):
        """Context manager for thread-safe session handling with proper cleanup"""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
            self.Session.remove()
    
    def create_user(self, telegram_id: int, username: Optional[str] = None,
                   first_name: Optional[str] = None, last_name: Optional[str] = None) -> User:
        user_lock = self._get_user_lock(telegram_id)
        with user_lock:
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
        with self.get_session() as session:
            try:
                user = session.query(User).filter(User.username == username).first()
                return user.telegram_id if user else None
            except Exception as e:
                logger.error(f"Error getting user by username: {e}")
                return None
    
    def update_user_activity(self, telegram_id: int):
        """Thread-safe update of user activity timestamp with per-user locking"""
        user_lock = self._get_user_lock(telegram_id)
        with user_lock:
            with self.get_session() as session:
                try:
                    user = session.query(User).filter(User.telegram_id == telegram_id).first()
                    
                    if user:
                        user.last_active = datetime.utcnow()
                        logger.debug(f"Updated activity for user {telegram_id}")
                except Exception as e:
                    logger.error(f"Error updating user activity: {e}")
    
    def is_authorized(self, telegram_id: int) -> bool:
        if telegram_id in self.config.AUTHORIZED_USER_IDS:
            return True
        
        if hasattr(self.config, 'ID_USER_PUBLIC') and telegram_id in self.config.ID_USER_PUBLIC:
            return True
        
        return False
    
    def is_admin(self, telegram_id: int) -> bool:
        user = self.get_user(telegram_id)
        return user.is_admin if user else False
    
    def get_all_users(self) -> List[User]:
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
        user_lock = self._get_user_lock(telegram_id)
        with user_lock:
            with self.get_session() as session:
                try:
                    user = session.query(User).filter(User.telegram_id == telegram_id).first()
                    
                    if user:
                        user.is_active = False
                        logger.info(f"Deactivated user: {telegram_id}")
                        return True
                    
                    return False
                except Exception as e:
                    logger.error(f"Error deactivating user: {e}")
                    return False
    
    def activate_user(self, telegram_id: int) -> bool:
        user_lock = self._get_user_lock(telegram_id)
        with user_lock:
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
        """Thread-safe atomic update of user statistics with per-user locking"""
        user_lock = self._get_user_lock(telegram_id)
        with user_lock:
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
        """Thread-safe atomic update of user preferences with per-user locking"""
        user_lock = self._get_user_lock(telegram_id)
        with user_lock:
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
        if telegram_id in self.config.AUTHORIZED_USER_IDS:
            return True
        
        if hasattr(self.config, 'ID_USER_PUBLIC') and telegram_id in self.config.ID_USER_PUBLIC:
            return True
        
        return False
