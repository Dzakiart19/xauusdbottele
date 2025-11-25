from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pytz
from bot.logger import setup_logger
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Float, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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
        
        engine = create_engine(f'sqlite:///{self.db_path}')
        Base.metadata.create_all(engine)
        
        Session = sessionmaker(bind=engine)
        self.session_factory = Session
        
        self.active_users = {}
        logger.info("User manager initialized")
    
    def get_session(self):
        return self.session_factory()
    
    def create_user(self, telegram_id: int, username: Optional[str] = None,
                   first_name: Optional[str] = None, last_name: Optional[str] = None) -> User:
        session = self.get_session()
        
        try:
            existing = session.query(User).filter(User.telegram_id == telegram_id).first()
            
            if existing:
                logger.info(f"User already exists: {telegram_id}")
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
            
            if not is_admin:
                user.subscription_tier = 'TRIAL'
                user.subscription_expires = datetime.utcnow() + timedelta(days=3)
                logger.info(f"User {telegram_id} assigned 3-day trial (expires: {user.subscription_expires})")
            
            session.add(user)
            session.commit()
            
            preferences = UserPreferences(telegram_id=telegram_id)
            session.add(preferences)
            session.commit()
            
            logger.info(f"Created new user: {telegram_id} ({username})")
            
            return user
            
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            session.rollback()
            return None
        finally:
            session.close()
    
    def get_user(self, telegram_id: int) -> Optional[User]:
        session = self.get_session()
        
        try:
            user = session.query(User).filter(User.telegram_id == telegram_id).first()
            
            if user:
                if user.subscription_tier in ['TRIAL', 'WEEKLY', 'MONTHLY', 'PREMIUM']:
                    if user.subscription_expires and user.subscription_expires <= datetime.utcnow():
                        user.subscription_tier = 'FREE'
                        user.subscription_expires = None
                        session.commit()
                        logger.info(f"User {telegram_id} subscription expired, downgraded to FREE")
            
            if user:
                session.expunge(user)
            
            return user
        finally:
            session.close()
    
    def get_user_by_username(self, username: str) -> Optional[int]:
        session = self.get_session()
        
        try:
            user = session.query(User).filter(User.username == username).first()
            return user.telegram_id if user else None
        finally:
            session.close()
    
    def update_user_activity(self, telegram_id: int):
        session = self.get_session()
        
        try:
            user = session.query(User).filter(User.telegram_id == telegram_id).first()
            
            if user:
                user.last_active = datetime.utcnow()
                session.commit()
                logger.debug(f"Updated activity for user {telegram_id}")
        except Exception as e:
            logger.error(f"Error updating user activity: {e}")
            session.rollback()
        finally:
            session.close()
    
    def is_authorized(self, telegram_id: int) -> bool:
        if not self.config.AUTHORIZED_USER_IDS:
            return True
        
        return telegram_id in self.config.AUTHORIZED_USER_IDS
    
    def is_admin(self, telegram_id: int) -> bool:
        user = self.get_user(telegram_id)
        return user.is_admin if user else False
    
    def get_all_users(self) -> List[User]:
        session = self.get_session()
        
        try:
            users = session.query(User).all()
            return users
        finally:
            session.close()
    
    def get_active_users(self) -> List[User]:
        session = self.get_session()
        
        try:
            users = session.query(User).filter(User.is_active == True).all()
            return users
        finally:
            session.close()
    
    def deactivate_user(self, telegram_id: int) -> bool:
        session = self.get_session()
        
        try:
            user = session.query(User).filter(User.telegram_id == telegram_id).first()
            
            if user:
                user.is_active = False
                session.commit()
                logger.info(f"Deactivated user: {telegram_id}")
                return True
            
            return False
        except Exception as e:
            logger.error(f"Error deactivating user: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
    def activate_user(self, telegram_id: int) -> bool:
        session = self.get_session()
        
        try:
            user = session.query(User).filter(User.telegram_id == telegram_id).first()
            
            if user:
                user.is_active = True
                session.commit()
                logger.info(f"Activated user: {telegram_id}")
                return True
            
            return False
        except Exception as e:
            logger.error(f"Error activating user: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
    def update_user_stats(self, telegram_id: int, profit: float):
        session = self.get_session()
        
        try:
            user = session.query(User).filter(User.telegram_id == telegram_id).first()
            
            if user:
                user.total_trades += 1
                user.total_profit += profit
                session.commit()
                logger.debug(f"Updated stats for user {telegram_id}: profit={profit}")
        except Exception as e:
            logger.error(f"Error updating user stats: {e}")
            session.rollback()
        finally:
            session.close()
    
    def get_user_preferences(self, telegram_id: int) -> Optional[UserPreferences]:
        session = self.get_session()
        
        try:
            prefs = session.query(UserPreferences).filter(
                UserPreferences.telegram_id == telegram_id
            ).first()
            return prefs
        finally:
            session.close()
    
    def update_user_preferences(self, telegram_id: int, **kwargs) -> bool:
        session = self.get_session()
        
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
            
            session.commit()
            logger.info(f"Updated preferences for user {telegram_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating preferences: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
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
            'total_profit': user.total_profit,
            'subscription_tier': user.subscription_tier
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
        session = self.get_session()
        
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
        finally:
            session.close()
    
    def is_premium(self, telegram_id: int) -> bool:
        session = self.get_session()
        
        try:
            user = session.query(User).filter(User.telegram_id == telegram_id).first()
            
            if not user:
                return False
            
            if user.is_admin:
                return True
            
            if user.subscription_tier in ['TRIAL', 'PREMIUM', 'WEEKLY', 'MONTHLY']:
                if user.subscription_expires:
                    if user.subscription_expires > datetime.utcnow():
                        return True
                    else:
                        user.subscription_tier = 'FREE'
                        user.subscription_expires = None
                        session.commit()
                        logger.info(f"Auto-downgraded expired user {telegram_id} to FREE")
                        return False
            
            return False
        finally:
            session.close()
    
    def upgrade_subscription(self, telegram_id: int, duration: str) -> bool:
        session = self.get_session()
        
        try:
            user = session.query(User).filter(User.telegram_id == telegram_id).first()
            
            if not user:
                logger.error(f"User not found: {telegram_id}")
                return False
            
            from datetime import timedelta
            
            if duration == '1week':
                days = 7
                tier = 'WEEKLY'
            elif duration == '1month':
                days = 30
                tier = 'MONTHLY'
            else:
                logger.error(f"Invalid duration: {duration}")
                return False
            
            now = datetime.utcnow()
            
            if user.subscription_expires and user.subscription_expires > now:
                new_expiry = user.subscription_expires + timedelta(days=days)
            else:
                new_expiry = now + timedelta(days=days)
            
            user.subscription_tier = tier
            user.subscription_expires = new_expiry
            session.commit()
            
            logger.info(f"Upgraded user {telegram_id} to {tier} until {new_expiry}")
            return True
            
        except Exception as e:
            logger.error(f"Error upgrading subscription: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
    def get_subscription_status(self, telegram_id: int) -> Dict:
        session = self.get_session()
        
        try:
            user = session.query(User).filter(User.telegram_id == telegram_id).first()
            
            if not user:
                return None
            
            if user.is_admin or telegram_id in self.config.AUTHORIZED_USER_IDS:
                return {
                    'tier': 'ADMIN',
                    'status': 'Unlimited',
                    'expires': None,
                    'days_left': None
                }
            
            if user.subscription_tier in ['TRIAL', 'WEEKLY', 'MONTHLY', 'PREMIUM']:
                if user.subscription_expires:
                    now = datetime.utcnow()
                    if user.subscription_expires > now:
                        days_left = (user.subscription_expires - now).days
                        jakarta_tz = pytz.timezone('Asia/Jakarta')
                        expires_local = user.subscription_expires.replace(tzinfo=pytz.UTC).astimezone(jakarta_tz)
                        
                        tier_label = 'TRIAL (3 Hari Gratis)' if user.subscription_tier == 'TRIAL' else user.subscription_tier
                        
                        return {
                            'tier': tier_label,
                            'status': 'Aktif',
                            'expires': expires_local.strftime('%d/%m/%Y %H:%M'),
                            'days_left': days_left
                        }
                    else:
                        user.subscription_tier = 'FREE'
                        user.subscription_expires = None
                        session.commit()
                        logger.info(f"Auto-downgraded expired user {telegram_id} to FREE in get_subscription_status")
                        
                        return {
                            'tier': 'FREE',
                            'status': 'Expired',
                            'expires': None,
                            'days_left': 0
                        }
            
            return {
                'tier': 'FREE',
                'status': 'Tidak Aktif',
                'expires': None,
                'days_left': None
            }
        finally:
            session.close()
    
    def extend_subscription(self, telegram_id: int, days: int) -> bool:
        session = self.get_session()
        
        try:
            user = session.query(User).filter(User.telegram_id == telegram_id).first()
            
            if not user:
                return False
            
            from datetime import timedelta
            now = datetime.utcnow()
            
            if user.subscription_expires and user.subscription_expires > now:
                user.subscription_expires += timedelta(days=days)
            else:
                user.subscription_expires = now + timedelta(days=days)
            
            session.commit()
            logger.info(f"Extended subscription for {telegram_id} by {days} days")
            return True
            
        except Exception as e:
            logger.error(f"Error extending subscription: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
    def has_access(self, telegram_id: int) -> bool:
        session = self.get_session()
        
        try:
            user = session.query(User).filter(User.telegram_id == telegram_id).first()
            
            if not user:
                return False
            
            if user.is_admin or telegram_id in self.config.AUTHORIZED_USER_IDS:
                return True
            
            if user.subscription_tier in ['TRIAL', 'PREMIUM', 'WEEKLY', 'MONTHLY']:
                if user.subscription_expires:
                    if user.subscription_expires > datetime.utcnow():
                        return True
                    else:
                        user.subscription_tier = 'FREE'
                        user.subscription_expires = None
                        session.commit()
                        logger.info(f"Auto-downgraded expired user {telegram_id} to FREE in has_access")
                        return False
            
            return False
        finally:
            session.close()
