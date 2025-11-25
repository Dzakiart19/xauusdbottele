from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, text, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError, OperationalError, IntegrityError
from sqlalchemy.exc import DatabaseError as SQLAlchemyDatabaseError
from datetime import datetime
import os
import time
from typing import Callable, Any
from functools import wraps
import logging

logger = logging.getLogger('DatabaseManager')

Base = declarative_base()

class DatabaseError(Exception):
    """Base exception for database errors"""
    pass

class RetryableError(DatabaseError):
    """Database error that can be retried"""
    pass

def retry_on_db_error(max_retries: int = 3, initial_delay: float = 0.1):
    """Decorator to retry database operations with exponential backoff"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            delay = initial_delay
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except OperationalError as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Database operational error in {func.__name__} (attempt {attempt + 1}/{max_retries}): {e}")
                        logger.info(f"Retrying in {delay:.2f}s...")
                        time.sleep(delay)
                        delay *= 2
                        last_exception = e
                    else:
                        logger.error(f"Max retries reached for {func.__name__}: {e}")
                        raise
                except IntegrityError as e:
                    logger.error(f"Integrity error in {func.__name__} (non-retryable): {e}")
                    raise
                except SQLAlchemyDatabaseError as e:
                    logger.error(f"Database error in {func.__name__} (non-retryable): {e}")
                    raise
                except Exception as e:
                    logger.error(f"Unexpected error in {func.__name__}: {type(e).__name__}: {e}")
                    raise
            
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator

class Trade(Base):
    """Trade record with support for large Telegram user IDs (BigInteger)"""
    __tablename__ = 'trades'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False)  # BigInteger for Telegram IDs (can exceed 32-bit)
    ticker = Column(String(20), nullable=False)
    signal_type = Column(String(10), nullable=False)
    signal_source = Column(String(10), default='auto')
    entry_price = Column(Float, nullable=False)
    stop_loss = Column(Float, nullable=False)
    take_profit = Column(Float, nullable=False)
    spread = Column(Float)
    estimated_pl = Column(Float)
    actual_pl = Column(Float)
    exit_price = Column(Float)
    status = Column(String(20), default='OPEN')
    signal_time = Column(DateTime, default=datetime.utcnow)
    close_time = Column(DateTime)
    timeframe = Column(String(10))
    result = Column(String(10))
    
class SignalLog(Base):
    """Signal log with support for large Telegram user IDs (BigInteger)"""
    __tablename__ = 'signal_logs'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False)  # BigInteger for Telegram IDs (can exceed 32-bit)
    ticker = Column(String(20), nullable=False)
    signal_type = Column(String(10), nullable=False)
    signal_source = Column(String(10), default='auto')
    entry_price = Column(Float, nullable=False)
    indicators = Column(Text)
    timestamp = Column(DateTime, default=datetime.utcnow)
    accepted = Column(Boolean, default=False)
    rejection_reason = Column(String(255))

class Position(Base):
    """Position tracking with support for large Telegram user IDs (BigInteger)"""
    __tablename__ = 'positions'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, nullable=False)  # BigInteger for Telegram IDs (can exceed 32-bit)
    trade_id = Column(Integer, nullable=False)
    ticker = Column(String(20), nullable=False)
    signal_type = Column(String(10), nullable=False)
    entry_price = Column(Float, nullable=False)
    stop_loss = Column(Float, nullable=False)
    take_profit = Column(Float, nullable=False)
    current_price = Column(Float)
    unrealized_pl = Column(Float)
    status = Column(String(20), default='ACTIVE')
    opened_at = Column(DateTime, default=datetime.utcnow)
    closed_at = Column(DateTime)
    original_sl = Column(Float)
    sl_adjustment_count = Column(Integer, default=0)
    max_profit_reached = Column(Float, default=0.0)
    last_price_update = Column(DateTime)

class Performance(Base):
    __tablename__ = 'performance'
    
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, default=datetime.utcnow)
    total_trades = Column(Integer, default=0)
    wins = Column(Integer, default=0)
    losses = Column(Integer, default=0)
    total_pl = Column(Float, default=0.0)
    max_drawdown = Column(Float, default=0.0)
    equity = Column(Float, default=0.0)

class CandleData(Base):
    __tablename__ = 'candle_data'
    
    id = Column(Integer, primary_key=True)
    timeframe = Column(String(3), nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

class DatabaseManager:
    def __init__(self, db_path='data/bot.db', database_url=''):
        """Initialize database with PostgreSQL or SQLite support
        
        Args:
            db_path: Path to SQLite database (used if database_url is not provided)
            database_url: PostgreSQL connection URL (e.g., postgresql://user:pass@host:port/dbname)
        """
        try:
            self.is_postgres = False
            
            if database_url and database_url.strip():
                logger.info(f"Using PostgreSQL from DATABASE_URL")
                db_url = database_url.strip()
                self.is_postgres = db_url.startswith('postgresql://') or db_url.startswith('postgres://')
                
                engine_kwargs = {
                    'echo': False,
                    'pool_pre_ping': True,
                    'pool_recycle': 3600,
                    'pool_size': 5,
                    'max_overflow': 10,
                    'pool_timeout': 30
                }
                
                if not self.is_postgres:
                    engine_kwargs['connect_args'] = {
                        'check_same_thread': False,
                        'timeout': 30.0
                    }
                
                self.engine = create_engine(db_url, **engine_kwargs)
                logger.info(f"✅ Database engine created: {'PostgreSQL' if self.is_postgres else 'SQLite (from URL)'}")
                
            else:
                if not db_path or not isinstance(db_path, str):
                    raise ValueError(f"Invalid db_path: {db_path}")
                
                db_dir = os.path.dirname(db_path)
                if db_dir:
                    os.makedirs(db_dir, exist_ok=True)
                
                logger.info(f"Using SQLite database: {db_path}")
                
                self.engine = create_engine(
                    f'sqlite:///{db_path}',
                    connect_args={
                        'check_same_thread': False,
                        'timeout': 30.0
                    },
                    echo=False,
                    pool_pre_ping=True,
                    pool_recycle=3600
                )
            
            self._configure_database()
            
            Base.metadata.create_all(self.engine)
            
            self._migrate_database()
            
            self.Session = scoped_session(sessionmaker(bind=self.engine))
            
            logger.info("✅ Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {type(e).__name__}: {e}")
            raise DatabaseError(f"Database initialization failed: {e}")
    
    def _configure_database(self):
        """Configure database with proper settings (SQLite only)"""
        if self.is_postgres:
            logger.info("PostgreSQL detected - skipping SQLite-specific configuration")
            return
            
        try:
            with self.engine.connect() as conn:
                conn.execute(text('PRAGMA journal_mode=WAL'))
                conn.execute(text('PRAGMA synchronous=NORMAL'))
                conn.execute(text('PRAGMA temp_store=MEMORY'))
                conn.execute(text('PRAGMA mmap_size=30000000000'))
                conn.execute(text('PRAGMA page_size=4096'))
                conn.commit()
                logger.debug("SQLite configuration applied successfully")
        except Exception as e:
            logger.error(f"Error configuring database: {e}")
            raise
    
    @retry_on_db_error(max_retries=3, initial_delay=0.1)
    def _migrate_database(self):
        """Auto-migrate database schema with error handling and validation"""
        logger.info("Checking database schema migrations...")
        
        try:
            with self.engine.connect() as conn:
                self._migrate_trades_table(conn)
                self._migrate_signal_logs_table(conn)
                self._migrate_positions_table(conn)
                
            logger.info("✅ Database migrations completed successfully")
                
        except Exception as e:
            logger.error(f"Error during database migration: {type(e).__name__}: {e}")
            raise DatabaseError(f"Migration failed: {e}")
    
    def _migrate_trades_table(self, conn):
        """Migrate trades table schema - convert user_id to BIGINT for large Telegram IDs"""
        try:
            if self.is_postgres:
                result = conn.execute(text("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'trades'
                """))
                columns = [row[0] for row in result]
            else:
                result = conn.execute(text("PRAGMA table_info(trades)"))
                columns = [row[1] for row in result]
                
            if 'signal_source' not in columns:
                conn.execute(text("ALTER TABLE trades ADD COLUMN signal_source VARCHAR(10) DEFAULT 'auto'"))
                conn.commit()
                logger.info("✅ Added signal_source column to trades table")
            
            if 'user_id' not in columns:
                conn.execute(text("ALTER TABLE trades ADD COLUMN user_id BIGINT DEFAULT 0"))
                conn.commit()
                logger.info("✅ Added user_id column (BIGINT) to trades table")
            else:
                # Migrate existing INTEGER user_id to BIGINT if needed
                try:
                    if self.is_postgres:
                        conn.execute(text("""
                            ALTER TABLE trades 
                            ALTER COLUMN user_id TYPE BIGINT
                        """))
                    else:
                        # SQLite: Create new column and copy data
                        result = conn.execute(text("PRAGMA table_info(trades)"))
                        columns = {row[1]: row[2] for row in result}
                        if 'user_id' in columns and columns['user_id'] != 'BIGINT':
                            logger.info("Migrating user_id from INTEGER to support larger Telegram IDs...")
                            conn.execute(text("ALTER TABLE trades ADD COLUMN user_id_new BIGINT"))
                            conn.execute(text("UPDATE trades SET user_id_new = user_id WHERE user_id IS NOT NULL"))
                            conn.execute(text("ALTER TABLE trades DROP COLUMN user_id"))
                            conn.execute(text("ALTER TABLE trades RENAME COLUMN user_id_new TO user_id"))
                            logger.info("✅ Migrated user_id to BIGINT")
                    conn.commit()
                except Exception as e:
                    logger.debug(f"Column type migration info: {e}")
                
        except Exception as e:
            logger.error(f"Error migrating trades table: {e}")
            raise
    
    def _migrate_signal_logs_table(self, conn):
        """Migrate signal_logs table schema - convert user_id to BIGINT"""
        try:
            if self.is_postgres:
                result = conn.execute(text("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'signal_logs'
                """))
                columns = [row[0] for row in result]
            else:
                result = conn.execute(text("PRAGMA table_info(signal_logs)"))
                columns = [row[1] for row in result]
            
            if 'signal_source' not in columns:
                conn.execute(text("ALTER TABLE signal_logs ADD COLUMN signal_source VARCHAR(10) DEFAULT 'auto'"))
                conn.commit()
                logger.info("✅ Added signal_source column to signal_logs table")
            
            if 'user_id' not in columns:
                conn.execute(text("ALTER TABLE signal_logs ADD COLUMN user_id BIGINT DEFAULT 0"))
                conn.commit()
                logger.info("✅ Added user_id column (BIGINT) to signal_logs table")
            else:
                # Migrate existing INTEGER user_id to BIGINT if needed
                try:
                    if self.is_postgres:
                        conn.execute(text("""
                            ALTER TABLE signal_logs 
                            ALTER COLUMN user_id TYPE BIGINT
                        """))
                    conn.commit()
                except Exception as e:
                    logger.debug(f"Column type migration info: {e}")
                
        except Exception as e:
            logger.error(f"Error migrating signal_logs table: {e}")
            raise
    
    def _migrate_positions_table(self, conn):
        """Migrate positions table schema - convert user_id to BIGINT"""
        try:
            if self.is_postgres:
                result = conn.execute(text("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'positions'
                """))
                columns = [row[0] for row in result]
            else:
                result = conn.execute(text("PRAGMA table_info(positions)"))
                columns = [row[1] for row in result]
            
            
            if 'user_id' not in columns:
                conn.execute(text("ALTER TABLE positions ADD COLUMN user_id BIGINT DEFAULT 0"))
                conn.commit()
                logger.info("✅ Added user_id column (BIGINT) to positions table")
            else:
                # Migrate existing INTEGER user_id to BIGINT if needed
                try:
                    if self.is_postgres:
                        conn.execute(text("""
                            ALTER TABLE positions 
                            ALTER COLUMN user_id TYPE BIGINT
                        """))
                    conn.commit()
                except Exception as e:
                    logger.debug(f"Column type migration info: {e}")
            
            if 'original_sl' not in columns:
                conn.execute(text("ALTER TABLE positions ADD COLUMN original_sl REAL"))
                conn.commit()
                conn.execute(text("UPDATE positions SET original_sl = stop_loss WHERE original_sl IS NULL"))
                conn.commit()
                logger.info("✅ Added original_sl column to positions table with backfill")
            
            if 'sl_adjustment_count' not in columns:
                conn.execute(text("ALTER TABLE positions ADD COLUMN sl_adjustment_count INTEGER DEFAULT 0"))
                conn.commit()
                conn.execute(text("UPDATE positions SET sl_adjustment_count = 0 WHERE sl_adjustment_count IS NULL"))
                conn.commit()
                logger.info("✅ Added sl_adjustment_count column to positions table")
            
            if 'max_profit_reached' not in columns:
                conn.execute(text("ALTER TABLE positions ADD COLUMN max_profit_reached REAL DEFAULT 0.0"))
                conn.commit()
                conn.execute(text("UPDATE positions SET max_profit_reached = 0.0 WHERE max_profit_reached IS NULL"))
                conn.commit()
                logger.info("✅ Added max_profit_reached column to positions table")
            
            if 'last_price_update' not in columns:
                conn.execute(text("ALTER TABLE positions ADD COLUMN last_price_update TIMESTAMP"))
                conn.commit()
                
                if self.is_postgres:
                    conn.execute(text("UPDATE positions SET last_price_update = NOW() WHERE last_price_update IS NULL"))
                else:
                    conn.execute(text("UPDATE positions SET last_price_update = datetime('now') WHERE last_price_update IS NULL"))
                
                conn.commit()
                logger.info("✅ Added last_price_update column to positions table")
                
        except Exception as e:
            logger.error(f"Error migrating positions table: {e}")
            raise
    
    @retry_on_db_error(max_retries=2, initial_delay=0.05)
    def get_session(self):
        """Get database session with retry logic"""
        try:
            return self.Session()
        except Exception as e:
            logger.error(f"Error creating database session: {e}")
            raise DatabaseError(f"Failed to create session: {e}")
    
    def close(self):
        """Close database connections with error handling"""
        try:
            logger.info("Closing database connections...")
            self.Session.remove()
            self.engine.dispose()
            logger.info("✅ Database connections closed successfully")
        except Exception as e:
            logger.error(f"Error closing database: {type(e).__name__}: {e}")
