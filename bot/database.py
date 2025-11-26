from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, text, BigInteger, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError, OperationalError, IntegrityError, TimeoutError as SATimeoutError
from sqlalchemy.exc import DatabaseError as SQLAlchemyDatabaseError
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
from datetime import datetime
import os
import time
import threading
from typing import Callable, Any, Optional, Generator, Dict, List
from functools import wraps
import logging

logger = logging.getLogger('DatabaseManager')

Base = declarative_base()

_transaction_lock = threading.Lock()

POOL_SIZE = 5
MAX_OVERFLOW = 10
POOL_TIMEOUT = 30
POOL_RECYCLE = 3600
POOL_PRE_PING = True
POOL_EXHAUSTED_MAX_RETRIES = 3
POOL_EXHAUSTED_INITIAL_DELAY = 0.5
POOL_HIGH_UTILIZATION_THRESHOLD = 80

class DatabaseError(Exception):
    """Base exception for database errors"""
    pass

class RetryableError(DatabaseError):
    """Database error that can be retried"""
    pass

class ConnectionPoolExhausted(DatabaseError):
    """Connection pool exhausted error after all retries failed"""
    pass

class PoolTimeoutError(DatabaseError):
    """Pool timeout error - could not acquire connection in time"""
    pass

def retry_on_db_error(max_retries: int = 3, initial_delay: float = 0.1):
    """Decorator to retry database operations with exponential backoff.
    
    Handles pool timeout errors and operational errors with retry logic.
    
    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds before first retry
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            delay = initial_delay
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except SATimeoutError as e:
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"Pool timeout in {func.__name__} (attempt {attempt + 1}/{max_retries}): {e}"
                        )
                        logger.info(f"Retrying in {delay:.2f}s...")
                        time.sleep(delay)
                        delay *= 2
                        last_exception = e
                    else:
                        logger.error(f"Pool timeout - max retries reached for {func.__name__}: {e}")
                        raise ConnectionPoolExhausted(
                            f"Pool exhausted after {max_retries} retries in {func.__name__}"
                        ) from e
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
    user_id = Column(BigInteger, nullable=False)
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
    user_id = Column(BigInteger, nullable=False)
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
    user_id = Column(BigInteger, nullable=False)
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
    """Database manager with connection pooling and rollback safety.
    
    Connection Pooling:
    - Uses SQLAlchemy QueuePool with configurable pool_size and max_overflow
    - pool_pre_ping ensures connections are valid before use
    - Pool monitoring via get_pool_status()
    
    Rollback Safety:
    - Per-operation rollback guarantees via try/except/finally
    - Safe session closure in finally blocks
    - transaction_scope() context manager for atomic operations
    """
    def __init__(self, db_path='data/bot.db', database_url=''):
        """Initialize database with PostgreSQL or SQLite support
        
        Args:
            db_path: Path to SQLite database (used if database_url is not provided)
            database_url: PostgreSQL connection URL (e.g., postgresql://user:pass@host:port/dbname)
        """
        self.is_postgres = False
        self.engine = None
        self.Session = None
        self._pool_stats = {
            'checkouts': 0,
            'checkins': 0,
            'connects': 0,
            'disconnects': 0,
            'overflow_connections': 0,
            'timeout_errors': 0,
            'high_utilization_warnings': 0,
            'total_wait_time_ms': 0.0,
            'max_wait_time_ms': 0.0,
            'checkout_attempts': 0
        }
        self._pool_stats_lock = threading.Lock()
        self._last_checkout_start = threading.local()
        
        try:
            if database_url and database_url.strip():
                logger.info(f"Using PostgreSQL from DATABASE_URL")
                db_url = database_url.strip()
                self.is_postgres = db_url.startswith('postgresql://') or db_url.startswith('postgres://')
                
                engine_kwargs = {
                    'echo': False,
                    'pool_pre_ping': POOL_PRE_PING,
                    'pool_recycle': POOL_RECYCLE,
                    'pool_size': POOL_SIZE,
                    'max_overflow': MAX_OVERFLOW,
                    'pool_timeout': POOL_TIMEOUT,
                    'poolclass': QueuePool
                }
                
                if not self.is_postgres:
                    engine_kwargs['connect_args'] = {
                        'check_same_thread': False,
                        'timeout': 30.0
                    }
                
                self.engine = create_engine(db_url, **engine_kwargs)
                logger.info(f"✅ Database engine created: {'PostgreSQL' if self.is_postgres else 'SQLite (from URL)'}")
                logger.info(f"   Pool config: size={POOL_SIZE}, max_overflow={MAX_OVERFLOW}, timeout={POOL_TIMEOUT}s")
                
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
                    pool_pre_ping=POOL_PRE_PING,
                    pool_recycle=POOL_RECYCLE
                )
            
            self._setup_pool_event_listeners()
            
            self._configure_database()
            
            Base.metadata.create_all(self.engine)
            
            self._migrate_database()
            
            self.Session = scoped_session(sessionmaker(bind=self.engine))
            
            logger.info("✅ Database initialized successfully")
            
        except ValueError as e:
            logger.error(f"Configuration error during database initialization: {e}")
            raise DatabaseError(f"Database configuration failed: {e}")
        except OperationalError as e:
            logger.error(f"Operational error during database initialization (connection/timeout): {e}")
            raise DatabaseError(f"Database connection failed: {e}")
        except IntegrityError as e:
            logger.error(f"Integrity error during database initialization: {e}")
            raise DatabaseError(f"Database integrity error: {e}")
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy error during database initialization: {e}")
            raise DatabaseError(f"Database initialization failed: {e}")
        except OSError as e:
            logger.error(f"OS error during database initialization (file/directory): {e}")
            raise DatabaseError(f"Database file system error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during database initialization: {type(e).__name__}: {e}")
            raise DatabaseError(f"Database initialization failed: {e}")
    
    def _setup_pool_event_listeners(self):
        """Setup event listeners for connection pool monitoring."""
        db_manager = self
        
        @event.listens_for(self.engine, 'checkout')
        def on_checkout(dbapi_conn, connection_record, connection_proxy):
            checkout_start = getattr(db_manager._last_checkout_start, 'start_time', None)
            wait_time_ms = 0.0
            if checkout_start is not None:
                wait_time_ms = (time.time() - checkout_start) * 1000
            
            with db_manager._pool_stats_lock:
                db_manager._pool_stats['checkouts'] += 1
                if wait_time_ms > 0:
                    db_manager._pool_stats['total_wait_time_ms'] += wait_time_ms
                    if wait_time_ms > db_manager._pool_stats['max_wait_time_ms']:
                        db_manager._pool_stats['max_wait_time_ms'] = wait_time_ms
            
            db_manager._check_and_warn_high_utilization()
        
        @event.listens_for(self.engine, 'checkin')
        def on_checkin(dbapi_conn, connection_record):
            with db_manager._pool_stats_lock:
                db_manager._pool_stats['checkins'] += 1
        
        @event.listens_for(self.engine, 'connect')
        def on_connect(dbapi_conn, connection_record):
            with db_manager._pool_stats_lock:
                db_manager._pool_stats['connects'] += 1
        
        @event.listens_for(self.engine, 'close')
        def on_close(dbapi_conn, connection_record):
            with db_manager._pool_stats_lock:
                db_manager._pool_stats['disconnects'] += 1
        
        logger.debug("Pool event listeners configured")
    
    def _check_and_warn_high_utilization(self):
        """Check pool utilization and log warning if above threshold."""
        try:
            pool = self.engine.pool
            if hasattr(pool, 'checkedout') and hasattr(pool, 'size'):
                checked_out = pool.checkedout()
                max_connections = pool.size() + MAX_OVERFLOW
                if max_connections > 0:
                    utilization = (checked_out / max_connections) * 100
                    if utilization >= POOL_HIGH_UTILIZATION_THRESHOLD:
                        with self._pool_stats_lock:
                            self._pool_stats['high_utilization_warnings'] += 1
                        logger.warning(
                            f"⚠️ High pool utilization: {utilization:.1f}% "
                            f"(checked_out={checked_out}, max={max_connections})"
                        )
        except Exception as e:
            logger.debug(f"Error checking pool utilization: {e}")
    
    def get_pool_status(self) -> Dict:
        """Get current connection pool status.
        
        Returns:
            Dict with pool statistics and current state
        """
        pool = self.engine.pool
        
        with self._pool_stats_lock:
            stats = self._pool_stats.copy()
        
        status = {
            'pool_size': pool.size() if hasattr(pool, 'size') else POOL_SIZE,
            'checked_in': pool.checkedin() if hasattr(pool, 'checkedin') else 'N/A',
            'checked_out': pool.checkedout() if hasattr(pool, 'checkedout') else 'N/A',
            'overflow': pool.overflow() if hasattr(pool, 'overflow') else 'N/A',
            'max_overflow': MAX_OVERFLOW,
            'pool_timeout': POOL_TIMEOUT,
            'total_checkouts': stats['checkouts'],
            'total_checkins': stats['checkins'],
            'total_connects': stats['connects'],
            'total_disconnects': stats['disconnects'],
            'is_postgres': self.is_postgres,
            'timeout_errors': stats['timeout_errors'],
            'high_utilization_warnings': stats['high_utilization_warnings'],
            'total_wait_time_ms': round(stats['total_wait_time_ms'], 2),
            'max_wait_time_ms': round(stats['max_wait_time_ms'], 2),
            'avg_wait_time_ms': round(stats['total_wait_time_ms'] / max(stats['checkout_attempts'], 1), 2)
        }
        
        active = stats['checkouts'] - stats['checkins']
        status['estimated_active_connections'] = max(0, active)
        
        if hasattr(pool, 'checkedout') and hasattr(pool, 'size'):
            utilization = pool.checkedout() / (pool.size() + MAX_OVERFLOW) * 100 if pool.size() > 0 else 0
            status['pool_utilization_percent'] = round(utilization, 1)
        
        return status
    
    def log_pool_status(self, level: str = 'info'):
        """Log current pool status for monitoring.
        
        Args:
            level: Log level - 'info', 'warning', or 'error'
        """
        status = self.get_pool_status()
        message = (
            f"Pool Status: checked_in={status['checked_in']}, "
            f"checked_out={status['checked_out']}, "
            f"overflow={status['overflow']}, "
            f"utilization={status.get('pool_utilization_percent', 'N/A')}%, "
            f"timeouts={status['timeout_errors']}, "
            f"avg_wait={status['avg_wait_time_ms']}ms"
        )
        
        if level == 'warning':
            logger.warning(message)
        elif level == 'error':
            logger.error(message)
        else:
            logger.info(message)
    
    def check_pool_health(self) -> Dict:
        """Perform periodic pool health check.
        
        Returns:
            Dict with health status and recommendations
        """
        status = self.get_pool_status()
        health = {
            'healthy': True,
            'status': status,
            'warnings': [],
            'recommendations': []
        }
        
        utilization = status.get('pool_utilization_percent', 0)
        if utilization >= POOL_HIGH_UTILIZATION_THRESHOLD:
            health['healthy'] = False
            health['warnings'].append(f"High pool utilization: {utilization}%")
            health['recommendations'].append("Consider increasing pool_size or max_overflow")
        
        if status['timeout_errors'] > 0:
            health['warnings'].append(f"Pool timeout errors occurred: {status['timeout_errors']}")
            if status['timeout_errors'] > 5:
                health['healthy'] = False
                health['recommendations'].append("Investigate connection leaks or increase pool timeout")
        
        avg_wait = status['avg_wait_time_ms']
        if avg_wait > 1000:
            health['warnings'].append(f"High average wait time: {avg_wait}ms")
            health['recommendations'].append("Pool may be undersized for current load")
        
        max_wait = status['max_wait_time_ms']
        if max_wait > 5000:
            health['warnings'].append(f"Very high max wait time: {max_wait}ms")
        
        checked_out = status.get('checked_out', 0)
        if isinstance(checked_out, int) and checked_out == status['pool_size'] + status['max_overflow']:
            health['healthy'] = False
            health['warnings'].append("Pool is at maximum capacity")
            health['recommendations'].append("All connections in use, requests may timeout")
        
        if health['warnings']:
            logger.warning(f"Pool health check warnings: {health['warnings']}")
        else:
            logger.debug("Pool health check: OK")
        
        return health
    
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
        except OperationalError as e:
            logger.error(f"Operational error configuring SQLite database: {e}")
            raise
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy error configuring database: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error configuring database: {type(e).__name__}: {e}")
            raise
    
    @retry_on_db_error(max_retries=3, initial_delay=0.1)
    def _migrate_database(self):
        """Auto-migrate database schema with error handling and validation"""
        logger.info("Checking database schema migrations...")
        
        try:
            with self.engine.connect() as conn:
                try:
                    self._migrate_trades_table(conn)
                except (OperationalError, IntegrityError, SQLAlchemyError) as e:
                    logger.error(f"Migration error on trades table: {type(e).__name__}: {e}")
                    raise DatabaseError(f"Trades table migration failed: {e}")
                
                try:
                    self._migrate_signal_logs_table(conn)
                except (OperationalError, IntegrityError, SQLAlchemyError) as e:
                    logger.error(f"Migration error on signal_logs table: {type(e).__name__}: {e}")
                    raise DatabaseError(f"Signal logs table migration failed: {e}")
                
                try:
                    self._migrate_positions_table(conn)
                except (OperationalError, IntegrityError, SQLAlchemyError) as e:
                    logger.error(f"Migration error on positions table: {type(e).__name__}: {e}")
                    raise DatabaseError(f"Positions table migration failed: {e}")
                
            logger.info("✅ Database migrations completed successfully")
        
        except DatabaseError:
            raise
        except OperationalError as e:
            logger.error(f"Operational error during database migration: {e}")
            raise DatabaseError(f"Migration failed (connection/lock issue): {e}")
        except IntegrityError as e:
            logger.error(f"Integrity error during database migration: {e}")
            raise DatabaseError(f"Migration failed (data integrity issue): {e}")
        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy error during database migration: {e}")
            raise DatabaseError(f"Migration failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during database migration: {type(e).__name__}: {e}")
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
                try:
                    if self.is_postgres:
                        conn.execute(text("""
                            ALTER TABLE trades 
                            ALTER COLUMN user_id TYPE BIGINT
                        """))
                    else:
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
    
    def _get_session_with_pool_retry(
        self, 
        max_retries: int = POOL_EXHAUSTED_MAX_RETRIES,
        initial_delay: float = POOL_EXHAUSTED_INITIAL_DELAY
    ):
        """Get session with retry logic for pool exhaustion.
        
        Implements exponential backoff when pool is exhausted.
        
        Args:
            max_retries: Maximum number of retry attempts
            initial_delay: Initial delay in seconds before first retry
            
        Returns:
            Session object for database operations
            
        Raises:
            ConnectionPoolExhausted: If pool is exhausted after all retries
            PoolTimeoutError: If timeout occurs and retries are exhausted
            DatabaseError: For other session creation failures
        """
        delay = initial_delay
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                with self._pool_stats_lock:
                    self._pool_stats['checkout_attempts'] += 1
                
                self._last_checkout_start.start_time = time.time()
                session = self.Session()
                return session
                
            except SATimeoutError as e:
                last_exception = e
                with self._pool_stats_lock:
                    self._pool_stats['timeout_errors'] += 1
                
                self.log_pool_status(level='warning')
                
                if attempt < max_retries:
                    logger.warning(
                        f"⚠️ Pool timeout on attempt {attempt + 1}/{max_retries + 1}: {e}. "
                        f"Retrying in {delay:.2f}s..."
                    )
                    time.sleep(delay)
                    delay *= 2
                else:
                    logger.error(
                        f"❌ Pool exhausted after {max_retries + 1} attempts. "
                        f"Last error: {e}"
                    )
                    self.log_pool_status(level='error')
                    raise ConnectionPoolExhausted(
                        f"Connection pool exhausted after {max_retries + 1} attempts. "
                        f"Pool timeout: {POOL_TIMEOUT}s. Consider increasing pool_size or max_overflow."
                    ) from e
                    
            except OperationalError as e:
                last_exception = e
                error_str = str(e).lower()
                if 'timeout' in error_str or 'pool' in error_str:
                    with self._pool_stats_lock:
                        self._pool_stats['timeout_errors'] += 1
                    
                    if attempt < max_retries:
                        logger.warning(
                            f"⚠️ Pool operational error on attempt {attempt + 1}/{max_retries + 1}: {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )
                        self.log_pool_status(level='warning')
                        time.sleep(delay)
                        delay *= 2
                    else:
                        logger.error(f"❌ Pool exhausted (operational error) after {max_retries + 1} attempts: {e}")
                        self.log_pool_status(level='error')
                        raise ConnectionPoolExhausted(
                            f"Connection pool exhausted (operational error) after {max_retries + 1} attempts"
                        ) from e
                else:
                    logger.error(f"Operational error creating session: {e}")
                    self.log_pool_status(level='error')
                    raise
                    
            except Exception as e:
                logger.error(f"Unexpected error creating database session: {type(e).__name__}: {e}")
                self.log_pool_status(level='error')
                raise DatabaseError(f"Failed to create session: {e}") from e
        
        if last_exception:
            raise ConnectionPoolExhausted(
                f"Connection pool exhausted after {max_retries + 1} attempts"
            ) from last_exception
    
    def get_session(self):
        """Get database session with pool timeout handling and retry logic.
        
        Returns:
            Session object for database operations
            
        Raises:
            ConnectionPoolExhausted: If pool is exhausted after all retries
            DatabaseError: If session creation fails
        """
        return self._get_session_with_pool_retry()
    
    @contextmanager
    def safe_session(self) -> Generator:
        """Context manager for safe session handling with guaranteed rollback and closure.
        
        Provides per-operation rollback guarantees via try/except and safe session
        closure in finally blocks. Includes pool timeout handling with graceful degradation.
        
        Usage:
            with db.safe_session() as session:
                # do database operations
                session.add(...)
                # auto-commit on success, auto-rollback on failure
                
        Raises:
            ConnectionPoolExhausted: If pool is exhausted after all retries
            DatabaseError: For other session creation failures
        """
        session = None
        try:
            session = self.get_session()
            yield session
            session.commit()
        except ConnectionPoolExhausted:
            logger.error("Safe session failed: connection pool exhausted")
            raise
        except SATimeoutError as e:
            if session:
                try:
                    session.rollback()
                except Exception as rollback_error:
                    logger.error(f"Error during rollback after pool timeout: {rollback_error}")
            logger.error(f"Pool timeout in safe_session: {e}")
            self.log_pool_status(level='error')
            raise PoolTimeoutError(f"Pool timeout during session operation: {e}") from e
        except IntegrityError as e:
            if session:
                try:
                    session.rollback()
                except Exception as rollback_error:
                    logger.error(f"Error during rollback after IntegrityError: {rollback_error}")
            logger.error(f"Integrity error in safe_session: {e}")
            raise
        except OperationalError as e:
            if session:
                try:
                    session.rollback()
                except Exception as rollback_error:
                    logger.error(f"Error during rollback after OperationalError: {rollback_error}")
            logger.error(f"Operational error in safe_session: {e}")
            raise
        except SQLAlchemyError as e:
            if session:
                try:
                    session.rollback()
                except Exception as rollback_error:
                    logger.error(f"Error during rollback after SQLAlchemyError: {rollback_error}")
            logger.error(f"SQLAlchemy error in safe_session: {e}")
            raise
        except Exception as e:
            if session:
                try:
                    session.rollback()
                except Exception as rollback_error:
                    logger.error(f"Error during rollback: {rollback_error}")
            logger.error(f"Unexpected error in safe_session: {type(e).__name__}: {e}")
            raise
        finally:
            if session:
                try:
                    session.close()
                except Exception as close_error:
                    logger.error(f"Error closing session: {close_error}")
    
    @contextmanager
    def transaction_scope(self, isolation_level: Optional[str] = None) -> Generator:
        """
        Provide a transactional scope with proper isolation and pool timeout handling.
        
        Includes graceful degradation with retry logic for pool exhaustion scenarios.
        Connection is always returned to pool in finally block.
        
        Args:
            isolation_level: Optional isolation level ('SERIALIZABLE', 'REPEATABLE READ', 'READ COMMITTED')
        
        Usage:
            with db.transaction_scope() as session:
                # do database operations
                session.add(...)
                # auto-commit on success, auto-rollback on failure
                
        Raises:
            ConnectionPoolExhausted: If pool is exhausted after all retries
            PoolTimeoutError: If timeout occurs during session operations
        """
        session = None
        transaction_exception = None
        
        try:
            session = self.get_session()
            
            if isolation_level and self.is_postgres:
                session.execute(text(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}"))
            
            yield session
            session.commit()
            
        except ConnectionPoolExhausted as e:
            transaction_exception = e
            logger.error(f"Transaction failed: connection pool exhausted")
            raise
        except SATimeoutError as e:
            transaction_exception = e
            if session:
                try:
                    session.rollback()
                except Exception as rollback_error:
                    logger.error(f"Error during rollback after pool timeout: {rollback_error}")
            logger.error(f"Transaction rolled back due to pool timeout: {e}")
            self.log_pool_status(level='error')
            raise PoolTimeoutError(f"Pool timeout during transaction: {e}") from e
        except IntegrityError as e:
            transaction_exception = e
            if session:
                try:
                    session.rollback()
                except Exception as rollback_error:
                    logger.error(f"Error during rollback after IntegrityError: {rollback_error}")
            logger.error(f"Transaction rolled back due to integrity error: {e}")
            raise
        except OperationalError as e:
            transaction_exception = e
            if session:
                try:
                    session.rollback()
                except Exception as rollback_error:
                    logger.error(f"Error during rollback after OperationalError: {rollback_error}")
            logger.error(f"Transaction rolled back due to operational error: {e}")
            raise
        except SQLAlchemyError as e:
            transaction_exception = e
            if session:
                try:
                    session.rollback()
                except Exception as rollback_error:
                    logger.error(f"Error during rollback after SQLAlchemyError: {rollback_error}")
            logger.error(f"Transaction rolled back due to SQLAlchemy error: {e}")
            raise
        except Exception as e:
            transaction_exception = e
            if session:
                try:
                    session.rollback()
                except Exception as rollback_error:
                    logger.error(f"Error during rollback: {rollback_error}")
            logger.error(f"Transaction rolled back: {type(e).__name__}: {e}")
            raise
        finally:
            if session:
                try:
                    session.close()
                except Exception as close_error:
                    logger.error(f"Error closing session: {close_error}")
                    if transaction_exception is None:
                        raise
    
    @contextmanager
    def serializable_transaction(self) -> Generator:
        """
        Provide a serializable transaction scope for concurrent user operations.
        Prevents race conditions when multiple users trading simultaneously.
        """
        with _transaction_lock:
            with self.transaction_scope('SERIALIZABLE' if self.is_postgres else None) as session:
                yield session
    
    def atomic_create_trade(self, session, trade_data: dict) -> Optional[int]:
        """
        Create trade atomically with proper locking.
        
        Args:
            session: Database session
            trade_data: Trade data dictionary
            
        Returns:
            Trade ID if successful, None otherwise
        """
        try:
            from bot.database import Trade
            
            trade = Trade(**trade_data)
            session.add(trade)
            session.flush()
            trade_id = trade.id
            
            return trade_id
            
        except IntegrityError as e:
            try:
                session.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback after IntegrityError: {rollback_error}")
            logger.error(f"Integrity error creating trade atomically: {e}")
            raise
        except OperationalError as e:
            try:
                session.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback after OperationalError: {rollback_error}")
            logger.error(f"Operational error creating trade atomically: {e}")
            raise
        except SQLAlchemyError as e:
            try:
                session.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback after SQLAlchemyError: {rollback_error}")
            logger.error(f"SQLAlchemy error creating trade atomically: {e}")
            raise
        except Exception as e:
            try:
                session.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback: {rollback_error}")
            logger.error(f"Unexpected error creating trade atomically: {type(e).__name__}: {e}")
            raise
    
    def atomic_create_position(self, session, position_data: dict) -> Optional[int]:
        """
        Create position atomically with proper locking.
        
        Args:
            session: Database session  
            position_data: Position data dictionary
            
        Returns:
            Position ID if successful, None otherwise
        """
        try:
            from bot.database import Position
            
            position = Position(**position_data)
            session.add(position)
            session.flush()
            position_id = position.id
            
            return position_id
            
        except IntegrityError as e:
            try:
                session.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback after IntegrityError: {rollback_error}")
            logger.error(f"Integrity error creating position atomically: {e}")
            raise
        except OperationalError as e:
            try:
                session.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback after OperationalError: {rollback_error}")
            logger.error(f"Operational error creating position atomically: {e}")
            raise
        except SQLAlchemyError as e:
            try:
                session.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback after SQLAlchemyError: {rollback_error}")
            logger.error(f"SQLAlchemy error creating position atomically: {e}")
            raise
        except Exception as e:
            try:
                session.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback: {rollback_error}")
            logger.error(f"Unexpected error creating position atomically: {type(e).__name__}: {e}")
            raise
    
    def close(self):
        """Close database connections with error handling and pool cleanup."""
        try:
            logger.info("Closing database connections...")
            self.log_pool_status()
            self.Session.remove()
            self.engine.dispose()
            logger.info("✅ Database connections closed successfully")
        except Exception as e:
            logger.error(f"Error closing database: {type(e).__name__}: {e}")
