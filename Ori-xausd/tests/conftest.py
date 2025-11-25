import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, MagicMock, patch
import pytz
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import Config
from bot.database import DatabaseManager, Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

@pytest.fixture(autouse=True)
def mock_websockets_connect():
    with patch('websockets.connect', new_callable=AsyncMock) as mock_connect:
        mock_ws = AsyncMock()
        mock_ws.send = AsyncMock()
        mock_ws.recv = AsyncMock()
        mock_ws.close = AsyncMock()
        mock_connect.return_value = mock_ws
        yield mock_connect

@pytest.fixture
def mock_config():
    config = Mock(spec=Config)
    config.EMA_PERIODS = [5, 10, 20]
    config.RSI_PERIOD = 14
    config.RSI_OVERSOLD_LEVEL = 30
    config.RSI_OVERBOUGHT_LEVEL = 70
    config.STOCH_K_PERIOD = 14
    config.STOCH_D_PERIOD = 3
    config.STOCH_SMOOTH_K = 3
    config.STOCH_OVERSOLD_LEVEL = 20
    config.STOCH_OVERBOUGHT_LEVEL = 80
    config.ATR_PERIOD = 14
    config.MACD_FAST = 12
    config.MACD_SLOW = 26
    config.MACD_SIGNAL = 9
    config.VOLUME_THRESHOLD_MULTIPLIER = 0.5
    config.MAX_SPREAD_PIPS = 10.0
    config.SL_ATR_MULTIPLIER = 1.0
    config.DEFAULT_SL_PIPS = 20.0
    config.TP_RR_RATIO = 1.5
    config.DEFAULT_TP_PIPS = 30.0
    config.SIGNAL_COOLDOWN_SECONDS = 30
    config.MAX_TRADES_PER_DAY = 999999
    config.DAILY_LOSS_PERCENT = 3.0
    config.RISK_PER_TRADE_PERCENT = 0.5
    config.FIXED_RISK_AMOUNT = 1.0
    config.DYNAMIC_SL_LOSS_THRESHOLD = 1.0
    config.DYNAMIC_SL_TIGHTENING_MULTIPLIER = 0.5
    config.TRAILING_STOP_PROFIT_THRESHOLD = 1.0
    config.TRAILING_STOP_DISTANCE_PIPS = 5.0
    config.XAUUSD_PIP_VALUE = 10.0
    config.LOT_SIZE = 0.01
    config.DATABASE_PATH = ':memory:'
    config.TELEGRAM_BOT_TOKEN = 'test_token'
    config.AUTHORIZED_USER_IDS = [123456789]
    config.DRY_RUN = True
    config.TICK_LOG_SAMPLE_RATE = 30
    return config

@pytest.fixture
def test_db():
    engine = create_engine('sqlite:///:memory:', echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    yield session
    
    session.close()
    engine.dispose()

@pytest.fixture
def mock_db_manager(test_db):
    db_manager = Mock(spec=DatabaseManager)
    db_manager.get_session = Mock(return_value=test_db)
    db_manager.Session = Mock(return_value=test_db)
    return db_manager

@pytest.fixture
def sample_ohlc_data():
    dates = pd.date_range(start='2024-01-01', periods=100, freq='1min')
    np.random.seed(42)
    
    base_price = 2650.0
    price_changes = np.cumsum(np.random.randn(100) * 2)
    closes = base_price + price_changes
    
    opens = closes + np.random.randn(100) * 0.5
    highs = np.maximum(opens, closes) + np.abs(np.random.randn(100)) * 0.5
    lows = np.minimum(opens, closes) - np.abs(np.random.randn(100)) * 0.5
    volumes = np.random.randint(50, 200, 100)
    
    df = pd.DataFrame({
        'open': opens,
        'high': highs,
        'low': lows,
        'close': closes,
        'volume': volumes
    }, index=dates)
    
    return df

@pytest.fixture
def sample_indicators(sample_ohlc_data):
    return {
        'ema_5': 2651.5,
        'ema_10': 2651.0,
        'ema_20': 2650.5,
        'rsi': 55.0,
        'rsi_prev': 52.0,
        'stoch_k': 65.0,
        'stoch_d': 60.0,
        'stoch_k_prev': 58.0,
        'stoch_d_prev': 55.0,
        'macd': 0.5,
        'macd_signal': 0.3,
        'macd_histogram': 0.2,
        'macd_prev': 0.3,
        'macd_signal_prev': 0.4,
        'atr': 2.5,
        'volume': 150,
        'volume_avg': 120,
        'close': 2652.0,
        'high': 2653.0,
        'low': 2651.0
    }

@pytest.fixture
def bullish_indicators():
    return {
        'ema_5': 2655.0,
        'ema_10': 2653.0,
        'ema_20': 2650.0,
        'rsi': 65.0,
        'rsi_prev': 55.0,
        'stoch_k': 75.0,
        'stoch_d': 70.0,
        'stoch_k_prev': 68.0,
        'stoch_d_prev': 65.0,
        'macd': 1.5,
        'macd_signal': 1.0,
        'macd_histogram': 0.5,
        'macd_prev': 0.8,
        'macd_signal_prev': 1.2,
        'atr': 3.0,
        'volume': 200,
        'volume_avg': 120,
        'close': 2656.0,
        'high': 2657.0,
        'low': 2654.0
    }

@pytest.fixture
def bearish_indicators():
    return {
        'ema_5': 2640.0,
        'ema_10': 2643.0,
        'ema_20': 2646.0,
        'rsi': 35.0,
        'rsi_prev': 45.0,
        'stoch_k': 25.0,
        'stoch_d': 30.0,
        'stoch_k_prev': 32.0,
        'stoch_d_prev': 35.0,
        'macd': -1.5,
        'macd_signal': -1.0,
        'macd_histogram': -0.5,
        'macd_prev': -0.8,
        'macd_signal_prev': -1.2,
        'atr': 3.0,
        'volume': 200,
        'volume_avg': 120,
        'close': 2639.0,
        'high': 2641.0,
        'low': 2638.0
    }

@pytest.fixture
def mock_telegram_app():
    app = AsyncMock()
    app.bot = AsyncMock()
    app.bot.send_message = AsyncMock()
    app.bot.get_chat = AsyncMock()
    return app

@pytest.fixture
def mock_websocket():
    ws = AsyncMock()
    ws.send = AsyncMock()
    ws.recv = AsyncMock()
    ws.close = AsyncMock()
    return ws

@pytest.fixture
def sample_tick_data():
    return {
        'bid': 2651.50,
        'ask': 2651.70,
        'quote': 2651.60,
        'timestamp': datetime.now(pytz.UTC)
    }

@pytest.fixture
def mock_market_data_client(mock_config):
    from bot.market_data import MarketDataClient
    client = Mock(spec=MarketDataClient)
    client.config = mock_config
    client.current_bid = 2651.50
    client.current_ask = 2651.70
    client.current_quote = 2651.60
    client.current_timestamp = datetime.now(pytz.UTC)
    client.connected = True
    client.is_connected = Mock(return_value=True)
    client.get_current_price = Mock(return_value=(2651.50, 2651.70, 2651.60))
    client.get_historical_data = AsyncMock()
    return client

@pytest.fixture
def utc_now():
    return datetime.now(pytz.UTC)

@pytest.fixture
def jakarta_time(utc_now):
    jakarta_tz = pytz.timezone('Asia/Jakarta')
    return utc_now.astimezone(jakarta_tz)
