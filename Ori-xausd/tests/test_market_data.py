import pytest
import asyncio
import pandas as pd
from datetime import datetime, timedelta
import pytz
from unittest.mock import AsyncMock, Mock, patch
from bot.market_data import MarketDataClient, OHLCBuilder

@pytest.mark.unit
class TestOHLCBuilder:
    
    def test_initialization(self):
        builder = OHLCBuilder(timeframe_minutes=1)
        
        assert builder.timeframe_minutes == 1
        assert builder.timeframe_seconds == 60
        assert builder.current_candle is None
        assert len(builder.candles) == 0
        assert builder.tick_count == 0
    
    def test_initialization_different_timeframes(self):
        builder_m1 = OHLCBuilder(timeframe_minutes=1)
        builder_m5 = OHLCBuilder(timeframe_minutes=5)
        
        assert builder_m1.timeframe_seconds == 60
        assert builder_m5.timeframe_seconds == 300
    
    def test_add_tick_first_tick(self):
        builder = OHLCBuilder(timeframe_minutes=1)
        timestamp = datetime(2024, 1, 1, 10, 0, 30, tzinfo=pytz.UTC)
        
        builder.add_tick(bid=2650.0, ask=2650.2, timestamp=timestamp)
        
        assert builder.current_candle is not None
        assert builder.current_candle['open'] == 2650.1
        assert builder.current_candle['close'] == 2650.1
        assert builder.current_candle['high'] == 2650.1
        assert builder.current_candle['low'] == 2650.1
        assert builder.tick_count == 1
    
    def test_add_tick_multiple_ticks_same_candle(self):
        builder = OHLCBuilder(timeframe_minutes=1)
        timestamp = datetime(2024, 1, 1, 10, 0, 0, tzinfo=pytz.UTC)
        
        builder.add_tick(bid=2650.0, ask=2650.2, timestamp=timestamp)
        builder.add_tick(bid=2651.0, ask=2651.2, timestamp=timestamp + timedelta(seconds=10))
        builder.add_tick(bid=2649.0, ask=2649.2, timestamp=timestamp + timedelta(seconds=20))
        
        assert builder.current_candle['open'] == 2650.1
        assert builder.current_candle['close'] == 2649.1
        assert builder.current_candle['high'] == 2651.1
        assert builder.current_candle['low'] == 2649.1
        assert builder.tick_count == 3
    
    def test_add_tick_new_candle(self):
        builder = OHLCBuilder(timeframe_minutes=1)
        timestamp1 = datetime(2024, 1, 1, 10, 0, 0, tzinfo=pytz.UTC)
        timestamp2 = datetime(2024, 1, 1, 10, 1, 0, tzinfo=pytz.UTC)
        
        builder.add_tick(bid=2650.0, ask=2650.2, timestamp=timestamp1)
        builder.add_tick(bid=2651.0, ask=2651.2, timestamp=timestamp2)
        
        assert len(builder.candles) == 1
        assert builder.current_candle['open'] == 2651.1
        assert builder.tick_count == 1
    
    def test_add_tick_timezone_handling(self):
        builder = OHLCBuilder(timeframe_minutes=1)
        timestamp_naive = datetime(2024, 1, 1, 10, 0, 0)
        
        builder.add_tick(bid=2650.0, ask=2650.2, timestamp=timestamp_naive)
        
        assert builder.current_candle is not None
        assert builder.current_candle['timestamp'].tzinfo is not None
    
    def test_get_dataframe_empty(self):
        builder = OHLCBuilder(timeframe_minutes=1)
        
        df = builder.get_dataframe()
        
        assert df is None
    
    def test_get_dataframe_with_data(self):
        builder = OHLCBuilder(timeframe_minutes=1)
        timestamp = datetime(2024, 1, 1, 10, 0, 0, tzinfo=pytz.UTC)
        
        for i in range(5):
            builder.add_tick(
                bid=2650.0 + i,
                ask=2650.2 + i,
                timestamp=timestamp + timedelta(minutes=i)
            )
        
        df = builder.get_dataframe()
        
        assert df is not None
        assert isinstance(df, pd.DataFrame)
        assert len(df) >= 4
        assert all(col in df.columns for col in ['open', 'high', 'low', 'close', 'volume'])
    
    def test_get_dataframe_limit(self):
        builder = OHLCBuilder(timeframe_minutes=1)
        timestamp = datetime(2024, 1, 1, 10, 0, 0, tzinfo=pytz.UTC)
        
        for i in range(150):
            builder.add_tick(
                bid=2650.0,
                ask=2650.2,
                timestamp=timestamp + timedelta(minutes=i)
            )
        
        df = builder.get_dataframe(limit=50)
        
        assert len(df) <= 50
    
    def test_candle_completion(self):
        builder = OHLCBuilder(timeframe_minutes=1)
        timestamp1 = datetime(2024, 1, 1, 10, 0, 30, tzinfo=pytz.UTC)
        timestamp2 = datetime(2024, 1, 1, 10, 1, 30, tzinfo=pytz.UTC)
        
        builder.add_tick(bid=2650.0, ask=2650.2, timestamp=timestamp1)
        
        assert len(builder.candles) == 0
        
        builder.add_tick(bid=2651.0, ask=2651.2, timestamp=timestamp2)
        
        assert len(builder.candles) == 1
        completed_candle = builder.candles[0]
        assert completed_candle['open'] == 2650.1
        assert completed_candle['close'] == 2650.1

@pytest.mark.unit
class TestMarketDataClient:
    
    def test_initialization(self, mock_config):
        client = MarketDataClient(mock_config)
        
        assert client.config == mock_config
        assert client.symbol == "frxXAUUSD"
        assert client.connected is False
        assert client.running is False
        assert isinstance(client.m1_builder, OHLCBuilder)
        assert isinstance(client.m5_builder, OHLCBuilder)
    
    def test_initialization_builders(self, mock_config):
        client = MarketDataClient(mock_config)
        
        assert client.m1_builder.timeframe_minutes == 1
        assert client.m5_builder.timeframe_minutes == 5
    
    def test_is_connected_initial_state(self, mock_config):
        client = MarketDataClient(mock_config)
        
        assert client.is_connected() is False
    
    def test_is_connected_after_connection(self, mock_config):
        client = MarketDataClient(mock_config)
        client.connected = True
        
        assert client.is_connected() is True
    
    @pytest.mark.asyncio
    async def test_get_current_price(self, mock_config):
        client = MarketDataClient(mock_config)
        client.current_bid = 2650.5
        client.current_ask = 2650.7
        client.current_quote = 2650.6
        
        price = await client.get_current_price()
        
        assert price == 2650.6
    
    @pytest.mark.asyncio
    async def test_get_current_price_none_values(self, mock_config):
        client = MarketDataClient(mock_config)
        
        price = await client.get_current_price()
        
        assert price is None
    
    @pytest.mark.asyncio
    async def test_subscribe_ticks(self, mock_config):
        client = MarketDataClient(mock_config)
        
        queue = await client.subscribe_ticks('test_subscriber')
        
        assert 'test_subscriber' in client.subscribers
        assert queue is not None
        assert isinstance(queue, asyncio.Queue)
    
    @pytest.mark.asyncio
    async def test_unsubscribe_ticks(self, mock_config):
        client = MarketDataClient(mock_config)
        
        await client.subscribe_ticks('test_subscriber')
        await client.unsubscribe_ticks('test_subscriber')
        
        assert 'test_subscriber' not in client.subscribers
    
    @pytest.mark.asyncio
    async def test_broadcast_tick(self, mock_config):
        client = MarketDataClient(mock_config)
        
        queue = await client.subscribe_ticks('test_subscriber')
        
        tick_data = {
            'bid': 2650.5,
            'ask': 2650.7,
            'quote': 2650.6,
            'timestamp': datetime.now(pytz.UTC)
        }
        
        await client._broadcast_tick(tick_data)
        
        assert queue.qsize() == 1
        received_tick = await queue.get()
        assert received_tick == tick_data
    
    @pytest.mark.asyncio
    async def test_broadcast_tick_multiple_subscribers(self, mock_config):
        client = MarketDataClient(mock_config)
        
        queue1 = await client.subscribe_ticks('subscriber1')
        queue2 = await client.subscribe_ticks('subscriber2')
        
        tick_data = {
            'bid': 2650.5,
            'ask': 2650.7,
            'quote': 2650.6,
            'timestamp': datetime.now(pytz.UTC)
        }
        
        await client._broadcast_tick(tick_data)
        
        assert queue1.qsize() == 1
        assert queue2.qsize() == 1
    
    @pytest.mark.asyncio
    async def test_get_historical_data_insufficient_data(self, mock_config):
        client = MarketDataClient(mock_config)
        
        df = await client.get_historical_data('M1', 100)
        
        assert df is None
    
    @pytest.mark.asyncio
    async def test_get_historical_data_with_candles(self, mock_config):
        client = MarketDataClient(mock_config)
        timestamp = datetime(2024, 1, 1, 10, 0, 0, tzinfo=pytz.UTC)
        
        for i in range(60):
            client.m1_builder.add_tick(
                bid=2650.0 + i * 0.1,
                ask=2650.2 + i * 0.1,
                timestamp=timestamp + timedelta(minutes=i)
            )
        
        df = await client.get_historical_data('M1', 50)
        
        assert df is not None
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
    
    def test_get_status_disconnected(self, mock_config):
        client = MarketDataClient(mock_config)
        
        status = client.get_status()
        
        assert isinstance(status, dict)
        assert status['connected'] is False
        assert status['simulator_mode'] is False
    
    def test_get_status_connected(self, mock_config):
        client = MarketDataClient(mock_config)
        client.connected = True
        client.current_bid = 2650.5
        client.current_ask = 2650.7
        
        status = client.get_status()
        
        assert isinstance(status, dict)
        assert status['connected'] is True
        assert status['has_data'] is True
    
    @pytest.mark.asyncio
    async def test_broadcast_tick_queue_full_handling(self, mock_config):
        client = MarketDataClient(mock_config)
        
        queue = await client.subscribe_ticks('test_subscriber')
        
        for i in range(150):
            try:
                queue.put_nowait({'tick': i})
            except asyncio.QueueFull:
                break
        
        tick_data = {
            'bid': 2650.5,
            'ask': 2650.7,
            'quote': 2650.6,
            'timestamp': datetime.now(pytz.UTC)
        }
        
        await client._broadcast_tick(tick_data)
    
    @pytest.mark.asyncio
    async def test_historical_data_different_timeframes(self, mock_config):
        client = MarketDataClient(mock_config)
        timestamp = datetime(2024, 1, 1, 10, 0, 0, tzinfo=pytz.UTC)
        
        for i in range(100):
            client.m1_builder.add_tick(
                bid=2650.0,
                ask=2650.2,
                timestamp=timestamp + timedelta(minutes=i)
            )
            client.m5_builder.add_tick(
                bid=2650.0,
                ask=2650.2,
                timestamp=timestamp + timedelta(minutes=i)
            )
        
        df_m1 = await client.get_historical_data('M1', 50)
        df_m5 = await client.get_historical_data('M5', 50)
        
        if df_m1 is not None and df_m5 is not None:
            assert len(df_m1) >= len(df_m5)
