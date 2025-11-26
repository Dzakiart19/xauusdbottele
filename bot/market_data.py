import asyncio
import websockets
import json
from datetime import datetime, timedelta
from collections import deque
import pandas as pd
import pytz
import random
from typing import Optional, Dict, List, Tuple
from bot.logger import setup_logger
from bot.resilience import CircuitBreaker

logger = setup_logger('MarketData')

class MarketDataError(Exception):
    """Base exception for market data errors"""
    pass

class WebSocketConnectionError(MarketDataError):
    """WebSocket connection error"""
    pass

class DataValidationError(MarketDataError):
    """Data validation error"""
    pass

class TimeoutError(MarketDataError):
    """Operation timeout error"""
    pass

class OHLCBuilder:
    def __init__(self, timeframe_minutes: int = 1):
        if timeframe_minutes <= 0:
            raise ValueError(f"Invalid timeframe_minutes: {timeframe_minutes}. Must be > 0")
        
        self.timeframe_minutes = timeframe_minutes
        self.timeframe_seconds = timeframe_minutes * 60
        self.current_candle = None
        self.candles = deque(maxlen=500)
        self.tick_count = 0
        logger.debug(f"OHLCBuilder initialized for M{timeframe_minutes}")
        
    def _validate_tick_data(self, bid: float, ask: float, timestamp: datetime) -> Tuple[bool, Optional[str]]:
        """Validate tick data before processing"""
        try:
            if bid is None or ask is None:
                return False, "Bid or Ask is None"
            
            if not isinstance(bid, (int, float)) or not isinstance(ask, (int, float)):
                return False, f"Invalid bid/ask type: bid={type(bid)}, ask={type(ask)}"
            
            if bid <= 0 or ask <= 0:
                return False, f"Invalid bid/ask values: bid={bid}, ask={ask}"
            
            if ask < bid:
                return False, f"Ask < Bid: ask={ask}, bid={bid}"
            
            spread = ask - bid
            if spread > 10.0:
                return False, f"Spread too wide: {spread:.2f}"
            
            if timestamp is None:
                return False, "Timestamp is None"
            
            if not isinstance(timestamp, datetime):
                return False, f"Invalid timestamp type: {type(timestamp)}"
            
            return True, None
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"
        
    def add_tick(self, bid: float, ask: float, timestamp: datetime):
        """Add tick data with validation and error handling"""
        try:
            is_valid, error_msg = self._validate_tick_data(bid, ask, timestamp)
            if not is_valid:
                logger.warning(f"Invalid tick data rejected: {error_msg}")
                return
            
            mid_price = (bid + ask) / 2.0
            
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=pytz.UTC)
            
            candle_start = timestamp.replace(
                second=0, 
                microsecond=0,
                minute=(timestamp.minute // self.timeframe_minutes) * self.timeframe_minutes
            )
            
            if self.current_candle is None or self.current_candle['timestamp'] != candle_start:
                if self.current_candle is not None:
                    self.candles.append(self.current_candle.copy())
                    logger.debug(f"M{self.timeframe_minutes} candle completed: O={self.current_candle['open']:.2f} H={self.current_candle['high']:.2f} L={self.current_candle['low']:.2f} C={self.current_candle['close']:.2f} V={self.current_candle['volume']}")
                
                self.current_candle = {
                    'timestamp': candle_start,
                    'open': mid_price,
                    'high': mid_price,
                    'low': mid_price,
                    'close': mid_price,
                    'volume': 0
                }
                self.tick_count = 0
            
            self.current_candle['high'] = max(self.current_candle['high'], mid_price)
            self.current_candle['low'] = min(self.current_candle['low'], mid_price)
            self.current_candle['close'] = mid_price
            self.tick_count += 1
            self.current_candle['volume'] = self.tick_count
            
        except Exception as e:
            logger.error(f"Error adding tick to M{self.timeframe_minutes} builder: {e}")
            logger.debug(f"Tick data: bid={bid}, ask={ask}, timestamp={timestamp}")
        
    def get_dataframe(self, limit: int = 100) -> Optional[pd.DataFrame]:
        """Get DataFrame with validation and error handling"""
        try:
            if limit <= 0:
                logger.warning(f"Invalid limit: {limit}. Using default 100")
                limit = 100
            
            all_candles = list(self.candles)
            if self.current_candle:
                all_candles.append(self.current_candle)
            
            if len(all_candles) == 0:
                logger.debug(f"No candles available for M{self.timeframe_minutes}")
                return None
            
            df = pd.DataFrame(all_candles)
            
            required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required_columns):
                logger.error(f"Missing required columns in candle data. Have: {df.columns.tolist()}")
                return None
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            df.index = pd.DatetimeIndex(df.index)
            
            if len(df) > limit:
                df = df.tail(limit)
            
            return df
            
        except Exception as e:
            logger.error(f"Error creating DataFrame for M{self.timeframe_minutes}: {e}")
            return None
    
    def clear(self):
        """Clear all candles and reset builder state (for safe reload from DB)"""
        self.candles.clear()
        self.current_candle = None
        self.tick_count = 0
        logger.debug(f"OHLCBuilder M{self.timeframe_minutes} cleared")

class MarketDataClient:
    def __init__(self, config):
        self.config = config
        self.ws_url = "wss://ws.derivws.com/websockets/v3?app_id=1089"
        self.symbol = "frxXAUUSD"
        self.current_bid = None
        self.current_ask = None
        self.current_quote = None
        self.current_timestamp = None
        self.ws = None
        self.connected = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.running = False
        self.use_simulator = False
        self.simulator_task = None
        self.last_ping = 0
        
        self.m1_builder = OHLCBuilder(timeframe_minutes=1)
        self.m5_builder = OHLCBuilder(timeframe_minutes=5)
        
        self.candle_lock = asyncio.Lock()
        self.db_write_lock = asyncio.Lock()
        
        self.reconnect_delay = 3
        self.max_reconnect_delay = 60
        self.base_price = 2650.0
        self.price_volatility = 2.0
        
        self.subscribers = {}
        self.subscriber_failures = {}
        self.max_consecutive_failures = 5
        self.tick_log_counter = 0
        
        self.ws_timeout = 30
        self.fetch_timeout = 10
        self.last_data_received = None
        self.data_stale_threshold = 60
        
        self._loading_from_db = False
        self._loaded_from_db = False
        
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=30.0,
            expected_exception=Exception,
            name="DerivWebSocket"
        )
        
        logger.info("MarketDataClient initialized with enhanced error handling")
        logger.info(f"WebSocket URL: {self.ws_url}, Symbol: {self.symbol}")
        logger.info("Pub/Sub mechanism initialized")
        logger.info("✅ Circuit breaker initialized for WebSocket connection")
    
    def _log_tick_sample(self, bid: float, ask: float, quote: float, spread: Optional[float] = None, mode: str = "") -> None:
        """Centralized tick logging dengan sampling - increment counter HANYA 1x per tick"""
        self.tick_log_counter += 1
        if self.tick_log_counter % self.config.TICK_LOG_SAMPLE_RATE == 0:
            if mode == "simulator":
                logger.info(f"💰 Simulator Tick Sample (setiap {self.config.TICK_LOG_SAMPLE_RATE}): Bid=${bid:.2f}, Ask=${ask:.2f}, Spread=${spread:.2f}")
            else:
                logger.info(f"💰 Tick Sample (setiap {self.config.TICK_LOG_SAMPLE_RATE}): Bid={bid:.2f}, Ask={ask:.2f}, Quote={quote:.2f}")
        else:
            if mode == "simulator":
                logger.debug(f"Simulator: Bid=${bid:.2f}, Ask=${ask:.2f}, Spread=${spread:.2f}")
            else:
                logger.debug(f"💰 Tick: Bid={bid:.2f}, Ask={ask:.2f}, Quote={quote:.2f}")
    
    async def subscribe_ticks(self, name: str) -> asyncio.Queue:
        queue = asyncio.Queue(maxsize=500)
        self.subscribers[name] = queue
        self.subscriber_failures[name] = 0
        logger.debug(f"Subscriber '{name}' registered untuk tick feed")
        return queue
    
    async def unsubscribe_ticks(self, name: str):
        if name in self.subscribers:
            del self.subscribers[name]
            if name in self.subscriber_failures:
                del self.subscriber_failures[name]
            logger.debug(f"Subscriber '{name}' unregistered dari tick feed")
    
    async def _broadcast_tick(self, tick_data: Dict):
        if not self.subscribers:
            return
        
        stale_subscribers = []
        
        for name, queue in list(self.subscribers.items()):
            success = False
            max_retries = 3
            
            for attempt in range(max_retries):
                try:
                    queue.put_nowait(tick_data)
                    success = True
                    if name in self.subscriber_failures:
                        self.subscriber_failures[name] = 0
                    break
                    
                except asyncio.QueueFull:
                    if attempt < max_retries - 1:
                        backoff_time = 0.1 * (2 ** attempt)
                        logger.debug(f"Queue full for '{name}', retry {attempt + 1}/{max_retries} after {backoff_time}s")
                        await asyncio.sleep(backoff_time)
                    else:
                        logger.warning(f"Queue full for subscriber '{name}' after {max_retries} retries, skipping tick")
                        
                except Exception as e:
                    logger.error(f"Error broadcasting tick to '{name}': {e}")
                    break
            
            if not success:
                if name in self.subscriber_failures:
                    self.subscriber_failures[name] += 1
                else:
                    self.subscriber_failures[name] = 1
                
                if self.subscriber_failures[name] >= self.max_consecutive_failures:
                    logger.warning(f"Subscriber '{name}' failed {self.subscriber_failures[name]} times consecutively, marking for removal")
                    stale_subscribers.append(name)
        
        for name in stale_subscribers:
            if name in self.subscribers:
                del self.subscribers[name]
            if name in self.subscriber_failures:
                del self.subscriber_failures[name]
            logger.info(f"Removed stale subscriber '{name}' due to consecutive failures")
    
    async def fetch_historical_candles(self, websocket, timeframe_minutes: int = 1, count: int = 100):
        """Fetch historical candles from Deriv API with timeout and validation"""
        try:
            if timeframe_minutes <= 0:
                logger.error(f"Invalid timeframe_minutes: {timeframe_minutes}")
                return False
            
            if count <= 0 or count > 5000:
                logger.warning(f"Invalid count: {count}. Using default 100")
                count = 100
            
            granularity = timeframe_minutes * 60
            
            history_request = {
                "ticks_history": self.symbol,
                "adjust_start_time": 1,
                "count": count,
                "end": "latest",
                "start": 1,
                "style": "candles",
                "granularity": granularity
            }
            
            await websocket.send(json.dumps(history_request))
            logger.debug(f"Requesting {count} historical M{timeframe_minutes} candles...")
            
            try:
                response = await asyncio.wait_for(websocket.recv(), timeout=self.fetch_timeout)
            except asyncio.TimeoutError:
                logger.error(f"Timeout fetching historical candles (timeout={self.fetch_timeout}s)")
                return False
            
            try:
                data = json.loads(response)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON response: {e}")
                logger.debug(f"Response: {response[:200]}")
                return False
            
            if 'error' in data:
                logger.error(f"API error fetching candles: {data['error'].get('message', 'Unknown error')}")
                return False
            
            if 'candles' in data:
                candles = data['candles']
                if not candles or len(candles) == 0:
                    logger.warning("Received empty candles array")
                    return False
                
                logger.info(f"Received {len(candles)} historical M{timeframe_minutes} candles")
                
                builder = self.m1_builder if timeframe_minutes == 1 else self.m5_builder
                
                valid_candles = 0
                for candle in candles:
                    try:
                        if not all(k in candle for k in ['epoch', 'open', 'high', 'low', 'close']):
                            logger.warning(f"Incomplete candle data: {candle}")
                            continue
                        
                        timestamp = datetime.fromtimestamp(candle['epoch'], tz=pytz.UTC)
                        timestamp = timestamp.replace(second=0, microsecond=0)
                        
                        open_price = float(candle['open'])
                        high_price = float(candle['high'])
                        low_price = float(candle['low'])
                        close_price = float(candle['close'])
                        
                        if high_price < low_price:
                            logger.warning(f"Invalid candle: high < low ({high_price} < {low_price})")
                            continue
                        
                        if not (low_price <= open_price <= high_price and low_price <= close_price <= high_price):
                            logger.warning(f"Invalid candle: prices out of range")
                            continue
                        
                        candle_data = {
                            'timestamp': pd.Timestamp(timestamp),
                            'open': open_price,
                            'high': high_price,
                            'low': low_price,
                            'close': close_price,
                            'volume': 100
                        }
                        builder.candles.append(candle_data)
                        valid_candles += 1
                        
                    except (ValueError, KeyError, TypeError) as e:
                        logger.warning(f"Error processing candle: {e}")
                        continue
                
                logger.info(f"Pre-populated {valid_candles} valid M{timeframe_minutes} candles")
                return valid_candles > 0
            else:
                logger.warning(f"No 'candles' key in response: {list(data.keys())}")
                return False
                
        except asyncio.CancelledError:
            logger.info("Historical candle fetch cancelled")
            raise
        except Exception as e:
            logger.error(f"Error fetching historical candles for M{timeframe_minutes}: {e}", exc_info=True)
            return False
        
    async def connect_websocket(self):
        """Connect to WebSocket with enhanced error handling and retry logic"""
        self.running = True
        
        while self.running:
            try:
                logger.info(f"Connecting to Deriv WebSocket (attempt {self.reconnect_attempts + 1}): {self.ws_url}")
                
                try:
                    async with websockets.connect(
                        self.ws_url,
                        ping_interval=None,
                        close_timeout=10,
                        open_timeout=self.ws_timeout
                    ) as websocket:
                        self.ws = websocket
                        self.connected = True
                        self.reconnect_attempts = 0
                        self.last_data_received = datetime.now()
                        
                        logger.info(f"✅ Connected to Deriv WebSocket successfully")
                        
                        if self._loaded_from_db and (len(self.m1_builder.candles) >= 30 or len(self.m5_builder.candles) >= 30):
                            logger.info("✅ Skipping historical fetch - already loaded from DB")
                            logger.info(f"Current candle counts: M1={len(self.m1_builder.candles)}, M5={len(self.m5_builder.candles)}")
                        else:
                            if self._loaded_from_db:
                                logger.warning(f"DB loaded but insufficient candles (M1={len(self.m1_builder.candles)}, M5={len(self.m5_builder.candles)}) - fetching from Deriv")
                            else:
                                logger.info("No DB load - fetching historical candles from Deriv API")
                            
                            try:
                                m1_success = await self.fetch_historical_candles(websocket, timeframe_minutes=1, count=100)
                                m5_success = await self.fetch_historical_candles(websocket, timeframe_minutes=5, count=100)
                                
                                if not m1_success and not m5_success:
                                    logger.warning("Failed to fetch any historical data, but continuing with live feed")
                            except Exception as e:
                                logger.error(f"Error fetching historical data: {e}. Continuing with live feed")
                        
                        logger.info(f"Final candle counts after init: M1={len(self.m1_builder.candles)}, M5={len(self.m5_builder.candles)}")
                        
                        try:
                            subscribe_msg = {"ticks": self.symbol}
                            await asyncio.wait_for(
                                websocket.send(json.dumps(subscribe_msg)),
                                timeout=5.0
                            )
                            logger.info(f"📡 Subscribed to {self.symbol}")
                        except asyncio.TimeoutError:
                            logger.error("Timeout subscribing to ticks")
                            raise WebSocketConnectionError("Subscribe timeout")
                        
                        heartbeat_task = asyncio.create_task(self._send_heartbeat())
                        data_monitor_task = asyncio.create_task(self._monitor_data_staleness())
                        
                        try:
                            async for message in websocket:
                                await self._on_message(message)
                        finally:
                            heartbeat_task.cancel()
                            data_monitor_task.cancel()
                            try:
                                await heartbeat_task
                            except asyncio.CancelledError:
                                pass
                            try:
                                await data_monitor_task
                            except asyncio.CancelledError:
                                pass
                                
                except asyncio.TimeoutError:
                    logger.error(f"WebSocket connection timeout ({self.ws_timeout}s)")
                    self.connected = False
                    await self._handle_reconnect()
                    
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"WebSocket connection closed: code={e.code}, reason={e.reason}")
                self.connected = False
                await self._handle_reconnect()
                
            except websockets.exceptions.WebSocketException as e:
                logger.error(f"WebSocket protocol error: {e}")
                self.connected = False
                await self._handle_reconnect()
                
            except Exception as e:
                logger.error(f"Unexpected WebSocket error: {type(e).__name__}: {e}", exc_info=True)
                self.connected = False
                await self._handle_reconnect()
    
    async def _send_heartbeat(self):
        while self.running and self.ws:
            try:
                import time
                current_time = time.time()
                if current_time - self.last_ping >= 20:
                    ping_msg = {"ping": 1}
                    await self.ws.send(json.dumps(ping_msg))
                    self.last_ping = current_time
                await asyncio.sleep(1)
            except Exception as e:
                logger.debug(f"Heartbeat error: {e}")
                break
    
    async def _monitor_data_staleness(self):
        """Monitor for stale data and trigger reconnection if needed"""
        try:
            while self.running and self.connected:
                await asyncio.sleep(10)
                
                if self.last_data_received:
                    elapsed = (datetime.now() - self.last_data_received).total_seconds()
                    
                    # Forced reconnect jika data stale > 120s
                    if elapsed > 120:
                        logger.error(f"Data stale for {elapsed:.0f}s - forcing reconnection")
                        self.connected = False
                        if self.ws:
                            try:
                                await self.ws.close()
                            except Exception as close_error:
                                logger.debug(f"Error closing stale WebSocket: {close_error}")
                        break
                    
                    # Warning jika mendekati threshold
                    elif elapsed > self.data_stale_threshold:
                        logger.warning(f"No data received for {elapsed:.0f}s (threshold: {self.data_stale_threshold}s)")
                        logger.warning("Data feed appears stale, will force reconnect if > 120s")
                        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in data staleness monitor: {e}")
    
    async def _handle_reconnect(self):
        """Handle reconnection with circuit breaker and exponential backoff"""
        if not self.running:
            return
        
        try:
            await self.circuit_breaker.call_async(self._attempt_reconnect)
        except Exception as e:
            logger.error(f"Circuit breaker prevented reconnection: {e}")
            logger.warning("Circuit is OPEN - waiting for cooldown period before retry")
            cb_state = self.circuit_breaker.get_state()
            logger.info(f"Circuit Breaker State: {cb_state}")
            
            if cb_state['state'] == 'OPEN':
                logger.warning("Falling back to simulator mode due to circuit breaker")
                self.use_simulator = True
                self.connected = False
                try:
                    self._seed_initial_tick()
                    await self._run_simulator()
                except Exception as sim_error:
                    logger.error(f"Failed to start simulator: {sim_error}", exc_info=True)
    
    async def _attempt_reconnect(self):
        """Internal reconnect logic protected by circuit breaker"""
        self.reconnect_attempts += 1
        
        if self.reconnect_attempts <= self.max_reconnect_attempts:
            delay = min(self.reconnect_delay * (2 ** (self.reconnect_attempts - 1)), self.max_reconnect_delay)
            
            logger.warning(
                f"WebSocket reconnection attempt {self.reconnect_attempts}/{self.max_reconnect_attempts} "
                f"in {delay:.1f}s (exponential backoff)"
            )
            logger.info(f"Connection status: URL accessible check for {self.ws_url}")
            
            await asyncio.sleep(delay)
        else:
            logger.error(f"Max reconnect attempts ({self.max_reconnect_attempts}) reached after multiple failures")
            logger.warning("Gracefully degrading to SIMULATOR MODE for continued operation")
            logger.info("Simulator provides synthetic market data for testing/fallback")
            
            self.use_simulator = True
            self.connected = False
            
            try:
                self._seed_initial_tick()
                await self._run_simulator()
            except Exception as e:
                logger.error(f"Failed to start simulator: {e}", exc_info=True)
    
    def _seed_initial_tick(self):
        spread = 0.40
        self.current_bid = self.base_price - (spread / 2)
        self.current_ask = self.base_price + (spread / 2)
        self.current_timestamp = datetime.now(pytz.UTC)
        
        self.m1_builder.add_tick(self.current_bid, self.current_ask, self.current_timestamp)
        self.m5_builder.add_tick(self.current_bid, self.current_ask, self.current_timestamp)
        
        logger.info(f"Initial tick seeded: Bid=${self.current_bid:.2f}, Ask=${self.current_ask:.2f}")
    
    async def _run_simulator(self):
        logger.info("Starting price simulator (fallback mode)")
        logger.info(f"Base price: ${self.base_price}, Volatility: ±${self.price_volatility}")
        
        while self.use_simulator:
            try:
                spread = 0.30 + random.uniform(0, 0.20)
                
                price_change = random.uniform(-self.price_volatility, self.price_volatility)
                mid_price = self.base_price + price_change
                
                self.current_bid = mid_price - (spread / 2)
                self.current_ask = mid_price + (spread / 2)
                self.current_timestamp = datetime.now(pytz.UTC)
                self.current_quote = mid_price
                
                if not self._loading_from_db:
                    self.m1_builder.add_tick(self.current_bid, self.current_ask, self.current_timestamp)
                    self.m5_builder.add_tick(self.current_bid, self.current_ask, self.current_timestamp)
                else:
                    logger.debug("Skipping simulator tick - loading from DB in progress")
                    await asyncio.sleep(0.1)
                    continue
                
                tick_data = {
                    'bid': self.current_bid,
                    'ask': self.current_ask,
                    'quote': self.current_quote,
                    'timestamp': self.current_timestamp
                }
                await self._broadcast_tick(tick_data)
                
                self._log_tick_sample(self.current_bid, self.current_ask, self.current_quote, spread, mode="simulator")
                
                self.base_price = mid_price
                
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Simulator error: {e}")
                await asyncio.sleep(5)
        
        logger.info("Price simulator stopped")
    
    async def _on_message(self, message: str):
        """Process incoming WebSocket message with validation"""
        try:
            if not message:
                logger.warning("Received empty message")
                return
            
            try:
                data = json.loads(message)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON message: {e}")
                logger.debug(f"Raw message: {message[:500]}")
                return
            
            if not isinstance(data, dict):
                logger.warning(f"Message is not a dict: {type(data)}")
                return
            
            if "tick" in data:
                tick = data["tick"]
                
                if not isinstance(tick, dict):
                    logger.warning(f"Tick data is not a dict: {type(tick)}")
                    return
                
                try:
                    epoch = tick.get("epoch", int(datetime.now(pytz.UTC).timestamp()))
                    bid = tick.get("bid")
                    ask = tick.get("ask")
                    quote = tick.get("quote")
                    
                    if bid is None or ask is None:
                        logger.warning(f"Missing bid/ask in tick: bid={bid}, ask={ask}")
                        return
                    
                    try:
                        bid_float = float(bid)
                        ask_float = float(ask)
                    except (ValueError, TypeError) as e:
                        logger.error(f"Invalid bid/ask values: bid={bid}, ask={ask}, error={e}")
                        return
                    
                    if bid_float <= 0 or ask_float <= 0:
                        logger.warning(f"Non-positive bid/ask: bid={bid_float}, ask={ask_float}")
                        return
                    
                    if ask_float < bid_float:
                        logger.warning(f"Ask < Bid: ask={ask_float}, bid={bid_float}")
                        return
                    
                    self.current_bid = bid_float
                    self.current_ask = ask_float
                    self.current_quote = float(quote) if quote else (self.current_bid + self.current_ask) / 2
                    self.current_timestamp = datetime.fromtimestamp(epoch, tz=pytz.UTC)
                    self.last_data_received = datetime.now()
                    
                    if self._loading_from_db:
                        logger.debug("Skipping tick processing - loading from DB in progress")
                        return
                    
                    self.m1_builder.add_tick(self.current_bid, self.current_ask, self.current_timestamp)
                    self.m5_builder.add_tick(self.current_bid, self.current_ask, self.current_timestamp)
                    
                    self._log_tick_sample(self.current_bid, self.current_ask, self.current_quote, mode="websocket")
                    
                    tick_data = {
                        'bid': self.current_bid,
                        'ask': self.current_ask,
                        'quote': self.current_quote,
                        'timestamp': self.current_timestamp
                    }
                    await self._broadcast_tick(tick_data)
                    
                except Exception as e:
                    logger.error(f"Error processing tick data: {e}")
                    logger.debug(f"Tick content: {tick}")
                
            elif "pong" in data:
                logger.debug("Pong received from server")
            
            elif "error" in data:
                error = data["error"]
                error_msg = error.get('message', 'Unknown error') if isinstance(error, dict) else str(error)
                error_code = error.get('code', 'N/A') if isinstance(error, dict) else 'N/A'
                logger.error(f"API Error (code {error_code}): {error_msg}")
                logger.debug(f"Full error data: {error}")
            
            elif "msg_type" in data:
                msg_type = data["msg_type"]
                if msg_type not in ["tick", "ping", "pong"]:
                    logger.debug(f"Received message type: {msg_type}")
                        
        except Exception as e:
            logger.error(f"Unexpected error processing message: {type(e).__name__}: {e}", exc_info=True)
            if message:
                logger.debug(f"Raw message (truncated): {message[:500]}")
    
    async def get_current_price(self) -> Optional[float]:
        """Get current mid price with validation"""
        try:
            if self.current_bid is not None and self.current_ask is not None:
                if self.current_bid > 0 and self.current_ask > 0 and self.current_ask >= self.current_bid:
                    mid_price = (self.current_bid + self.current_ask) / 2.0
                    return mid_price
                else:
                    logger.warning(f"Invalid bid/ask for price calculation: bid={self.current_bid}, ask={self.current_ask}")
            
            logger.debug("No valid current price available")
            return None
            
        except Exception as e:
            logger.error(f"Error calculating current price: {e}")
            return None
    
    async def get_bid_ask(self) -> Optional[Tuple[float, float]]:
        """Get current bid/ask with validation"""
        try:
            if self.current_bid is not None and self.current_ask is not None:
                if self.current_bid > 0 and self.current_ask > 0 and self.current_ask >= self.current_bid:
                    return (self.current_bid, self.current_ask)
                else:
                    logger.warning(f"Invalid bid/ask: bid={self.current_bid}, ask={self.current_ask}")
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting bid/ask: {e}")
            return None
    
    async def get_spread(self) -> Optional[float]:
        """Get current spread with validation"""
        try:
            if self.current_bid is not None and self.current_ask is not None:
                if self.current_bid > 0 and self.current_ask > 0 and self.current_ask >= self.current_bid:
                    spread = self.current_ask - self.current_bid
                    if spread >= 0:
                        return spread
                    logger.warning(f"Negative spread calculated: {spread}")
            
            return None
            
        except Exception as e:
            logger.error(f"Error calculating spread: {e}")
            return None
    
    async def get_historical_data(self, timeframe: str = 'M1', limit: int = 100) -> Optional[pd.DataFrame]:
        """Get historical data with validation and timeout handling"""
        try:
            if not timeframe or timeframe not in ['M1', 'M5']:
                logger.warning(f"Invalid timeframe: {timeframe}. Must be 'M1' or 'M5'")
                return None
            
            if limit <= 0:
                logger.warning(f"Invalid limit: {limit}. Using default 100")
                limit = 100
            
            if timeframe == 'M1':
                df = self.m1_builder.get_dataframe(limit)
                if df is not None and len(df) > 0:
                    logger.debug(f"Retrieved {len(df)} M1 candles from tick feed")
                    return df
                else:
                    logger.debug("No M1 data available")
                    
            elif timeframe == 'M5':
                df = self.m5_builder.get_dataframe(limit)
                if df is not None and len(df) > 0:
                    logger.debug(f"Retrieved {len(df)} M5 candles from tick feed")
                    return df
                else:
                    logger.debug("No M5 data available")
            
            return None
                        
        except Exception as e:
            logger.error(f"Error fetching historical data for {timeframe}: {e}", exc_info=True)
            return None
    
    async def save_candles_to_db(self, db_manager):
        """Save latest 100 candles to database for persistence with race condition protection"""
        async with self.candle_lock:
            try:
                from bot.database import CandleData
                from sqlalchemy import delete
                
                snapshots = {}
                for timeframe, builder in [('M1', self.m1_builder), ('M5', self.m5_builder)]:
                    snapshots[timeframe] = list(builder.candles)
                    if builder.current_candle:
                        snapshots[timeframe + '_current'] = dict(builder.current_candle)
                        logger.debug(f"Deep copied current_candle for {timeframe} to prevent mutation")
                    else:
                        snapshots[timeframe + '_current'] = None
                
                logger.debug("Created thread-safe snapshots of candle data (including current_candle) for DB save")
            except Exception as e:
                logger.error(f"Error creating candle snapshots: {e}")
                return False
        
        async with self.db_write_lock:
            session = None
            try:
                session = db_manager.get_session()
                
                saved_m1 = 0
                saved_m5 = 0
                
                for timeframe in ['M1', 'M5']:
                    all_candles = snapshots[timeframe].copy()
                    
                    current_candle = snapshots.get(timeframe + '_current')
                    if current_candle:
                        all_candles.append(current_candle)
                        logger.debug(f"Including current_candle in {timeframe} save (total: {len(all_candles)})")
                    
                    if len(all_candles) == 0:
                        logger.debug(f"No {timeframe} candles to save")
                        continue
                    
                    seen_timestamps = set()
                    deduplicated_candles = []
                    for candle in all_candles:
                        ts = candle['timestamp']
                        if ts not in seen_timestamps:
                            seen_timestamps.add(ts)
                            deduplicated_candles.append(candle)
                    
                    if len(deduplicated_candles) < len(all_candles):
                        removed = len(all_candles) - len(deduplicated_candles)
                        logger.debug(f"Removed {removed} duplicate candle(s) from {timeframe} before saving")
                    
                    seen_timestamps.clear()
                    
                    candles_to_save = deduplicated_candles[-100:]
                    
                    session.execute(delete(CandleData).where(CandleData.timeframe == timeframe))
                    
                    for candle_dict in candles_to_save:
                        candle_record = CandleData(
                            timeframe=timeframe,
                            timestamp=candle_dict['timestamp'],
                            open=float(candle_dict['open']),
                            high=float(candle_dict['high']),
                            low=float(candle_dict['low']),
                            close=float(candle_dict['close']),
                            volume=float(candle_dict.get('volume', 0))
                        )
                        session.add(candle_record)
                    
                    if timeframe == 'M1':
                        saved_m1 = len(candles_to_save)
                    else:
                        saved_m5 = len(candles_to_save)
                
                session.commit()
                session.close()
                
                logger.info(f"✅ Saved candles to database: M1={saved_m1}, M5={saved_m5}")
                return True
                
            except Exception as e:
                logger.error(f"Error saving candles to database: {e}", exc_info=True)
                if session is not None:
                    try:
                        session.rollback()
                        session.close()
                    except:
                        pass
                return False
    
    async def load_candles_from_db(self, db_manager):
        """Load candles from database on startup with race condition protection
        
        Memory optimization: Uses sliding window deque for duplicate detection
        instead of unbounded set to prevent large memory consumption.
        """
        self._loading_from_db = True
        logger.debug("Set _loading_from_db=True - blocking WebSocket tick processing")
        
        async with self.candle_lock:
            session = None
            try:
                from bot.database import CandleData
                
                self.m1_builder.clear()
                self.m5_builder.clear()
                logger.info("Cleared existing candle builders before loading from database")
                
                session = db_manager.get_session()
                
                loaded_m1 = 0
                loaded_m5 = 0
                
                for timeframe, builder in [('M1', self.m1_builder), ('M5', self.m5_builder)]:
                    candles = session.query(CandleData).filter(
                        CandleData.timeframe == timeframe
                    ).order_by(CandleData.timestamp.asc()).all()
                    
                    if not candles:
                        logger.info(f"No {timeframe} candles found in database (first run?)")
                        continue
                    
                    recent_timestamps = deque(maxlen=20)
                    duplicates_skipped = 0
                    
                    for candle in candles:
                        timestamp = candle.timestamp
                        
                        if timestamp.tzinfo is None:
                            ts = pd.Timestamp(timestamp).tz_localize('UTC')
                        else:
                            ts = pd.Timestamp(timestamp).tz_convert('UTC')
                        
                        if ts in recent_timestamps:
                            duplicates_skipped += 1
                            logger.debug(f"Skipping duplicate candle at {ts} for {timeframe}")
                            continue
                        
                        recent_timestamps.append(ts)
                        
                        candle_dict = {
                            'timestamp': ts,
                            'open': float(candle.open),
                            'high': float(candle.high),
                            'low': float(candle.low),
                            'close': float(candle.close),
                            'volume': float(candle.volume) if candle.volume else 0
                        }
                        builder.candles.append(candle_dict)
                    
                    recent_timestamps.clear()
                    
                    if duplicates_skipped > 0:
                        logger.info(f"Skipped {duplicates_skipped} duplicate candle(s) from {timeframe} during load")
                    
                    if timeframe == 'M1':
                        loaded_m1 = len(builder.candles)
                    else:
                        loaded_m5 = len(builder.candles)
                
                session.close()
                
                if loaded_m1 > 0 or loaded_m5 > 0:
                    logger.info(f"✅ Loaded candles from database: M1={loaded_m1}, M5={loaded_m5}")
                    logger.info("Bot has candles immediately - no waiting for Deriv API!")
                    
                    if loaded_m1 >= 30 or loaded_m5 >= 30:
                        self._loaded_from_db = True
                        logger.info("✅ Set _loaded_from_db=True - will skip historical fetch from Deriv")
                    else:
                        logger.warning(f"Loaded candles ({loaded_m1} M1, {loaded_m5} M5) below threshold (30) - will fetch from Deriv")
                    
                    self._loading_from_db = False
                    logger.debug("Set _loading_from_db=False - WebSocket tick processing enabled")
                    return True
                else:
                    logger.info("No candles in database - will fetch from Deriv API")
                    self._loaded_from_db = False
                    self._loading_from_db = False
                    logger.debug("Set _loading_from_db=False - WebSocket tick processing enabled")
                    return False
                    
            except Exception as e:
                logger.error(f"Error loading candles from database: {e}", exc_info=True)
                logger.warning("Falling back to fetching candles from Deriv API")
                if session is not None:
                    try:
                        session.close()
                    except:
                        pass
                self._loading_from_db = False
                logger.debug("Set _loading_from_db=False - WebSocket tick processing enabled (after error)")
                return False
    
    def _prune_old_candles(self, db_manager, keep_count: int = 150):
        """Prune old candles from database to prevent bloat
        
        Args:
            db_manager: Database manager instance
            keep_count: Number of newest candles to keep per timeframe (default: 150)
                       Must be >= 1 and <= 10000
        
        Returns:
            Number of pruned candles, or 0 on error
        """
        if keep_count is None or not isinstance(keep_count, int):
            logger.warning(f"Invalid keep_count type: {type(keep_count)}. Using default 150")
            keep_count = 150
        elif keep_count < 1:
            logger.warning(f"Invalid keep_count: {keep_count}. Must be >= 1. Using default 150")
            keep_count = 150
        elif keep_count > 10000:
            logger.warning(f"keep_count too large: {keep_count}. Capping at 10000")
            keep_count = 10000
        
        session = None
        try:
            from bot.database import CandleData
            from sqlalchemy import func
            
            session = db_manager.get_session()
            
            pruned_total = 0
            
            for timeframe in ['M1', 'M5']:
                total_count = session.query(func.count(CandleData.id)).filter(
                    CandleData.timeframe == timeframe
                ).scalar()
                
                if total_count is None or total_count <= keep_count:
                    logger.debug(f"{timeframe}: {total_count or 0} candles <= keep_count ({keep_count}), skipping prune")
                    continue
                
                excess_count = total_count - keep_count
                logger.debug(f"{timeframe}: {total_count} candles, need to prune {excess_count}")
                
                candles = session.query(CandleData).filter(
                    CandleData.timeframe == timeframe
                ).order_by(CandleData.timestamp.desc()).limit(keep_count).all()
                
                if candles and len(candles) > 0:
                    oldest_to_keep = candles[-1].timestamp
                    
                    deleted = session.query(CandleData).filter(
                        CandleData.timeframe == timeframe,
                        CandleData.timestamp < oldest_to_keep
                    ).delete(synchronize_session=False)
                    
                    if deleted > 0:
                        logger.debug(f"{timeframe}: Deleted {deleted} candles older than {oldest_to_keep}")
                    pruned_total += deleted
                else:
                    logger.warning(f"{timeframe}: Failed to get candles for pruning reference")
            
            session.commit()
            session.close()
            
            if pruned_total > 0:
                logger.info(f"Pruned {pruned_total} old candles from database (keep_count={keep_count})")
            
            return pruned_total
            
        except Exception as e:
            logger.error(f"Error pruning old candles: {e}", exc_info=True)
            if session is not None:
                try:
                    session.rollback()
                    session.close()
                except:
                    pass
            return 0
    
    def disconnect(self):
        self.running = False
        self.use_simulator = False
        self.connected = False
        if self.ws:
            asyncio.create_task(self.ws.close())
        logger.info("MarketData client disconnected")
    
    def is_connected(self) -> bool:
        return self.connected or self.use_simulator
    
    def get_status(self) -> Dict:
        return {
            'connected': self.connected,
            'simulator_mode': self.use_simulator,
            'reconnect_attempts': self.reconnect_attempts,
            'has_data': self.current_bid is not None and self.current_ask is not None,
            'websocket_url': self.ws_url
        }
