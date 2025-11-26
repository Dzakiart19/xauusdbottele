import asyncio
import websockets
import json
import math
import time
from datetime import datetime, timedelta
from collections import deque
from enum import Enum
import pandas as pd
import pytz
import random
from typing import Optional, Dict, List, Tuple, Any
from bot.logger import setup_logger
from bot.resilience import CircuitBreaker

logger = setup_logger('MarketData')


class ConnectionState(Enum):
    """WebSocket connection state machine states"""
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"


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


def is_valid_price(price: Any) -> bool:
    """Check if price is a valid numeric value (not None, not NaN, positive)
    
    Args:
        price: Value to check
        
    Returns:
        True if price is valid, False otherwise
    """
    if price is None:
        return False
    
    if not isinstance(price, (int, float)):
        return False
    
    if math.isnan(price) or math.isinf(price):
        return False
    
    if price <= 0:
        return False
    
    return True


def sanitize_price_data(data: Dict) -> Tuple[bool, Dict, Optional[str]]:
    """Sanitize price data dictionary, removing or flagging NaN values
    
    Args:
        data: Dictionary containing price data
        
    Returns:
        Tuple of (is_valid, sanitized_data, error_message)
    """
    if not isinstance(data, dict):
        return False, {}, "Data is not a dictionary"
    
    sanitized = {}
    price_fields = ['bid', 'ask', 'quote', 'open', 'high', 'low', 'close']
    
    for key, value in data.items():
        if key in price_fields:
            if not is_valid_price(value):
                return False, {}, f"Invalid price value for {key}: {value}"
            sanitized[key] = float(value)
        else:
            sanitized[key] = value
    
    return True, sanitized, None


class OHLCBuilder:
    def __init__(self, timeframe_minutes: int = 1):
        if timeframe_minutes <= 0:
            raise ValueError(f"Invalid timeframe_minutes: {timeframe_minutes}. Must be > 0")
        
        self.timeframe_minutes = timeframe_minutes
        self.timeframe_seconds = timeframe_minutes * 60
        self.current_candle = None
        self.candles = deque(maxlen=500)
        self.tick_count = 0
        self.nan_scrub_count = 0
        logger.debug(f"OHLCBuilder initialized for M{timeframe_minutes}")
    
    def _scrub_nan_prices(self, prices: Dict) -> Tuple[bool, Dict]:
        """Scrub NaN values from price dictionary at builder boundary
        
        Args:
            prices: Dictionary with open, high, low, close values
            
        Returns:
            Tuple of (is_valid, scrubbed_prices)
        """
        scrubbed = {}
        for key in ['open', 'high', 'low', 'close', 'volume']:
            value = prices.get(key)
            if value is None:
                if key == 'volume':
                    scrubbed[key] = 0
                else:
                    return False, {}
            elif isinstance(value, (int, float)):
                if math.isnan(value) or math.isinf(value):
                    self.nan_scrub_count += 1
                    logger.warning(f"NaN/Inf scrubbed from {key} in M{self.timeframe_minutes} builder (total scrubs: {self.nan_scrub_count})")
                    return False, {}
                scrubbed[key] = float(value)
            else:
                return False, {}
        
        if 'timestamp' in prices:
            scrubbed['timestamp'] = prices['timestamp']
        
        return True, scrubbed
        
    def _validate_tick_data(self, bid: float, ask: float, timestamp: datetime) -> Tuple[bool, Optional[str]]:
        """Validate tick data before processing with NaN check"""
        try:
            if bid is None or ask is None:
                return False, "Bid or Ask is None"
            
            if not isinstance(bid, (int, float)) or not isinstance(ask, (int, float)):
                return False, f"Invalid bid/ask type: bid={type(bid)}, ask={type(ask)}"
            
            if math.isnan(bid) or math.isnan(ask):
                return False, f"NaN detected in bid/ask: bid={bid}, ask={ask}"
            
            if math.isinf(bid) or math.isinf(ask):
                return False, f"Inf detected in bid/ask: bid={bid}, ask={ask}"
            
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
        """Add tick data with validation, NaN scrubbing, and error handling"""
        try:
            is_valid, error_msg = self._validate_tick_data(bid, ask, timestamp)
            if not is_valid:
                logger.warning(f"Invalid tick data rejected: {error_msg}")
                return
            
            mid_price = (bid + ask) / 2.0
            
            if math.isnan(mid_price) or math.isinf(mid_price):
                logger.warning(f"NaN/Inf mid_price calculated from bid={bid}, ask={ask}")
                return
            
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=pytz.UTC)
            
            candle_start = timestamp.replace(
                second=0, 
                microsecond=0,
                minute=(timestamp.minute // self.timeframe_minutes) * self.timeframe_minutes
            )
            
            if self.current_candle is None or self.current_candle['timestamp'] != candle_start:
                if self.current_candle is not None:
                    is_valid_candle, scrubbed_candle = self._scrub_nan_prices(self.current_candle)
                    if is_valid_candle:
                        self.candles.append(scrubbed_candle.copy())
                        logger.debug(f"M{self.timeframe_minutes} candle completed: O={scrubbed_candle['open']:.2f} H={scrubbed_candle['high']:.2f} L={scrubbed_candle['low']:.2f} C={scrubbed_candle['close']:.2f} V={scrubbed_candle['volume']}")
                    else:
                        logger.warning(f"Discarding invalid M{self.timeframe_minutes} candle due to NaN values")
                
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
        """Get DataFrame with validation, NaN filtering, and error handling"""
        try:
            if limit <= 0:
                logger.warning(f"Invalid limit: {limit}. Using default 100")
                limit = 100
            
            all_candles = []
            for candle in self.candles:
                is_valid, scrubbed = self._scrub_nan_prices(candle)
                if is_valid:
                    all_candles.append(scrubbed)
            
            if self.current_candle:
                is_valid, scrubbed = self._scrub_nan_prices(self.current_candle)
                if is_valid:
                    all_candles.append(scrubbed)
            
            if len(all_candles) == 0:
                logger.debug(f"No valid candles available for M{self.timeframe_minutes}")
                return None
            
            df = pd.DataFrame(all_candles)
            
            required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required_columns):
                logger.error(f"Missing required columns in candle data. Have: {df.columns.tolist()}")
                return None
            
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            nan_rows = df[['open', 'high', 'low', 'close']].isna().any(axis=1).sum()
            if nan_rows > 0:
                logger.warning(f"Dropping {nan_rows} rows with NaN values from M{self.timeframe_minutes} DataFrame")
                df = df.dropna(subset=['open', 'high', 'low', 'close'])
            
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
    
    def get_stats(self) -> Dict:
        """Get builder statistics including NaN scrub count"""
        return {
            'timeframe': f"M{self.timeframe_minutes}",
            'candle_count': len(self.candles),
            'has_current_candle': self.current_candle is not None,
            'tick_count': self.tick_count,
            'nan_scrub_count': self.nan_scrub_count
        }


class SubscriberHealthMetrics:
    """Track individual subscriber health metrics for monitoring and eviction decisions"""
    
    def __init__(self, name: str):
        self.name = name
        self.created_at: float = time.time()
        self.last_success_time: float = time.time()
        self.last_activity_time: float = time.time()
        self.total_messages_sent: int = 0
        self.total_messages_dropped: int = 0
        self.consecutive_failures: int = 0
        self.last_failure_time: Optional[float] = None
        self.drop_rate_window: deque = deque(maxlen=100)
    
    def record_success(self):
        """Record a successful message delivery"""
        current_time = time.time()
        self.last_success_time = current_time
        self.last_activity_time = current_time
        self.total_messages_sent += 1
        self.consecutive_failures = 0
        self.drop_rate_window.append(True)
    
    def record_drop(self):
        """Record a dropped message"""
        current_time = time.time()
        self.last_activity_time = current_time
        self.last_failure_time = current_time
        self.total_messages_dropped += 1
        self.consecutive_failures += 1
        self.drop_rate_window.append(False)
    
    def get_drop_rate(self) -> float:
        """Calculate drop rate from recent window (0.0 to 1.0)"""
        if not self.drop_rate_window:
            return 0.0
        drops = sum(1 for success in self.drop_rate_window if not success)
        return drops / len(self.drop_rate_window)
    
    def get_inactive_seconds(self) -> float:
        """Get seconds since last successful delivery"""
        return time.time() - self.last_success_time
    
    def get_zombie_seconds(self) -> float:
        """Get seconds since last activity (success or failure)"""
        return time.time() - self.last_activity_time
    
    def is_high_drop_rate(self, threshold: float = 0.3) -> bool:
        """Check if drop rate exceeds threshold"""
        if len(self.drop_rate_window) < 10:
            return False
        return self.get_drop_rate() > threshold
    
    def get_stats(self) -> Dict:
        """Get subscriber statistics"""
        return {
            'name': self.name,
            'uptime_seconds': round(time.time() - self.created_at, 1),
            'messages_sent': self.total_messages_sent,
            'messages_dropped': self.total_messages_dropped,
            'drop_rate': round(self.get_drop_rate() * 100, 2),
            'consecutive_failures': self.consecutive_failures,
            'inactive_seconds': round(self.get_inactive_seconds(), 1),
            'zombie_seconds': round(self.get_zombie_seconds(), 1),
            'is_high_drop_rate': self.is_high_drop_rate()
        }


class ConnectionMetrics:
    """Track WebSocket connection metrics for monitoring"""
    
    def __init__(self):
        self.total_connections = 0
        self.total_disconnections = 0
        self.total_reconnect_attempts = 0
        self.successful_reconnects = 0
        self.failed_reconnects = 0
        self.last_connected_at: Optional[datetime] = None
        self.last_disconnected_at: Optional[datetime] = None
        self.connection_durations: deque = deque(maxlen=100)
        self.state_transitions: deque = deque(maxlen=50)
    
    def record_connection(self):
        """Record a successful connection"""
        self.total_connections += 1
        self.last_connected_at = datetime.now(pytz.UTC)
        self._add_transition(ConnectionState.CONNECTED)
    
    def record_disconnection(self):
        """Record a disconnection"""
        self.total_disconnections += 1
        now = datetime.now(pytz.UTC)
        self.last_disconnected_at = now
        
        if self.last_connected_at:
            duration = (now - self.last_connected_at).total_seconds()
            self.connection_durations.append(duration)
        
        self._add_transition(ConnectionState.DISCONNECTED)
    
    def record_reconnect_attempt(self, success: bool):
        """Record a reconnection attempt"""
        self.total_reconnect_attempts += 1
        if success:
            self.successful_reconnects += 1
        else:
            self.failed_reconnects += 1
    
    def _add_transition(self, state: ConnectionState):
        """Add state transition record"""
        self.state_transitions.append({
            'state': state.value,
            'timestamp': datetime.now(pytz.UTC).isoformat()
        })
    
    def get_stats(self) -> Dict:
        """Get connection metrics"""
        avg_duration = 0.0
        if self.connection_durations:
            avg_duration = sum(self.connection_durations) / len(self.connection_durations)
        
        return {
            'total_connections': self.total_connections,
            'total_disconnections': self.total_disconnections,
            'total_reconnect_attempts': self.total_reconnect_attempts,
            'successful_reconnects': self.successful_reconnects,
            'failed_reconnects': self.failed_reconnects,
            'average_connection_duration_seconds': round(avg_duration, 2),
            'last_connected_at': self.last_connected_at.isoformat() if self.last_connected_at else None,
            'last_disconnected_at': self.last_disconnected_at.isoformat() if self.last_disconnected_at else None,
            'recent_transitions': list(self.state_transitions)[-10:]
        }


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
        
        self._connection_state = ConnectionState.DISCONNECTED
        self._connection_state_lock = asyncio.Lock()
        self.connection_metrics = ConnectionMetrics()
        
        self.m1_builder = OHLCBuilder(timeframe_minutes=1)
        self.m5_builder = OHLCBuilder(timeframe_minutes=5)
        
        self.candle_lock = asyncio.Lock()
        self.db_write_lock = asyncio.Lock()
        
        self.reconnect_delay = 3
        self.max_reconnect_delay = 60
        self.base_price = 2650.0
        self.price_volatility = 2.0
        
        self.subscribers = {}
        self.subscriber_health: Dict[str, SubscriberHealthMetrics] = {}
        self.subscriber_lock = asyncio.Lock()
        self.max_consecutive_failures = 5
        self.subscriber_stale_timeout = 300
        self.subscriber_zombie_timeout = 120
        self.subscriber_cleanup_interval = 60
        self.high_drop_rate_threshold = 0.3
        self.high_drop_rate_warning_interval = 30
        self.last_drop_rate_warning: Dict[str, float] = {}
        self.tick_log_counter = 0
        
        self.simulator_price_min = 1800.0
        self.simulator_price_max = 3500.0
        self.simulator_spread_min = 0.20
        self.simulator_spread_max = 0.80
        self.simulator_last_timestamp: Optional[datetime] = None
        
        self.ws_timeout = 30
        self.fetch_timeout = 10
        self.last_data_received = None
        self.data_stale_threshold = 60
        
        self._loading_from_db = False
        self._loaded_from_db = False
        self._shutdown_in_progress = False
        
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=45.0,
            expected_exception=Exception,
            name="DerivWebSocket"
        )
        
        logger.info("MarketDataClient initialized with enhanced error handling")
        logger.info(f"WebSocket URL: {self.ws_url}, Symbol: {self.symbol}")
        logger.info("Pub/Sub mechanism initialized with lifecycle management")
        logger.info("✅ Circuit breaker initialized for WebSocket connection (threshold=3, timeout=45s)")
        logger.info("✅ Connection state machine initialized")
    
    async def _set_connection_state(self, new_state: ConnectionState):
        """Thread-safe state transition with logging"""
        async with self._connection_state_lock:
            old_state = self._connection_state
            self._connection_state = new_state
            self.connection_metrics._add_transition(new_state)
            logger.info(f"🔄 Connection state: {old_state.value} → {new_state.value}")
    
    def get_connection_state(self) -> ConnectionState:
        """Get current connection state"""
        return self._connection_state
    
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
        """Subscribe to tick feed with proper lifecycle tracking and health metrics"""
        async with self.subscriber_lock:
            if self._shutdown_in_progress:
                raise RuntimeError("Cannot subscribe during shutdown")
            
            queue = asyncio.Queue(maxsize=500)
            self.subscribers[name] = queue
            self.subscriber_health[name] = SubscriberHealthMetrics(name)
            logger.info(f"✅ Subscriber '{name}' registered with health metrics tracking")
            return queue
    
    async def unsubscribe_ticks(self, name: str):
        """Unsubscribe from tick feed with proper cleanup and metrics logging"""
        async with self.subscriber_lock:
            health = self.subscriber_health.get(name)
            if health:
                stats = health.get_stats()
                logger.info(
                    f"📊 Subscriber '{name}' final stats: "
                    f"sent={stats['messages_sent']}, dropped={stats['messages_dropped']}, "
                    f"drop_rate={stats['drop_rate']}%, uptime={stats['uptime_seconds']}s"
                )
            
            if name in self.subscribers:
                try:
                    queue = self.subscribers[name]
                    while not queue.empty():
                        try:
                            queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                except Exception as e:
                    logger.debug(f"Error draining queue for '{name}': {e}")
                
                del self.subscribers[name]
            
            if name in self.subscriber_health:
                del self.subscriber_health[name]
            if name in self.last_drop_rate_warning:
                del self.last_drop_rate_warning[name]
            
            logger.debug(f"Subscriber '{name}' unregistered dari tick feed")
    
    async def cleanup_stale_subscribers(self) -> List[str]:
        """Clean up stale and zombie subscribers with detailed metrics
        
        Eviction criteria:
        1. Stale: No successful delivery for subscriber_stale_timeout seconds
        2. Zombie: No activity (success or failure) for subscriber_zombie_timeout seconds
        3. High failures: max_consecutive_failures reached
        4. High drop rate: Sustained high drop rate (logged as warning, evicted if combined with other issues)
        
        Returns:
            List of removed subscriber names with eviction reasons
        """
        removed = []
        eviction_reasons = {}
        current_time = time.time()
        
        async with self.subscriber_lock:
            stale_names = []
            
            for name in list(self.subscribers.keys()):
                health = self.subscriber_health.get(name)
                if not health:
                    stale_names.append(name)
                    eviction_reasons[name] = "missing health metrics"
                    logger.warning(f"⚠️ Subscriber '{name}' has no health metrics - marking for eviction")
                    continue
                
                inactive_time = health.get_inactive_seconds()
                zombie_time = health.get_zombie_seconds()
                failures = health.consecutive_failures
                drop_rate = health.get_drop_rate()
                is_high_drop = health.is_high_drop_rate(self.high_drop_rate_threshold)
                
                is_stale = False
                reasons = []
                
                if zombie_time > self.subscriber_zombie_timeout:
                    is_stale = True
                    reasons.append(f"zombie for {zombie_time:.0f}s (no activity)")
                    logger.warning(
                        f"🧟 ZOMBIE subscriber '{name}': no activity for {zombie_time:.0f}s "
                        f"(threshold: {self.subscriber_zombie_timeout}s)"
                    )
                
                if inactive_time > self.subscriber_stale_timeout:
                    is_stale = True
                    reasons.append(f"stale for {inactive_time:.0f}s (no success)")
                
                if failures >= self.max_consecutive_failures:
                    is_stale = True
                    reasons.append(f"{failures} consecutive failures")
                
                if is_high_drop and (inactive_time > 60 or failures >= 3):
                    is_stale = True
                    reasons.append(f"high drop rate {drop_rate*100:.1f}% with issues")
                
                if is_high_drop and not is_stale:
                    last_warning = self.last_drop_rate_warning.get(name, 0)
                    if current_time - last_warning > self.high_drop_rate_warning_interval:
                        logger.warning(
                            f"⚠️ HIGH DROP RATE for '{name}': {drop_rate*100:.1f}% "
                            f"(dropped {health.total_messages_dropped}/{health.total_messages_sent + health.total_messages_dropped} messages)"
                        )
                        self.last_drop_rate_warning[name] = current_time
                
                if is_stale:
                    stale_names.append(name)
                    eviction_reasons[name] = "; ".join(reasons)
            
            for name in stale_names:
                health = self.subscriber_health.get(name)
                reason = eviction_reasons.get(name, "unknown")
                
                if health:
                    stats = health.get_stats()
                    logger.warning(
                        f"🚫 EVICTING subscriber '{name}': {reason} | "
                        f"Stats: sent={stats['messages_sent']}, dropped={stats['messages_dropped']}, "
                        f"drop_rate={stats['drop_rate']}%, failures={stats['consecutive_failures']}"
                    )
                else:
                    logger.warning(f"🚫 EVICTING subscriber '{name}': {reason}")
                
                try:
                    queue = self.subscribers.get(name)
                    if queue:
                        drained = 0
                        while not queue.empty():
                            try:
                                queue.get_nowait()
                                drained += 1
                            except asyncio.QueueEmpty:
                                break
                        if drained > 0:
                            logger.debug(f"Drained {drained} pending messages from '{name}' queue")
                except Exception as e:
                    logger.debug(f"Error draining stale queue '{name}': {e}")
                
                if name in self.subscribers:
                    del self.subscribers[name]
                if name in self.subscriber_health:
                    del self.subscriber_health[name]
                if name in self.last_drop_rate_warning:
                    del self.last_drop_rate_warning[name]
                
                removed.append(name)
        
        if removed:
            logger.info(f"✅ Cleanup completed: evicted {len(removed)} subscribers")
            for name in removed:
                logger.info(f"   - {name}: {eviction_reasons.get(name, 'unknown')}")
        
        return removed
    
    async def get_subscriber_health_report(self) -> Dict:
        """Get health report for all subscribers
        
        Returns:
            Dictionary with subscriber health statistics
        """
        async with self.subscriber_lock:
            report = {
                'total_subscribers': len(self.subscribers),
                'healthy': 0,
                'warning': 0,
                'critical': 0,
                'subscribers': {}
            }
            
            for name, health in self.subscriber_health.items():
                stats = health.get_stats()
                
                status = 'healthy'
                if health.consecutive_failures >= self.max_consecutive_failures:
                    status = 'critical'
                elif health.is_high_drop_rate(self.high_drop_rate_threshold):
                    status = 'warning'
                elif health.get_inactive_seconds() > 60:
                    status = 'warning'
                
                stats['status'] = status
                report['subscribers'][name] = stats
                
                if status == 'healthy':
                    report['healthy'] += 1
                elif status == 'warning':
                    report['warning'] += 1
                else:
                    report['critical'] += 1
            
            return report
    
    async def _unsubscribe_all(self):
        """Unsubscribe all subscribers during shutdown with final stats"""
        async with self.subscriber_lock:
            subscriber_names = list(self.subscribers.keys())
            
            for name in subscriber_names:
                health = self.subscriber_health.get(name)
                if health:
                    stats = health.get_stats()
                    logger.info(
                        f"📊 Shutdown stats for '{name}': "
                        f"sent={stats['messages_sent']}, dropped={stats['messages_dropped']}, "
                        f"drop_rate={stats['drop_rate']}%"
                    )
                
                try:
                    queue = self.subscribers.get(name)
                    if queue:
                        while not queue.empty():
                            try:
                                queue.get_nowait()
                            except asyncio.QueueEmpty:
                                break
                except Exception as e:
                    logger.debug(f"Error draining queue '{name}' during shutdown: {e}")
            
            self.subscribers.clear()
            self.subscriber_health.clear()
            self.last_drop_rate_warning.clear()
            
            if subscriber_names:
                logger.info(f"Unsubscribed all {len(subscriber_names)} subscribers during shutdown")
    
    async def _broadcast_tick(self, tick_data: Dict):
        """Broadcast tick to subscribers with NaN validation and health metrics tracking"""
        if not self.subscribers:
            return
        
        is_valid, sanitized_data, error_msg = sanitize_price_data(tick_data)
        if not is_valid:
            logger.warning(f"Tick data failed validation before broadcast: {error_msg}")
            return
        
        stale_subscribers = []
        
        async with self.subscriber_lock:
            for name, queue in list(self.subscribers.items()):
                health = self.subscriber_health.get(name)
                success = False
                max_retries = 3
                
                for attempt in range(max_retries):
                    try:
                        queue.put_nowait(sanitized_data)
                        success = True
                        if health:
                            health.record_success()
                        break
                        
                    except asyncio.QueueFull:
                        if attempt < max_retries - 1:
                            backoff_time = 0.1 * (2 ** attempt)
                            logger.debug(f"Queue full for '{name}', retry {attempt + 1}/{max_retries} after {backoff_time}s")
                            await asyncio.sleep(backoff_time)
                        else:
                            logger.debug(f"Queue full for subscriber '{name}' after {max_retries} retries, message dropped")
                            
                    except Exception as e:
                        logger.error(f"Error broadcasting tick to '{name}': {e}")
                        break
                
                if not success:
                    if health:
                        health.record_drop()
                        
                        if health.consecutive_failures >= self.max_consecutive_failures:
                            logger.warning(
                                f"⚠️ Subscriber '{name}' exceeded failure threshold: "
                                f"{health.consecutive_failures} consecutive failures, marking for removal"
                            )
                            stale_subscribers.append(name)
        
        for name in stale_subscribers:
            await self.unsubscribe_ticks(name)
    
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
                nan_skipped = 0
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
                        
                        if any(math.isnan(p) or math.isinf(p) for p in [open_price, high_price, low_price, close_price]):
                            nan_skipped += 1
                            continue
                        
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
                
                if nan_skipped > 0:
                    logger.warning(f"Skipped {nan_skipped} candles with NaN/Inf values")
                
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
    
    def _calculate_backoff_with_jitter(self, attempt: int) -> float:
        """Calculate exponential backoff with jitter for reconnection
        
        Uses decorrelated jitter algorithm for better distribution:
        delay = min(max_delay, random(base_delay, previous_delay * 3))
        
        Args:
            attempt: Current reconnection attempt number (1-based)
            
        Returns:
            Delay in seconds with jitter applied
        """
        base_delay = self.reconnect_delay
        max_delay = self.max_reconnect_delay
        
        exponential_delay = base_delay * (2 ** (attempt - 1))
        
        jitter_range = exponential_delay * 0.5
        jitter = random.uniform(-jitter_range, jitter_range)
        
        delay_with_jitter = exponential_delay + jitter
        
        final_delay = max(base_delay, min(delay_with_jitter, max_delay))
        
        return final_delay
        
    async def connect_websocket(self):
        """Connect to WebSocket with state machine and enhanced error handling"""
        self.running = True
        
        while self.running:
            try:
                await self._set_connection_state(ConnectionState.CONNECTING)
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
                        
                        await self._set_connection_state(ConnectionState.CONNECTED)
                        self.connection_metrics.record_connection()
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
                        stale_cleanup_task = asyncio.create_task(self._periodic_stale_cleanup())
                        
                        try:
                            async for message in websocket:
                                await self._on_message(message)
                        finally:
                            heartbeat_task.cancel()
                            data_monitor_task.cancel()
                            stale_cleanup_task.cancel()
                            try:
                                await heartbeat_task
                            except asyncio.CancelledError:
                                pass
                            try:
                                await data_monitor_task
                            except asyncio.CancelledError:
                                pass
                            try:
                                await stale_cleanup_task
                            except asyncio.CancelledError:
                                pass
                                
                except asyncio.TimeoutError:
                    logger.error(f"WebSocket connection timeout ({self.ws_timeout}s)")
                    self.connected = False
                    self.connection_metrics.record_disconnection()
                    await self._handle_reconnect()
                    
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"WebSocket connection closed: code={e.code}, reason={e.reason}")
                self.connected = False
                self.connection_metrics.record_disconnection()
                await self._handle_reconnect()
                
            except websockets.exceptions.WebSocketException as e:
                logger.error(f"WebSocket protocol error: {e}")
                self.connected = False
                self.connection_metrics.record_disconnection()
                await self._handle_reconnect()
                
            except Exception as e:
                logger.error(f"Unexpected WebSocket error: {type(e).__name__}: {e}", exc_info=True)
                self.connected = False
                self.connection_metrics.record_disconnection()
                await self._handle_reconnect()
    
    async def _periodic_stale_cleanup(self):
        """Periodically clean up stale and zombie subscribers with configurable interval"""
        cleanup_count = 0
        try:
            logger.info(
                f"🔄 Starting periodic subscriber cleanup (interval={self.subscriber_cleanup_interval}s, "
                f"stale_timeout={self.subscriber_stale_timeout}s, zombie_timeout={self.subscriber_zombie_timeout}s)"
            )
            while self.running and self.connected:
                await asyncio.sleep(self.subscriber_cleanup_interval)
                
                try:
                    removed = await self.cleanup_stale_subscribers()
                    cleanup_count += 1
                    
                    if cleanup_count % 10 == 0:
                        health_report = await self.get_subscriber_health_report()
                        if health_report['total_subscribers'] > 0:
                            logger.info(
                                f"📊 Subscriber health report (cycle {cleanup_count}): "
                                f"total={health_report['total_subscribers']}, "
                                f"healthy={health_report['healthy']}, "
                                f"warning={health_report['warning']}, "
                                f"critical={health_report['critical']}"
                            )
                except Exception as cleanup_err:
                    logger.error(f"Error during cleanup cycle {cleanup_count}: {cleanup_err}")
                    
        except asyncio.CancelledError:
            logger.debug(f"Periodic cleanup task cancelled after {cleanup_count} cycles")
        except Exception as e:
            logger.error(f"Fatal error in periodic stale cleanup: {e}", exc_info=True)
    
    async def _send_heartbeat(self):
        while self.running and self.ws:
            try:
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
                    
                    if elapsed > 120:
                        logger.error(f"Data stale for {elapsed:.0f}s - forcing reconnection")
                        self.connected = False
                        if self.ws:
                            try:
                                await self.ws.close()
                            except Exception as close_error:
                                logger.debug(f"Error closing stale WebSocket: {close_error}")
                        break
                    
                    elif elapsed > self.data_stale_threshold:
                        logger.warning(f"No data received for {elapsed:.0f}s (threshold: {self.data_stale_threshold}s)")
                        logger.warning("Data feed appears stale, will force reconnect if > 120s")
                        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in data staleness monitor: {e}")
    
    async def _handle_reconnect(self):
        """Handle reconnection with state machine, circuit breaker, and enhanced backoff"""
        if not self.running:
            return
        
        await self._set_connection_state(ConnectionState.RECONNECTING)
        
        cb_state = self.circuit_breaker.get_state()
        if cb_state['state'] == 'OPEN':
            remaining = self.circuit_breaker.recovery_timeout - (time.time() - (self.circuit_breaker.last_failure_time or 0))
            if remaining > 0:
                logger.warning(f"Circuit breaker OPEN - waiting {remaining:.1f}s before retry")
                logger.warning("Falling back to simulator mode due to circuit breaker")
                await self._set_connection_state(ConnectionState.DISCONNECTED)
                self.use_simulator = True
                self.connected = False
                try:
                    self._seed_initial_tick()
                    await self._run_simulator()
                except Exception as sim_error:
                    logger.error(f"Failed to start simulator: {sim_error}", exc_info=True)
                return
        
        try:
            await self.circuit_breaker.call_async(self._attempt_reconnect)
            self.connection_metrics.record_reconnect_attempt(success=True)
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            self.connection_metrics.record_reconnect_attempt(success=False)
            
            cb_state = self.circuit_breaker.get_state()
            logger.info(f"Circuit Breaker State: {cb_state}")
            
            if cb_state['state'] == 'OPEN':
                logger.warning("Circuit is OPEN - falling back to simulator mode")
                await self._set_connection_state(ConnectionState.DISCONNECTED)
                self.use_simulator = True
                self.connected = False
                try:
                    self._seed_initial_tick()
                    await self._run_simulator()
                except Exception as sim_error:
                    logger.error(f"Failed to start simulator: {sim_error}", exc_info=True)
    
    async def _attempt_reconnect(self):
        """Internal reconnect logic with enhanced exponential backoff and jitter"""
        self.reconnect_attempts += 1
        
        if self.reconnect_attempts <= self.max_reconnect_attempts:
            delay = self._calculate_backoff_with_jitter(self.reconnect_attempts)
            
            logger.warning(
                f"WebSocket reconnection attempt {self.reconnect_attempts}/{self.max_reconnect_attempts} "
                f"in {delay:.1f}s (exponential backoff with jitter)"
            )
            logger.info(f"Connection status: URL accessible check for {self.ws_url}")
            
            await asyncio.sleep(delay)
        else:
            logger.error(f"Max reconnect attempts ({self.max_reconnect_attempts}) reached after multiple failures")
            logger.warning("Gracefully degrading to SIMULATOR MODE for continued operation")
            logger.info("Simulator provides synthetic market data for testing/fallback")
            
            await self._set_connection_state(ConnectionState.DISCONNECTED)
            self.use_simulator = True
            self.connected = False
            
            try:
                self._seed_initial_tick()
                await self._run_simulator()
            except Exception as e:
                logger.error(f"Failed to start simulator: {e}", exc_info=True)
    
    def _seed_initial_tick(self):
        """Seed initial tick data with validated prices for simulator startup"""
        spread = max(self.simulator_spread_min, min(0.40, self.simulator_spread_max))
        
        mid_price = max(
            self.simulator_price_min + spread,
            min(self.base_price, self.simulator_price_max - spread)
        )
        
        self.current_bid = mid_price - (spread / 2)
        self.current_ask = mid_price + (spread / 2)
        self.current_timestamp = datetime.now(pytz.UTC)
        self.current_quote = mid_price
        self.simulator_last_timestamp = self.current_timestamp
        
        is_valid_bid, _ = self._validate_simulator_price(self.current_bid)
        is_valid_ask, _ = self._validate_simulator_price(self.current_ask)
        
        if not (is_valid_bid and is_valid_ask):
            logger.warning(
                f"Initial tick prices out of range, using safe defaults: "
                f"range=[${self.simulator_price_min:.2f}-${self.simulator_price_max:.2f}]"
            )
            safe_price = (self.simulator_price_min + self.simulator_price_max) / 2
            self.current_bid = safe_price - (spread / 2)
            self.current_ask = safe_price + (spread / 2)
            self.base_price = safe_price
        
        self.m1_builder.add_tick(self.current_bid, self.current_ask, self.current_timestamp)
        self.m5_builder.add_tick(self.current_bid, self.current_ask, self.current_timestamp)
        
        logger.info(
            f"Initial tick seeded: Bid=${self.current_bid:.2f}, Ask=${self.current_ask:.2f}, "
            f"Spread=${spread:.2f}, Timestamp={self.current_timestamp.isoformat()}"
        )
    
    def _validate_simulator_price(self, price: float) -> Tuple[bool, Optional[str]]:
        """Validate simulator price is within realistic XAU/USD range
        
        Args:
            price: Price to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not is_valid_price(price):
            return False, f"Invalid price value: {price}"
        
        if price < self.simulator_price_min:
            return False, f"Price ${price:.2f} below min ${self.simulator_price_min:.2f}"
        
        if price > self.simulator_price_max:
            return False, f"Price ${price:.2f} above max ${self.simulator_price_max:.2f}"
        
        return True, None
    
    def _validate_simulator_spread(self, spread: float) -> Tuple[bool, Optional[str]]:
        """Validate simulator spread is realistic
        
        Args:
            spread: Spread value to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if spread < self.simulator_spread_min:
            return False, f"Spread ${spread:.2f} below min ${self.simulator_spread_min:.2f}"
        
        if spread > self.simulator_spread_max:
            return False, f"Spread ${spread:.2f} above max ${self.simulator_spread_max:.2f}"
        
        return True, None
    
    def _validate_timestamp_monotonicity(self, new_timestamp: datetime) -> Tuple[bool, Optional[str]]:
        """Validate that timestamp is always moving forward
        
        Args:
            new_timestamp: New timestamp to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if self.simulator_last_timestamp is None:
            return True, None
        
        if new_timestamp <= self.simulator_last_timestamp:
            return False, (
                f"Timestamp not advancing: new={new_timestamp.isoformat()} "
                f"<= last={self.simulator_last_timestamp.isoformat()}"
            )
        
        return True, None
    
    async def _run_simulator(self):
        """Run price simulator with realistic XAU/USD data validation"""
        logger.info("Starting price simulator (fallback mode)")
        logger.info(
            f"Simulator config: base_price=${self.base_price:.2f}, volatility=±${self.price_volatility:.2f}, "
            f"price_range=[${self.simulator_price_min:.2f}-${self.simulator_price_max:.2f}], "
            f"spread_range=[${self.simulator_spread_min:.2f}-${self.simulator_spread_max:.2f}]"
        )
        
        tick_count = 0
        validation_errors = 0
        
        while self.use_simulator:
            try:
                spread = self.simulator_spread_min + random.uniform(0, self.simulator_spread_max - self.simulator_spread_min)
                
                price_change = random.uniform(-self.price_volatility, self.price_volatility)
                mid_price = self.base_price + price_change
                
                mid_price = max(self.simulator_price_min, min(mid_price, self.simulator_price_max))
                
                current_bid = mid_price - (spread / 2)
                current_ask = mid_price + (spread / 2)
                new_timestamp = datetime.now(pytz.UTC)
                
                is_valid_bid, bid_error = self._validate_simulator_price(current_bid)
                is_valid_ask, ask_error = self._validate_simulator_price(current_ask)
                is_valid_spread, spread_error = self._validate_simulator_spread(spread)
                is_valid_ts, ts_error = self._validate_timestamp_monotonicity(new_timestamp)
                
                if not is_valid_bid:
                    logger.warning(f"Simulator bid validation failed: {bid_error}")
                    validation_errors += 1
                    await asyncio.sleep(0.1)
                    continue
                
                if not is_valid_ask:
                    logger.warning(f"Simulator ask validation failed: {ask_error}")
                    validation_errors += 1
                    await asyncio.sleep(0.1)
                    continue
                
                if not is_valid_spread:
                    logger.debug(f"Simulator spread out of range, clamping: {spread_error}")
                    spread = max(self.simulator_spread_min, min(spread, self.simulator_spread_max))
                    current_bid = mid_price - (spread / 2)
                    current_ask = mid_price + (spread / 2)
                
                if not is_valid_ts:
                    logger.warning(f"Simulator timestamp validation failed: {ts_error}")
                    await asyncio.sleep(0.05)
                    continue
                
                self.current_bid = current_bid
                self.current_ask = current_ask
                self.current_timestamp = new_timestamp
                self.current_quote = mid_price
                self.simulator_last_timestamp = new_timestamp
                
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
                
                tick_count += 1
                self._log_tick_sample(self.current_bid, self.current_ask, self.current_quote, spread, mode="simulator")
                
                drift = random.uniform(-0.5, 0.5)
                self.base_price = max(
                    self.simulator_price_min + self.price_volatility,
                    min(mid_price + drift, self.simulator_price_max - self.price_volatility)
                )
                
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Simulator error: {e}")
                await asyncio.sleep(5)
        
        logger.info(
            f"Price simulator stopped: {tick_count} ticks generated, "
            f"{validation_errors} validation errors"
        )
    
    async def _on_message(self, message: str):
        """Process incoming WebSocket message with validation and NaN handling"""
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
                    
                    if not is_valid_price(bid_float) or not is_valid_price(ask_float):
                        logger.warning(f"Invalid prices detected (NaN/Inf/negative): bid={bid_float}, ask={ask_float}")
                        return
                    
                    if ask_float < bid_float:
                        logger.warning(f"Ask < Bid: ask={ask_float}, bid={bid_float}")
                        return
                    
                    self.current_bid = bid_float
                    self.current_ask = ask_float
                    self.current_quote = float(quote) if quote and is_valid_price(float(quote)) else (self.current_bid + self.current_ask) / 2
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
            if is_valid_price(self.current_bid) and is_valid_price(self.current_ask):
                if self.current_ask >= self.current_bid:
                    mid_price = (self.current_bid + self.current_ask) / 2.0
                    if is_valid_price(mid_price):
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
            if is_valid_price(self.current_bid) and is_valid_price(self.current_ask):
                if self.current_ask >= self.current_bid:
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
            if is_valid_price(self.current_bid) and is_valid_price(self.current_ask):
                if self.current_ask >= self.current_bid:
                    spread = self.current_ask - self.current_bid
                    if spread >= 0 and not math.isnan(spread) and not math.isinf(spread):
                        return spread
                    logger.warning(f"Invalid spread calculated: {spread}")
            
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
                    valid_candles = []
                    for candle in builder.candles:
                        is_valid, scrubbed = builder._scrub_nan_prices(candle)
                        if is_valid:
                            valid_candles.append(scrubbed)
                    snapshots[timeframe] = valid_candles
                    
                    if builder.current_candle:
                        is_valid, scrubbed = builder._scrub_nan_prices(builder.current_candle)
                        if is_valid:
                            snapshots[timeframe + '_current'] = scrubbed
                        else:
                            snapshots[timeframe + '_current'] = None
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
                nan_skipped = 0
                
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
                        
                        open_val = float(candle.open)
                        high_val = float(candle.high)
                        low_val = float(candle.low)
                        close_val = float(candle.close)
                        
                        if any(math.isnan(v) or math.isinf(v) for v in [open_val, high_val, low_val, close_val]):
                            nan_skipped += 1
                            logger.warning(f"Skipping candle with NaN/Inf at {ts} for {timeframe}")
                            continue
                        
                        candle_dict = {
                            'timestamp': ts,
                            'open': open_val,
                            'high': high_val,
                            'low': low_val,
                            'close': close_val,
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
                
                if nan_skipped > 0:
                    logger.warning(f"Skipped {nan_skipped} candles with NaN/Inf values during load")
                
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
    
    async def shutdown(self):
        """Graceful shutdown with proper cleanup of all resources"""
        logger.info("Initiating MarketDataClient shutdown...")
        self._shutdown_in_progress = True
        
        self.running = False
        self.use_simulator = False
        
        await self._unsubscribe_all()
        
        self.connected = False
        if self.ws:
            try:
                await self.ws.close()
                logger.debug("WebSocket closed during shutdown")
            except Exception as e:
                logger.debug(f"Error closing WebSocket during shutdown: {e}")
        
        await self._set_connection_state(ConnectionState.DISCONNECTED)
        
        logger.info("MarketDataClient shutdown complete")
    
    def disconnect(self):
        """Synchronous disconnect - use shutdown() for async cleanup"""
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
            'connection_state': self._connection_state.value,
            'reconnect_attempts': self.reconnect_attempts,
            'has_data': self.current_bid is not None and self.current_ask is not None,
            'websocket_url': self.ws_url,
            'subscriber_count': len(self.subscribers),
            'circuit_breaker': self.circuit_breaker.get_state(),
            'connection_metrics': self.connection_metrics.get_stats(),
            'm1_builder_stats': self.m1_builder.get_stats(),
            'm5_builder_stats': self.m5_builder.get_stats()
        }
