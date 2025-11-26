from typing import Optional, Dict, Tuple, Any, List, NamedTuple
import json
from bot.logger import setup_logger
import math
from dataclasses import dataclass, field

logger = setup_logger('Strategy')

class StrategyError(Exception):
    """Base exception for strategy errors"""
    pass

class IndicatorValidationError(StrategyError):
    """Indicator data validation error"""
    pass

class PriceValidationError(StrategyError):
    """Price data validation error for NaN/Inf/negative values"""
    pass


@dataclass
class ValidationResult:
    """Result of price/indicator validation with warnings"""
    is_valid: bool
    value: float
    warnings: List[str] = field(default_factory=list)
    error: Optional[str] = None
    
    def add_warning(self, msg: str):
        self.warnings.append(msg)
        logger.warning(f"Validation warning: {msg}")


class PriceDataValidator:
    """Centralized price data validation pipeline for NaN/Inf/Negative handling"""
    
    def __init__(self):
        self._validation_warnings: List[str] = []
        self._rejected_count = 0
    
    def reset_warnings(self):
        """Reset validation warnings for new validation cycle"""
        self._validation_warnings = []
    
    def get_warnings(self) -> List[str]:
        """Get accumulated validation warnings"""
        return self._validation_warnings.copy()
    
    def get_rejected_count(self) -> int:
        """Get count of rejected values"""
        return self._rejected_count
    
    def _add_warning(self, msg: str):
        """Add a validation warning and log it"""
        self._validation_warnings.append(msg)
        logger.warning(f"Price validation: {msg}")
    
    def validate(self, value: Any, name: str = "", 
                 min_val: Optional[float] = None, 
                 max_val: Optional[float] = None,
                 allow_zero: bool = False,
                 allow_negative: bool = False) -> ValidationResult:
        """Validate a single price/numeric value
        
        Args:
            value: Value to validate
            name: Name for logging/warnings
            min_val: Minimum allowed value
            max_val: Maximum allowed value
            allow_zero: Whether zero is allowed
            allow_negative: Whether negative values are allowed
            
        Returns:
            ValidationResult with is_valid, cleaned value, and any warnings
        """
        result = ValidationResult(is_valid=True, value=0.0)
        
        if value is None:
            result.is_valid = False
            result.error = f"{name or 'Value'} is None"
            self._add_warning(result.error)
            self._rejected_count += 1
            return result
        
        if not isinstance(value, (int, float)):
            try:
                value = float(value)
            except (TypeError, ValueError) as e:
                result.is_valid = False
                result.error = f"{name or 'Value'} is not a number: {type(value).__name__}"
                self._add_warning(result.error)
                self._rejected_count += 1
                return result
        
        try:
            if math.isnan(value):
                result.is_valid = False
                result.error = f"{name or 'Value'} is NaN"
                self._add_warning(result.error)
                self._rejected_count += 1
                return result
            
            if math.isinf(value):
                result.is_valid = False
                result.error = f"{name or 'Value'} is Inf (sign={'+' if value > 0 else '-'})"
                self._add_warning(result.error)
                self._rejected_count += 1
                return result
        except TypeError:
            result.is_valid = False
            result.error = f"{name or 'Value'} type error in NaN/Inf check"
            self._add_warning(result.error)
            self._rejected_count += 1
            return result
        
        if not allow_negative and value < 0:
            result.is_valid = False
            result.error = f"{name or 'Value'} is negative: {value}"
            self._add_warning(result.error)
            self._rejected_count += 1
            return result
        
        if not allow_zero and value == 0:
            result.is_valid = False
            result.error = f"{name or 'Value'} is zero (not allowed)"
            self._add_warning(result.error)
            self._rejected_count += 1
            return result
        
        if min_val is not None and value < min_val:
            result.add_warning(f"{name or 'Value'} ({value}) below minimum ({min_val})")
            result.value = min_val
            return result
        
        if max_val is not None and value > max_val:
            result.add_warning(f"{name or 'Value'} ({value}) above maximum ({max_val})")
            result.value = max_val
            return result
        
        result.value = float(value)
        return result
    
    def validate_price(self, price: Any, name: str = "price") -> ValidationResult:
        """Validate a price value (must be positive, no NaN/Inf)"""
        return self.validate(price, name, min_val=0.01, allow_zero=False, allow_negative=False)
    
    def validate_ratio(self, ratio: Any, name: str = "ratio", 
                       min_val: float = 0.0, max_val: float = 100.0) -> ValidationResult:
        """Validate a ratio/percentage value"""
        return self.validate(ratio, name, min_val=min_val, max_val=max_val, allow_zero=True)
    
    def validate_atr(self, atr: Any, name: str = "atr") -> ValidationResult:
        """Validate ATR value (must be positive)"""
        return self.validate(atr, name, min_val=0.0001, allow_zero=False, allow_negative=False)


_price_validator = PriceDataValidator()


def validate_price_data(prices: Dict[str, Any], 
                        required_fields: Optional[List[str]] = None) -> Tuple[bool, Dict[str, float], List[str]]:
    """Centralized price data validation with NaN/Inf/Negative rejection
    
    Args:
        prices: Dictionary of price field names to values
        required_fields: List of required field names (all must pass validation)
        
    Returns:
        Tuple of (all_valid, cleaned_prices, warnings)
        - all_valid: True if all required fields are valid
        - cleaned_prices: Dictionary with validated float values
        - warnings: List of validation warning messages
    """
    _price_validator.reset_warnings()
    cleaned = {}
    all_valid = True
    
    if required_fields is None:
        required_fields = list(prices.keys())
    
    for field_name, value in prices.items():
        is_required = field_name in required_fields
        
        if 'price' in field_name.lower() or field_name in ['close', 'open', 'high', 'low']:
            result = _price_validator.validate_price(value, field_name)
        elif 'atr' in field_name.lower():
            result = _price_validator.validate_atr(value, field_name)
        elif 'rsi' in field_name.lower() or 'stoch' in field_name.lower():
            result = _price_validator.validate_ratio(value, field_name, 0, 100)
        else:
            result = _price_validator.validate(value, field_name, allow_negative=True, allow_zero=True)
        
        if result.is_valid:
            cleaned[field_name] = result.value
        elif is_required:
            all_valid = False
            logger.error(f"Required price field validation failed: {result.error}")
    
    return all_valid, cleaned, _price_validator.get_warnings()


def is_valid_number(value: Any) -> bool:
    """Check if value is a valid finite number (not None, NaN, or Inf)
    
    Args:
        value: Any value to check
        
    Returns:
        True if value is a valid finite number, False otherwise
    """
    if value is None:
        return False
    if not isinstance(value, (int, float)):
        return False
    try:
        if math.isnan(value) or math.isinf(value):
            return False
        return True
    except (TypeError, ValueError):
        return False


def safe_float(value: Any, default: float = 0.0, name: str = "") -> float:
    """Safely convert value to float with NaN/Inf protection
    
    Args:
        value: Value to convert
        default: Default value to return if conversion fails
        name: Optional name for logging
        
    Returns:
        Float value or default if invalid
    """
    if value is None:
        if name:
            logger.warning(f"NaN/Inf check: {name} is None, using default {default}")
        return default
    
    try:
        result = float(value)
        if math.isnan(result):
            if name:
                logger.warning(f"NaN detected in {name}, using default {default}")
            return default
        if math.isinf(result):
            if name:
                logger.warning(f"Inf detected in {name}, using default {default}")
            return default
        return result
    except (TypeError, ValueError) as e:
        if name:
            logger.warning(f"Invalid number in {name}: {e}, using default {default}")
        return default


def safe_divide(numerator: Any, denominator: Any, default: float = 0.0, name: str = "") -> float:
    """Safely divide two numbers with protection against division by zero and NaN/Inf
    
    Args:
        numerator: The numerator value
        denominator: The denominator value
        default: Default value to return if division fails
        name: Optional name for logging
        
    Returns:
        Division result or default if invalid
    """
    num = safe_float(numerator, 0.0)
    denom = safe_float(denominator, 0.0)
    
    if denom == 0.0:
        if name:
            logger.warning(f"Division by zero in {name}, using default {default}")
        return default
    
    try:
        result = num / denom
        if math.isnan(result) or math.isinf(result):
            if name:
                logger.warning(f"NaN/Inf result in {name} division, using default {default}")
            return default
        return result
    except (TypeError, ValueError, ZeroDivisionError, OverflowError) as e:
        if name:
            logger.warning(f"Division error in {name}: {e}, using default {default}")
        return default


def validate_rsi_history(rsi_history: Any) -> List[float]:
    """Validate and clean RSI history list
    
    Args:
        rsi_history: List of RSI values
        
    Returns:
        Cleaned list of valid RSI values (0-100 range)
    """
    if not rsi_history or not isinstance(rsi_history, (list, tuple)):
        return []
    
    cleaned = []
    for val in rsi_history:
        if is_valid_number(val):
            if 0 <= val <= 100:
                cleaned.append(float(val))
            else:
                logger.warning(f"RSI history value out of range: {val}, skipping")
    
    return cleaned


def validate_indicator_value(name: str, value: Any, min_val: Optional[float] = None, max_val: Optional[float] = None) -> Tuple[bool, Optional[str]]:
    """Validate individual indicator value with range checks"""
    try:
        if value is None:
            return False, f"{name} is None"
        
        if not isinstance(value, (int, float)):
            return False, f"{name} has invalid type: {type(value)}"
        
        if math.isnan(value):
            return False, f"{name} is NaN"
        
        if math.isinf(value):
            return False, f"{name} is infinite"
        
        if min_val is not None and value < min_val:
            return False, f"{name} out of range: {value} < {min_val}"
        
        if max_val is not None and value > max_val:
            return False, f"{name} out of range: {value} > {max_val}"
        
        return True, None
        
    except Exception as e:
        return False, f"{name} validation error: {str(e)}"

def validate_indicators(indicators: Dict) -> Tuple[bool, Optional[str]]:
    """Validate all indicator data before processing"""
    try:
        if not indicators or not isinstance(indicators, dict):
            return False, "Indicators must be a non-empty dictionary"
        
        required_indicators = ['close', 'rsi', 'atr']
        missing = [ind for ind in required_indicators if ind not in indicators or indicators[ind] is None]
        if missing:
            return False, f"Missing required indicators: {missing}"
        
        close = indicators.get('close')
        is_valid, error = validate_indicator_value('close', close, min_val=0.01)
        if not is_valid:
            return False, error
        
        rsi = indicators.get('rsi')
        is_valid, error = validate_indicator_value('rsi', rsi, min_val=0, max_val=100)
        if not is_valid:
            return False, error
        
        atr = indicators.get('atr')
        is_valid, error = validate_indicator_value('atr', atr, min_val=0)
        if not is_valid:
            return False, error
        
        macd = indicators.get('macd')
        if macd is not None:
            is_valid, error = validate_indicator_value('macd', macd)
            if not is_valid:
                return False, error
        
        for ema_key in ['ema_5', 'ema_10', 'ema_20', 'ema_50']:
            ema_val = indicators.get(ema_key)
            if ema_val is not None:
                is_valid, error = validate_indicator_value(ema_key, ema_val, min_val=0)
                if not is_valid:
                    return False, error
        
        stoch_k = indicators.get('stoch_k')
        if stoch_k is not None:
            is_valid, error = validate_indicator_value('stoch_k', stoch_k, min_val=0, max_val=100)
            if not is_valid:
                logger.warning(f"Stochastic K validation failed: {error}")
                return False, error
        
        stoch_d = indicators.get('stoch_d')
        if stoch_d is not None:
            is_valid, error = validate_indicator_value('stoch_d', stoch_d, min_val=0, max_val=100)
            if not is_valid:
                logger.warning(f"Stochastic D validation failed: {error}")
                return False, error
        
        return True, None
        
    except Exception as e:
        return False, f"Indicator validation error: {str(e)}"

class TradingStrategy:
    def __init__(self, config, alert_system=None):
        self.config = config
        self.alert_system = alert_system
        self.last_volatility_alert = None
    
    def calculate_trend_strength(self, indicators: Dict) -> Tuple[float, str]:
        """Calculate trend strength with validation and error handling
        
        Returns: (strength_score, description)
        """
        default_score = 0.3
        default_desc = "MEDIUM ⚡"
        
        try:
            if not indicators or not isinstance(indicators, dict):
                logger.warning("calculate_trend_strength: Invalid indicators dict, returning default")
                return default_score, default_desc
            
            is_valid, error_msg = validate_indicators(indicators)
            if not is_valid:
                logger.warning(f"Indicator validation failed in trend strength calculation: {error_msg}")
                return default_score, default_desc
            
            score = 0.0
            factors = []
            
            ema_short_raw = indicators.get(f'ema_{self.config.EMA_PERIODS[0]}')
            ema_mid_raw = indicators.get(f'ema_{self.config.EMA_PERIODS[1]}')
            ema_long_raw = indicators.get(f'ema_{self.config.EMA_PERIODS[2]}')
            macd_histogram_raw = indicators.get('macd_histogram')
            rsi_raw = indicators.get('rsi')
            close_raw = indicators.get('close')
            volume_raw = indicators.get('volume')
            volume_avg_raw = indicators.get('volume_avg')
            
            ema_short = safe_float(ema_short_raw, 0.0) if is_valid_number(ema_short_raw) else None
            ema_mid = safe_float(ema_mid_raw, 0.0) if is_valid_number(ema_mid_raw) else None
            ema_long = safe_float(ema_long_raw, 0.0) if is_valid_number(ema_long_raw) else None
            close = safe_float(close_raw, 0.0) if is_valid_number(close_raw) else None
            
            if (ema_short is not None and ema_mid is not None and 
                ema_long is not None and close is not None and close > 0):
                ema_separation = safe_divide(abs(ema_short - ema_long), close, 0.0, "ema_separation")
                if is_valid_number(ema_separation):
                    if ema_separation > 0.003:
                        score += 0.25
                        factors.append("EMA spread lebar")
                    elif ema_separation > 0.0015:
                        score += 0.15
                        factors.append("EMA spread medium")
            
            if is_valid_number(macd_histogram_raw):
                macd_histogram = safe_float(macd_histogram_raw, 0.0)
                macd_strength = abs(macd_histogram)
                if is_valid_number(macd_strength):
                    if macd_strength > 0.5:
                        score += 0.25
                        factors.append("MACD histogram kuat")
                    elif macd_strength > 0.2:
                        score += 0.15
                        factors.append("MACD histogram medium")
            
            if is_valid_number(rsi_raw):
                rsi = safe_float(rsi_raw, 50.0)
                if 0 <= rsi <= 100:
                    rsi_momentum = safe_divide(abs(rsi - 50), 50, 0.0, "rsi_momentum")
                    if is_valid_number(rsi_momentum):
                        if rsi_momentum > 0.4:
                            score += 0.25
                            factors.append("RSI momentum tinggi")
                        elif rsi_momentum > 0.2:
                            score += 0.15
                            factors.append("RSI momentum medium")
                else:
                    logger.warning(f"RSI out of range in trend strength: {rsi}")
            
            if is_valid_number(volume_raw) and is_valid_number(volume_avg_raw):
                volume = safe_float(volume_raw, 0.0)
                volume_avg = safe_float(volume_avg_raw, 0.0)
                if volume_avg > 0:
                    volume_ratio = safe_divide(volume, volume_avg, 0.0, "volume_ratio")
                    if is_valid_number(volume_ratio):
                        if volume_ratio > 1.5:
                            score += 0.25
                            factors.append("Volume sangat tinggi")
                        elif volume_ratio > 1.0:
                            score += 0.15
                            factors.append("Volume tinggi")
            
            if math.isnan(score) or math.isinf(score):
                logger.warning(f"NaN/Inf detected in trend strength score, returning default")
                return default_score, default_desc
            
            score = min(max(score, 0.0), 1.0)
            
            if score >= 0.75:
                description = "SANGAT KUAT 🔥"
            elif score >= 0.5:
                description = "KUAT 💪"
            elif score >= 0.3:
                description = "MEDIUM ⚡"
            else:
                description = "LEMAH 📊"
            
            return score, description
            
        except Exception as e:
            logger.error(f"Error calculating trend strength: {e}")
            logger.warning(f"Trend strength calculation fallback triggered: Using default MEDIUM score due to error: {str(e)}")
            return default_score, default_desc
    
    def check_high_volatility(self, indicators: Dict):
        """Check for high volatility and send alert if detected"""
        try:
            if not indicators or not isinstance(indicators, dict):
                return
            
            atr_raw = indicators.get('atr')
            close_raw = indicators.get('close')
            
            if not is_valid_number(atr_raw) or not is_valid_number(close_raw):
                return
            
            atr = safe_float(atr_raw, 0.0)
            close = safe_float(close_raw, 0.0)
            
            if atr <= 0 or close <= 0:
                return
            
            volatility_percent = safe_divide(atr, close, 0.0, "volatility_percent") * 100
            
            if not is_valid_number(volatility_percent):
                logger.warning("NaN/Inf detected in volatility calculation, skipping alert")
                return
            
            high_volatility_threshold = 0.15
            
            if volatility_percent >= high_volatility_threshold:
                from datetime import datetime, timedelta
                import pytz
                
                current_time = datetime.now(pytz.UTC)
                
                if self.last_volatility_alert is None or (current_time - self.last_volatility_alert).total_seconds() > 3600:
                    self.last_volatility_alert = current_time
                    
                    if self.alert_system:
                        import asyncio
                        try:
                            asyncio.create_task(
                                self.alert_system.send_high_volatility_alert(
                                    "XAUUSD",
                                    volatility_percent
                                )
                            )
                            logger.warning(f"High volatility detected: {volatility_percent:.2f}% (ATR: ${atr:.2f}, Price: ${close:.2f})")
                        except Exception as alert_error:
                            logger.error(f"Failed to send high volatility alert: {alert_error}")
                            
        except Exception as e:
            logger.error(f"Error checking high volatility: {e}")
    
    def check_pullback_confirmation(self, rsi_history: list, signal_type: str) -> bool:
        """Check if there was a proper pullback before the signal
        
        BUY: RSI should have dropped to 40-45 range and then recovered
        SELL: RSI should have risen to 55-60 range and then declined
        
        Args:
            rsi_history: List of recent RSI values (last 20 values)
            signal_type: 'BUY' or 'SELL'
        
        Returns:
            True if pullback confirmed, False otherwise
        """
        try:
            if signal_type not in ['BUY', 'SELL']:
                logger.warning(f"Invalid signal_type in pullback confirmation: {signal_type}")
                return False
            
            cleaned_history = validate_rsi_history(rsi_history)
            
            if len(cleaned_history) < 5:
                return False
            
            recent_history = cleaned_history[-10:] if len(cleaned_history) >= 10 else cleaned_history
            current_rsi = cleaned_history[-1]
            
            if not is_valid_number(current_rsi):
                logger.warning("Invalid current RSI in pullback confirmation")
                return False
            
            if signal_type == 'BUY':
                pullback_detected = any(40 <= rsi <= 45 for rsi in recent_history if is_valid_number(rsi))
                if pullback_detected and current_rsi > 45:
                    return True
            elif signal_type == 'SELL':
                pullback_detected = any(55 <= rsi <= 60 for rsi in recent_history if is_valid_number(rsi))
                if pullback_detected and current_rsi < 55:
                    return True
            
            return False
        except Exception as e:
            logger.error(f"Error checking pullback confirmation: {e}")
            return False
    
    def is_optimal_trading_session(self) -> bool:
        """Check if current time is within London-NY overlap (07:00-16:00 UTC)
        
        Returns:
            True if within optimal trading session, False otherwise
        """
        try:
            from datetime import datetime
            import pytz
            
            current_time = datetime.now(pytz.UTC)
            current_hour = current_time.hour
            
            if 7 <= current_hour < 16:
                return True
            return False
        except Exception as e:
            logger.error(f"Error checking trading session: {e}")
            return False
        
    def detect_signal(self, indicators: Dict, timeframe: str = 'M1', signal_source: str = 'auto') -> Optional[Dict]:  # pyright: ignore[reportGeneralTypeIssues]
        """Detect trading signal with comprehensive validation and error handling
        
        Note: This function is intentionally complex due to multi-indicator trading logic.
        Pyright complexity warning is suppressed as it does not affect runtime behavior.
        """
        if not indicators or not isinstance(indicators, dict):
            logger.warning("Invalid or empty indicators provided")
            return None
        
        try:
            is_valid, error_msg = validate_indicators(indicators)
            if not is_valid:
                logger.warning(f"Indicator validation failed: {error_msg}")
                logger.error(f"Signal detection aborted: Indicator validation error - {error_msg}")
                return None
            
            self.check_high_volatility(indicators)
            ema_short = indicators.get(f'ema_{self.config.EMA_PERIODS[0]}')
            ema_mid = indicators.get(f'ema_{self.config.EMA_PERIODS[1]}')
            ema_long = indicators.get(f'ema_{self.config.EMA_PERIODS[2]}')
            ema_50 = indicators.get('ema_50')
            
            rsi = indicators.get('rsi')
            rsi_prev = indicators.get('rsi_prev')
            rsi_history = indicators.get('rsi_history', [])
            
            stoch_k = indicators.get('stoch_k')
            stoch_d = indicators.get('stoch_d')
            stoch_k_prev = indicators.get('stoch_k_prev')
            stoch_d_prev = indicators.get('stoch_d_prev')
            
            macd = indicators.get('macd')
            macd_signal = indicators.get('macd_signal')
            macd_histogram = indicators.get('macd_histogram')
            macd_prev = indicators.get('macd_prev')
            macd_signal_prev = indicators.get('macd_signal_prev')
            
            atr = indicators.get('atr')
            close = indicators.get('close')
            volume = indicators.get('volume')
            volume_avg = indicators.get('volume_avg')
            
            trf_trend = indicators.get('trf_trend')
            trf_trend_prev = indicators.get('trf_trend_prev')
            trf_upper = indicators.get('trf_upper')
            trf_lower = indicators.get('trf_lower')
            
            cerebr_value = indicators.get('cerebr_value')
            cerebr_signal = indicators.get('cerebr_signal')
            cerebr_bias = indicators.get('cerebr_bias')
            cerebr_bias_prev = indicators.get('cerebr_bias_prev')
            
            if None in [ema_short, ema_mid, ema_long, rsi, macd, macd_signal, atr, close]:
                missing = []
                if ema_short is None: missing.append("ema_short")
                if ema_mid is None: missing.append("ema_mid")
                if ema_long is None: missing.append("ema_long")
                if rsi is None: missing.append("rsi")
                if macd is None: missing.append("macd")
                if macd_signal is None: missing.append("macd_signal")
                if atr is None: missing.append("atr")
                if close is None: missing.append("close")
                logger.warning(f"Missing required indicators for signal detection: {missing}")
                return None
            
            if trf_trend is None or cerebr_bias is None:
                missing_new = []
                if trf_trend is None: missing_new.append("trf_trend")
                if cerebr_bias is None: missing_new.append("cerebr_bias")
                logger.warning(f"Missing new required indicators for signal detection: {missing_new} - Signal blocked")
                return None
            
            if trf_trend == 0 or cerebr_bias == 0:
                logger.info("Twin Range Filter or CEREBR in neutral state - No clear trend, signal blocked")
                return None
            
            signal = None
            confidence_reasons = []
            
            ema_short_valid = is_valid_number(ema_short)
            ema_mid_valid = is_valid_number(ema_mid)
            ema_long_valid = is_valid_number(ema_long)
            
            ema_trend_bullish = (ema_short_valid and ema_mid_valid and ema_long_valid and 
                                 ema_short > ema_mid > ema_long)
            ema_trend_bearish = (ema_short_valid and ema_mid_valid and ema_long_valid and 
                                 ema_short < ema_mid < ema_long)
            
            ema_crossover_bullish = False
            ema_crossover_bearish = False
            if ema_short_valid and ema_mid_valid and ema_mid > 0:
                ema_diff_ratio = safe_divide(abs(ema_short - ema_mid), ema_mid, 999.0, "ema_crossover_diff")
                if is_valid_number(ema_diff_ratio):
                    ema_crossover_bullish = (ema_short > ema_mid and ema_diff_ratio < 0.001)
                    ema_crossover_bearish = (ema_short < ema_mid and ema_diff_ratio < 0.001)
            
            macd_bullish_crossover = False
            macd_bearish_crossover = False
            macd_valid = is_valid_number(macd)
            macd_signal_valid = is_valid_number(macd_signal)
            macd_prev_valid = is_valid_number(macd_prev)
            macd_signal_prev_valid = is_valid_number(macd_signal_prev)
            
            if macd_prev_valid and macd_signal_prev_valid and macd_valid and macd_signal_valid:
                macd_bullish_crossover = (macd_prev <= macd_signal_prev and macd > macd_signal)
                macd_bearish_crossover = (macd_prev >= macd_signal_prev and macd < macd_signal)
            
            macd_bullish = macd_valid and macd_signal_valid and macd > macd_signal
            macd_bearish = macd_valid and macd_signal_valid and macd < macd_signal
            macd_above_zero = macd_valid and macd > 0
            macd_below_zero = macd_valid and macd < 0
            
            rsi_valid = is_valid_number(rsi) and 0 <= rsi <= 100
            rsi_prev_valid = is_valid_number(rsi_prev) and 0 <= rsi_prev <= 100
            
            rsi_oversold_crossup = False
            rsi_overbought_crossdown = False
            if rsi_valid and rsi_prev_valid:
                rsi_oversold_crossup = (rsi_prev < self.config.RSI_OVERSOLD_LEVEL and rsi >= self.config.RSI_OVERSOLD_LEVEL)
                rsi_overbought_crossdown = (rsi_prev > self.config.RSI_OVERBOUGHT_LEVEL and rsi <= self.config.RSI_OVERBOUGHT_LEVEL)
            
            rsi_bullish = rsi_valid and rsi > 50
            rsi_bearish = rsi_valid and rsi < 50
            
            stoch_k_valid = is_valid_number(stoch_k) and 0 <= stoch_k <= 100
            stoch_d_valid = is_valid_number(stoch_d) and 0 <= stoch_d <= 100
            stoch_k_prev_valid = is_valid_number(stoch_k_prev) and 0 <= stoch_k_prev <= 100
            stoch_d_prev_valid = is_valid_number(stoch_d_prev) and 0 <= stoch_d_prev <= 100
            
            stoch_bullish = False
            stoch_bearish = False
            if stoch_k_prev_valid and stoch_d_prev_valid and stoch_k_valid and stoch_d_valid:
                stoch_bullish = (stoch_k_prev < stoch_d_prev and stoch_k > stoch_d and 
                                stoch_k < self.config.STOCH_OVERBOUGHT_LEVEL)
                stoch_bearish = (stoch_k_prev > stoch_d_prev and stoch_k < stoch_d and 
                                stoch_k > self.config.STOCH_OVERSOLD_LEVEL)
            
            volume_strong = True
            volume_valid = is_valid_number(volume)
            volume_avg_valid = is_valid_number(volume_avg) and volume_avg > 0
            if volume_valid and volume_avg_valid:
                volume_strong = volume > volume_avg * self.config.VOLUME_THRESHOLD_MULTIPLIER
            
            if signal_source == 'auto':
                bullish_score = 0
                bearish_score = 0
                
                ema_50_trend_bullish = False
                ema_50_trend_bearish = False
                if ema_50 is not None and close is not None:
                    ema_50_trend_bullish = close > ema_50
                    ema_50_trend_bearish = close < ema_50
                
                if ema_50_trend_bullish:
                    bullish_score += 2
                if ema_50_trend_bearish:
                    bearish_score += 2
                
                if macd_bullish_crossover:
                    bullish_score += 2
                elif macd_bullish:
                    bullish_score += 1
                    
                if macd_bearish_crossover:
                    bearish_score += 2
                elif macd_bearish:
                    bearish_score += 1
                
                pullback_buy_confirmed = self.check_pullback_confirmation(rsi_history, 'BUY')
                pullback_sell_confirmed = self.check_pullback_confirmation(rsi_history, 'SELL')
                
                if pullback_buy_confirmed:
                    bullish_score += 1
                if pullback_sell_confirmed:
                    bearish_score += 1
                
                if volume_strong:
                    if bullish_score > bearish_score:
                        bullish_score += 1
                    elif bearish_score > bullish_score:
                        bearish_score += 1
                
                if trf_trend is not None and trf_trend_prev is not None:
                    trf_bullish_entry = (trf_trend == 1 and trf_trend_prev != 1)
                    trf_bearish_entry = (trf_trend == -1 and trf_trend_prev != -1)
                    trf_bullish_trend = (trf_trend == 1)
                    trf_bearish_trend = (trf_trend == -1)
                    
                    if trf_bullish_entry:
                        bullish_score += 2
                    elif trf_bullish_trend:
                        bullish_score += 1
                    
                    if trf_bearish_entry:
                        bearish_score += 2
                    elif trf_bearish_trend:
                        bearish_score += 1
                
                if cerebr_bias is not None:
                    if cerebr_bias == 1:
                        bullish_score += 1
                    elif cerebr_bias == -1:
                        bearish_score += 1
                
                optimal_session = self.is_optimal_trading_session()
                
                min_score_required = 5
                
                trf_bullish_confirmed = (trf_trend is not None and trf_trend == 1)
                trf_bearish_confirmed = (trf_trend is not None and trf_trend == -1)
                cerebr_bullish_confirmed = (cerebr_bias is not None and cerebr_bias == 1)
                cerebr_bearish_confirmed = (cerebr_bias is not None and cerebr_bias == -1)
                
                if (bullish_score >= min_score_required and bullish_score > bearish_score and 
                    ema_50_trend_bullish and trf_bullish_confirmed and cerebr_bullish_confirmed):
                    signal = 'BUY'
                    confidence_reasons.append("✅ Price > EMA 50 (Uptrend Confirmed)")
                    if macd_bullish_crossover:
                        confidence_reasons.append("MACD bullish crossover (konfirmasi kuat)")
                    elif macd_bullish:
                        confidence_reasons.append("MACD bullish")
                    if pullback_buy_confirmed:
                        confidence_reasons.append("Pullback confirmed (RSI 40-45 zone)")
                    if volume_strong:
                        confidence_reasons.append("Volume tinggi konfirmasi")
                    if trf_trend is not None:
                        if trf_trend == 1 and trf_trend_prev != 1:
                            confidence_reasons.append("🎯 Twin Range Filter ENTRY (bullish)")
                        elif trf_trend == 1:
                            confidence_reasons.append("Twin Range Filter: Bullish trend")
                    if cerebr_bias is not None and cerebr_bias == 1:
                        confidence_reasons.append(f"📊 Market Bias CEREBR: Bullish ({cerebr_value:.1f}%)")
                    if optimal_session:
                        confidence_reasons.append("Optimal session (London-NY overlap)")
                    confidence_reasons.append(f"Signal score: {bullish_score}/{bearish_score}")
                        
                elif (bearish_score >= min_score_required and bearish_score > bullish_score and 
                      ema_50_trend_bearish and trf_bearish_confirmed and cerebr_bearish_confirmed):
                    signal = 'SELL'
                    confidence_reasons.append("✅ Price < EMA 50 (Downtrend Confirmed)")
                    if macd_bearish_crossover:
                        confidence_reasons.append("MACD bearish crossover (konfirmasi kuat)")
                    elif macd_bearish:
                        confidence_reasons.append("MACD bearish")
                    if pullback_sell_confirmed:
                        confidence_reasons.append("Pullback confirmed (RSI 55-60 zone)")
                    if volume_strong:
                        confidence_reasons.append("Volume tinggi konfirmasi")
                    if trf_trend is not None:
                        if trf_trend == -1 and trf_trend_prev != -1:
                            confidence_reasons.append("🎯 Twin Range Filter ENTRY (bearish)")
                        elif trf_trend == -1:
                            confidence_reasons.append("Twin Range Filter: Bearish trend")
                    if cerebr_bias is not None and cerebr_bias == -1:
                        confidence_reasons.append(f"📊 Market Bias CEREBR: Bearish ({cerebr_value:.1f}%)")
                    if optimal_session:
                        confidence_reasons.append("Optimal session (London-NY overlap)")
                    confidence_reasons.append(f"Signal score: {bearish_score}/{bullish_score}")
            else:
                ema_50_trend_bullish = False
                ema_50_trend_bearish = False
                if ema_50 is not None and close is not None:
                    ema_50_trend_bullish = close > ema_50
                    ema_50_trend_bearish = close < ema_50
                
                ema_condition_bullish = ema_trend_bullish or ema_crossover_bullish
                ema_condition_bearish = ema_trend_bearish or ema_crossover_bearish
                
                rsi_condition_bullish = rsi_oversold_crossup or rsi_bullish
                rsi_condition_bearish = rsi_overbought_crossdown or rsi_bearish
                
                trf_bullish_confirmed = (trf_trend is not None and trf_trend == 1)
                trf_bearish_confirmed = (trf_trend is not None and trf_trend == -1)
                cerebr_bullish_confirmed = (cerebr_bias is not None and cerebr_bias == 1)
                cerebr_bearish_confirmed = (cerebr_bias is not None and cerebr_bias == -1)
                
                if (ema_condition_bullish and macd_bullish and rsi_condition_bullish and 
                    ema_50_trend_bullish and trf_bullish_confirmed and cerebr_bullish_confirmed):
                    signal = 'BUY'
                    confidence_reasons.append("Manual: EMA bullish")
                    confidence_reasons.append("✅ Price > EMA 50 (Uptrend Confirmed)")
                    confidence_reasons.append("MACD bullish (konfirmasi)")
                    if macd_bullish_crossover:
                        confidence_reasons.append("MACD fresh crossover")
                    if rsi_oversold_crossup:
                        confidence_reasons.append("RSI keluar dari oversold")
                    elif rsi_bullish:
                        confidence_reasons.append("RSI bullish")
                    if stoch_bullish:
                        confidence_reasons.append("Stochastic konfirmasi bullish")
                    if trf_trend is not None:
                        if trf_trend == 1 and trf_trend_prev != 1:
                            confidence_reasons.append("🎯 Twin Range Filter ENTRY (bullish)")
                        elif trf_trend == 1:
                            confidence_reasons.append("Twin Range Filter: Bullish trend")
                    if cerebr_bias is not None and cerebr_bias == 1:
                        confidence_reasons.append(f"📊 Market Bias CEREBR: Bullish ({cerebr_value:.1f}%)")
                    pullback_buy_confirmed = self.check_pullback_confirmation(rsi_history, 'BUY')
                    if pullback_buy_confirmed:
                        confidence_reasons.append("Pullback confirmed (RSI 40-45 zone)")
                        
                elif (ema_condition_bearish and macd_bearish and rsi_condition_bearish and 
                      ema_50_trend_bearish and trf_bearish_confirmed and cerebr_bearish_confirmed):
                    signal = 'SELL'
                    confidence_reasons.append("Manual: EMA bearish")
                    confidence_reasons.append("✅ Price < EMA 50 (Downtrend Confirmed)")
                    confidence_reasons.append("MACD bearish (konfirmasi)")
                    if macd_bearish_crossover:
                        confidence_reasons.append("MACD fresh crossover")
                    if rsi_overbought_crossdown:
                        confidence_reasons.append("RSI keluar dari overbought")
                    elif rsi_bearish:
                        confidence_reasons.append("RSI bearish")
                    if stoch_bearish:
                        confidence_reasons.append("Stochastic konfirmasi bearish")
                    if trf_trend is not None:
                        if trf_trend == -1 and trf_trend_prev != -1:
                            confidence_reasons.append("🎯 Twin Range Filter ENTRY (bearish)")
                        elif trf_trend == -1:
                            confidence_reasons.append("Twin Range Filter: Bearish trend")
                    if cerebr_bias is not None and cerebr_bias == -1:
                        confidence_reasons.append(f"📊 Market Bias CEREBR: Bearish ({cerebr_value:.1f}%)")
                    pullback_sell_confirmed = self.check_pullback_confirmation(rsi_history, 'SELL')
                    if pullback_sell_confirmed:
                        confidence_reasons.append("Pullback confirmed (RSI 55-60 zone)")
            
            if signal:
                try:
                    trend_strength, trend_desc = self.calculate_trend_strength(indicators)
                except Exception as e:
                    logger.error(f"Error calculating trend strength: {e}")
                    trend_strength, trend_desc = 0.3, "MEDIUM ⚡"
                
                if not is_valid_number(trend_strength):
                    logger.warning(f"NaN/Inf detected in trend_strength: {trend_strength}, using default 0.3")
                    trend_strength = 0.3
                    trend_desc = "MEDIUM ⚡"
                
                trend_strength = float(min(max(trend_strength, 0.0), 1.0))
                
                if signal_source == 'auto' and trend_strength < 0.3:
                    logger.info(f"Auto signal rejected - trend strength too weak: {trend_strength:.2f} ({trend_desc})")
                    return None
                
                try:
                    dynamic_tp_ratio = 1.45 + (trend_strength * 1.05)
                    
                    if not is_valid_number(dynamic_tp_ratio):
                        logger.warning(f"NaN/Inf in dynamic_tp_ratio: {dynamic_tp_ratio}, using default 1.5")
                        dynamic_tp_ratio = 1.5
                    
                    dynamic_tp_ratio = float(min(max(dynamic_tp_ratio, 1.45), 2.50))
                    
                    if not (1.0 <= dynamic_tp_ratio <= 3.0):
                        logger.warning(f"Invalid TP ratio: {dynamic_tp_ratio}, using default 1.5")
                        dynamic_tp_ratio = 1.5
                    
                    atr_raw = indicators.get('atr', 1.0)
                    atr = safe_float(atr_raw, 1.0, "signal_atr")
                    if atr <= 0 or not is_valid_number(atr):
                        logger.warning(f"Invalid ATR: {atr}, using default 1.0")
                        atr = 1.0
                    
                    if signal_source == 'auto':
                        sl_atr_mult = safe_float(self.config.SL_ATR_MULTIPLIER, 1.5, "SL_ATR_MULTIPLIER")
                        default_sl_pips = safe_float(self.config.DEFAULT_SL_PIPS, 10.0, "DEFAULT_SL_PIPS")
                        pip_value = safe_float(self.config.XAUUSD_PIP_VALUE, 10.0, "XAUUSD_PIP_VALUE")
                        
                        if pip_value <= 0:
                            logger.warning(f"Invalid pip value: {pip_value}, using default 10.0")
                            pip_value = 10.0
                        
                        sl_distance = max(atr * sl_atr_mult, safe_divide(default_sl_pips, pip_value, 1.0, "sl_distance_calc"))
                    else:
                        sl_distance = max(atr * 1.2, 1.0)
                    
                    if not is_valid_number(sl_distance) or sl_distance <= 0 or sl_distance > 100:
                        logger.error(f"Invalid SL distance: {sl_distance}")
                        return None
                    
                    tp_distance = sl_distance * dynamic_tp_ratio
                    
                    if not is_valid_number(tp_distance) or tp_distance <= 0:
                        logger.error(f"Invalid TP distance: {tp_distance}")
                        return None
                    
                    close_val = safe_float(close, 0.0, "close_for_sl_tp")
                    if close_val <= 0 or not is_valid_number(close_val):
                        logger.error(f"Invalid close price for SL/TP calculation: {close}")
                        return None
                    
                    if signal == 'BUY':
                        stop_loss = close_val - sl_distance
                        take_profit = close_val + tp_distance
                    else:
                        stop_loss = close_val + sl_distance
                        take_profit = close_val - tp_distance
                    
                    if not is_valid_number(stop_loss) or not is_valid_number(take_profit):
                        logger.error(f"NaN/Inf in SL/TP: SL={stop_loss}, TP={take_profit}")
                        return None
                    
                    if stop_loss <= 0 or take_profit <= 0:
                        logger.error(f"Invalid SL/TP calculated: SL={stop_loss}, TP={take_profit}")
                        return None
                    
                    pip_value = safe_float(self.config.XAUUSD_PIP_VALUE, 10.0, "XAUUSD_PIP_VALUE")
                    sl_pips = abs(stop_loss - close_val) * pip_value
                    tp_pips = abs(take_profit - close_val) * pip_value
                    
                    if not is_valid_number(sl_pips) or sl_pips <= 0:
                        logger.error(f"Invalid SL pips: {sl_pips}")
                        return None
                    
                    if not is_valid_number(tp_pips) or tp_pips <= 0:
                        logger.error(f"Invalid TP pips: {tp_pips}")
                        return None
                    
                    fixed_risk = safe_float(self.config.FIXED_RISK_AMOUNT, 10.0, "FIXED_RISK_AMOUNT")
                    default_lot = safe_float(self.config.LOT_SIZE, 0.01, "LOT_SIZE")
                    
                    lot_size = safe_divide(fixed_risk, sl_pips, default_lot, "lot_size_calc")
                    lot_size = float(max(0.01, min(lot_size, 1.0)))
                    
                    if not is_valid_number(lot_size):
                        logger.warning(f"Invalid lot size calculated: {lot_size}, using default")
                        lot_size = default_lot
                    
                    expected_loss = fixed_risk
                    expected_profit = expected_loss * dynamic_tp_ratio
                    
                    if not is_valid_number(expected_profit):
                        logger.warning(f"Invalid expected_profit: {expected_profit}")
                        expected_profit = expected_loss * 1.5
                    
                except (ValueError, ZeroDivisionError, OverflowError, TypeError) as e:
                    logger.error(f"Calculation error in signal generation: {type(e).__name__}: {e}")
                    return None
                
                logger.info(f"{signal} signal detected ({signal_source}) on {timeframe}")
                logger.info(f"Trend Strength: {trend_desc} (score: {trend_strength:.2f})")
                logger.info(f"Dynamic TP Ratio: {dynamic_tp_ratio:.2f}x (Expected profit: ${expected_profit:.2f})")
                logger.info(f"Risk: ${expected_loss:.2f} | Reward: ${expected_profit:.2f} | R:R = 1:{dynamic_tp_ratio:.2f}")
                
                def safe_indicator_float(val, default=None):
                    """Convert indicator value to float safely for JSON serialization"""
                    if val is None:
                        return default
                    if not is_valid_number(val):
                        return default
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        return default
                
                def safe_indicator_int(val, default=None):
                    """Convert indicator value to int safely for JSON serialization"""
                    if val is None:
                        return default
                    if not is_valid_number(val):
                        return default
                    try:
                        return int(val)
                    except (ValueError, TypeError):
                        return default
                
                return {
                    'signal': signal,
                    'signal_source': signal_source,
                    'entry_price': float(close_val),
                    'stop_loss': float(stop_loss),
                    'take_profit': float(take_profit),
                    'timeframe': timeframe,
                    'trend_strength': float(trend_strength),
                    'trend_description': trend_desc,
                    'expected_profit': float(expected_profit),
                    'expected_loss': float(expected_loss),
                    'rr_ratio': float(dynamic_tp_ratio),
                    'lot_size': float(lot_size),
                    'sl_pips': float(sl_pips),
                    'tp_pips': float(tp_pips),
                    'indicators': json.dumps({
                        'ema_short': safe_indicator_float(ema_short),
                        'ema_mid': safe_indicator_float(ema_mid),
                        'ema_long': safe_indicator_float(ema_long),
                        'rsi': safe_indicator_float(rsi),
                        'macd': safe_indicator_float(macd),
                        'macd_signal': safe_indicator_float(macd_signal),
                        'macd_histogram': safe_indicator_float(macd_histogram),
                        'stoch_k': safe_indicator_float(stoch_k),
                        'stoch_d': safe_indicator_float(stoch_d),
                        'atr': safe_indicator_float(atr),
                        'volume': safe_indicator_int(volume),
                        'volume_avg': safe_indicator_float(volume_avg)
                    }),
                    'confidence_reasons': confidence_reasons
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error detecting signal: {e}")
            return None
    
    def validate_signal(self, signal: Dict, current_spread: float = 0) -> Tuple[bool, Optional[str]]:
        """Validate signal with comprehensive checks and error handling"""
        try:
            if not signal or not isinstance(signal, dict):
                return False, "Signal must be a non-empty dictionary"
            
            required_fields = ['entry_price', 'stop_loss', 'take_profit', 'signal']
            missing = [f for f in required_fields if f not in signal]
            if missing:
                return False, f"Missing required fields: {missing}"
            
            entry_raw = signal['entry_price']
            sl_raw = signal['stop_loss']
            tp_raw = signal['take_profit']
            signal_type = signal['signal']
            
            if not is_valid_number(entry_raw):
                logger.warning(f"NaN/Inf detected in entry_price: {entry_raw}")
                return False, f"Entry price is NaN or Inf: {entry_raw}"
            if not is_valid_number(sl_raw):
                logger.warning(f"NaN/Inf detected in stop_loss: {sl_raw}")
                return False, f"Stop loss is NaN or Inf: {sl_raw}"
            if not is_valid_number(tp_raw):
                logger.warning(f"NaN/Inf detected in take_profit: {tp_raw}")
                return False, f"Take profit is NaN or Inf: {tp_raw}"
            
            entry = safe_float(entry_raw, 0.0)
            sl = safe_float(sl_raw, 0.0)
            tp = safe_float(tp_raw, 0.0)
            
            if entry <= 0 or sl <= 0 or tp <= 0:
                return False, f"Invalid prices: entry={entry}, sl={sl}, tp={tp}"
            
            if signal_type not in ['BUY', 'SELL']:
                return False, f"Invalid signal type: {signal_type}"
            
            try:
                spread_safe = safe_float(current_spread, 0.0, "current_spread")
                pip_value = safe_float(self.config.XAUUSD_PIP_VALUE, 10.0, "XAUUSD_PIP_VALUE")
                
                if pip_value <= 0:
                    pip_value = 10.0
                
                spread_pips = spread_safe * pip_value
                
                if not is_valid_number(spread_pips):
                    logger.warning(f"NaN/Inf in spread calculation: {spread_pips}, using 0")
                    spread_pips = 0.0
                
                if spread_pips < 0:
                    logger.warning(f"Negative spread: {spread_pips}, using 0")
                    spread_pips = 0.0
                
                max_spread = safe_float(self.config.MAX_SPREAD_PIPS, 50.0, "MAX_SPREAD_PIPS")
                if spread_pips > max_spread:
                    return False, f"Spread too high: {spread_pips:.2f} pips (max: {max_spread})"
            except Exception as e:
                logger.warning(f"Error calculating spread pips: {e}")
            
            pip_value = safe_float(self.config.XAUUSD_PIP_VALUE, 10.0, "XAUUSD_PIP_VALUE_validate")
            if pip_value <= 0:
                pip_value = 10.0
            
            sl_pips = abs(entry - sl) * pip_value
            tp_pips = abs(entry - tp) * pip_value
            
            if not is_valid_number(sl_pips):
                logger.warning(f"NaN/Inf in SL pips validation: {sl_pips}")
                return False, f"SL pips calculation resulted in NaN/Inf: {sl_pips}"
            
            if not is_valid_number(tp_pips):
                logger.warning(f"NaN/Inf in TP pips validation: {tp_pips}")
                return False, f"TP pips calculation resulted in NaN/Inf: {tp_pips}"
            
            if sl_pips < 5:
                return False, f"Stop loss too tight: {sl_pips:.1f} pips (min: 5 pips)"
            
            if tp_pips < 10:
                return False, f"Take profit too tight: {tp_pips:.1f} pips (min: 10 pips)"
            
            if signal_type == 'BUY':
                if sl >= entry:
                    return False, f"BUY signal: SL ({sl}) must be < entry ({entry})"
                if tp <= entry:
                    return False, f"BUY signal: TP ({tp}) must be > entry ({entry})"
            else:
                if sl <= entry:
                    return False, f"SELL signal: SL ({sl}) must be > entry ({entry})"
                if tp >= entry:
                    return False, f"SELL signal: TP ({tp}) must be < entry ({entry})"
            
            return True, None
            
        except KeyError as e:
            return False, f"Missing key in signal: {e}"
        except Exception as e:
            logger.error(f"Signal validation error: {type(e).__name__}: {e}")
            return False, f"Validation error: {str(e)}"
