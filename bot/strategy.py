from typing import Optional, Dict, Tuple, Any
import json
from bot.logger import setup_logger
import math

logger = setup_logger('Strategy')

class StrategyError(Exception):
    """Base exception for strategy errors"""
    pass

class IndicatorValidationError(StrategyError):
    """Indicator data validation error"""
    pass

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
        try:
            is_valid, error_msg = validate_indicators(indicators)
            if not is_valid:
                logger.warning(f"Indicator validation failed in trend strength calculation: {error_msg}")
                return 0.3, "MEDIUM ⚡"
            
            score = 0.0
            factors = []
            
            ema_short = indicators.get(f'ema_{self.config.EMA_PERIODS[0]}')
            ema_mid = indicators.get(f'ema_{self.config.EMA_PERIODS[1]}')
            ema_long = indicators.get(f'ema_{self.config.EMA_PERIODS[2]}')
            macd_histogram = indicators.get('macd_histogram')
            rsi = indicators.get('rsi')
            atr = indicators.get('atr')
            close = indicators.get('close')
            volume = indicators.get('volume')
            volume_avg = indicators.get('volume_avg')
            
            if (ema_short is not None and ema_mid is not None and 
                ema_long is not None and close is not None and close > 0):
                ema_separation = abs(ema_short - ema_long) / close
                if ema_separation > 0.003:
                    score += 0.25
                    factors.append("EMA spread lebar")
                elif ema_separation > 0.0015:
                    score += 0.15
                    factors.append("EMA spread medium")
            
            if macd_histogram is not None:
                macd_strength = abs(macd_histogram)
                if macd_strength > 0.5:
                    score += 0.25
                    factors.append("MACD histogram kuat")
                elif macd_strength > 0.2:
                    score += 0.15
                    factors.append("MACD histogram medium")
            
            if rsi is not None:
                rsi_momentum = abs(rsi - 50) / 50
                if rsi_momentum > 0.4:
                    score += 0.25
                    factors.append("RSI momentum tinggi")
                elif rsi_momentum > 0.2:
                    score += 0.15
                    factors.append("RSI momentum medium")
            
            if volume is not None and volume_avg is not None and volume_avg > 0:
                volume_ratio = volume / volume_avg
                if volume_ratio > 1.5:
                    score += 0.25
                    factors.append("Volume sangat tinggi")
                elif volume_ratio > 1.0:
                    score += 0.15
                    factors.append("Volume tinggi")
            
            if score >= 0.75:
                description = "SANGAT KUAT 🔥"
            elif score >= 0.5:
                description = "KUAT 💪"
            elif score >= 0.3:
                description = "MEDIUM ⚡"
            else:
                description = "LEMAH 📊"
            
            return min(score, 1.0), description
            
        except Exception as e:
            logger.error(f"Error calculating trend strength: {e}")
            logger.warning(f"Trend strength calculation fallback triggered: Using default MEDIUM score due to error: {str(e)}")
            return 0.3, "MEDIUM ⚡"
    
    def check_high_volatility(self, indicators: Dict):
        """Check for high volatility and send alert if detected"""
        try:
            atr = indicators.get('atr')
            close = indicators.get('close')
            
            if atr is None or close is None or close <= 0:
                return
            
            volatility_percent = (atr / close) * 100
            
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
            if not rsi_history or len(rsi_history) < 5:
                return False
            
            if signal_type == 'BUY':
                pullback_detected = any(40 <= rsi <= 45 for rsi in rsi_history[-10:])
                if pullback_detected and rsi_history[-1] > 45:
                    return True
            elif signal_type == 'SELL':
                pullback_detected = any(55 <= rsi <= 60 for rsi in rsi_history[-10:])
                if pullback_detected and rsi_history[-1] < 55:
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
        
    def detect_signal(self, indicators: Dict, timeframe: str = 'M1', signal_source: str = 'auto') -> Optional[Dict]:
        """Detect trading signal with comprehensive validation and error handling"""
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
            
            ema_trend_bullish = (ema_short is not None and ema_mid is not None and ema_long is not None and 
                                 ema_short > ema_mid > ema_long)
            ema_trend_bearish = (ema_short is not None and ema_mid is not None and ema_long is not None and 
                                 ema_short < ema_mid < ema_long)
            
            ema_crossover_bullish = (ema_short is not None and ema_mid is not None and 
                                     ema_short > ema_mid and 
                                     abs(ema_short - ema_mid) / ema_mid < 0.001)
            ema_crossover_bearish = (ema_short is not None and ema_mid is not None and 
                                     ema_short < ema_mid and 
                                     abs(ema_short - ema_mid) / ema_mid < 0.001)
            
            macd_bullish_crossover = False
            macd_bearish_crossover = False
            if macd_prev is not None and macd_signal_prev is not None and macd is not None and macd_signal is not None:
                macd_bullish_crossover = (macd_prev <= macd_signal_prev and macd > macd_signal)
                macd_bearish_crossover = (macd_prev >= macd_signal_prev and macd < macd_signal)
            
            macd_bullish = macd is not None and macd_signal is not None and macd > macd_signal
            macd_bearish = macd is not None and macd_signal is not None and macd < macd_signal
            macd_above_zero = macd is not None and macd > 0
            macd_below_zero = macd is not None and macd < 0
            
            rsi_oversold_crossup = False
            rsi_overbought_crossdown = False
            if rsi_prev is not None:
                rsi_oversold_crossup = (rsi_prev < self.config.RSI_OVERSOLD_LEVEL and rsi >= self.config.RSI_OVERSOLD_LEVEL)
                rsi_overbought_crossdown = (rsi_prev > self.config.RSI_OVERBOUGHT_LEVEL and rsi <= self.config.RSI_OVERBOUGHT_LEVEL)
            
            rsi_bullish = rsi is not None and rsi > 50
            rsi_bearish = rsi is not None and rsi < 50
            
            stoch_bullish = False
            stoch_bearish = False
            if stoch_k_prev is not None and stoch_d_prev is not None and stoch_k is not None and stoch_d is not None:
                stoch_bullish = (stoch_k_prev < stoch_d_prev and stoch_k > stoch_d and 
                                stoch_k < self.config.STOCH_OVERBOUGHT_LEVEL)
                stoch_bearish = (stoch_k_prev > stoch_d_prev and stoch_k < stoch_d and 
                                stoch_k > self.config.STOCH_OVERSOLD_LEVEL)
            
            volume_strong = True
            if volume is not None and volume_avg is not None:
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
                
                if signal_source == 'auto' and trend_strength < 0.3:
                    logger.info(f"Auto signal rejected - trend strength too weak: {trend_strength:.2f} ({trend_desc})")
                    return None
                
                try:
                    dynamic_tp_ratio = 1.45 + (trend_strength * 1.05)
                    dynamic_tp_ratio = min(max(dynamic_tp_ratio, 1.45), 2.50)
                    
                    if not (1.0 <= dynamic_tp_ratio <= 3.0):
                        logger.warning(f"Invalid TP ratio: {dynamic_tp_ratio}, using default 1.5")
                        dynamic_tp_ratio = 1.5
                    
                    atr = indicators.get('atr', 1.0)
                    if atr <= 0:
                        logger.warning(f"Invalid ATR: {atr}, using default 1.0")
                        atr = 1.0
                    
                    if signal_source == 'auto':
                        sl_distance = max(atr * self.config.SL_ATR_MULTIPLIER, self.config.DEFAULT_SL_PIPS / self.config.XAUUSD_PIP_VALUE)
                    else:
                        sl_distance = max(atr * 1.2, 1.0)
                    
                    if sl_distance <= 0 or sl_distance > 100:
                        logger.error(f"Invalid SL distance: {sl_distance}")
                        return None
                    
                    tp_distance = sl_distance * dynamic_tp_ratio
                    
                    if close is None or close <= 0:
                        logger.error(f"Invalid close price for SL/TP calculation: {close}")
                        return None
                    
                    if signal == 'BUY':
                        stop_loss = close - sl_distance
                        take_profit = close + tp_distance
                    else:
                        stop_loss = close + sl_distance
                        take_profit = close - tp_distance
                    
                    if stop_loss <= 0 or take_profit <= 0:
                        logger.error(f"Invalid SL/TP calculated: SL={stop_loss}, TP={take_profit}")
                        return None
                    
                    sl_pips = abs(stop_loss - close) * self.config.XAUUSD_PIP_VALUE
                    tp_pips = abs(take_profit - close) * self.config.XAUUSD_PIP_VALUE
                    
                    if sl_pips <= 0:
                        logger.error(f"Invalid SL pips: {sl_pips}")
                        return None
                    
                    lot_size = self.config.FIXED_RISK_AMOUNT / sl_pips if sl_pips > 0 else self.config.LOT_SIZE
                    lot_size = max(0.01, min(lot_size, 1.0))
                    
                    expected_loss = self.config.FIXED_RISK_AMOUNT
                    expected_profit = expected_loss * dynamic_tp_ratio
                    
                except (ValueError, ZeroDivisionError, OverflowError) as e:
                    logger.error(f"Calculation error in signal generation: {type(e).__name__}: {e}")
                    return None
                
                logger.info(f"{signal} signal detected ({signal_source}) on {timeframe}")
                logger.info(f"Trend Strength: {trend_desc} (score: {trend_strength:.2f})")
                logger.info(f"Dynamic TP Ratio: {dynamic_tp_ratio:.2f}x (Expected profit: ${expected_profit:.2f})")
                logger.info(f"Risk: ${expected_loss:.2f} | Reward: ${expected_profit:.2f} | R:R = 1:{dynamic_tp_ratio:.2f}")
                
                if close is None:
                    logger.error("Close price is None, cannot create signal")
                    return None
                
                return {
                    'signal': signal,
                    'signal_source': signal_source,
                    'entry_price': float(close),
                    'stop_loss': float(stop_loss),
                    'take_profit': float(take_profit),
                    'timeframe': timeframe,
                    'trend_strength': trend_strength,
                    'trend_description': trend_desc,
                    'expected_profit': expected_profit,
                    'expected_loss': expected_loss,
                    'rr_ratio': dynamic_tp_ratio,
                    'lot_size': lot_size,
                    'sl_pips': sl_pips,
                    'tp_pips': tp_pips,
                    'indicators': json.dumps({
                        'ema_short': float(ema_short) if ema_short is not None else None,
                        'ema_mid': float(ema_mid) if ema_mid is not None else None,
                        'ema_long': float(ema_long) if ema_long is not None else None,
                        'rsi': float(rsi) if rsi is not None else None,
                        'macd': float(macd) if macd is not None else None,
                        'macd_signal': float(macd_signal) if macd_signal is not None else None,
                        'macd_histogram': float(macd_histogram) if macd_histogram is not None else None,
                        'stoch_k': float(stoch_k) if stoch_k is not None else None,
                        'stoch_d': float(stoch_d) if stoch_d is not None else None,
                        'atr': float(atr) if atr is not None else None,
                        'volume': int(volume) if volume is not None else None,
                        'volume_avg': float(volume_avg) if volume_avg is not None else None
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
            
            entry = signal['entry_price']
            sl = signal['stop_loss']
            tp = signal['take_profit']
            signal_type = signal['signal']
            
            if entry <= 0 or sl <= 0 or tp <= 0:
                return False, f"Invalid prices: entry={entry}, sl={sl}, tp={tp}"
            
            if signal_type not in ['BUY', 'SELL']:
                return False, f"Invalid signal type: {signal_type}"
            
            try:
                spread_pips = current_spread * self.config.XAUUSD_PIP_VALUE
                
                if spread_pips < 0:
                    logger.warning(f"Negative spread: {spread_pips}, using 0")
                    spread_pips = 0
                
                if spread_pips > self.config.MAX_SPREAD_PIPS:
                    return False, f"Spread too high: {spread_pips:.2f} pips (max: {self.config.MAX_SPREAD_PIPS})"
            except Exception as e:
                logger.warning(f"Error calculating spread pips: {e}")
            
            sl_pips = abs(entry - sl) * self.config.XAUUSD_PIP_VALUE
            tp_pips = abs(entry - tp) * self.config.XAUUSD_PIP_VALUE
            
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
