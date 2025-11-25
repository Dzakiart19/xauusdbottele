import pandas as pd
import numpy as np
from typing import Dict, Optional

class IndicatorEngine:
    def __init__(self, config):
        self.config = config
        self.ema_periods = config.EMA_PERIODS
        self.ema_periods_long = config.EMA_PERIODS_LONG
        self.rsi_period = config.RSI_PERIOD
        self.stoch_k_period = config.STOCH_K_PERIOD
        self.stoch_d_period = config.STOCH_D_PERIOD
        self.stoch_smooth_k = config.STOCH_SMOOTH_K
        self.atr_period = config.ATR_PERIOD
        self.macd_fast = config.MACD_FAST
        self.macd_slow = config.MACD_SLOW
        self.macd_signal = config.MACD_SIGNAL
    
    def calculate_ema(self, df: pd.DataFrame, period: int) -> pd.Series:
        return df['close'].ewm(span=period, adjust=False).mean()
    
    def calculate_rsi(self, df: pd.DataFrame, period: int) -> pd.Series:
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_stochastic(self, df: pd.DataFrame, k_period: int, d_period: int, smooth_k: int) -> tuple:
        low_min = df['low'].rolling(window=k_period).min()
        high_max = df['high'].rolling(window=k_period).max()
        
        stoch_k = 100 * (df['close'] - low_min) / (high_max - low_min)
        stoch_k = stoch_k.rolling(window=smooth_k).mean()
        stoch_d = stoch_k.rolling(window=d_period).mean()
        
        return stoch_k, stoch_d
    
    def calculate_atr(self, df: pd.DataFrame, period: int) -> pd.Series:
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift())
        low_close = np.abs(df['low'] - df['close'].shift())
        
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()
        
        return atr
    
    def calculate_volume_average(self, df: pd.DataFrame, period: int = 20) -> pd.Series:
        return df['volume'].rolling(window=period).mean()
    
    def calculate_twin_range_filter(self, df: pd.DataFrame, period: int = 27, multiplier: float = 2.0) -> tuple:
        """
        Twin Range Filter - Indikator untuk filter trend menggunakan smooth range
        
        Args:
            df: DataFrame dengan kolom OHLC
            period: Periode untuk smoothing (default 27)
            multiplier: Multiplier untuk range calculation (default 2.0)
        
        Returns:
            tuple: (upper_filter, lower_filter, trend_direction)
                   trend_direction: 1 untuk bullish, -1 untuk bearish, 0 untuk neutral
        """
        close = df['close']
        high = df['high']
        low = df['low']
        
        range_val = high - low
        smooth_range = range_val.ewm(span=period, adjust=False).mean()
        
        range_filter = smooth_range * multiplier
        
        upper_filter = close + range_filter
        lower_filter = close - range_filter
        
        upper_filter = upper_filter.ewm(span=period, adjust=False).mean()
        lower_filter = lower_filter.ewm(span=period, adjust=False).mean()
        
        trend = pd.Series(0, index=df.index)
        for i in range(1, len(df)):
            if close.iloc[i] > upper_filter.iloc[i-1]:
                trend.iloc[i] = 1
            elif close.iloc[i] < lower_filter.iloc[i-1]:
                trend.iloc[i] = -1
            else:
                trend.iloc[i] = trend.iloc[i-1]
        
        return upper_filter, lower_filter, trend
    
    def calculate_market_bias_cerebr(self, df: pd.DataFrame, period: int = 60, smoothing: int = 10) -> tuple:
        """
        Market Bias (CEREBR) - Indikator untuk deteksi bias pasar
        
        Args:
            df: DataFrame dengan kolom OHLC
            period: Periode untuk CEREBR calculation (default 60)
            smoothing: Periode smoothing (default 10)
        
        Returns:
            tuple: (cerebr_value, cerebr_signal, bias_direction)
                   cerebr_value: Nilai CEREBR
                   cerebr_signal: Signal line (smoothed)
                   bias_direction: 1 untuk bullish bias, -1 untuk bearish bias, 0 untuk neutral
        """
        close = df['close']
        high = df['high']
        low = df['low']
        
        high_period = high.rolling(window=period).max()
        low_period = low.rolling(window=period).min()
        
        range_period = high_period - low_period
        range_period = range_period.replace(0, 1)
        
        cerebr_raw = ((close - low_period) / range_period) * 100
        
        cerebr_value = cerebr_raw.ewm(span=smoothing, adjust=False).mean()
        cerebr_signal = cerebr_value.ewm(span=smoothing, adjust=False).mean()
        
        bias_direction = pd.Series(0, index=df.index)
        for i in range(len(df)):
            if cerebr_value.iloc[i] > 60:
                bias_direction.iloc[i] = 1
            elif cerebr_value.iloc[i] < 40:
                bias_direction.iloc[i] = -1
            else:
                bias_direction.iloc[i] = 0
        
        return cerebr_value, cerebr_signal, bias_direction
    
    def calculate_macd(self, df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple:
        ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        macd_signal = macd_line.ewm(span=signal, adjust=False).mean()
        macd_histogram = macd_line - macd_signal
        return macd_line, macd_signal, macd_histogram
    
    def get_indicators(self, df: pd.DataFrame) -> Optional[Dict]:
        all_periods = self.ema_periods + self.ema_periods_long
        min_required = max(30, max(all_periods + [self.rsi_period, self.stoch_k_period, self.atr_period]) + 10)
        if len(df) < min_required:
            return None
        
        indicators = {}
        
        for period in self.ema_periods:
            indicators[f'ema_{period}'] = self.calculate_ema(df, period).iloc[-1]
        
        for period in self.ema_periods_long:
            indicators[f'ema_{period}'] = self.calculate_ema(df, period).iloc[-1]
        
        rsi_series = self.calculate_rsi(df, self.rsi_period)
        indicators['rsi'] = rsi_series.iloc[-1]
        indicators['rsi_prev'] = rsi_series.iloc[-2]
        indicators['rsi_history'] = rsi_series.tail(20).tolist()
        
        stoch_k, stoch_d = self.calculate_stochastic(
            df, self.stoch_k_period, self.stoch_d_period, self.stoch_smooth_k
        )
        indicators['stoch_k'] = stoch_k.iloc[-1]
        indicators['stoch_d'] = stoch_d.iloc[-1]
        indicators['stoch_k_prev'] = stoch_k.iloc[-2]
        indicators['stoch_d_prev'] = stoch_d.iloc[-2]
        
        indicators['atr'] = self.calculate_atr(df, self.atr_period).iloc[-1]
        
        macd_line, macd_signal, macd_histogram = self.calculate_macd(
            df, self.macd_fast, self.macd_slow, self.macd_signal
        )
        indicators['macd'] = macd_line.iloc[-1]
        indicators['macd_signal'] = macd_signal.iloc[-1]
        indicators['macd_histogram'] = macd_histogram.iloc[-1]
        indicators['macd_prev'] = macd_line.iloc[-2]
        indicators['macd_signal_prev'] = macd_signal.iloc[-2]
        
        indicators['volume'] = df['volume'].iloc[-1]
        indicators['volume_avg'] = self.calculate_volume_average(df).iloc[-1]
        
        trf_upper, trf_lower, trf_trend = self.calculate_twin_range_filter(df, period=27, multiplier=2.0)
        indicators['trf_upper'] = trf_upper.iloc[-1]
        indicators['trf_lower'] = trf_lower.iloc[-1]
        indicators['trf_trend'] = trf_trend.iloc[-1]
        indicators['trf_trend_prev'] = trf_trend.iloc[-2] if len(trf_trend) > 1 else 0
        
        cerebr_value, cerebr_signal, cerebr_bias = self.calculate_market_bias_cerebr(df, period=60, smoothing=10)
        indicators['cerebr_value'] = cerebr_value.iloc[-1]
        indicators['cerebr_signal'] = cerebr_signal.iloc[-1]
        indicators['cerebr_bias'] = cerebr_bias.iloc[-1]
        indicators['cerebr_bias_prev'] = cerebr_bias.iloc[-2] if len(cerebr_bias) > 1 else 0
        
        indicators['close'] = df['close'].iloc[-1]
        indicators['high'] = df['high'].iloc[-1]
        indicators['low'] = df['low'].iloc[-1]
        
        return indicators
