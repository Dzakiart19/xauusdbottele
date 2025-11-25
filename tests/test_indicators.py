import pytest
import pandas as pd
import numpy as np
from bot.indicators import IndicatorEngine

@pytest.mark.unit
class TestIndicatorEngine:
    
    def test_initialization(self, mock_config):
        engine = IndicatorEngine(mock_config)
        assert engine.ema_periods == [5, 10, 20]
        assert engine.rsi_period == 14
        assert engine.atr_period == 14
        assert engine.macd_fast == 12
        assert engine.macd_slow == 26
        assert engine.macd_signal == 9
    
    def test_calculate_ema(self, mock_config, sample_ohlc_data):
        engine = IndicatorEngine(mock_config)
        ema = engine.calculate_ema(sample_ohlc_data, 10)
        
        assert isinstance(ema, pd.Series)
        assert len(ema) == len(sample_ohlc_data)
        assert not ema.iloc[-1] != ema.iloc[-1]
        assert ema.iloc[-1] > 0
    
    def test_calculate_ema_different_periods(self, mock_config, sample_ohlc_data):
        engine = IndicatorEngine(mock_config)
        ema_5 = engine.calculate_ema(sample_ohlc_data, 5)
        ema_20 = engine.calculate_ema(sample_ohlc_data, 20)
        
        assert len(ema_5) == len(ema_20)
        assert ema_5.iloc[-10:].std() >= ema_20.iloc[-10:].std()
    
    def test_calculate_rsi(self, mock_config, sample_ohlc_data):
        engine = IndicatorEngine(mock_config)
        rsi = engine.calculate_rsi(sample_ohlc_data, 14)
        
        assert isinstance(rsi, pd.Series)
        assert len(rsi) == len(sample_ohlc_data)
        
        valid_rsi = rsi.dropna()
        assert (valid_rsi >= 0).all()
        assert (valid_rsi <= 100).all()
    
    def test_calculate_rsi_bounds(self, mock_config):
        engine = IndicatorEngine(mock_config)
        
        dates = pd.date_range(start='2024-01-01', periods=50, freq='1min')
        uptrend_data = pd.DataFrame({
            'close': np.linspace(2600, 2700, 50),
            'high': np.linspace(2601, 2701, 50),
            'low': np.linspace(2599, 2699, 50),
            'open': np.linspace(2600, 2700, 50),
            'volume': [100] * 50
        }, index=dates)
        
        rsi = engine.calculate_rsi(uptrend_data, 14)
        assert rsi.iloc[-1] > 50
    
    def test_calculate_stochastic(self, mock_config, sample_ohlc_data):
        engine = IndicatorEngine(mock_config)
        stoch_k, stoch_d = engine.calculate_stochastic(
            sample_ohlc_data, 14, 3, 3
        )
        
        assert isinstance(stoch_k, pd.Series)
        assert isinstance(stoch_d, pd.Series)
        assert len(stoch_k) == len(sample_ohlc_data)
        assert len(stoch_d) == len(sample_ohlc_data)
        
        valid_k = stoch_k.dropna()
        valid_d = stoch_d.dropna()
        assert (valid_k >= 0).all()
        assert (valid_k <= 100).all()
        assert (valid_d >= 0).all()
        assert (valid_d <= 100).all()
    
    def test_calculate_atr(self, mock_config, sample_ohlc_data):
        engine = IndicatorEngine(mock_config)
        atr = engine.calculate_atr(sample_ohlc_data, 14)
        
        assert isinstance(atr, pd.Series)
        assert len(atr) == len(sample_ohlc_data)
        
        valid_atr = atr.dropna()
        assert (valid_atr >= 0).all()
    
    def test_calculate_atr_volatility(self, mock_config):
        engine = IndicatorEngine(mock_config)
        
        dates = pd.date_range(start='2024-01-01', periods=50, freq='1min')
        
        low_volatility = pd.DataFrame({
            'close': [2650.0] * 50,
            'high': [2650.5] * 50,
            'low': [2649.5] * 50,
            'open': [2650.0] * 50,
            'volume': [100] * 50
        }, index=dates)
        
        high_volatility = pd.DataFrame({
            'close': 2650 + np.random.randn(50) * 10,
            'high': 2650 + np.random.randn(50) * 10 + 5,
            'low': 2650 + np.random.randn(50) * 10 - 5,
            'open': 2650 + np.random.randn(50) * 10,
            'volume': [100] * 50
        }, index=dates)
        
        atr_low = engine.calculate_atr(low_volatility, 14)
        atr_high = engine.calculate_atr(high_volatility, 14)
        
        assert atr_high.iloc[-1] > atr_low.iloc[-1]
    
    def test_calculate_volume_average(self, mock_config, sample_ohlc_data):
        engine = IndicatorEngine(mock_config)
        vol_avg = engine.calculate_volume_average(sample_ohlc_data, 20)
        
        assert isinstance(vol_avg, pd.Series)
        assert len(vol_avg) == len(sample_ohlc_data)
        assert (vol_avg.dropna() > 0).all()
    
    def test_calculate_macd(self, mock_config, sample_ohlc_data):
        engine = IndicatorEngine(mock_config)
        macd_line, macd_signal, macd_histogram = engine.calculate_macd(
            sample_ohlc_data, 12, 26, 9
        )
        
        assert isinstance(macd_line, pd.Series)
        assert isinstance(macd_signal, pd.Series)
        assert isinstance(macd_histogram, pd.Series)
        assert len(macd_line) == len(sample_ohlc_data)
        
        np.testing.assert_array_almost_equal(
            macd_histogram.values,
            (macd_line - macd_signal).values,
            decimal=5
        )
    
    def test_get_indicators_insufficient_data(self, mock_config):
        engine = IndicatorEngine(mock_config)
        
        dates = pd.date_range(start='2024-01-01', periods=10, freq='1min')
        small_df = pd.DataFrame({
            'close': [2650.0] * 10,
            'high': [2651.0] * 10,
            'low': [2649.0] * 10,
            'open': [2650.0] * 10,
            'volume': [100] * 10
        }, index=dates)
        
        result = engine.get_indicators(small_df)
        assert result is None
    
    def test_get_indicators_complete(self, mock_config, sample_ohlc_data):
        engine = IndicatorEngine(mock_config)
        indicators = engine.get_indicators(sample_ohlc_data)
        
        assert indicators is not None
        assert isinstance(indicators, dict)
        
        expected_keys = [
            'ema_5', 'ema_10', 'ema_20',
            'rsi', 'rsi_prev',
            'stoch_k', 'stoch_d', 'stoch_k_prev', 'stoch_d_prev',
            'atr',
            'macd', 'macd_signal', 'macd_histogram', 'macd_prev', 'macd_signal_prev',
            'volume', 'volume_avg',
            'close', 'high', 'low'
        ]
        
        for key in expected_keys:
            assert key in indicators, f"Missing indicator: {key}"
            assert indicators[key] is not None
            assert not pd.isna(indicators[key])
    
    def test_get_indicators_values_range(self, mock_config, sample_ohlc_data):
        engine = IndicatorEngine(mock_config)
        indicators = engine.get_indicators(sample_ohlc_data)
        
        assert 0 <= indicators['rsi'] <= 100
        assert 0 <= indicators['rsi_prev'] <= 100
        assert 0 <= indicators['stoch_k'] <= 100
        assert 0 <= indicators['stoch_d'] <= 100
        assert indicators['atr'] >= 0
        assert indicators['volume'] >= 0
        assert indicators['volume_avg'] >= 0
    
    def test_ema_ordering(self, mock_config):
        engine = IndicatorEngine(mock_config)
        
        dates = pd.date_range(start='2024-01-01', periods=100, freq='1min')
        uptrend_data = pd.DataFrame({
            'close': np.linspace(2600, 2700, 100),
            'high': np.linspace(2601, 2701, 100),
            'low': np.linspace(2599, 2699, 100),
            'open': np.linspace(2600, 2700, 100),
            'volume': [100] * 100
        }, index=dates)
        
        indicators = engine.get_indicators(uptrend_data)
        
        assert indicators['ema_5'] > indicators['ema_10']
        assert indicators['ema_10'] > indicators['ema_20']
    
    def test_indicators_consistency(self, mock_config, sample_ohlc_data):
        engine = IndicatorEngine(mock_config)
        
        indicators1 = engine.get_indicators(sample_ohlc_data)
        indicators2 = engine.get_indicators(sample_ohlc_data)
        
        for key in indicators1:
            if not pd.isna(indicators1[key]) and not pd.isna(indicators2[key]):
                assert indicators1[key] == indicators2[key]
    
    @pytest.mark.parametrize("period", [5, 10, 20, 50])
    def test_ema_different_periods(self, mock_config, sample_ohlc_data, period):
        engine = IndicatorEngine(mock_config)
        ema = engine.calculate_ema(sample_ohlc_data, period)
        
        assert not ema.iloc[-1] != ema.iloc[-1]
        assert ema.iloc[-1] > 0
    
    @pytest.mark.parametrize("period", [7, 14, 21])
    def test_rsi_different_periods(self, mock_config, sample_ohlc_data, period):
        engine = IndicatorEngine(mock_config)
        rsi = engine.calculate_rsi(sample_ohlc_data, period)
        
        valid_rsi = rsi.dropna()
        assert (valid_rsi >= 0).all()
        assert (valid_rsi <= 100).all()
