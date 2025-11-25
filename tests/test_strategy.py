import pytest
from bot.strategy import TradingStrategy

@pytest.mark.unit
class TestTradingStrategy:
    
    def test_initialization(self, mock_config):
        strategy = TradingStrategy(mock_config)
        assert strategy.config == mock_config
    
    def test_calculate_trend_strength_strong_trend(self, mock_config, bullish_indicators):
        strategy = TradingStrategy(mock_config)
        
        strength, description = strategy.calculate_trend_strength(bullish_indicators)
        
        assert 0.0 <= strength <= 1.0
        assert isinstance(description, str)
        assert strength > 0.5
    
    def test_calculate_trend_strength_weak_trend(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        weak_indicators = {
            'ema_5': 2650.5,
            'ema_10': 2650.4,
            'ema_20': 2650.3,
            'macd_histogram': 0.1,
            'rsi': 50.0,
            'volume': 100,
            'volume_avg': 100,
            'close': 2650.0
        }
        
        strength, description = strategy.calculate_trend_strength(weak_indicators)
        
        assert 0.0 <= strength <= 1.0
        assert strength < 0.5
        assert 'LEMAH' in description or 'MEDIUM' in description
    
    def test_calculate_trend_strength_descriptions(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        very_strong = {
            'ema_5': 2660.0,
            'ema_10': 2655.0,
            'ema_20': 2650.0,
            'macd_histogram': 1.5,
            'rsi': 75.0,
            'volume': 300,
            'volume_avg': 100,
            'close': 2660.0
        }
        
        strength, description = strategy.calculate_trend_strength(very_strong)
        
        if strength >= 0.75:
            assert 'SANGAT KUAT' in description
        elif strength >= 0.5:
            assert 'KUAT' in description
    
    def test_calculate_trend_strength_missing_indicators(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        incomplete_indicators = {
            'ema_5': 2650.0,
            'close': 2650.0
        }
        
        strength, description = strategy.calculate_trend_strength(incomplete_indicators)
        
        assert 0.0 <= strength <= 1.0
        assert isinstance(description, str)
    
    def test_calculate_trend_strength_zero_close(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        zero_close = {
            'ema_5': 2650.0,
            'ema_10': 2650.0,
            'ema_20': 2650.0,
            'close': 0.0
        }
        
        strength, description = strategy.calculate_trend_strength(zero_close)
        
        assert 0.0 <= strength <= 1.0
    
    def test_detect_signal_empty_indicators(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        signal = strategy.detect_signal({})
        
        assert signal is None
    
    def test_detect_signal_missing_required_indicators(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        incomplete = {
            'ema_5': 2650.0,
            'rsi': 55.0
        }
        
        signal = strategy.detect_signal(incomplete)
        
        assert signal is None
    
    def test_detect_signal_bullish_auto_mode(self, mock_config, bullish_indicators):
        strategy = TradingStrategy(mock_config)
        
        signal = strategy.detect_signal(bullish_indicators, timeframe='M1', signal_source='auto')
        
        if signal is not None:
            assert signal['signal'] in ['BUY', 'SELL']
            assert 'entry_price' in signal
            assert 'stop_loss' in signal
            assert 'take_profit' in signal
            assert 'trend_strength' in signal
            assert 'timeframe' in signal
    
    def test_detect_signal_bearish_auto_mode(self, mock_config, bearish_indicators):
        strategy = TradingStrategy(mock_config)
        
        signal = strategy.detect_signal(bearish_indicators, timeframe='M1', signal_source='auto')
        
        if signal is not None:
            assert signal['signal'] in ['BUY', 'SELL']
            assert 'entry_price' in signal
            assert 'stop_loss' in signal
            assert 'take_profit' in signal
    
    def test_detect_signal_manual_buy(self, mock_config, sample_indicators):
        strategy = TradingStrategy(mock_config)
        
        signal = strategy.detect_signal(
            sample_indicators, 
            timeframe='M1', 
            signal_source='manual_buy'
        )
        
        assert signal is not None
        assert signal['signal'] == 'BUY'
        assert signal['signal_source'] == 'manual_buy'
        assert signal['entry_price'] == sample_indicators['close']
    
    def test_detect_signal_manual_sell(self, mock_config, bearish_indicators):
        strategy = TradingStrategy(mock_config)
        
        signal = strategy.detect_signal(
            bearish_indicators, 
            timeframe='M1', 
            signal_source='manual_sell'
        )
        
        assert signal is not None
        assert signal['signal'] == 'SELL'
        assert signal['signal_source'] == 'manual_sell'
        assert signal['entry_price'] == bearish_indicators['close']
    
    def test_detect_signal_structure(self, mock_config, bullish_indicators):
        strategy = TradingStrategy(mock_config)
        
        signal = strategy.detect_signal(bullish_indicators)
        
        if signal is not None:
            required_keys = [
                'signal', 'entry_price', 'stop_loss', 'take_profit',
                'trend_strength', 'timeframe', 'indicators', 'signal_source',
                'rr_ratio', 'lot_size', 'sl_pips', 'tp_pips', 'expected_profit', 'expected_loss'
            ]
            
            for key in required_keys:
                assert key in signal
    
    def test_detect_signal_stop_loss_calculation(self, mock_config, sample_indicators):
        strategy = TradingStrategy(mock_config)
        
        signal = strategy.detect_signal(
            sample_indicators,
            signal_source='manual_buy'
        )
        
        assert signal is not None
        
        if signal['signal'] == 'BUY':
            assert signal['stop_loss'] < signal['entry_price']
        else:
            assert signal['stop_loss'] > signal['entry_price']
    
    def test_detect_signal_take_profit_calculation(self, mock_config, sample_indicators):
        strategy = TradingStrategy(mock_config)
        
        signal = strategy.detect_signal(
            sample_indicators,
            signal_source='manual_buy'
        )
        
        assert signal is not None
        
        if signal['signal'] == 'BUY':
            assert signal['take_profit'] > signal['entry_price']
        else:
            assert signal['take_profit'] < signal['entry_price']
    
    def test_detect_signal_risk_reward_ratio(self, mock_config, sample_indicators):
        strategy = TradingStrategy(mock_config)
        
        signal = strategy.detect_signal(
            sample_indicators,
            signal_source='manual_buy'
        )
        
        assert signal is not None
        
        sl_distance = abs(signal['entry_price'] - signal['stop_loss'])
        tp_distance = abs(signal['take_profit'] - signal['entry_price'])
        
        actual_rr = tp_distance / sl_distance if sl_distance > 0 else 0
        
        assert actual_rr > 0
    
    def test_detect_signal_trend_strength_range(self, mock_config, bullish_indicators):
        strategy = TradingStrategy(mock_config)
        
        signal = strategy.detect_signal(bullish_indicators)
        
        if signal is not None:
            assert 0.0 <= signal['trend_strength'] <= 1.0
    
    def test_detect_signal_timeframe_propagation(self, mock_config, sample_indicators):
        strategy = TradingStrategy(mock_config)
        
        signal = strategy.detect_signal(
            sample_indicators,
            timeframe='M5',
            signal_source='manual_buy'
        )
        
        assert signal is not None
        assert signal['timeframe'] == 'M5'
    
    def test_detect_signal_indicators_included(self, mock_config, sample_indicators):
        strategy = TradingStrategy(mock_config)
        
        signal = strategy.detect_signal(
            sample_indicators,
            signal_source='manual_buy'
        )
        
        assert signal is not None
        assert 'indicators' in signal
        assert isinstance(signal['indicators'], str)
        import json
        indicators_dict = json.loads(signal['indicators'])
        assert isinstance(indicators_dict, dict)
    
    @pytest.mark.parametrize("signal_source", ['auto', 'manual_buy', 'manual_sell'])
    def test_detect_signal_different_sources(self, mock_config, sample_indicators, signal_source):
        strategy = TradingStrategy(mock_config)
        
        signal = strategy.detect_signal(
            sample_indicators,
            signal_source=signal_source
        )
        
        if signal is not None:
            assert signal['signal_source'] in ['auto', 'manual_buy', 'manual_sell']
    
    def test_detect_signal_neutral_market(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        neutral_indicators = {
            'ema_5': 2650.0,
            'ema_10': 2650.0,
            'ema_20': 2650.0,
            'rsi': 50.0,
            'rsi_prev': 50.0,
            'stoch_k': 50.0,
            'stoch_d': 50.0,
            'stoch_k_prev': 50.0,
            'stoch_d_prev': 50.0,
            'macd': 0.0,
            'macd_signal': 0.0,
            'macd_histogram': 0.0,
            'macd_prev': 0.0,
            'macd_signal_prev': 0.0,
            'atr': 2.0,
            'volume': 100,
            'volume_avg': 100,
            'close': 2650.0,
            'high': 2651.0,
            'low': 2649.0
        }
        
        signal = strategy.detect_signal(neutral_indicators, signal_source='auto')
        
        assert signal is None or signal['trend_strength'] < 0.7
    
    def test_calculate_trend_strength_volume_factor(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        high_volume = {
            'ema_5': 2655.0,
            'ema_10': 2653.0,
            'ema_20': 2650.0,
            'macd_histogram': 0.5,
            'rsi': 60.0,
            'volume': 300,
            'volume_avg': 100,
            'close': 2655.0
        }
        
        low_volume = {
            'ema_5': 2655.0,
            'ema_10': 2653.0,
            'ema_20': 2650.0,
            'macd_histogram': 0.5,
            'rsi': 60.0,
            'volume': 80,
            'volume_avg': 100,
            'close': 2655.0
        }
        
        strength_high, _ = strategy.calculate_trend_strength(high_volume)
        strength_low, _ = strategy.calculate_trend_strength(low_volume)
        
        assert strength_high >= strength_low
    
    def test_auto_signal_exact_buy_generation(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        perfect_buy_indicators = {
            'ema_5': 2660.0,
            'ema_10': 2655.0,
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
            'volume': 250,
            'volume_avg': 120,
            'close': 2660.0,
            'high': 2661.0,
            'low': 2658.0
        }
        
        signal = strategy.detect_signal(perfect_buy_indicators, timeframe='M1', signal_source='auto')
        
        assert signal is not None
        assert signal['signal'] == 'BUY'
        assert signal['signal_source'] == 'auto'
        assert signal['entry_price'] == 2660.0
        assert signal['stop_loss'] < signal['entry_price']
        assert signal['take_profit'] > signal['entry_price']
        assert signal['sl_pips'] > 0
        assert signal['tp_pips'] > 0
        assert signal['rr_ratio'] >= 1.45
        assert 0.0 <= signal['trend_strength'] <= 1.0
    
    def test_auto_signal_exact_sell_generation(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        perfect_sell_indicators = {
            'ema_5': 2640.0,
            'ema_10': 2645.0,
            'ema_20': 2650.0,
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
            'volume': 250,
            'volume_avg': 120,
            'close': 2640.0,
            'high': 2642.0,
            'low': 2638.0
        }
        
        signal = strategy.detect_signal(perfect_sell_indicators, timeframe='M1', signal_source='auto')
        
        assert signal is not None
        assert signal['signal'] == 'SELL'
        assert signal['signal_source'] == 'auto'
        assert signal['entry_price'] == 2640.0
        assert signal['stop_loss'] > signal['entry_price']
        assert signal['take_profit'] < signal['entry_price']
        assert signal['sl_pips'] > 0
        assert signal['tp_pips'] > 0
        assert signal['rr_ratio'] >= 1.45
    
    def test_auto_signal_strict_contract_insufficient_score(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        weak_indicators = {
            'ema_5': 2650.1,
            'ema_10': 2650.2,
            'ema_20': 2650.0,
            'rsi': 49.0,
            'rsi_prev': 49.5,
            'stoch_k': 48.0,
            'stoch_d': 49.0,
            'stoch_k_prev': 49.0,
            'stoch_d_prev': 50.0,
            'macd': -0.05,
            'macd_signal': -0.03,
            'macd_histogram': -0.02,
            'macd_prev': -0.06,
            'macd_signal_prev': -0.04,
            'atr': 2.0,
            'volume': 50,
            'volume_avg': 120,
            'close': 2650.1,
            'high': 2650.3,
            'low': 2649.9
        }
        
        signal = strategy.detect_signal(weak_indicators, timeframe='M1', signal_source='auto')
        
        assert signal is None
    
    def test_manual_signal_relaxed_contract_buy(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        relaxed_buy_indicators = {
            'ema_5': 2653.0,
            'ema_10': 2651.0,
            'ema_20': 2650.0,
            'rsi': 55.0,
            'rsi_prev': 52.0,
            'stoch_k': 60.0,
            'stoch_d': 58.0,
            'stoch_k_prev': 55.0,
            'stoch_d_prev': 54.0,
            'macd': 0.3,
            'macd_signal': 0.2,
            'macd_histogram': 0.1,
            'macd_prev': 0.2,
            'macd_signal_prev': 0.25,
            'atr': 2.5,
            'volume': 130,
            'volume_avg': 120,
            'close': 2653.0,
            'high': 2654.0,
            'low': 2652.0
        }
        
        signal = strategy.detect_signal(relaxed_buy_indicators, timeframe='M1', signal_source='manual_buy')
        
        assert signal is not None
        assert signal['signal'] == 'BUY'
        assert signal['signal_source'] == 'manual_buy'
    
    def test_manual_signal_relaxed_contract_sell(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        relaxed_sell_indicators = {
            'ema_5': 2647.0,
            'ema_10': 2649.0,
            'ema_20': 2650.0,
            'rsi': 45.0,
            'rsi_prev': 48.0,
            'stoch_k': 40.0,
            'stoch_d': 42.0,
            'stoch_k_prev': 45.0,
            'stoch_d_prev': 46.0,
            'macd': -0.3,
            'macd_signal': -0.2,
            'macd_histogram': -0.1,
            'macd_prev': -0.2,
            'macd_signal_prev': -0.25,
            'atr': 2.5,
            'volume': 130,
            'volume_avg': 120,
            'close': 2647.0,
            'high': 2648.0,
            'low': 2646.0
        }
        
        signal = strategy.detect_signal(relaxed_sell_indicators, timeframe='M1', signal_source='manual_sell')
        
        assert signal is not None
        assert signal['signal'] == 'SELL'
        assert signal['signal_source'] == 'manual_sell'
    
    def test_validate_signal_spread_too_wide(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        signal = {
            'entry_price': 2650.0,
            'stop_loss': 2640.0,
            'take_profit': 2670.0
        }
        
        wide_spread = 1.5
        
        is_valid, reason = strategy.validate_signal(signal, wide_spread)
        
        assert is_valid is False
        assert 'Spread too high' in reason
    
    def test_validate_signal_spread_acceptable(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        signal = {
            'entry_price': 2650.0,
            'stop_loss': 2640.0,
            'take_profit': 2670.0
        }
        
        acceptable_spread = 0.5
        
        is_valid, reason = strategy.validate_signal(signal, acceptable_spread)
        
        assert is_valid is True
        assert reason is None
    
    def test_validate_signal_sl_too_tight(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        signal = {
            'entry_price': 2650.0,
            'stop_loss': 2649.95,
            'take_profit': 2670.0
        }
        
        is_valid, reason = strategy.validate_signal(signal, 0.3)
        
        assert is_valid is False
        assert 'too tight' in reason.lower()
    
    def test_validate_signal_tp_too_tight(self, mock_config):
        strategy = TradingStrategy(mock_config)
        
        signal = {
            'entry_price': 2650.0,
            'stop_loss': 2640.0,
            'take_profit': 2650.05
        }
        
        is_valid, reason = strategy.validate_signal(signal, 0.3)
        
        assert is_valid is False
        assert 'too tight' in reason.lower()
    
    def test_signal_complete_structure_validation(self, mock_config, bullish_indicators):
        strategy = TradingStrategy(mock_config)
        
        signal = strategy.detect_signal(bullish_indicators, timeframe='M1', signal_source='auto')
        
        if signal is not None:
            assert isinstance(signal['signal'], str)
            assert signal['signal'] in ['BUY', 'SELL']
            assert isinstance(signal['entry_price'], float)
            assert isinstance(signal['stop_loss'], float)
            assert isinstance(signal['take_profit'], float)
            assert isinstance(signal['timeframe'], str)
            assert isinstance(signal['trend_strength'], float)
            assert isinstance(signal['rr_ratio'], float)
            assert isinstance(signal['lot_size'], float)
            assert isinstance(signal['sl_pips'], float)
            assert isinstance(signal['tp_pips'], float)
            assert isinstance(signal['expected_profit'], float)
            assert isinstance(signal['expected_loss'], float)
            assert isinstance(signal['indicators'], str)
            assert isinstance(signal['confidence_reasons'], list)
