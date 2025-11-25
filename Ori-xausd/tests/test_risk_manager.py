import pytest
from datetime import datetime, timedelta
import pytz
from bot.risk_manager import RiskManager
from bot.database import Trade

@pytest.mark.unit
class TestRiskManager:
    
    def test_initialization(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        assert risk_manager.config == mock_config
        assert risk_manager.db == mock_db_manager
        assert isinstance(risk_manager.last_signal_time, dict)
        assert isinstance(risk_manager.daily_stats, dict)
    
    def test_can_trade_no_cooldown(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        can_trade, reason = risk_manager.can_trade(123456, 'BUY')
        
        assert can_trade is True
        assert reason is None
    
    def test_can_trade_cooldown_active(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        risk_manager.record_signal(123456)
        can_trade, reason = risk_manager.can_trade(123456, 'BUY')
        
        assert can_trade is False
        assert 'Cooldown aktif' in reason
    
    def test_can_trade_cooldown_expired(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        past_time = datetime.now(pytz.UTC) - timedelta(seconds=35)
        risk_manager.last_signal_time[123456] = past_time
        
        can_trade, reason = risk_manager.can_trade(123456, 'BUY')
        
        assert can_trade is True
        assert reason is None
    
    def test_can_trade_daily_loss_limit_not_reached(self, mock_config, test_db):
        from unittest.mock import Mock
        db_manager = Mock()
        db_manager.get_session = Mock(return_value=test_db)
        
        risk_manager = RiskManager(mock_config, db_manager)
        
        utc_now = datetime.now(pytz.UTC)
        
        trade1 = Trade(
            user_id=123456,
            ticker='XAUUSD',
            signal_type='BUY',
            entry_price=2650.0,
            stop_loss=2640.0,
            take_profit=2670.0,
            actual_pl=-0.5,
            signal_time=utc_now
        )
        test_db.add(trade1)
        test_db.commit()
        
        can_trade, reason = risk_manager.can_trade(123456, 'BUY')
        
        assert can_trade is True
        assert reason is None
    
    def test_can_trade_daily_loss_limit_reached(self, mock_config, test_db):
        from unittest.mock import Mock
        db_manager = Mock()
        db_manager.get_session = Mock(return_value=test_db)
        
        risk_manager = RiskManager(mock_config, db_manager)
        
        utc_now = datetime.now(pytz.UTC)
        
        trade1 = Trade(
            user_id=123456,
            ticker='XAUUSD',
            signal_type='BUY',
            entry_price=2650.0,
            stop_loss=2640.0,
            take_profit=2670.0,
            actual_pl=-3.5,
            signal_time=utc_now
        )
        test_db.add(trade1)
        test_db.commit()
        
        can_trade, reason = risk_manager.can_trade(123456, 'BUY')
        
        assert can_trade is False
        assert 'Batas kerugian harian' in reason
    
    def test_can_trade_different_users(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        risk_manager.record_signal(123456)
        
        can_trade_1, _ = risk_manager.can_trade(123456, 'BUY')
        can_trade_2, _ = risk_manager.can_trade(789012, 'BUY')
        
        assert can_trade_1 is False
        assert can_trade_2 is True
    
    def test_record_signal(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        before = datetime.now(pytz.UTC)
        risk_manager.record_signal(123456)
        after = datetime.now(pytz.UTC)
        
        assert 123456 in risk_manager.last_signal_time
        recorded_time = risk_manager.last_signal_time[123456]
        assert before <= recorded_time <= after
    
    def test_calculate_position_size_basic(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        account_balance = 1000.0
        entry_price = 2650.0
        stop_loss = 2640.0
        
        lot_size = risk_manager.calculate_position_size(
            account_balance, entry_price, stop_loss, 'BUY'
        )
        
        assert isinstance(lot_size, float)
        assert 0.01 <= lot_size <= 1.0
    
    def test_calculate_position_size_buy_signal(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        lot_size = risk_manager.calculate_position_size(
            1000.0, 2650.0, 2640.0, 'BUY'
        )
        
        assert lot_size > 0
    
    def test_calculate_position_size_sell_signal(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        lot_size = risk_manager.calculate_position_size(
            1000.0, 2650.0, 2660.0, 'SELL'
        )
        
        assert lot_size > 0
    
    def test_calculate_position_size_bounds(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        very_tight_sl = risk_manager.calculate_position_size(
            1000.0, 2650.0, 2649.0, 'BUY'
        )
        assert very_tight_sl <= 1.0
        
        very_wide_sl = risk_manager.calculate_position_size(
            1000.0, 2650.0, 2500.0, 'BUY'
        )
        assert very_wide_sl >= 0.01
    
    def test_calculate_position_size_zero_risk(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        lot_size = risk_manager.calculate_position_size(
            1000.0, 2650.0, 2650.0, 'BUY'
        )
        
        assert lot_size == mock_config.LOT_SIZE
    
    def test_calculate_pl_buy_profit(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        pl = risk_manager.calculate_pl(
            entry_price=2650.0,
            exit_price=2660.0,
            signal_type='BUY',
            lot_size=0.01
        )
        
        assert pl > 0
    
    def test_calculate_pl_buy_loss(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        pl = risk_manager.calculate_pl(
            entry_price=2650.0,
            exit_price=2640.0,
            signal_type='BUY',
            lot_size=0.01
        )
        
        assert pl < 0
    
    def test_calculate_pl_sell_profit(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        pl = risk_manager.calculate_pl(
            entry_price=2650.0,
            exit_price=2640.0,
            signal_type='SELL',
            lot_size=0.01
        )
        
        assert pl > 0
    
    def test_calculate_pl_sell_loss(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        pl = risk_manager.calculate_pl(
            entry_price=2650.0,
            exit_price=2660.0,
            signal_type='SELL',
            lot_size=0.01
        )
        
        assert pl < 0
    
    def test_calculate_pl_no_movement(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        pl_buy = risk_manager.calculate_pl(
            entry_price=2650.0,
            exit_price=2650.0,
            signal_type='BUY',
            lot_size=0.01
        )
        
        pl_sell = risk_manager.calculate_pl(
            entry_price=2650.0,
            exit_price=2650.0,
            signal_type='SELL',
            lot_size=0.01
        )
        
        assert pl_buy == 0.0
        assert pl_sell == 0.0
    
    def test_calculate_pl_default_lot_size(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        pl = risk_manager.calculate_pl(
            entry_price=2650.0,
            exit_price=2660.0,
            signal_type='BUY'
        )
        
        assert pl is not None
    
    def test_calculate_pl_different_lot_sizes(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        pl_small = risk_manager.calculate_pl(
            entry_price=2650.0,
            exit_price=2660.0,
            signal_type='BUY',
            lot_size=0.01
        )
        
        pl_large = risk_manager.calculate_pl(
            entry_price=2650.0,
            exit_price=2660.0,
            signal_type='BUY',
            lot_size=0.10
        )
        
        assert pl_large > pl_small
        assert abs(pl_large - pl_small * 10) < 0.01
    
    @pytest.mark.parametrize("entry,exit,signal,expected_sign", [
        (2650.0, 2660.0, 'BUY', 1),
        (2650.0, 2640.0, 'BUY', -1),
        (2650.0, 2660.0, 'SELL', -1),
        (2650.0, 2640.0, 'SELL', 1),
    ])
    def test_calculate_pl_sign(self, mock_config, mock_db_manager, entry, exit, signal, expected_sign):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        pl = risk_manager.calculate_pl(
            entry_price=entry,
            exit_price=exit,
            signal_type=signal,
            lot_size=0.01
        )
        
        if expected_sign > 0:
            assert pl > 0
        elif expected_sign < 0:
            assert pl < 0
        else:
            assert pl == 0
    
    def test_multiple_users_independent_cooldowns(self, mock_config, mock_db_manager):
        risk_manager = RiskManager(mock_config, mock_db_manager)
        
        risk_manager.record_signal(111111)
        risk_manager.record_signal(222222)
        
        assert 111111 in risk_manager.last_signal_time
        assert 222222 in risk_manager.last_signal_time
        
        can_trade_1, _ = risk_manager.can_trade(111111, 'BUY')
        can_trade_2, _ = risk_manager.can_trade(222222, 'BUY')
        
        assert can_trade_1 is False
        assert can_trade_2 is False
