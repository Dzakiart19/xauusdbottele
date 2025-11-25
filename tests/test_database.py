import pytest
from datetime import datetime
import pytz
from bot.database import DatabaseManager, Trade, SignalLog, Position, Performance

@pytest.mark.unit
class TestDatabaseManager:
    
    def test_initialization(self, tmp_path):
        db_path = tmp_path / "test.db"
        db_manager = DatabaseManager(str(db_path))
        
        assert db_manager.engine is not None
        assert db_manager.Session is not None
        
        db_manager.close()
    
    def test_get_session(self, tmp_path):
        db_path = tmp_path / "test.db"
        db_manager = DatabaseManager(str(db_path))
        
        session = db_manager.get_session()
        
        assert session is not None
        session.close()
        db_manager.close()
    
    def test_close(self, tmp_path):
        db_path = tmp_path / "test.db"
        db_manager = DatabaseManager(str(db_path))
        
        db_manager.close()

@pytest.mark.unit
class TestTradeModel:
    
    def test_create_trade(self, test_db):
        trade = Trade(
            user_id=123456,
            ticker='XAUUSD',
            signal_type='BUY',
            signal_source='auto',
            entry_price=2650.0,
            stop_loss=2640.0,
            take_profit=2670.0,
            spread=0.2,
            estimated_pl=2.0,
            status='OPEN',
            timeframe='M1'
        )
        
        test_db.add(trade)
        test_db.commit()
        
        assert trade.id is not None
        assert trade.user_id == 123456
        assert trade.ticker == 'XAUUSD'
        assert trade.signal_type == 'BUY'
    
    def test_trade_default_values(self, test_db):
        trade = Trade(
            user_id=123456,
            ticker='XAUUSD',
            signal_type='BUY',
            entry_price=2650.0,
            stop_loss=2640.0,
            take_profit=2670.0
        )
        
        test_db.add(trade)
        test_db.commit()
        
        assert trade.status == 'OPEN'
        assert trade.signal_source == 'auto'
        assert trade.signal_time is not None
    
    def test_trade_update(self, test_db):
        trade = Trade(
            user_id=123456,
            ticker='XAUUSD',
            signal_type='BUY',
            entry_price=2650.0,
            stop_loss=2640.0,
            take_profit=2670.0,
            status='OPEN'
        )
        
        test_db.add(trade)
        test_db.commit()
        
        trade.status = 'CLOSED'
        trade.exit_price = 2670.0
        trade.actual_pl = 2.0
        trade.result = 'WIN'
        trade.close_time = datetime.utcnow()
        
        test_db.commit()
        
        updated_trade = test_db.query(Trade).filter_by(id=trade.id).first()
        assert updated_trade.status == 'CLOSED'
        assert updated_trade.exit_price == 2670.0
        assert updated_trade.actual_pl == 2.0
        assert updated_trade.result == 'WIN'
    
    def test_trade_query(self, test_db):
        trade1 = Trade(
            user_id=123456,
            ticker='XAUUSD',
            signal_type='BUY',
            entry_price=2650.0,
            stop_loss=2640.0,
            take_profit=2670.0
        )
        trade2 = Trade(
            user_id=123456,
            ticker='XAUUSD',
            signal_type='SELL',
            entry_price=2660.0,
            stop_loss=2670.0,
            take_profit=2640.0
        )
        
        test_db.add_all([trade1, trade2])
        test_db.commit()
        
        buy_trades = test_db.query(Trade).filter_by(signal_type='BUY').all()
        sell_trades = test_db.query(Trade).filter_by(signal_type='SELL').all()
        
        assert len(buy_trades) == 1
        assert len(sell_trades) == 1

@pytest.mark.unit
class TestSignalLogModel:
    
    def test_create_signal_log(self, test_db):
        signal_log = SignalLog(
            user_id=123456,
            ticker='XAUUSD',
            signal_type='BUY',
            signal_source='auto',
            entry_price=2650.0,
            indicators='{"rsi": 55, "macd": 0.5}',
            accepted=True
        )
        
        test_db.add(signal_log)
        test_db.commit()
        
        assert signal_log.id is not None
        assert signal_log.user_id == 123456
        assert signal_log.accepted is True
    
    def test_signal_log_rejection(self, test_db):
        signal_log = SignalLog(
            user_id=123456,
            ticker='XAUUSD',
            signal_type='BUY',
            entry_price=2650.0,
            accepted=False,
            rejection_reason='Cooldown active'
        )
        
        test_db.add(signal_log)
        test_db.commit()
        
        assert signal_log.accepted is False
        assert signal_log.rejection_reason == 'Cooldown active'
    
    def test_signal_log_query_by_user(self, test_db):
        log1 = SignalLog(
            user_id=123456,
            ticker='XAUUSD',
            signal_type='BUY',
            entry_price=2650.0
        )
        log2 = SignalLog(
            user_id=789012,
            ticker='XAUUSD',
            signal_type='SELL',
            entry_price=2660.0
        )
        
        test_db.add_all([log1, log2])
        test_db.commit()
        
        user_logs = test_db.query(SignalLog).filter_by(user_id=123456).all()
        
        assert len(user_logs) == 1
        assert user_logs[0].user_id == 123456

@pytest.mark.unit
class TestPositionModel:
    
    def test_create_position(self, test_db):
        position = Position(
            user_id=123456,
            trade_id=1,
            ticker='XAUUSD',
            signal_type='BUY',
            entry_price=2650.0,
            stop_loss=2640.0,
            take_profit=2670.0,
            current_price=2655.0,
            unrealized_pl=0.5,
            status='ACTIVE'
        )
        
        test_db.add(position)
        test_db.commit()
        
        assert position.id is not None
        assert position.status == 'ACTIVE'
        assert position.current_price == 2655.0
    
    def test_position_default_values(self, test_db):
        position = Position(
            user_id=123456,
            trade_id=1,
            ticker='XAUUSD',
            signal_type='BUY',
            entry_price=2650.0,
            stop_loss=2640.0,
            take_profit=2670.0
        )
        
        test_db.add(position)
        test_db.commit()
        
        assert position.status == 'ACTIVE'
        assert position.opened_at is not None
        assert position.sl_adjustment_count == 0
        assert position.max_profit_reached == 0.0
    
    def test_position_update_price(self, test_db):
        position = Position(
            user_id=123456,
            trade_id=1,
            ticker='XAUUSD',
            signal_type='BUY',
            entry_price=2650.0,
            stop_loss=2640.0,
            take_profit=2670.0,
            current_price=2650.0
        )
        
        test_db.add(position)
        test_db.commit()
        
        position.current_price = 2660.0
        position.unrealized_pl = 1.0
        position.max_profit_reached = 1.0
        
        test_db.commit()
        
        updated_position = test_db.query(Position).filter_by(id=position.id).first()
        assert updated_position.current_price == 2660.0
        assert updated_position.unrealized_pl == 1.0
        assert updated_position.max_profit_reached == 1.0
    
    def test_position_stop_loss_adjustment(self, test_db):
        position = Position(
            user_id=123456,
            trade_id=1,
            ticker='XAUUSD',
            signal_type='BUY',
            entry_price=2650.0,
            stop_loss=2640.0,
            take_profit=2670.0,
            original_sl=2640.0
        )
        
        test_db.add(position)
        test_db.commit()
        
        position.stop_loss = 2645.0
        position.sl_adjustment_count = 1
        
        test_db.commit()
        
        updated_position = test_db.query(Position).filter_by(id=position.id).first()
        assert updated_position.stop_loss == 2645.0
        assert updated_position.original_sl == 2640.0
        assert updated_position.sl_adjustment_count == 1
    
    def test_position_close(self, test_db):
        position = Position(
            user_id=123456,
            trade_id=1,
            ticker='XAUUSD',
            signal_type='BUY',
            entry_price=2650.0,
            stop_loss=2640.0,
            take_profit=2670.0,
            status='ACTIVE'
        )
        
        test_db.add(position)
        test_db.commit()
        
        position.status = 'CLOSED'
        position.closed_at = datetime.utcnow()
        
        test_db.commit()
        
        closed_position = test_db.query(Position).filter_by(id=position.id).first()
        assert closed_position.status == 'CLOSED'
        assert closed_position.closed_at is not None
    
    def test_position_query_active(self, test_db):
        pos1 = Position(
            user_id=123456,
            trade_id=1,
            ticker='XAUUSD',
            signal_type='BUY',
            entry_price=2650.0,
            stop_loss=2640.0,
            take_profit=2670.0,
            status='ACTIVE'
        )
        pos2 = Position(
            user_id=123456,
            trade_id=2,
            ticker='XAUUSD',
            signal_type='SELL',
            entry_price=2660.0,
            stop_loss=2670.0,
            take_profit=2640.0,
            status='CLOSED'
        )
        
        test_db.add_all([pos1, pos2])
        test_db.commit()
        
        active_positions = test_db.query(Position).filter_by(status='ACTIVE').all()
        
        assert len(active_positions) == 1
        assert active_positions[0].status == 'ACTIVE'

@pytest.mark.unit
class TestPerformanceModel:
    
    def test_create_performance_record(self, test_db):
        perf = Performance(
            date=datetime.utcnow(),
            total_trades=10,
            wins=6,
            losses=4,
            total_pl=5.5,
            max_drawdown=-2.0,
            equity=1005.5
        )
        
        test_db.add(perf)
        test_db.commit()
        
        assert perf.id is not None
        assert perf.total_trades == 10
        assert perf.wins == 6
    
    def test_performance_default_values(self, test_db):
        perf = Performance()
        
        test_db.add(perf)
        test_db.commit()
        
        assert perf.total_trades == 0
        assert perf.wins == 0
        assert perf.losses == 0
        assert perf.total_pl == 0.0
        assert perf.max_drawdown == 0.0
        assert perf.equity == 0.0
    
    def test_performance_update(self, test_db):
        perf = Performance(
            total_trades=5,
            wins=3,
            losses=2,
            total_pl=2.5
        )
        
        test_db.add(perf)
        test_db.commit()
        
        perf.total_trades = 6
        perf.wins = 4
        perf.total_pl = 3.5
        
        test_db.commit()
        
        updated_perf = test_db.query(Performance).filter_by(id=perf.id).first()
        assert updated_perf.total_trades == 6
        assert updated_perf.wins == 4
        assert updated_perf.total_pl == 3.5
    
    def test_performance_win_rate_calculation(self, test_db):
        perf = Performance(
            total_trades=10,
            wins=7,
            losses=3
        )
        
        test_db.add(perf)
        test_db.commit()
        
        win_rate = (perf.wins / perf.total_trades * 100) if perf.total_trades > 0 else 0
        
        assert win_rate == 70.0

@pytest.mark.integration
class TestDatabaseIntegration:
    
    def test_trade_and_position_relationship(self, test_db):
        trade = Trade(
            user_id=123456,
            ticker='XAUUSD',
            signal_type='BUY',
            entry_price=2650.0,
            stop_loss=2640.0,
            take_profit=2670.0
        )
        
        test_db.add(trade)
        test_db.commit()
        
        position = Position(
            user_id=123456,
            trade_id=trade.id,
            ticker='XAUUSD',
            signal_type='BUY',
            entry_price=2650.0,
            stop_loss=2640.0,
            take_profit=2670.0
        )
        
        test_db.add(position)
        test_db.commit()
        
        assert position.trade_id == trade.id
    
    def test_multiple_users_isolation(self, test_db):
        trade1 = Trade(
            user_id=111111,
            ticker='XAUUSD',
            signal_type='BUY',
            entry_price=2650.0,
            stop_loss=2640.0,
            take_profit=2670.0
        )
        trade2 = Trade(
            user_id=222222,
            ticker='XAUUSD',
            signal_type='SELL',
            entry_price=2660.0,
            stop_loss=2670.0,
            take_profit=2640.0
        )
        
        test_db.add_all([trade1, trade2])
        test_db.commit()
        
        user1_trades = test_db.query(Trade).filter_by(user_id=111111).all()
        user2_trades = test_db.query(Trade).filter_by(user_id=222222).all()
        
        assert len(user1_trades) == 1
        assert len(user2_trades) == 1
        assert user1_trades[0].user_id != user2_trades[0].user_id
