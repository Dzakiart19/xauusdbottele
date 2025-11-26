from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, cast
import pytz
import json
import csv
from collections import defaultdict
from functools import wraps
import time
from sqlalchemy.orm import Session
from bot.logger import setup_logger
from bot.database import Trade, Position

logger = setup_logger('Analytics')

class AnalyticsCache:
    """Simple time-based cache for expensive analytics queries"""
    def __init__(self, ttl_seconds: int = 300):
        self.ttl_seconds = ttl_seconds
        self._cache = {}
        self._timestamps = {}
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached value if not expired"""
        if key in self._cache:
            if time.time() - self._timestamps[key] < self.ttl_seconds:
                logger.debug(f"Cache hit for key: {key}")
                return self._cache[key]
            else:
                logger.debug(f"Cache expired for key: {key}")
                del self._cache[key]
                del self._timestamps[key]
        return None
    
    def set(self, key: str, value: Any):
        """Set cache value with current timestamp"""
        self._cache[key] = value
        self._timestamps[key] = time.time()
        logger.debug(f"Cache set for key: {key}")
    
    def clear(self):
        """Clear all cached values"""
        self._cache.clear()
        self._timestamps.clear()
        logger.info("Analytics cache cleared")

def cached(cache_instance: AnalyticsCache):
    """Decorator to cache function results"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = f"{func.__name__}:{args}:{kwargs}"
            
            cached_result = cache_instance.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            result = func(*args, **kwargs)
            cache_instance.set(cache_key, result)
            return result
        return wrapper
    return decorator

class TradingAnalytics:
    """Comprehensive trading analytics and performance tracking"""
    
    def __init__(self, db_manager, config=None):
        self.db = db_manager
        self.config = config
        self.cache = AnalyticsCache(ttl_seconds=300)
        logger.info("TradingAnalytics initialized with 5-minute cache")
    
    def clear_cache(self):
        """Clear analytics cache"""
        self.cache.clear()
    
    @cached(cache_instance=AnalyticsCache(ttl_seconds=300))
    def get_trading_performance(self, user_id: Optional[int] = None, days: int = 30) -> Dict[str, Any]:
        """Get overall trading performance metrics
        
        Args:
            user_id: Filter by user (None for all users)
            days: Number of days to analyze (default 30)
        
        Returns:
            Dict with winrate, total PL, average PL, trades count, etc.
        """
        session: Optional[Session] = None
        try:
            session = self.db.get_session()
            assert session is not None, "Failed to create database session"
            
            cutoff_date = datetime.now(pytz.UTC) - timedelta(days=days)
            
            query = session.query(Trade).filter(
                Trade.status == 'CLOSED',
                Trade.close_time >= cutoff_date,
                Trade.actual_pl.isnot(None)
            )
            
            if user_id:
                query = query.filter(Trade.user_id == user_id)
            
            trades = query.all()
            
            if not trades:
                return {
                    'total_trades': 0,
                    'wins': 0,
                    'losses': 0,
                    'winrate': 0.0,
                    'total_pl': 0.0,
                    'avg_pl': 0.0,
                    'avg_win': 0.0,
                    'avg_loss': 0.0,
                    'largest_win': 0.0,
                    'largest_loss': 0.0,
                    'profit_factor': 0.0,
                    'period_days': days
                }
            
            total_trades = len(trades)
            wins = [t for t in trades if t.actual_pl is not None and cast(float, t.actual_pl) > 0]
            losses = [t for t in trades if t.actual_pl is not None and cast(float, t.actual_pl) <= 0]
            
            win_count = len(wins)
            loss_count = len(losses)
            
            winrate = (win_count / total_trades * 100) if total_trades > 0 else 0.0
            
            total_pl = sum(cast(float, t.actual_pl) for t in trades if t.actual_pl is not None)
            avg_pl = total_pl / total_trades if total_trades > 0 else 0.0
            
            total_wins = sum(cast(float, t.actual_pl) for t in wins if t.actual_pl is not None)
            total_losses = abs(sum(cast(float, t.actual_pl) for t in losses if t.actual_pl is not None))
            
            avg_win = total_wins / win_count if win_count > 0 else 0.0
            avg_loss = total_losses / loss_count if loss_count > 0 else 0.0
            
            largest_win = max((cast(float, t.actual_pl) for t in wins if t.actual_pl is not None), default=0.0)
            largest_loss = min((cast(float, t.actual_pl) for t in losses if t.actual_pl is not None), default=0.0)
            
            profit_factor = (total_wins / total_losses) if total_losses > 0 else 0.0
            
            return {
                'total_trades': total_trades,
                'wins': win_count,
                'losses': loss_count,
                'winrate': round(float(winrate), 2),
                'total_pl': round(float(total_pl), 2),
                'avg_pl': round(float(avg_pl), 2),
                'avg_win': round(float(avg_win), 2),
                'avg_loss': round(float(avg_loss), 2),
                'largest_win': round(float(largest_win), 2),
                'largest_loss': round(float(largest_loss), 2),
                'profit_factor': round(float(profit_factor), 2),
                'period_days': days
            }
            
        except Exception as e:
            logger.error(f"Error getting trading performance: {e}", exc_info=True)
            return {'error': str(e)}
        finally:
            if session:
                session.close()
    
    @cached(cache_instance=AnalyticsCache(ttl_seconds=300))
    def get_hourly_stats(self, user_id: Optional[int] = None, days: int = 30) -> Dict[str, Any]:
        """Get performance breakdown by hour of day
        
        Args:
            user_id: Filter by user (None for all users)
            days: Number of days to analyze
        
        Returns:
            Dict with performance stats for each hour (0-23)
        """
        session: Optional[Session] = None
        try:
            session = self.db.get_session()
            assert session is not None, "Failed to create database session"
            
            cutoff_date = datetime.now(pytz.UTC) - timedelta(days=days)
            
            query = session.query(Trade).filter(
                Trade.status == 'CLOSED',
                Trade.close_time >= cutoff_date,
                Trade.actual_pl.isnot(None)
            )
            
            if user_id:
                query = query.filter(Trade.user_id == user_id)
            
            trades = query.all()
            
            jakarta_tz = pytz.timezone('Asia/Jakarta')
            hourly_stats = defaultdict(lambda: {
                'trades': 0,
                'wins': 0,
                'losses': 0,
                'total_pl': 0.0,
                'winrate': 0.0
            })
            
            for trade in trades:
                if trade.signal_time is not None:
                    jakarta_time = trade.signal_time.astimezone(jakarta_tz)
                    hour = jakarta_time.hour
                    
                    hourly_stats[hour]['trades'] += 1
                    if trade.actual_pl is not None:
                        hourly_stats[hour]['total_pl'] += cast(float, trade.actual_pl)
                    
                    if trade.actual_pl is not None and cast(float, trade.actual_pl) > 0:
                        hourly_stats[hour]['wins'] += 1
                    else:
                        hourly_stats[hour]['losses'] += 1
            
            for hour in range(24):
                if hour in hourly_stats:
                    stats = hourly_stats[hour]
                    total = stats['trades']
                    if total > 0:
                        stats['winrate'] = round((stats['wins'] / total * 100), 2)
                        stats['total_pl'] = round(stats['total_pl'], 2)
                        stats['avg_pl'] = round(stats['total_pl'] / total, 2)
                    else:
                        stats['avg_pl'] = 0.0
            
            best_hour = max(hourly_stats.items(), key=lambda x: x[1]['total_pl']) if hourly_stats else (None, {})
            worst_hour = min(hourly_stats.items(), key=lambda x: x[1]['total_pl']) if hourly_stats else (None, {})
            
            return {
                'hourly_breakdown': dict(hourly_stats),
                'best_hour': {'hour': best_hour[0], 'stats': best_hour[1]},
                'worst_hour': {'hour': worst_hour[0], 'stats': worst_hour[1]},
                'period_days': days
            }
            
        except Exception as e:
            logger.error(f"Error getting hourly stats: {e}", exc_info=True)
            return {'error': str(e)}
        finally:
            if session:
                session.close()
    
    @cached(cache_instance=AnalyticsCache(ttl_seconds=300))
    def get_signal_source_performance(self, user_id: Optional[int] = None, days: int = 30) -> Dict[str, Any]:
        """Compare performance between auto and manual signals
        
        Args:
            user_id: Filter by user (None for all users)
            days: Number of days to analyze
        
        Returns:
            Dict comparing auto vs manual signal performance
        """
        session: Optional[Session] = None
        try:
            session = self.db.get_session()
            assert session is not None, "Failed to create database session"
            
            cutoff_date = datetime.now(pytz.UTC) - timedelta(days=days)
            
            query = session.query(Trade).filter(
                Trade.status == 'CLOSED',
                Trade.close_time >= cutoff_date,
                Trade.actual_pl.isnot(None)
            )
            
            if user_id:
                query = query.filter(Trade.user_id == user_id)
            
            trades = query.all()
            
            auto_trades = [t for t in trades if t.signal_source is not None and str(t.signal_source) == 'auto']
            manual_trades = [t for t in trades if t.signal_source is not None and str(t.signal_source) == 'manual']
            
            def calculate_stats(trade_list: List[Trade]) -> Dict[str, Any]:
                if not trade_list:
                    return {
                        'total_trades': 0,
                        'wins': 0,
                        'losses': 0,
                        'winrate': 0.0,
                        'total_pl': 0.0,
                        'avg_pl': 0.0
                    }
                
                total = len(trade_list)
                wins = sum(1 for t in trade_list if t.actual_pl is not None and cast(float, t.actual_pl) > 0)
                losses = total - wins
                winrate = (wins / total * 100) if total > 0 else 0.0
                total_pl = sum(cast(float, t.actual_pl) for t in trade_list if t.actual_pl is not None)
                avg_pl = total_pl / total if total > 0 else 0.0
                
                return {
                    'total_trades': total,
                    'wins': wins,
                    'losses': losses,
                    'winrate': round(winrate, 2),
                    'total_pl': round(total_pl, 2),
                    'avg_pl': round(avg_pl, 2)
                }
            
            auto_stats = calculate_stats(auto_trades)
            manual_stats = calculate_stats(manual_trades)
            
            comparison = {
                'winrate_diff': round(auto_stats['winrate'] - manual_stats['winrate'], 2),
                'pl_diff': round(auto_stats['total_pl'] - manual_stats['total_pl'], 2),
                'avg_pl_diff': round(auto_stats['avg_pl'] - manual_stats['avg_pl'], 2)
            }
            
            return {
                'auto': auto_stats,
                'manual': manual_stats,
                'comparison': comparison,
                'period_days': days
            }
            
        except Exception as e:
            logger.error(f"Error getting signal source performance: {e}", exc_info=True)
            return {'error': str(e)}
        finally:
            if session:
                session.close()
    
    @cached(cache_instance=AnalyticsCache(ttl_seconds=300))
    def get_position_tracking_stats(self, user_id: Optional[int] = None, days: int = 30) -> Dict[str, Any]:
        """Get position tracking statistics
        
        Args:
            user_id: Filter by user (None for all users)
            days: Number of days to analyze
        
        Returns:
            Dict with avg hold time, max profit reached, SL adjustments, etc.
        """
        session: Optional[Session] = None
        try:
            session = self.db.get_session()
            assert session is not None, "Failed to create database session"
            
            cutoff_date = datetime.now(pytz.UTC) - timedelta(days=days)
            
            query = session.query(Position).filter(
                Position.status == 'CLOSED',
                Position.closed_at >= cutoff_date
            )
            
            if user_id:
                query = query.filter(Position.user_id == user_id)
            
            positions = query.all()
            
            if not positions:
                return {
                    'total_positions': 0,
                    'avg_hold_time_minutes': 0.0,
                    'avg_max_profit': 0.0,
                    'avg_sl_adjustments': 0.0,
                    'positions_with_sl_adjusted': 0,
                    'avg_profit_captured': 0.0,
                    'period_days': days
                }
            
            total_positions = len(positions)
            
            hold_times = []
            max_profits = []
            sl_adjustments = []
            profit_captured_ratios = []
            
            for pos in positions:
                if pos.opened_at is not None and pos.closed_at is not None:
                    hold_time = (pos.closed_at - pos.opened_at).total_seconds() / 60
                    hold_times.append(hold_time)
                
                if pos.max_profit_reached is not None:
                    max_profit_val = cast(float, pos.max_profit_reached)
                    max_profits.append(max_profit_val)
                    
                    if pos.unrealized_pl is not None and max_profit_val > 0:
                        capture_ratio = (cast(float, pos.unrealized_pl) / max_profit_val * 100)
                        profit_captured_ratios.append(capture_ratio)
                
                if pos.sl_adjustment_count is not None:
                    sl_adjustments.append(cast(int, pos.sl_adjustment_count))
            
            avg_hold_time = sum(hold_times) / len(hold_times) if hold_times else 0.0
            avg_max_profit = sum(max_profits) / len(max_profits) if max_profits else 0.0
            avg_sl_adjustments = sum(sl_adjustments) / len(sl_adjustments) if sl_adjustments else 0.0
            positions_with_sl = sum(1 for adj in sl_adjustments if adj > 0)
            avg_profit_captured = sum(profit_captured_ratios) / len(profit_captured_ratios) if profit_captured_ratios else 0.0
            
            return {
                'total_positions': total_positions,
                'avg_hold_time_minutes': round(avg_hold_time, 2),
                'avg_hold_time_hours': round(avg_hold_time / 60, 2),
                'avg_max_profit': round(avg_max_profit, 2),
                'avg_sl_adjustments': round(avg_sl_adjustments, 2),
                'positions_with_sl_adjusted': positions_with_sl,
                'sl_adjustment_rate': round((positions_with_sl / total_positions * 100), 2) if total_positions > 0 else 0.0,
                'avg_profit_captured': round(avg_profit_captured, 2),
                'period_days': days
            }
            
        except Exception as e:
            logger.error(f"Error getting position tracking stats: {e}", exc_info=True)
            return {'error': str(e)}
        finally:
            if session:
                session.close()
    
    @cached(cache_instance=AnalyticsCache(ttl_seconds=300))
    def get_risk_metrics(self, user_id: Optional[int] = None, days: int = 30) -> Dict[str, Any]:
        """Get risk management effectiveness metrics
        
        Args:
            user_id: Filter by user (None for all users)
            days: Number of days to analyze
        
        Returns:
            Dict with SL hit rate, TP hit rate, actual vs planned RR, etc.
        """
        session: Optional[Session] = None
        try:
            session = self.db.get_session()
            assert session is not None, "Failed to create database session"
            
            cutoff_date = datetime.now(pytz.UTC) - timedelta(days=days)
            
            query = session.query(Trade).filter(
                Trade.status == 'CLOSED',
                Trade.close_time >= cutoff_date
            )
            
            if user_id:
                query = query.filter(Trade.user_id == user_id)
            
            trades = query.all()
            
            if not trades:
                return {
                    'total_trades': 0,
                    'sl_hit_count': 0,
                    'tp_hit_count': 0,
                    'sl_hit_rate': 0.0,
                    'tp_hit_rate': 0.0,
                    'avg_planned_rr': 0.0,
                    'avg_actual_rr': 0.0,
                    'period_days': days
                }
            
            total_trades = len(trades)
            
            tp_hits = sum(1 for t in trades if t.result is not None and str(t.result) == 'WIN')
            sl_hits = sum(1 for t in trades if t.result is not None and str(t.result) == 'LOSS')
            
            tp_hit_rate = (tp_hits / total_trades * 100) if total_trades > 0 else 0.0
            sl_hit_rate = (sl_hits / total_trades * 100) if total_trades > 0 else 0.0
            
            planned_rr_ratios = []
            actual_rr_ratios = []
            
            for trade in trades:
                if trade.entry_price is not None and trade.stop_loss is not None and trade.take_profit is not None:
                    entry = cast(float, trade.entry_price)
                    sl = cast(float, trade.stop_loss)
                    tp = cast(float, trade.take_profit)
                    sl_distance = abs(entry - sl)
                    tp_distance = abs(entry - tp)
                    
                    if sl_distance > 0:
                        planned_rr = tp_distance / sl_distance
                        planned_rr_ratios.append(planned_rr)
                    
                    if trade.actual_pl is not None and sl_distance > 0:
                        actual_pl_val = cast(float, trade.actual_pl)
                        if trade.signal_type is not None and str(trade.signal_type) == 'BUY':
                            risk = sl_distance * (self.config.LOT_SIZE if self.config else 0.01) * (self.config.XAUUSD_PIP_VALUE if self.config else 10.0)
                        else:
                            risk = sl_distance * (self.config.LOT_SIZE if self.config else 0.01) * (self.config.XAUUSD_PIP_VALUE if self.config else 10.0)
                        
                        if risk > 0:
                            actual_rr = actual_pl_val / risk if actual_pl_val > 0 else -(abs(actual_pl_val) / risk)
                            actual_rr_ratios.append(actual_rr)
            
            avg_planned_rr = sum(planned_rr_ratios) / len(planned_rr_ratios) if planned_rr_ratios else 0.0
            avg_actual_rr = sum(actual_rr_ratios) / len(actual_rr_ratios) if actual_rr_ratios else 0.0
            
            return {
                'total_trades': total_trades,
                'sl_hit_count': sl_hits,
                'tp_hit_count': tp_hits,
                'sl_hit_rate': round(sl_hit_rate, 2),
                'tp_hit_rate': round(tp_hit_rate, 2),
                'avg_planned_rr': round(avg_planned_rr, 2),
                'avg_actual_rr': round(avg_actual_rr, 2),
                'rr_efficiency': round((avg_actual_rr / avg_planned_rr * 100), 2) if avg_planned_rr > 0 else 0.0,
                'period_days': days
            }
            
        except Exception as e:
            logger.error(f"Error getting risk metrics: {e}", exc_info=True)
            return {'error': str(e)}
        finally:
            if session:
                session.close()
    
    def export_to_json(self, user_id: Optional[int] = None, days: int = 30, filepath: str = 'analytics_export.json') -> bool:
        """Export all analytics to JSON file
        
        Args:
            user_id: Filter by user (None for all users)
            days: Number of days to analyze
            filepath: Output file path
        
        Returns:
            bool indicating success
        """
        try:
            analytics_data = {
                'export_timestamp': datetime.now(pytz.UTC).isoformat(),
                'period_days': days,
                'user_id': user_id,
                'trading_performance': self.get_trading_performance(user_id, days),
                'hourly_stats': self.get_hourly_stats(user_id, days),
                'signal_source_performance': self.get_signal_source_performance(user_id, days),
                'position_tracking_stats': self.get_position_tracking_stats(user_id, days),
                'risk_metrics': self.get_risk_metrics(user_id, days)
            }
            
            with open(filepath, 'w') as f:
                json.dump(analytics_data, f, indent=2)
            
            logger.info(f"Analytics exported to JSON: {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to JSON: {e}", exc_info=True)
            return False
    
    def export_trades_to_csv(self, user_id: Optional[int] = None, days: int = 30, filepath: str = 'trades_export.csv') -> bool:
        """Export trade history to CSV file
        
        Args:
            user_id: Filter by user (None for all users)
            days: Number of days to export
            filepath: Output file path
        
        Returns:
            bool indicating success
        """
        session: Optional[Session] = None
        try:
            session = self.db.get_session()
            assert session is not None, "Failed to create database session"
            
            cutoff_date = datetime.now(pytz.UTC) - timedelta(days=days)
            
            query = session.query(Trade).filter(
                Trade.status == 'CLOSED',
                Trade.close_time >= cutoff_date
            )
            
            if user_id:
                query = query.filter(Trade.user_id == user_id)
            
            trades = query.all()
            
            with open(filepath, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'Trade ID', 'User ID', 'Ticker', 'Signal Type', 'Signal Source',
                    'Entry Price', 'Exit Price', 'Stop Loss', 'Take Profit',
                    'Actual P/L', 'Result', 'Signal Time', 'Close Time',
                    'Timeframe', 'Spread'
                ])
                
                for trade in trades:
                    writer.writerow([
                        trade.id,
                        trade.user_id,
                        trade.ticker,
                        trade.signal_type,
                        trade.signal_source,
                        trade.entry_price,
                        trade.exit_price,
                        trade.stop_loss,
                        trade.take_profit,
                        trade.actual_pl,
                        trade.result,
                        trade.signal_time.isoformat() if trade.signal_time is not None else '',
                        trade.close_time.isoformat() if trade.close_time is not None else '',
                        trade.timeframe,
                        trade.spread
                    ])
            
            logger.info(f"Trades exported to CSV: {filepath} ({len(trades)} trades)")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}", exc_info=True)
            return False
        finally:
            if session:
                session.close()
    
    def get_visualization_data(self, user_id: Optional[int] = None, days: int = 30) -> Dict[str, Any]:
        """Prepare data for visualization/charting
        
        Args:
            user_id: Filter by user
            days: Number of days to analyze
        
        Returns:
            Dict with data formatted for charts
        """
        try:
            performance = self.get_trading_performance(user_id, days)
            hourly = self.get_hourly_stats(user_id, days)
            source_perf = self.get_signal_source_performance(user_id, days)
            risk = self.get_risk_metrics(user_id, days)
            
            hourly_data = hourly.get('hourly_breakdown', {})
            hours = sorted(hourly_data.keys())
            hourly_pl = [hourly_data[h]['total_pl'] for h in hours]
            hourly_trades = [hourly_data[h]['trades'] for h in hours]
            
            return {
                'performance_summary': {
                    'labels': ['Wins', 'Losses'],
                    'values': [performance.get('wins', 0), performance.get('losses', 0)]
                },
                'hourly_pl_chart': {
                    'hours': hours,
                    'pl_values': hourly_pl,
                    'trade_counts': hourly_trades
                },
                'source_comparison': {
                    'labels': ['Auto', 'Manual'],
                    'winrates': [
                        source_perf.get('auto', {}).get('winrate', 0),
                        source_perf.get('manual', {}).get('winrate', 0)
                    ],
                    'total_pl': [
                        source_perf.get('auto', {}).get('total_pl', 0),
                        source_perf.get('manual', {}).get('total_pl', 0)
                    ]
                },
                'risk_analysis': {
                    'labels': ['TP Hit Rate', 'SL Hit Rate'],
                    'values': [
                        risk.get('tp_hit_rate', 0),
                        risk.get('sl_hit_rate', 0)
                    ]
                }
            }
            
        except Exception as e:
            logger.error(f"Error preparing visualization data: {e}", exc_info=True)
            return {'error': str(e)}
