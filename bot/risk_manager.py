from datetime import datetime, timedelta
from typing import Optional
import pytz
from bot.logger import setup_logger

logger = setup_logger('RiskManager')

class RiskManager:
    def __init__(self, config, db_manager, alert_system=None):
        self.config = config
        self.db = db_manager
        self.alert_system = alert_system
        self.last_signal_time = {}
        self.daily_stats = {}
        self.risk_warning_sent_today = {}
        
    def can_trade(self, user_id: int, signal_type: str) -> tuple[bool, Optional[str]]:
        utc_now = datetime.now(pytz.UTC)
        jakarta_tz = pytz.timezone('Asia/Jakarta')
        jakarta_time = utc_now.astimezone(jakarta_tz)
        today_str = jakarta_time.strftime('%Y-%m-%d')
        
        if user_id in self.last_signal_time:
            time_since_last = (utc_now - self.last_signal_time[user_id]).total_seconds()
            if time_since_last < self.config.SIGNAL_COOLDOWN_SECONDS:
                remaining = self.config.SIGNAL_COOLDOWN_SECONDS - time_since_last
                return False, f"Cooldown aktif. Tunggu {int(remaining)} detik lagi"
        
        cache_key = f"{user_id}_{today_str}"
        cache_ttl = 60
        
        if cache_key in self.daily_stats:
            cached_data = self.daily_stats[cache_key]
            cache_age = (utc_now - cached_data['timestamp']).total_seconds()
            if cache_age < cache_ttl:
                total_daily_pl = cached_data['total_pl']
                logger.debug(f"Using cached daily P/L for user {user_id}: ${total_daily_pl:.2f} (age: {cache_age:.1f}s)")
            else:
                total_daily_pl = None
        else:
            total_daily_pl = None
        
        if total_daily_pl is None:
            session = self.db.get_session()
            try:
                from bot.database import Trade
                
                today_start = jakarta_time.replace(hour=0, minute=0, second=0, microsecond=0)
                today_start_utc = today_start.astimezone(pytz.UTC)
                
                daily_pl = session.query(Trade).filter(
                    Trade.user_id == user_id,
                    Trade.signal_time >= today_start_utc,
                    Trade.actual_pl.isnot(None)
                ).with_entities(Trade.actual_pl).all()
                
                total_daily_pl = sum([pl[0] for pl in daily_pl if pl[0] is not None])
                
                self.daily_stats[cache_key] = {
                    'total_pl': total_daily_pl,
                    'timestamp': utc_now
                }
                logger.debug(f"Cached daily P/L for user {user_id}: ${total_daily_pl:.2f}")
                
            except Exception as e:
                logger.error(f"Error checking trade eligibility: {e}")
                return False, f"Error: {str(e)}"
            finally:
                session.close()
        
        if total_daily_pl < 0:
            loss_percent = abs(total_daily_pl)
            
            warning_threshold = self.config.DAILY_LOSS_PERCENT * 0.8
            if loss_percent >= warning_threshold and loss_percent < self.config.DAILY_LOSS_PERCENT:
                warning_key = f"{user_id}_{today_str}"
                if warning_key not in self.risk_warning_sent_today:
                    self.risk_warning_sent_today[warning_key] = True
                    if self.alert_system:
                        import asyncio
                        try:
                            asyncio.create_task(
                                self.alert_system.send_risk_warning(
                                    "Daily Loss Approaching Limit",
                                    f"Current loss: ${loss_percent:.2f} ({loss_percent/self.config.DAILY_LOSS_PERCENT*100:.1f}% of limit)\n"
                                    f"Limit: ${self.config.DAILY_LOSS_PERCENT:.2f}\n"
                                    f"⚠️ Trading akan dihentikan saat limit tercapai"
                                )
                            )
                            logger.warning(f"Risk warning sent for user {user_id}: loss ${loss_percent:.2f} approaching limit")
                        except Exception as alert_error:
                            logger.error(f"Failed to send risk warning alert: {alert_error}")
            
            if loss_percent >= self.config.DAILY_LOSS_PERCENT:
                return False, f"Batas kerugian harian ${self.config.DAILY_LOSS_PERCENT:.2f} tercapai"
        
        return True, None
    
    def record_signal(self, user_id: int):
        self.last_signal_time[user_id] = datetime.now(pytz.UTC)
        logger.debug(f"Signal recorded for user {user_id}, cooldown timer started")
    
    def calculate_position_size(self, account_balance: float, entry_price: float, 
                               stop_loss: float, signal_type: str) -> float:
        risk_amount = self.config.FIXED_RISK_AMOUNT
        
        pips_at_risk = abs(entry_price - stop_loss) * self.config.XAUUSD_PIP_VALUE
        
        if pips_at_risk > 0:
            lot_size = risk_amount / pips_at_risk
        else:
            lot_size = self.config.LOT_SIZE
        
        lot_size = max(0.01, min(lot_size, 1.0))
        
        logger.info(f"Calculated position size: {lot_size} lots (Fixed Risk: ${risk_amount:.2f})")
        return lot_size
    
    def calculate_pl(self, entry_price: float, exit_price: float, 
                    signal_type: str, lot_size: Optional[float] = None) -> float:
        if lot_size is None:
            lot_size = self.config.LOT_SIZE
        
        if signal_type == 'BUY':
            price_diff = exit_price - entry_price
        else:
            price_diff = entry_price - exit_price
        
        pips = price_diff * self.config.XAUUSD_PIP_VALUE
        pl = pips * lot_size
        
        return pl
