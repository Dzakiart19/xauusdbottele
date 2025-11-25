import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, List
import pytz
from bot.logger import setup_logger
from bot.database import Trade, Position
from bot.resilience import RateLimiter

logger = setup_logger('AlertSystem')

class AlertType:
    TRADE_ENTRY = "TRADE_ENTRY"
    TRADE_EXIT = "TRADE_EXIT"
    PRICE_ALERT = "PRICE_ALERT"
    STOP_LOSS_HIT = "STOP_LOSS_HIT"
    TAKE_PROFIT_HIT = "TAKE_PROFIT_HIT"
    DAILY_SUMMARY = "DAILY_SUMMARY"
    RISK_WARNING = "RISK_WARNING"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    MARKET_CLOSE = "MARKET_CLOSE"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"

class Alert:
    def __init__(self, alert_type: str, message: str, priority: str = "NORMAL",
                 data: Optional[Dict] = None):
        self.alert_type = alert_type
        self.message = message
        self.priority = priority
        self.data = data or {}
        self.timestamp = datetime.now(pytz.UTC)
        self.sent = False
    
    def to_dict(self):
        return {
            'alert_type': self.alert_type,
            'message': self.message,
            'priority': self.priority,
            'data': self.data,
            'timestamp': self.timestamp.isoformat(),
            'sent': self.sent
        }

class AlertSystem:
    def __init__(self, config, db_manager):
        self.config = config
        self.db = db_manager
        self.alert_queue = []
        self.alert_history = []
        self.telegram_app = None
        self.chat_ids = []
        self.enabled = True
        self.max_history = 100
        self.send_message_callback = None
        logger.info("Alert system initialized")
    
    def set_telegram_app(self, app, chat_ids: List[int], send_message_callback=None):
        self.telegram_app = app
        self.chat_ids = chat_ids
        self.send_message_callback = send_message_callback
        logger.info(f"Telegram app set with {len(chat_ids)} chat IDs")
    
    async def send_alert(self, alert: Alert):
        if not self.enabled:
            logger.info(f"Alert system disabled, skipping: {alert.alert_type}")
            return
        
        self.alert_queue.append(alert)
        logger.info(f"Alert queued: {alert.alert_type} - {alert.message}")
        
        await self._process_alert(alert)
    
    async def _process_alert(self, alert: Alert):
        try:
            formatted_msg = self._format_alert_message(alert)
            
            if self.send_message_callback:
                for chat_id in self.chat_ids:
                    try:
                        await self.send_message_callback(
                            chat_id=chat_id,
                            text=formatted_msg,
                            parse_mode='Markdown'
                        )
                        logger.info(f"Alert sent to chat {chat_id}: {alert.alert_type}")
                    except Exception as e:
                        logger.error(f"Failed to send alert to chat {chat_id}: {e}")
            elif self.telegram_app and self.telegram_app.bot:
                logger.warning("No send_message_callback provided, falling back to direct bot.send_message (not recommended)")
                for chat_id in self.chat_ids:
                    try:
                        await self.telegram_app.bot.send_message(
                            chat_id=chat_id,
                            text=formatted_msg,
                            parse_mode='Markdown'
                        )
                        logger.info(f"Alert sent to chat {chat_id}: {alert.alert_type}")
                    except Exception as e:
                        logger.error(f"Failed to send alert to chat {chat_id}: {e}")
            
            alert.sent = True
            self.alert_history.append(alert)
            
            if len(self.alert_history) > self.max_history:
                self.alert_history = self.alert_history[-self.max_history:]
            
        except Exception as e:
            logger.error(f"Error processing alert: {e}")
    
    def _format_alert_message(self, alert: Alert) -> str:
        priority_emoji = {
            'LOW': '📘',
            'NORMAL': '📗',
            'HIGH': '📙',
            'CRITICAL': '📕'
        }
        
        emoji = priority_emoji.get(alert.priority, '📗')
        
        jakarta_tz = pytz.timezone('Asia/Jakarta')
        local_time = alert.timestamp.astimezone(jakarta_tz)
        time_str = local_time.strftime('%H:%M:%S WIB')
        
        msg = f"{emoji} *{alert.alert_type}*\n\n"
        msg += f"{alert.message}\n\n"
        msg += f"🕐 {time_str}"
        
        return msg
    
    async def send_trade_entry_alert(self, trade_data: Dict):
        message = (
            f"🚨 *SINYAL {trade_data['signal_type']}*\n\n"
            f"Entry: ${trade_data['entry_price']:.2f}\n"
            f"SL: ${trade_data['stop_loss']:.2f}\n"
            f"TP: ${trade_data['take_profit']:.2f}\n"
            f"Timeframe: {trade_data.get('timeframe', 'M1')}"
        )
        
        alert = Alert(
            alert_type=AlertType.TRADE_ENTRY,
            message=message,
            priority='HIGH',
            data=trade_data
        )
        
        await self.send_alert(alert)
    
    async def send_trade_exit_alert(self, trade_data: Dict, result: str):
        result_emoji = '✅' if result == 'WIN' else '❌'
        pl_value = trade_data.get('actual_pl', 0)
        
        message = (
            f"{result_emoji} *Trade {result}*\n\n"
            f"Type: {trade_data['signal_type']}\n"
            f"Entry: ${trade_data['entry_price']:.2f}\n"
            f"Exit: ${trade_data.get('exit_price', 0):.2f}\n"
            f"P/L: ${pl_value:.2f}"
        )
        
        priority = 'HIGH' if result == 'WIN' else 'NORMAL'
        
        alert = Alert(
            alert_type=AlertType.TRADE_EXIT,
            message=message,
            priority=priority,
            data=trade_data
        )
        
        await self.send_alert(alert)
    
    async def send_daily_summary(self):
        try:
            session = self.db.get_session()
            
            jakarta_tz = pytz.timezone('Asia/Jakarta')
            today = datetime.now(jakarta_tz).replace(hour=0, minute=0, second=0, microsecond=0)
            today_utc = today.astimezone(pytz.UTC)
            
            today_trades = session.query(Trade).filter(
                Trade.signal_time >= today_utc
            ).all()
            
            total_trades = len(today_trades)
            closed_trades = [t for t in today_trades if t.status == 'CLOSED']
            wins = len([t for t in closed_trades if t.result == 'WIN'])
            losses = len([t for t in closed_trades if t.result == 'LOSS'])
            
            total_pl = sum([t.actual_pl for t in closed_trades if t.actual_pl])
            win_rate = (wins / len(closed_trades) * 100) if closed_trades else 0
            
            message = (
                "📊 *Ringkasan Harian*\n\n"
                f"Total Sinyal: {total_trades}\n"
                f"Closed: {len(closed_trades)}\n"
                f"Wins: {wins} ✅\n"
                f"Losses: {losses} ❌\n"
                f"Win Rate: {win_rate:.1f}%\n"
                f"Total P/L: ${total_pl:.2f}"
            )
            
            session.close()
            
            alert = Alert(
                alert_type=AlertType.DAILY_SUMMARY,
                message=message,
                priority='NORMAL',
                data={'trades': total_trades, 'pl': total_pl}
            )
            
            await self.send_alert(alert)
            
        except Exception as e:
            logger.error(f"Error sending daily summary: {e}")
    
    async def send_risk_warning(self, warning_type: str, details: str):
        message = (
            f"⚠️ *Risk Warning: {warning_type}*\n\n"
            f"{details}"
        )
        
        alert = Alert(
            alert_type=AlertType.RISK_WARNING,
            message=message,
            priority='CRITICAL',
            data={'warning_type': warning_type, 'details': details}
        )
        
        await self.send_alert(alert)
    
    async def send_system_error(self, error_msg: str):
        message = f"❌ *System Error*\n\n{error_msg}"
        
        alert = Alert(
            alert_type=AlertType.SYSTEM_ERROR,
            message=message,
            priority='CRITICAL',
            data={'error': error_msg}
        )
        
        await self.send_alert(alert)
    
    async def send_price_alert(self, symbol: str, current_price: float, 
                              target_price: float, condition: str):
        message = (
            f"💰 *Price Alert: {symbol}*\n\n"
            f"Current: ${current_price:.2f}\n"
            f"Target: ${target_price:.2f}\n"
            f"Condition: {condition}"
        )
        
        alert = Alert(
            alert_type=AlertType.PRICE_ALERT,
            message=message,
            priority='NORMAL',
            data={'symbol': symbol, 'price': current_price}
        )
        
        await self.send_alert(alert)
    
    async def send_high_volatility_alert(self, symbol: str, volatility_level: float):
        message = (
            f"⚡ *High Volatility Detected*\n\n"
            f"Symbol: {symbol}\n"
            f"Volatility: {volatility_level:.2f}%\n"
            f"⚠️ Consider reducing position sizes"
        )
        
        alert = Alert(
            alert_type=AlertType.HIGH_VOLATILITY,
            message=message,
            priority='HIGH',
            data={'symbol': symbol, 'volatility': volatility_level}
        )
        
        await self.send_alert(alert)
    
    def get_alert_history(self, limit: int = 10) -> List[Dict]:
        recent_alerts = self.alert_history[-limit:]
        return [alert.to_dict() for alert in recent_alerts]
    
    def clear_alert_queue(self):
        self.alert_queue.clear()
        logger.info("Alert queue cleared")
    
    def enable(self):
        self.enabled = True
        logger.info("Alert system enabled")
    
    def disable(self):
        self.enabled = False
        logger.info("Alert system disabled")
    
    def get_stats(self) -> Dict:
        return {
            'total_alerts': len(self.alert_history),
            'queued_alerts': len(self.alert_queue),
            'enabled': self.enabled,
            'chat_ids_count': len(self.chat_ids)
        }
