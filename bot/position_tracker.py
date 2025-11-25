import asyncio
from datetime import datetime
from typing import Dict, Optional, Tuple
import pytz
from telegram.error import TimedOut
from bot.logger import setup_logger
from bot.database import Position, Trade
from bot.signal_session_manager import SignalSessionManager

logger = setup_logger('PositionTracker')

class PositionError(Exception):
    """Base exception for position tracking errors"""
    pass

class ValidationError(PositionError):
    """Position data validation error"""
    pass

def validate_position_data(user_id: int, trade_id: int, signal_type: str,
                          entry_price: float, stop_loss: float, take_profit: float) -> Tuple[bool, Optional[str]]:
    """Validate position data before processing"""
    try:
        if user_id is None or user_id <= 0:
            return False, f"Invalid user_id: {user_id}"
        
        if trade_id is None or trade_id <= 0:
            return False, f"Invalid trade_id: {trade_id}"
        
        if signal_type not in ['BUY', 'SELL']:
            return False, f"Invalid signal_type: {signal_type}. Must be 'BUY' or 'SELL'"
        
        if entry_price is None or entry_price <= 0:
            return False, f"Invalid entry_price: {entry_price}"
        
        if stop_loss is None or stop_loss <= 0:
            return False, f"Invalid stop_loss: {stop_loss}"
        
        if take_profit is None or take_profit <= 0:
            return False, f"Invalid take_profit: {take_profit}"
        
        if signal_type == 'BUY':
            if stop_loss >= entry_price:
                return False, f"BUY: stop_loss ({stop_loss}) must be < entry_price ({entry_price})"
            if take_profit <= entry_price:
                return False, f"BUY: take_profit ({take_profit}) must be > entry_price ({entry_price})"
        else:
            if stop_loss <= entry_price:
                return False, f"SELL: stop_loss ({stop_loss}) must be > entry_price ({entry_price})"
            if take_profit >= entry_price:
                return False, f"SELL: take_profit ({take_profit}) must be < entry_price ({entry_price})"
        
        sl_distance = abs(entry_price - stop_loss)
        tp_distance = abs(entry_price - take_profit)
        
        if sl_distance < 0.10:
            return False, f"SL distance too small: ${sl_distance:.2f}"
        
        if tp_distance < 0.10:
            return False, f"TP distance too small: ${tp_distance:.2f}"
        
        return True, None
        
    except Exception as e:
        return False, f"Validation error: {str(e)}"

class PositionTracker:
    MAX_SLIPPAGE_PIPS = 5.0
    
    def __init__(self, config, db_manager, risk_manager, alert_system=None, user_manager=None, 
                 chart_generator=None, market_data=None, telegram_app=None, signal_session_manager=None):
        self.config = config
        self.db = db_manager
        self.risk_manager = risk_manager
        self.alert_system = alert_system
        self.user_manager = user_manager
        self.chart_generator = chart_generator
        self.market_data = market_data
        self.telegram_app = telegram_app
        self.signal_session_manager = signal_session_manager
        self.active_positions = {}
        self.monitoring = False
    
    def set_signal_session_manager(self, signal_session_manager: SignalSessionManager):
        """Set signal session manager for dependency injection"""
        self.signal_session_manager = signal_session_manager
        logger.info("Signal session manager injected to position tracker")
    
    def check_slippage(self, expected_price: float, actual_price: float, operation: str, user_id: int, position_id: Optional[int] = None):
        """Check for excessive slippage and send alert if threshold is exceeded
        
        Args:
            expected_price: Expected price (signal price or TP/SL price)
            actual_price: Actual execution price
            operation: Operation type ('OPEN' or 'CLOSE')
            user_id: User ID
            position_id: Position ID (optional for OPEN)
        """
        try:
            slippage_dollars = abs(actual_price - expected_price)
            slippage_pips = slippage_dollars * self.config.XAUUSD_PIP_VALUE
            
            if slippage_pips > self.MAX_SLIPPAGE_PIPS:
                logger.warning(f"⚠️ High slippage detected on {operation} - Expected: ${expected_price:.2f}, Actual: ${actual_price:.2f}, Slippage: {slippage_pips:.1f} pips")
                
                if self.alert_system:
                    try:
                        import asyncio
                        asyncio.create_task(
                            self.alert_system.send_system_error(
                                f"⚠️ High Slippage Alert\n\n"
                                f"Operation: {operation}\n"
                                f"Position ID: {position_id if position_id else 'N/A'}\n"
                                f"User: {user_id}\n"
                                f"Expected Price: ${expected_price:.2f}\n"
                                f"Actual Price: ${actual_price:.2f}\n"
                                f"Slippage: {slippage_pips:.1f} pips (>${self.MAX_SLIPPAGE_PIPS} pips threshold)\n\n"
                                f"⚠️ Check market conditions and broker execution quality"
                            )
                        )
                    except Exception as e:
                        logger.error(f"Failed to send slippage alert: {e}")
            else:
                logger.debug(f"Slippage within threshold on {operation}: {slippage_pips:.1f} pips")
                
        except Exception as e:
            logger.error(f"Error checking slippage: {e}")
    
    def _normalize_position_dict(self, pos: Dict) -> Dict:
        """Ensure all required keys exist in position dict with safe defaults"""
        if 'original_sl' not in pos or pos['original_sl'] is None:
            pos['original_sl'] = pos.get('stop_loss', 0.0)
        if 'sl_adjustment_count' not in pos or pos['sl_adjustment_count'] is None:
            pos['sl_adjustment_count'] = 0
        if 'max_profit_reached' not in pos or pos['max_profit_reached'] is None:
            pos['max_profit_reached'] = 0.0
        if 'last_price_update' not in pos:
            pos['last_price_update'] = datetime.now(pytz.UTC)
        return pos
        
    async def add_position(self, user_id: int, trade_id: int, signal_type: str, entry_price: float,
                          stop_loss: float, take_profit: float):
        """Add position with comprehensive validation and error handling"""
        is_valid, error_msg = validate_position_data(user_id, trade_id, signal_type, entry_price, stop_loss, take_profit)
        if not is_valid:
            logger.error(f"Position validation failed: {error_msg}")
            logger.debug(f"Invalid data: user_id={user_id}, trade_id={trade_id}, signal={signal_type}, entry={entry_price}, sl={stop_loss}, tp={take_profit}")
            return None
        
        session = self.db.get_session()
        try:
            position = Position(
                user_id=user_id,
                trade_id=trade_id,
                ticker='XAUUSD',
                signal_type=signal_type,
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                current_price=entry_price,
                unrealized_pl=0.0,
                status='ACTIVE',
                original_sl=stop_loss,
                sl_adjustment_count=0,
                max_profit_reached=0.0,
                last_price_update=datetime.now(pytz.UTC)
            )
            session.add(position)
            session.flush()
            position_id = position.id
            session.commit()
            
            if user_id not in self.active_positions:
                self.active_positions[user_id] = {}
            
            self.active_positions[user_id][position_id] = {
                'trade_id': trade_id,
                'signal_type': signal_type,
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'take_profit': take_profit,
                'original_sl': stop_loss,
                'sl_adjustment_count': 0,
                'max_profit_reached': 0.0
            }
            
            logger.info(f"✅ Position added - User:{user_id} ID:{position_id} {signal_type} @${entry_price:.2f} SL:${stop_loss:.2f} TP:${take_profit:.2f}")
            
            if self.signal_session_manager:
                await self.signal_session_manager.update_session(
                    user_id,
                    position_id=position_id,
                    trade_id=trade_id
                )
            
            return position_id
            
        except Exception as e:
            logger.error(f"Database error adding position for User:{user_id} Trade:{trade_id}: {type(e).__name__}: {e}", exc_info=True)
            try:
                session.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback: {rollback_error}")
            return None
        finally:
            try:
                session.close()
            except Exception as close_error:
                logger.error(f"Error closing session: {close_error}")
    
    async def apply_dynamic_sl(self, user_id: int, position_id: int, current_price: float, unrealized_pl: float) -> tuple[bool, Optional[float]]:
        """Apply dynamic SL tightening when loss >= threshold
        
        Returns:
            tuple[bool, Optional[float]]: (sl_adjusted, new_stop_loss)
        """
        if user_id not in self.active_positions or position_id not in self.active_positions[user_id]:
            return False, None
        
        pos = self._normalize_position_dict(self.active_positions[user_id][position_id])
        signal_type = pos['signal_type']
        entry_price = pos['entry_price']
        stop_loss = pos['stop_loss']
        original_sl = pos.get('original_sl')
        
        if original_sl is None:
            original_sl = stop_loss
            pos['original_sl'] = stop_loss
            logger.warning(f"original_sl was None for position {position_id}, using current stop_loss")
        
        if unrealized_pl >= 0 or abs(unrealized_pl) < self.config.DYNAMIC_SL_LOSS_THRESHOLD:
            return False, None
        
        original_sl_distance = abs(entry_price - original_sl)
        new_sl_distance = original_sl_distance * self.config.DYNAMIC_SL_TIGHTENING_MULTIPLIER
        
        new_stop_loss = None
        sl_adjusted = False
        
        if signal_type == 'BUY':
            new_stop_loss = entry_price - new_sl_distance
            if new_stop_loss > stop_loss:
                pos['stop_loss'] = new_stop_loss
                pos['sl_adjustment_count'] = pos.get('sl_adjustment_count', 0) + 1
                sl_adjusted = True
                logger.info(f"🛡️ Dynamic SL activated! Loss ${abs(unrealized_pl):.2f} >= ${self.config.DYNAMIC_SL_LOSS_THRESHOLD}. SL tightened from ${stop_loss:.2f} → ${new_stop_loss:.2f} (protect capital)")
                
                if self.telegram_app:
                    try:
                        msg = (
                            f"🛡️ *Dynamic SL Activated*\n\n"
                            f"Position ID: {position_id}\n"
                            f"Type: {signal_type}\n"
                            f"Current Loss: ${abs(unrealized_pl):.2f}\n"
                            f"SL Updated: ${stop_loss:.2f} → ${new_stop_loss:.2f}\n"
                            f"Protection: Capital preservation mode"
                        )
                        await self.telegram_app.bot.send_message(chat_id=user_id, text=msg, parse_mode='Markdown')
                    except Exception as e:
                        logger.error(f"Failed to send dynamic SL notification: {e}")
        else:
            new_stop_loss = entry_price + new_sl_distance
            if new_stop_loss < stop_loss:
                pos['stop_loss'] = new_stop_loss
                pos['sl_adjustment_count'] = pos.get('sl_adjustment_count', 0) + 1
                sl_adjusted = True
                logger.info(f"🛡️ Dynamic SL activated! Loss ${abs(unrealized_pl):.2f} >= ${self.config.DYNAMIC_SL_LOSS_THRESHOLD}. SL tightened from ${stop_loss:.2f} → ${new_stop_loss:.2f} (protect capital)")
                
                if self.telegram_app:
                    try:
                        msg = (
                            f"🛡️ *Dynamic SL Activated*\n\n"
                            f"Position ID: {position_id}\n"
                            f"Type: {signal_type}\n"
                            f"Current Loss: ${abs(unrealized_pl):.2f}\n"
                            f"SL Updated: ${stop_loss:.2f} → ${new_stop_loss:.2f}\n"
                            f"Protection: Capital preservation mode"
                        )
                        await self.telegram_app.bot.send_message(chat_id=user_id, text=msg, parse_mode='Markdown')
                    except Exception as e:
                        logger.error(f"Failed to send dynamic SL notification: {e}")
        
        return sl_adjusted, new_stop_loss if sl_adjusted else None
    
    async def apply_trailing_stop(self, user_id: int, position_id: int, current_price: float, unrealized_pl: float) -> tuple[bool, Optional[float]]:
        """Apply trailing stop when profit >= threshold
        
        Returns:
            tuple[bool, Optional[float]]: (sl_adjusted, new_stop_loss)
        """
        if user_id not in self.active_positions or position_id not in self.active_positions[user_id]:
            return False, None
        
        pos = self._normalize_position_dict(self.active_positions[user_id][position_id])
        signal_type = pos['signal_type']
        stop_loss = pos['stop_loss']
        
        if unrealized_pl <= 0 or unrealized_pl < self.config.TRAILING_STOP_PROFIT_THRESHOLD:
            return False, None
        
        max_profit = pos.get('max_profit_reached', 0.0)
        if max_profit is None:
            max_profit = 0.0
        
        if unrealized_pl > max_profit:
            pos['max_profit_reached'] = unrealized_pl
        
        trailing_distance = self.config.TRAILING_STOP_DISTANCE_PIPS / self.config.XAUUSD_PIP_VALUE
        
        new_trailing_sl = None
        sl_adjusted = False
        
        if signal_type == 'BUY':
            new_trailing_sl = current_price - trailing_distance
            if new_trailing_sl > stop_loss:
                pos['stop_loss'] = new_trailing_sl
                pos['sl_adjustment_count'] = pos.get('sl_adjustment_count', 0) + 1
                sl_adjusted = True
                logger.info(f"💎 Trailing stop activated! Profit ${unrealized_pl:.2f}. SL moved to ${new_trailing_sl:.2f} (lock-in profit)")
                
                if self.telegram_app:
                    try:
                        msg = (
                            f"💎 *Trailing Stop Active*\n\n"
                            f"Position ID: {position_id}\n"
                            f"Type: {signal_type}\n"
                            f"Current Profit: ${unrealized_pl:.2f}\n"
                            f"Max Profit: ${pos['max_profit_reached']:.2f}\n"
                            f"SL Updated: ${stop_loss:.2f} → ${new_trailing_sl:.2f}\n"
                            f"Status: Profit locked-in!"
                        )
                        await self.telegram_app.bot.send_message(chat_id=user_id, text=msg, parse_mode='Markdown')
                    except Exception as e:
                        logger.error(f"Failed to send trailing stop notification: {e}")
        else:
            new_trailing_sl = current_price + trailing_distance
            if new_trailing_sl < stop_loss:
                pos['stop_loss'] = new_trailing_sl
                pos['sl_adjustment_count'] = pos.get('sl_adjustment_count', 0) + 1
                sl_adjusted = True
                logger.info(f"💎 Trailing stop activated! Profit ${unrealized_pl:.2f}. SL moved to ${new_trailing_sl:.2f} (lock-in profit)")
                
                if self.telegram_app:
                    try:
                        msg = (
                            f"💎 *Trailing Stop Active*\n\n"
                            f"Position ID: {position_id}\n"
                            f"Type: {signal_type}\n"
                            f"Current Profit: ${unrealized_pl:.2f}\n"
                            f"Max Profit: ${pos['max_profit_reached']:.2f}\n"
                            f"SL Updated: ${stop_loss:.2f} → ${new_trailing_sl:.2f}\n"
                            f"Status: Profit locked-in!"
                        )
                        await self.telegram_app.bot.send_message(chat_id=user_id, text=msg, parse_mode='Markdown')
                    except Exception as e:
                        logger.error(f"Failed to send trailing stop notification: {e}")
        
        return sl_adjusted, new_trailing_sl if sl_adjusted else None
    
    async def update_position(self, user_id: int, position_id: int, current_price: float) -> Optional[str]:
        """Update position with current price and apply dynamic SL/TP logic with validation"""
        try:
            if current_price is None or current_price <= 0:
                logger.warning(f"Invalid current_price for position {position_id}: {current_price}")
                return None
            
            if user_id not in self.active_positions or position_id not in self.active_positions[user_id]:
                logger.debug(f"Position {position_id} for User:{user_id} not found in active positions")
                return None
            
            pos = self.active_positions[user_id][position_id]
            
            try:
                signal_type = pos['signal_type']
                entry_price = pos['entry_price']
                stop_loss = pos['stop_loss']
                take_profit = pos['take_profit']
                
                if None in [signal_type, entry_price, stop_loss, take_profit]:
                    logger.error(f"Position {position_id} has None values: signal={signal_type}, entry={entry_price}, sl={stop_loss}, tp={take_profit}")
                    return None
                    
            except KeyError as e:
                logger.error(f"Missing required key in position {position_id}: {e}")
                return None
            
            unrealized_pl = self.risk_manager.calculate_pl(entry_price, current_price, signal_type)
        
            sl_adjusted = False
            dynamic_sl_applied = False
            
            try:
                dynamic_sl_applied, new_sl = await self.apply_dynamic_sl(user_id, position_id, current_price, unrealized_pl)
                if dynamic_sl_applied:
                    sl_adjusted = True
                    stop_loss = new_sl
            except Exception as e:
                logger.error(f"Error applying dynamic SL for position {position_id}: {e}")
            
            if not dynamic_sl_applied:
                try:
                    trailing_applied, new_sl = await self.apply_trailing_stop(user_id, position_id, current_price, unrealized_pl)
                    if trailing_applied:
                        sl_adjusted = True
                        stop_loss = new_sl
                except Exception as e:
                    logger.error(f"Error applying trailing stop for position {position_id}: {e}")
            
            stop_loss = pos['stop_loss']
            
            session = self.db.get_session()
            try:
                position = session.query(Position).filter(Position.id == position_id, Position.user_id == user_id).first()
                if not position:
                    logger.warning(f"Position {position_id} not found in database for User:{user_id}")
                    return None
                
                position.current_price = current_price
                position.unrealized_pl = unrealized_pl
                position.last_price_update = datetime.now(pytz.UTC)
                
                current_max_profit = position.max_profit_reached if position.max_profit_reached is not None else 0.0
                if unrealized_pl > 0 and unrealized_pl > current_max_profit:
                    position.max_profit_reached = unrealized_pl
                
                if sl_adjusted:
                    position.stop_loss = stop_loss
                    position.sl_adjustment_count = pos['sl_adjustment_count']
                
                session.commit()
            except Exception as e:
                logger.error(f"Database error updating position {position_id}: {type(e).__name__}: {e}", exc_info=True)
                try:
                    session.rollback()
                except Exception as rollback_error:
                    logger.error(f"Error during rollback: {rollback_error}")
            finally:
                try:
                    session.close()
                except Exception as close_error:
                    logger.error(f"Error closing session: {close_error}")
            
            hit_tp = False
            hit_sl = False
            
            try:
                if signal_type == 'BUY':
                    hit_tp = current_price >= take_profit
                    hit_sl = current_price <= stop_loss
                else:
                    hit_tp = current_price <= take_profit
                    hit_sl = current_price >= stop_loss
            except Exception as e:
                logger.error(f"Error checking TP/SL conditions: {e}")
                return None
            
            if hit_tp:
                await self.close_position(user_id, position_id, current_price, 'TP_HIT')
                return 'TP_HIT'
            elif hit_sl:
                reason = 'DYNAMIC_SL_HIT' if sl_adjusted else 'SL_HIT'
                await self.close_position(user_id, position_id, current_price, reason)
                return reason
            
            return None
            
        except Exception as e:
            logger.error(f"Critical error updating position {position_id} for User:{user_id}: {type(e).__name__}: {e}", exc_info=True)
            return None
    
    async def close_position(self, user_id: int, position_id: int, exit_price: float, reason: str):
        if user_id not in self.active_positions or position_id not in self.active_positions[user_id]:
            return
        
        pos = self.active_positions[user_id][position_id]
        trade_id = pos['trade_id']
        signal_type = pos['signal_type']
        entry_price = pos['entry_price']
        
        actual_pl = self.risk_manager.calculate_pl(entry_price, exit_price, signal_type)
        
        trade_result = None
        
        session = self.db.get_session()
        try:
            position = session.query(Position).filter(Position.id == position_id, Position.user_id == user_id).first()
            if position:
                position.status = 'CLOSED'
                position.current_price = exit_price
                position.unrealized_pl = actual_pl
                position.closed_at = datetime.now(pytz.UTC)
                
            trade = session.query(Trade).filter(Trade.id == trade_id, Trade.user_id == user_id).first()
            if trade:
                trade.status = 'CLOSED'
                trade.exit_price = exit_price
                trade.actual_pl = actual_pl
                trade.close_time = datetime.now(pytz.UTC)
                trade.result = 'WIN' if actual_pl > 0 else 'LOSS'
                trade_result = trade.result
                
            session.commit()
            
            del self.active_positions[user_id][position_id]
            if not self.active_positions[user_id]:
                del self.active_positions[user_id]
            
            logger.info(f"Position closed - User:{user_id} ID:{position_id} {reason} P/L:${actual_pl:.2f}")
            
            if self.telegram_app and self.chart_generator and self.market_data:
                try:
                    df_m1 = await self.market_data.get_historical_data('M1', 100)
                    
                    if df_m1 is not None and len(df_m1) >= 30:
                        exit_signal = {
                            'signal': signal_type,
                            'entry_price': entry_price,
                            'stop_loss': pos['stop_loss'],
                            'take_profit': pos['take_profit'],
                            'timeframe': 'M1'
                        }
                        
                        chart_path = await self.chart_generator.generate_chart_async(df_m1, exit_signal, 'M1')
                        
                        if chart_path and self.signal_session_manager:
                            await self.signal_session_manager.update_session(user_id, chart_path=chart_path)
                        
                        result_emoji = '✅' if trade_result == 'WIN' else '❌'
                        exit_label = "TRADE_EXIT" if reason == "TP_HIT" else "Trade LOSS"
                        
                        exit_msg = (
                            f"{result_emoji} *{exit_label}*\n\n"
                            f"Type: {signal_type}\n"
                            f"Entry: ${entry_price:.2f}\n"
                            f"Exit: ${exit_price:.2f}\n"
                            f"P/L: ${actual_pl:.2f}"
                        )
                        
                        try:
                            # Send text message with timeout protection (NO PHOTO for exit notification)
                            await asyncio.wait_for(
                                self.telegram_app.bot.send_message(
                                    chat_id=user_id,
                                    text=exit_msg,
                                    parse_mode='Markdown'
                                ),
                                timeout=10.0
                            )
                            logger.info(f"Exit notification sent to user {user_id} - TEXT ONLY (no photo)")
                            
                            # Auto-delete exit chart if it was generated (tidak dikirim ke user)
                            if chart_path and self.config.CHART_AUTO_DELETE:
                                await asyncio.sleep(1)
                                self.chart_generator.delete_chart(chart_path)
                                logger.info(f"Auto-deleted unused exit chart: {chart_path}")
                        except asyncio.TimeoutError:
                            logger.error(f"Failed to send exit notification to user {user_id}: asyncio.TimeoutError after 10s")
                            # Fallback: send simple text without markdown
                            try:
                                simple_msg = f"{result_emoji} {exit_label}\nType: {signal_type}\nEntry: ${entry_price:.2f}\nExit: ${exit_price:.2f}\nP/L: ${actual_pl:.2f}"
                                await asyncio.wait_for(
                                    self.telegram_app.bot.send_message(chat_id=user_id, text=simple_msg),
                                    timeout=5.0
                                )
                                logger.info(f"Fallback notification sent to user {user_id}")
                            except Exception as fallback_error:
                                logger.error(f"Fallback notification also failed: {fallback_error}")
                        except TimedOut as telegram_err:
                            # Catch telegram.error.TimedOut explicitly
                            logger.error(f"Failed to send exit notification to user {user_id}: telegram.error.TimedOut")
                            # Fallback: send simple text without markdown
                            try:
                                simple_msg = f"{result_emoji} {exit_label}\nType: {signal_type}\nEntry: ${entry_price:.2f}\nExit: ${exit_price:.2f}\nP/L: ${actual_pl:.2f}"
                                await asyncio.wait_for(
                                    self.telegram_app.bot.send_message(chat_id=user_id, text=simple_msg),
                                    timeout=5.0
                                )
                                logger.info(f"Fallback notification sent to user {user_id}")
                            except Exception as fallback_error:
                                logger.error(f"Fallback notification also failed: {fallback_error}")
                        except Exception as telegram_err:
                            # Catch other Telegram errors
                            logger.error(f"Failed to send exit notification to user {user_id}: {telegram_err}")
                    else:
                        logger.warning(f"Not enough candles for exit chart: {len(df_m1) if df_m1 else 0}")
                        
                        if self.alert_system and trade_result:
                            await self.alert_system.send_trade_exit_alert({
                                'signal_type': signal_type,
                                'entry_price': entry_price,
                                'exit_price': exit_price,
                                'actual_pl': actual_pl
                            }, trade_result)
                except Exception as e:
                    logger.error(f"Error sending exit chart: {e}")
                    
                    if self.alert_system and trade_result:
                        await self.alert_system.send_trade_exit_alert({
                            'signal_type': signal_type,
                            'entry_price': entry_price,
                            'exit_price': exit_price,
                            'actual_pl': actual_pl
                        }, trade_result)
            elif self.alert_system and trade_result:
                await self.alert_system.send_trade_exit_alert({
                    'signal_type': signal_type,
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'actual_pl': actual_pl
                }, trade_result)
            
            if self.user_manager:
                self.user_manager.update_user_stats(user_id, actual_pl)
            
            if self.signal_session_manager:
                await self.signal_session_manager.end_session(user_id, reason)
            
        except Exception as e:
            logger.error(f"Error closing position {position_id}: {e}", exc_info=True)
            try:
                session.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during rollback: {rollback_error}")
        finally:
            try:
                session.close()
            except Exception as close_error:
                logger.error(f"Error closing session: {close_error}")
    
    async def monitor_active_positions(self):
        """Monitor all active positions and apply dynamic SL/TP
        
        This method is called by the scheduler every 10 seconds.
        Returns a list of updated positions.
        """
        if not self.active_positions:
            return []
        
        if not self.market_data:
            logger.warning("Market data not available for position monitoring")
            return []
        
        updated_positions = []
        
        try:
            current_price = await self.market_data.get_current_price()
            
            if not current_price:
                logger.warning("No current price available for position monitoring")
                return []
            
            for user_id in list(self.active_positions.keys()):
                for position_id in list(self.active_positions[user_id].keys()):
                    try:
                        result = await self.update_position(user_id, position_id, current_price)
                        if result:
                            updated_positions.append({
                                'user_id': user_id,
                                'position_id': position_id,
                                'result': result,
                                'price': current_price
                            })
                            logger.info(f"Position {position_id} User:{user_id} closed: {result} at ${current_price:.2f}")
                    except Exception as e:
                        logger.error(f"Error monitoring position {position_id} for user {user_id}: {e}")
            
            return updated_positions
            
        except Exception as e:
            logger.error(f"Error in monitor_active_positions: {e}")
            return []
    
    async def monitor_positions(self, market_data_client):
        tick_queue = await market_data_client.subscribe_ticks('position_tracker')
        logger.info("Position tracker monitoring started")
        
        self.monitoring = True
        
        try:
            while self.monitoring:
                try:
                    tick = await tick_queue.get()
                    
                    if self.active_positions:
                        mid_price = tick['quote']
                        
                        for user_id in list(self.active_positions.keys()):
                            for position_id in list(self.active_positions[user_id].keys()):
                                result = await self.update_position(user_id, position_id, mid_price)
                                if result:
                                    logger.info(f"Position {position_id} User:{user_id} closed: {result}")
                    
                except Exception as e:
                    logger.error(f"Error processing tick dalam position monitoring: {e}")
                    await asyncio.sleep(1)
                    
        finally:
            await market_data_client.unsubscribe_ticks('position_tracker')
            logger.info("Position tracker monitoring stopped")
    
    def stop_monitoring(self):
        self.monitoring = False
        logger.info("Position monitoring stopped")
    
    def get_active_positions(self, user_id: Optional[int] = None) -> Dict:
        if user_id is not None:
            return self.active_positions.get(user_id, {}).copy()
        return self.active_positions.copy()
    
    def has_active_position(self, user_id: int) -> bool:
        if self.signal_session_manager:
            return self.signal_session_manager.has_active_session(user_id)
        return user_id in self.active_positions and len(self.active_positions[user_id]) > 0
    
    def get_active_position_count(self, user_id: Optional[int] = None) -> int:
        if user_id is not None:
            return len(self.active_positions.get(user_id, {}))
        return sum(len(positions) for positions in self.active_positions.values())
