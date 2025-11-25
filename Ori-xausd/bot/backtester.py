import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import pytz
from bot.logger import setup_logger
from bot.indicators import IndicatorEngine
from bot.strategy import TradingStrategy

logger = setup_logger('Backtester')

class BacktestTrade:
    def __init__(self, signal_type: str, entry_price: float, entry_time: datetime,
                 stop_loss: float, take_profit: float):
        self.signal_type = signal_type
        self.entry_price = entry_price
        self.entry_time = entry_time
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.exit_price = None
        self.exit_time = None
        self.profit_loss = 0.0
        self.result = None
        self.duration = None
    
    def close_trade(self, exit_price: float, exit_time: datetime, pip_value: float = 10.0):
        self.exit_price = exit_price
        self.exit_time = exit_time
        self.duration = (exit_time - self.entry_time).total_seconds() / 60
        
        if self.signal_type == 'BUY':
            price_diff = exit_price - self.entry_price
        else:
            price_diff = self.entry_price - exit_price
        
        pips = price_diff * pip_value
        self.profit_loss = pips * 0.01
        
        if exit_price >= self.take_profit and self.signal_type == 'BUY':
            self.result = 'WIN'
        elif exit_price <= self.take_profit and self.signal_type == 'SELL':
            self.result = 'WIN'
        elif exit_price <= self.stop_loss and self.signal_type == 'BUY':
            self.result = 'LOSS'
        elif exit_price >= self.stop_loss and self.signal_type == 'SELL':
            self.result = 'LOSS'
        else:
            self.result = 'UNKNOWN'
    
    def to_dict(self) -> Dict:
        return {
            'signal_type': self.signal_type,
            'entry_price': self.entry_price,
            'entry_time': self.entry_time.isoformat() if self.entry_time else None,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'exit_price': self.exit_price,
            'exit_time': self.exit_time.isoformat() if self.exit_time else None,
            'profit_loss': self.profit_loss,
            'result': self.result,
            'duration': self.duration
        }

class BacktestResult:
    def __init__(self):
        self.trades: List[BacktestTrade] = []
        self.initial_balance = 10000.0
        self.final_balance = 10000.0
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.win_rate = 0.0
        self.total_profit = 0.0
        self.total_loss = 0.0
        self.net_profit = 0.0
        self.profit_factor = 0.0
        self.max_drawdown = 0.0
        self.max_consecutive_wins = 0
        self.max_consecutive_losses = 0
        self.avg_win = 0.0
        self.avg_loss = 0.0
        self.avg_trade_duration = 0.0
        self.sharpe_ratio = 0.0
    
    def calculate_metrics(self):
        if not self.trades:
            return
        
        self.total_trades = len(self.trades)
        
        wins = [t for t in self.trades if t.result == 'WIN']
        losses = [t for t in self.trades if t.result == 'LOSS']
        
        self.winning_trades = len(wins)
        self.losing_trades = len(losses)
        self.win_rate = (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0
        
        self.total_profit = sum([t.profit_loss for t in wins])
        self.total_loss = abs(sum([t.profit_loss for t in losses]))
        self.net_profit = self.total_profit - self.total_loss
        
        self.profit_factor = (self.total_profit / self.total_loss) if self.total_loss > 0 else 0
        
        self.avg_win = (self.total_profit / self.winning_trades) if self.winning_trades > 0 else 0
        self.avg_loss = (self.total_loss / self.losing_trades) if self.losing_trades > 0 else 0
        
        durations = [t.duration for t in self.trades if t.duration]
        self.avg_trade_duration = np.mean(durations) if durations else 0
        
        self._calculate_drawdown()
        self._calculate_consecutive_streaks()
        self._calculate_sharpe_ratio()
        
        self.final_balance = self.initial_balance + self.net_profit
    
    def _calculate_drawdown(self):
        if not self.trades:
            return
        
        balance = self.initial_balance
        peak = balance
        max_dd = 0
        
        for trade in self.trades:
            balance += trade.profit_loss
            
            if balance > peak:
                peak = balance
            
            drawdown = ((peak - balance) / peak * 100) if peak > 0 else 0
            
            if drawdown > max_dd:
                max_dd = drawdown
        
        self.max_drawdown = max_dd
    
    def _calculate_consecutive_streaks(self):
        if not self.trades:
            return
        
        max_wins = 0
        max_losses = 0
        current_wins = 0
        current_losses = 0
        
        for trade in self.trades:
            if trade.result == 'WIN':
                current_wins += 1
                current_losses = 0
                max_wins = max(max_wins, current_wins)
            elif trade.result == 'LOSS':
                current_losses += 1
                current_wins = 0
                max_losses = max(max_losses, current_losses)
        
        self.max_consecutive_wins = max_wins
        self.max_consecutive_losses = max_losses
    
    def _calculate_sharpe_ratio(self):
        if not self.trades:
            return
        
        returns = [t.profit_loss / self.initial_balance for t in self.trades]
        
        if len(returns) < 2:
            self.sharpe_ratio = 0
            return
        
        avg_return = np.mean(returns)
        std_return = np.std(returns)
        
        if std_return > 0:
            self.sharpe_ratio = (avg_return / std_return) * np.sqrt(252)
        else:
            self.sharpe_ratio = 0
    
    def to_dict(self) -> Dict:
        return {
            'initial_balance': self.initial_balance,
            'final_balance': self.final_balance,
            'net_profit': self.net_profit,
            'total_trades': self.total_trades,
            'winning_trades': self.winning_trades,
            'losing_trades': self.losing_trades,
            'win_rate': self.win_rate,
            'total_profit': self.total_profit,
            'total_loss': self.total_loss,
            'profit_factor': self.profit_factor,
            'max_drawdown': self.max_drawdown,
            'max_consecutive_wins': self.max_consecutive_wins,
            'max_consecutive_losses': self.max_consecutive_losses,
            'avg_win': self.avg_win,
            'avg_loss': self.avg_loss,
            'avg_trade_duration': self.avg_trade_duration,
            'sharpe_ratio': self.sharpe_ratio
        }

class Backtester:
    def __init__(self, config):
        self.config = config
        self.indicator_engine = IndicatorEngine(config)
        self.strategy = TradingStrategy(config)
        logger.info("Backtester initialized")
    
    def run_backtest(self, df: pd.DataFrame, initial_balance: float = 10000.0) -> BacktestResult:
        logger.info(f"Starting backtest with {len(df)} candles")
        
        result = BacktestResult()
        result.initial_balance = initial_balance
        
        open_trades: List[BacktestTrade] = []
        
        for i in range(50, len(df)):
            df_slice = df.iloc[:i+1]
            current_candle = df.iloc[i]
            
            indicators = self.indicator_engine.get_indicators(df_slice)
            
            if not indicators:
                continue
            
            signal = self.strategy.detect_signal(indicators, 'M1')
            
            if signal and len(open_trades) == 0:
                trade = BacktestTrade(
                    signal_type=signal['signal'],
                    entry_price=signal['entry_price'],
                    entry_time=current_candle.name if isinstance(current_candle.name, datetime) else datetime.now(),
                    stop_loss=signal['stop_loss'],
                    take_profit=signal['take_profit']
                )
                open_trades.append(trade)
                logger.debug(f"Opened {trade.signal_type} trade at {trade.entry_price}")
            
            for trade in open_trades[:]:
                high = current_candle['high']
                low = current_candle['low']
                close = current_candle['close']
                
                if trade.signal_type == 'BUY':
                    if low <= trade.stop_loss:
                        trade.close_trade(
                            trade.stop_loss,
                            current_candle.name if isinstance(current_candle.name, datetime) else datetime.now(),
                            self.config.XAUUSD_PIP_VALUE
                        )
                        result.trades.append(trade)
                        open_trades.remove(trade)
                        logger.debug(f"Closed BUY trade (SL hit) - P/L: {trade.profit_loss}")
                    
                    elif high >= trade.take_profit:
                        trade.close_trade(
                            trade.take_profit,
                            current_candle.name if isinstance(current_candle.name, datetime) else datetime.now(),
                            self.config.XAUUSD_PIP_VALUE
                        )
                        result.trades.append(trade)
                        open_trades.remove(trade)
                        logger.debug(f"Closed BUY trade (TP hit) - P/L: {trade.profit_loss}")
                
                else:
                    if high >= trade.stop_loss:
                        trade.close_trade(
                            trade.stop_loss,
                            current_candle.name if isinstance(current_candle.name, datetime) else datetime.now(),
                            self.config.XAUUSD_PIP_VALUE
                        )
                        result.trades.append(trade)
                        open_trades.remove(trade)
                        logger.debug(f"Closed SELL trade (SL hit) - P/L: {trade.profit_loss}")
                    
                    elif low <= trade.take_profit:
                        trade.close_trade(
                            trade.take_profit,
                            current_candle.name if isinstance(current_candle.name, datetime) else datetime.now(),
                            self.config.XAUUSD_PIP_VALUE
                        )
                        result.trades.append(trade)
                        open_trades.remove(trade)
                        logger.debug(f"Closed SELL trade (TP hit) - P/L: {trade.profit_loss}")
        
        for trade in open_trades:
            last_candle = df.iloc[-1]
            trade.close_trade(
                last_candle['close'],
                last_candle.name if isinstance(last_candle.name, datetime) else datetime.now(),
                self.config.XAUUSD_PIP_VALUE
            )
            result.trades.append(trade)
        
        result.calculate_metrics()
        
        logger.info(f"Backtest completed: {result.total_trades} trades, Win Rate: {result.win_rate:.1f}%")
        
        return result
    
    def format_backtest_report(self, result: BacktestResult) -> str:
        report = "📊 *Backtest Results*\n\n"
        report += f"*Performance:*\n"
        report += f"Initial Balance: ${result.initial_balance:,.2f}\n"
        report += f"Final Balance: ${result.final_balance:,.2f}\n"
        report += f"Net Profit: ${result.net_profit:,.2f}\n"
        report += f"Return: {(result.net_profit/result.initial_balance*100):.2f}%\n\n"
        
        report += f"*Statistics:*\n"
        report += f"Total Trades: {result.total_trades}\n"
        report += f"Wins: {result.winning_trades} ✅\n"
        report += f"Losses: {result.losing_trades} ❌\n"
        report += f"Win Rate: {result.win_rate:.1f}%\n\n"
        
        report += f"*Profit Analysis:*\n"
        report += f"Total Profit: ${result.total_profit:.2f}\n"
        report += f"Total Loss: ${result.total_loss:.2f}\n"
        report += f"Profit Factor: {result.profit_factor:.2f}\n"
        report += f"Avg Win: ${result.avg_win:.2f}\n"
        report += f"Avg Loss: ${result.avg_loss:.2f}\n\n"
        
        report += f"*Risk Metrics:*\n"
        report += f"Max Drawdown: {result.max_drawdown:.2f}%\n"
        report += f"Sharpe Ratio: {result.sharpe_ratio:.2f}\n"
        report += f"Max Consecutive Wins: {result.max_consecutive_wins}\n"
        report += f"Max Consecutive Losses: {result.max_consecutive_losses}\n"
        report += f"Avg Trade Duration: {result.avg_trade_duration:.1f} min\n"
        
        return report
