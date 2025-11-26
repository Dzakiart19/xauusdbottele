import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import mplfinance as mpf
from datetime import datetime
from typing import Optional, Tuple
import json
import asyncio
import gc
from concurrent.futures import ThreadPoolExecutor
from bot.logger import setup_logger

logger = setup_logger('ChartGenerator')

class ChartError(Exception):
    """Base exception for chart generation errors"""
    pass

class DataValidationError(ChartError):
    """Chart data validation error"""
    pass

def validate_chart_data(df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
    """Validate DataFrame for chart generation"""
    try:
        if df is None:
            return False, "DataFrame is None"
        
        if not isinstance(df, pd.DataFrame):
            return False, f"Invalid type: {type(df)}. Expected pandas.DataFrame"
        
        if len(df) < 10:
            return False, f"Insufficient data: {len(df)} rows (minimum 10 required)"
        
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, f"Missing required columns: {missing_cols}"
        
        for col in ['open', 'high', 'low', 'close']:
            if df[col].isnull().any():
                null_count = df[col].isnull().sum()
                return False, f"Column '{col}' contains {null_count} null values"
            
            if (df[col] <= 0).any():
                return False, f"Column '{col}' contains non-positive values"
        
        if (df['high'] < df['low']).any():
            invalid_count = (df['high'] < df['low']).sum()
            return False, f"Found {invalid_count} candles where high < low"
        
        if not isinstance(df.index, pd.DatetimeIndex):
            if 'timestamp' not in df.columns:
                return False, f"Index must be DatetimeIndex or DataFrame must have 'timestamp' column. Got {type(df.index)} with columns: {df.columns.tolist()}"
        
        return True, None
        
    except Exception as e:
        return False, f"Validation error: {str(e)}"

class ChartGenerator:
    def __init__(self, config):
        self.config = config
        self.chart_dir = 'charts'
        os.makedirs(self.chart_dir, exist_ok=True)
        max_workers = 1 if self.config.FREE_TIER_MODE else 2
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="chart_gen")
        self._shutdown_requested = False
        self._pending_charts: set = set()
        self._chart_lock = asyncio.Lock()
        logger.info(f"ChartGenerator initialized dengan max_workers={max_workers} (FREE_TIER_MODE={self.config.FREE_TIER_MODE})")
    
    def generate_chart(self, df: pd.DataFrame, signal: Optional[dict] = None,
                      timeframe: str = 'M1') -> Optional[str]:
        """Generate chart with comprehensive validation and error handling"""
        chart_path = None
        try:
            is_valid, error_msg = validate_chart_data(df)
            if not is_valid:
                logger.warning(f"Chart data validation failed: {error_msg}")
                return None
            
            df_copy = df.copy()
            
            if not isinstance(df_copy.index, pd.DatetimeIndex):
                if 'timestamp' in df_copy.columns:
                    try:
                        df_copy['timestamp'] = pd.to_datetime(df_copy['timestamp'])
                        df_copy.set_index('timestamp', inplace=True)
                    except Exception as e:
                        logger.error(f"Error converting timestamp to DatetimeIndex: {e}")
                        return None
                else:
                    logger.error("DataFrame has no DatetimeIndex and no 'timestamp' column")
                    return None
            
            required_cols = ['open', 'high', 'low', 'close', 'volume']
            if not all(col in df_copy.columns for col in required_cols):
                logger.error(f"Missing required columns. Have: {df_copy.columns.tolist()}, Need: {required_cols}")
                return None
            
            addplot = []
            
            from bot.indicators import IndicatorEngine
            indicator_engine = IndicatorEngine(self.config)
            
            ema_5 = df_copy['close'].ewm(span=self.config.EMA_PERIODS[0], adjust=False).mean().bfill().ffill()
            ema_10 = df_copy['close'].ewm(span=self.config.EMA_PERIODS[1], adjust=False).mean().bfill().ffill()
            ema_20 = df_copy['close'].ewm(span=self.config.EMA_PERIODS[2], adjust=False).mean().bfill().ffill()
            
            addplot.append(mpf.make_addplot(ema_5, color='blue', width=1.5, panel=0, label=f'EMA {self.config.EMA_PERIODS[0]}'))
            addplot.append(mpf.make_addplot(ema_10, color='orange', width=1.5, panel=0, label=f'EMA {self.config.EMA_PERIODS[1]}'))
            addplot.append(mpf.make_addplot(ema_20, color='red', width=1.5, panel=0, label=f'EMA {self.config.EMA_PERIODS[2]}'))
            
            delta = df_copy['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=self.config.RSI_PERIOD).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=self.config.RSI_PERIOD).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            rsi = rsi.fillna(50)
            
            addplot.append(mpf.make_addplot(rsi, color='purple', width=1.5, panel=1, ylabel='RSI', ylim=(0, 100)))
            
            rsi_70 = pd.Series([70] * len(df_copy), index=df_copy.index)
            rsi_30 = pd.Series([30] * len(df_copy), index=df_copy.index)
            addplot.append(mpf.make_addplot(rsi_70, color='red', width=0.8, panel=1, linestyle='--', alpha=0.5))
            addplot.append(mpf.make_addplot(rsi_30, color='green', width=0.8, panel=1, linestyle='--', alpha=0.5))
            
            low_min = df_copy['low'].rolling(window=self.config.STOCH_K_PERIOD).min()
            high_max = df_copy['high'].rolling(window=self.config.STOCH_K_PERIOD).max()
            stoch_k = 100 * (df_copy['close'] - low_min) / (high_max - low_min)
            stoch_k = stoch_k.rolling(window=self.config.STOCH_SMOOTH_K).mean()
            stoch_d = stoch_k.rolling(window=self.config.STOCH_D_PERIOD).mean()
            stoch_k = stoch_k.fillna(50)
            stoch_d = stoch_d.fillna(50)
            
            addplot.append(mpf.make_addplot(stoch_k, color='blue', width=1.5, panel=2, ylabel='Stochastic', ylim=(0, 100)))
            addplot.append(mpf.make_addplot(stoch_d, color='orange', width=1.5, panel=2))
            
            stoch_80 = pd.Series([80] * len(df_copy), index=df_copy.index)
            stoch_20 = pd.Series([20] * len(df_copy), index=df_copy.index)
            addplot.append(mpf.make_addplot(stoch_80, color='red', width=0.8, panel=2, linestyle='--', alpha=0.5))
            addplot.append(mpf.make_addplot(stoch_20, color='green', width=0.8, panel=2, linestyle='--', alpha=0.5))
            
            trf_upper, trf_lower, trf_trend = indicator_engine.calculate_twin_range_filter(df_copy, period=27, multiplier=2.0)
            trf_upper = trf_upper.bfill().ffill()
            trf_lower = trf_lower.bfill().ffill()
            addplot.append(mpf.make_addplot(trf_upper, color='cyan', width=1.5, panel=0, linestyle='--', alpha=0.7, label='TRF Upper'))
            addplot.append(mpf.make_addplot(trf_lower, color='magenta', width=1.5, panel=0, linestyle='--', alpha=0.7, label='TRF Lower'))
            
            cerebr_value, cerebr_signal, cerebr_bias = indicator_engine.calculate_market_bias_cerebr(df_copy, period=60, smoothing=10)
            cerebr_value = cerebr_value.fillna(50)
            cerebr_signal = cerebr_signal.fillna(50)
            addplot.append(mpf.make_addplot(cerebr_value, color='teal', width=1.5, panel=3, ylabel='CEREBR', ylim=(0, 100)))
            addplot.append(mpf.make_addplot(cerebr_signal, color='orange', width=1.0, panel=3, linestyle='--', alpha=0.7))
            cerebr_60 = pd.Series([60] * len(df_copy), index=df_copy.index)
            cerebr_40 = pd.Series([40] * len(df_copy), index=df_copy.index)
            addplot.append(mpf.make_addplot(cerebr_60, color='red', width=0.8, panel=3, linestyle=':', alpha=0.5))
            addplot.append(mpf.make_addplot(cerebr_40, color='green', width=0.8, panel=3, linestyle=':', alpha=0.5))
            
            if signal:
                entry_price = signal.get('entry_price')
                stop_loss = signal.get('stop_loss')
                take_profit = signal.get('take_profit')
                signal_type = signal.get('signal')
                
                if entry_price and signal_type:
                    marker_color = 'lime' if signal_type == 'BUY' else 'red'
                    marker_symbol = '^' if signal_type == 'BUY' else 'v'
                    
                    import numpy as np
                    marker_series = pd.Series(index=df_copy.index, data=[np.nan] * len(df_copy))
                    marker_series.iloc[-1] = entry_price
                    
                    addplot.append(
                        mpf.make_addplot(marker_series, type='scatter', 
                                        markersize=150, marker=marker_symbol, 
                                        color=marker_color, panel=0)
                    )
                
                if stop_loss:
                    sl_line = pd.Series(index=df_copy.index, data=[stop_loss] * len(df_copy))
                    addplot.append(
                        mpf.make_addplot(sl_line, type='line', 
                                        color='darkred', linestyle='--', 
                                        width=2.5, panel=0, 
                                        alpha=0.8)
                    )
                
                if take_profit:
                    tp_line = pd.Series(index=df_copy.index, data=[take_profit] * len(df_copy))
                    addplot.append(
                        mpf.make_addplot(tp_line, type='line', 
                                        color='darkgreen', linestyle='--', 
                                        width=2.5, panel=0, 
                                        alpha=0.8)
                    )
            
            mc = mpf.make_marketcolors(
                up='lime', down='red',
                edge='inherit',
                wick='inherit',
                volume='in'
            )
            
            style = mpf.make_mpf_style(
                marketcolors=mc,
                gridstyle=':',
                gridcolor='gray',
                gridaxis='both',
                y_on_right=False,
                rc={'font.size': 10}
            )
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'xauusd_{timeframe}_{timestamp}.png'
            filepath = os.path.join(self.chart_dir, filename)
            
            title = f'XAUUSD {timeframe} - Analisis Teknikal'
            if signal:
                title += f" ({signal.get('signal', 'SIGNAL')} Signal)"
            
            try:
                fig, axes = mpf.plot(
                    df_copy,
                    type='candle',
                    style=style,
                    title=title,
                    ylabel='Price (USD)',
                    volume=True,
                    addplot=addplot if addplot else None,
                    savefig=filepath,
                    figsize=(14, 14),
                    returnfig=True,
                    panel_ratios=(3, 1, 1, 1)
                )
                chart_path = filepath
                logger.info(f"✅ Chart generated successfully: {filepath} ({len(df_copy)} candles)")
                
            except Exception as plot_error:
                logger.error(f"Plotting error: {type(plot_error).__name__}: {plot_error}")
                return None
            finally:
                try:
                    import matplotlib.pyplot as plt
                    plt.close('all')
                    gc.collect()
                except Exception as cleanup_error:
                    logger.warning(f"Error during matplotlib cleanup: {cleanup_error}")
            
            return chart_path
            
        except MemoryError:
            logger.error("Memory error generating chart - insufficient memory")
            try:
                gc.collect()
            except Exception:
                pass
            return None
        except Exception as e:
            logger.error(f"Unexpected error generating chart: {type(e).__name__}: {e}", exc_info=True)
            return None
    
    async def generate_chart_async(self, df: pd.DataFrame, signal: Optional[dict] = None,
                                   timeframe: str = 'M1') -> Optional[str]:
        """Generate chart asynchronously with timeout and error handling"""
        try:
            if df is None or len(df) < 10:
                logger.warning(f"Insufficient data for async chart: {len(df) if df is not None else 0} candles")
                return None
            
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.executor,
                self.generate_chart,
                df,
                signal,
                timeframe
            )
            return result
        except asyncio.CancelledError:
            logger.info("Chart generation cancelled")
            return None
        except Exception as e:
            logger.error(f"Error in async chart generation: {type(e).__name__}: {e}", exc_info=True)
            return None
    
    def delete_chart(self, filepath: str):
        """Delete chart file with validation and graceful error handling"""
        try:
            if not filepath or not isinstance(filepath, str):
                logger.warning(f"Invalid filepath for deletion: {filepath}")
                return False
            
            if not os.path.exists(filepath):
                logger.debug(f"Chart file does not exist (already deleted): {filepath}")
                return True
            
            if not filepath.endswith('.png'):
                logger.warning(f"Attempted to delete non-PNG file: {filepath}")
                return False
            
            os.remove(filepath)
            logger.debug(f"Chart deleted: {filepath}")
            return True
        except FileNotFoundError:
            logger.debug(f"Chart file not found (already deleted): {filepath}")
            return True
        except PermissionError as e:
            logger.error(f"Permission denied deleting chart {filepath}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error deleting chart {filepath}: {type(e).__name__}: {e}")
            return False
    
    def shutdown(self):
        """Graceful shutdown dengan cleanup semua pending charts"""
        try:
            logger.info("Shutting down ChartGenerator executor...")
            self._shutdown_requested = True
            
            self.executor.shutdown(wait=True, cancel_futures=True)
            
            self._cleanup_pending_charts()
            
            logger.info("ChartGenerator executor shut down successfully")
        except Exception as e:
            logger.error(f"Error shutting down executor: {e}")
    
    def _cleanup_pending_charts(self):
        """Cleanup semua pending chart files"""
        try:
            cleaned = 0
            for chart_path in list(self._pending_charts):
                try:
                    if os.path.exists(chart_path):
                        os.remove(chart_path)
                        cleaned += 1
                except Exception as e:
                    logger.warning(f"Failed to cleanup pending chart {chart_path}: {e}")
            
            self._pending_charts.clear()
            
            if cleaned > 0:
                logger.info(f"🗑️ Cleaned up {cleaned} pending chart files")
        except Exception as e:
            logger.error(f"Error cleaning up pending charts: {e}")
    
    def track_chart(self, filepath: str):
        """Track chart file untuk cleanup nanti"""
        if filepath:
            self._pending_charts.add(filepath)
    
    def untrack_chart(self, filepath: str):
        """Untrack chart file setelah berhasil dikirim"""
        self._pending_charts.discard(filepath)
    
    async def cleanup_orphan_charts(self, max_age_minutes: int = 30) -> int:
        """Cleanup orphan chart files yang lebih tua dari max_age"""
        try:
            now = datetime.now()
            cleaned = 0
            
            async with self._chart_lock:
                for filename in os.listdir(self.chart_dir):
                    filepath = os.path.join(self.chart_dir, filename)
                    if os.path.isfile(filepath) and filename.endswith('.png'):
                        try:
                            file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                            age_minutes = (now - file_time).total_seconds() / 60
                            
                            if age_minutes > max_age_minutes:
                                os.remove(filepath)
                                self._pending_charts.discard(filepath)
                                cleaned += 1
                        except Exception as e:
                            logger.warning(f"Error checking chart age {filepath}: {e}")
            
            if cleaned > 0:
                logger.info(f"🗑️ Cleaned up {cleaned} orphan chart files (older than {max_age_minutes}min)")
            
            return cleaned
        except Exception as e:
            logger.error(f"Error cleaning orphan charts: {e}")
            return 0
    
    def cleanup_old_charts(self, days: int = 7):
        """Cleanup chart files yang lebih tua dari X hari"""
        try:
            now = datetime.now()
            cleaned = 0
            for filename in os.listdir(self.chart_dir):
                filepath = os.path.join(self.chart_dir, filename)
                if os.path.isfile(filepath):
                    try:
                        file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                        if (now - file_time).days > days:
                            os.remove(filepath)
                            self._pending_charts.discard(filepath)
                            cleaned += 1
                            logger.debug(f"Deleted old chart: {filename}")
                    except FileNotFoundError:
                        pass
                    except Exception as e:
                        logger.warning(f"Error deleting chart {filename}: {e}")
            
            if cleaned > 0:
                logger.info(f"🗑️ Cleaned up {cleaned} old chart files (older than {days} days)")
        except Exception as e:
            logger.error(f"Error cleaning up charts: {e}")
    
    def get_stats(self) -> dict:
        """Dapatkan statistik chart generator"""
        try:
            chart_count = len([f for f in os.listdir(self.chart_dir) if f.endswith('.png')])
            pending_count = len(self._pending_charts)
            
            return {
                'chart_dir': self.chart_dir,
                'total_charts': chart_count,
                'pending_charts': pending_count,
                'shutdown_requested': self._shutdown_requested,
                'free_tier_mode': self.config.FREE_TIER_MODE
            }
        except Exception as e:
            logger.error(f"Error getting chart stats: {e}")
            return {'error': str(e)}
