from typing import Dict, List, Optional
from dataclasses import dataclass
from bot.logger import setup_logger

logger = setup_logger('PairConfig')

@dataclass
class TradingPairConfig:
    symbol: str
    name: str
    pip_value: float
    min_lot: float
    max_lot: float
    default_lot: float
    max_spread_pips: float
    default_sl_pips: float
    default_tp_pips: float
    commission_per_lot: float
    swap_long: float
    swap_short: float
    trading_hours: Dict[str, str]
    enabled: bool = True

class PairConfigManager:
    def __init__(self, config):
        self.config = config
        self.pairs = self._initialize_pairs()
        logger.info(f"Initialized {len(self.pairs)} trading pairs")
    
    def _initialize_pairs(self) -> Dict[str, TradingPairConfig]:
        pairs = {
            'XAUUSD': TradingPairConfig(
                symbol='XAUUSD',
                name='Gold vs US Dollar',
                pip_value=10.0,
                min_lot=0.01,
                max_lot=10.0,
                default_lot=0.01,
                max_spread_pips=5.0,
                default_sl_pips=20.0,
                default_tp_pips=30.0,
                commission_per_lot=0.0,
                swap_long=-0.5,
                swap_short=0.3,
                trading_hours={
                    'monday': '00:00-23:59',
                    'tuesday': '00:00-23:59',
                    'wednesday': '00:00-23:59',
                    'thursday': '00:00-23:59',
                    'friday': '00:00-23:00',
                    'saturday': 'closed',
                    'sunday': '23:00-23:59'
                },
                enabled=True
            ),
            'XAGUSD': TradingPairConfig(
                symbol='XAGUSD',
                name='Silver vs US Dollar',
                pip_value=50.0,
                min_lot=0.01,
                max_lot=5.0,
                default_lot=0.01,
                max_spread_pips=5.0,
                default_sl_pips=30.0,
                default_tp_pips=50.0,
                commission_per_lot=0.0,
                swap_long=-0.3,
                swap_short=0.2,
                trading_hours={
                    'monday': '00:00-23:59',
                    'tuesday': '00:00-23:59',
                    'wednesday': '00:00-23:59',
                    'thursday': '00:00-23:59',
                    'friday': '00:00-23:00',
                    'saturday': 'closed',
                    'sunday': '23:00-23:59'
                },
                enabled=False
            ),
            'EURUSD': TradingPairConfig(
                symbol='EURUSD',
                name='Euro vs US Dollar',
                pip_value=10.0,
                min_lot=0.01,
                max_lot=10.0,
                default_lot=0.01,
                max_spread_pips=2.0,
                default_sl_pips=15.0,
                default_tp_pips=25.0,
                commission_per_lot=0.0,
                swap_long=-0.4,
                swap_short=0.1,
                trading_hours={
                    'monday': '00:00-23:59',
                    'tuesday': '00:00-23:59',
                    'wednesday': '00:00-23:59',
                    'thursday': '00:00-23:59',
                    'friday': '00:00-23:00',
                    'saturday': 'closed',
                    'sunday': '23:00-23:59'
                },
                enabled=False
            ),
            'GBPUSD': TradingPairConfig(
                symbol='GBPUSD',
                name='British Pound vs US Dollar',
                pip_value=10.0,
                min_lot=0.01,
                max_lot=10.0,
                default_lot=0.01,
                max_spread_pips=2.5,
                default_sl_pips=20.0,
                default_tp_pips=30.0,
                commission_per_lot=0.0,
                swap_long=-0.5,
                swap_short=0.2,
                trading_hours={
                    'monday': '00:00-23:59',
                    'tuesday': '00:00-23:59',
                    'wednesday': '00:00-23:59',
                    'thursday': '00:00-23:59',
                    'friday': '00:00-23:00',
                    'saturday': 'closed',
                    'sunday': '23:00-23:59'
                },
                enabled=False
            )
        }
        
        return pairs
    
    def get_pair(self, symbol: str) -> Optional[TradingPairConfig]:
        return self.pairs.get(symbol.upper())
    
    def get_enabled_pairs(self) -> List[TradingPairConfig]:
        return [pair for pair in self.pairs.values() if pair.enabled]
    
    def get_all_pairs(self) -> List[TradingPairConfig]:
        return list(self.pairs.values())
    
    def enable_pair(self, symbol: str) -> bool:
        pair = self.pairs.get(symbol.upper())
        if pair:
            pair.enabled = True
            logger.info(f"Enabled trading pair: {symbol}")
            return True
        logger.warning(f"Pair not found: {symbol}")
        return False
    
    def disable_pair(self, symbol: str) -> bool:
        pair = self.pairs.get(symbol.upper())
        if pair:
            pair.enabled = False
            logger.info(f"Disabled trading pair: {symbol}")
            return True
        logger.warning(f"Pair not found: {symbol}")
        return False
    
    def get_pip_value(self, symbol: str) -> float:
        pair = self.get_pair(symbol)
        return pair.pip_value if pair else 10.0
    
    def get_max_spread(self, symbol: str) -> float:
        pair = self.get_pair(symbol)
        return pair.max_spread_pips if pair else 5.0
    
    def get_default_sl(self, symbol: str) -> float:
        pair = self.get_pair(symbol)
        return pair.default_sl_pips if pair else 20.0
    
    def get_default_tp(self, symbol: str) -> float:
        pair = self.get_pair(symbol)
        return pair.default_tp_pips if pair else 30.0
    
    def get_lot_limits(self, symbol: str) -> tuple:
        pair = self.get_pair(symbol)
        if pair:
            return (pair.min_lot, pair.max_lot, pair.default_lot)
        return (0.01, 10.0, 0.01)
    
    def validate_lot_size(self, symbol: str, lot_size: float) -> bool:
        min_lot, max_lot, _ = self.get_lot_limits(symbol)
        return min_lot <= lot_size <= max_lot
    
    def format_pair_info(self, symbol: str) -> Optional[str]:
        pair = self.get_pair(symbol)
        if not pair:
            return None
        
        info = f"*{pair.name} ({pair.symbol})*\n\n"
        info += f"Pip Value: {pair.pip_value}\n"
        info += f"Lot Range: {pair.min_lot} - {pair.max_lot}\n"
        info += f"Default Lot: {pair.default_lot}\n"
        info += f"Max Spread: {pair.max_spread_pips} pips\n"
        info += f"Default SL: {pair.default_sl_pips} pips\n"
        info += f"Default TP: {pair.default_tp_pips} pips\n"
        info += f"Commission: ${pair.commission_per_lot}/lot\n"
        info += f"Swap Long: {pair.swap_long}\n"
        info += f"Swap Short: {pair.swap_short}\n"
        info += f"Status: {'✅ Enabled' if pair.enabled else '⛔ Disabled'}\n"
        
        return info
