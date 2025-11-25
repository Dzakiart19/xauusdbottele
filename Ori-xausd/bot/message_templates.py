"""
Message Templates untuk Telegram
Format pesan yang konsisten dan rapi
"""
from typing import Optional
from datetime import datetime
import pytz

class MessageFormatter:
    """Helper untuk format pesan Telegram yang rapi"""
    
    @staticmethod
    def escape_markdown(text: str) -> str:
        """Escape karakter special untuk Markdown"""
        special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        for char in special_chars:
            text = text.replace(char, f'\\{char}')
        return text
    
    @staticmethod
    def progress_bar(current: float, target: float, total_length: int = 10) -> str:
        """Buat progress bar visual"""
        if target == 0:
            return "▱" * total_length
        
        percentage = min(abs(current / target), 1.0)
        filled = int(percentage * total_length)
        empty = total_length - filled
        
        return "▰" * filled + "▱" * empty
    
    @staticmethod
    def signal_alert(signal: dict, signal_source: str = 'auto') -> str:
        """Format pesan alert sinyal trading"""
        icon = "🤖" if signal_source == 'auto' else "👤"
        mode = "OTOMATIS" if signal_source == 'auto' else "MANUAL"
        
        signal_type = signal['signal']
        direction_icon = "🟢" if signal_type == 'BUY' else "🔴"
        
        entry = signal['entry_price']
        sl = signal['stop_loss']
        tp = signal['take_profit']
        
        sl_pips = signal.get('sl_pips', 0)
        tp_pips = signal.get('tp_pips', 0)
        rr_ratio = signal.get('rr_ratio', 0)
        
        trend_desc = signal.get('trend_description', 'MEDIUM')
        
        confidence_reasons = signal.get('confidence_reasons', [])
        confidence_text = "\n".join(f"  • {reason}" for reason in confidence_reasons[:5])
        
        msg = (
            f"{direction_icon} *SINYAL {signal_type}* {icon} {mode}\n"
            f"{'━' * 32}\n\n"
            f"💰 *Entry:* `${entry:.2f}`\n"
            f"🛡️ *Stop Loss:* `${sl:.2f}` ({sl_pips:.1f} pips)\n"
            f"🎯 *Take Profit:* `${tp:.2f}` ({tp_pips:.1f} pips)\n"
            f"📊 *Risk:Reward:* `1:{rr_ratio:.2f}`\n\n"
            f"⚡ *Trend Strength:* {trend_desc}\n\n"
            f"📌 *Alasan:*\n{confidence_text}\n\n"
            f"⏰ {datetime.now(pytz.timezone('Asia/Jakarta')).strftime('%H:%M:%S WIB')}"
        )
        
        return msg
    
    @staticmethod
    def position_update(position_data: dict) -> str:
        """Format update posisi real-time"""
        signal_type = position_data['signal_type']
        direction_icon = "🟢" if signal_type == 'BUY' else "🔴"
        
        entry = position_data['entry_price']
        current = position_data['current_price']
        sl = position_data['stop_loss']
        tp = position_data['take_profit']
        pl = position_data['unrealized_pl']
        
        price_change = current - entry
        price_change_pct = (price_change / entry) * 100
        
        if signal_type == 'BUY':
            tp_distance_total = tp - entry
            tp_distance_current = current - entry
            sl_distance_total = entry - sl
            sl_distance_current = entry - current
        else:
            tp_distance_total = entry - tp
            tp_distance_current = entry - current
            sl_distance_total = sl - entry
            sl_distance_current = current - entry
        
        tp_progress = max(0, min(100, (tp_distance_current / tp_distance_total * 100) if tp_distance_total > 0 else 0))
        sl_progress = max(0, min(100, (sl_distance_current / sl_distance_total * 100) if sl_distance_total > 0 else 0))
        
        tp_bar = MessageFormatter.progress_bar(tp_distance_current, tp_distance_total, 10)
        
        pl_icon = "💰" if pl >= 0 else "📉"
        pl_text = f"+${pl:.2f}" if pl >= 0 else f"-${abs(pl):.2f}"
        
        msg = (
            f"{direction_icon} *POSISI {signal_type} AKTIF*\n"
            f"{'━' * 32}\n\n"
            f"📍 *Entry:* `${entry:.2f}`\n"
            f"📊 *Current:* `${current:.2f}` ({price_change_pct:+.3f}%)\n"
            f"{pl_icon} *P/L:* `{pl_text}`\n\n"
            f"🎯 *Progress ke TP:*\n"
            f"{tp_bar} {tp_progress:.1f}%\n"
            f"Target: `${tp:.2f}`\n\n"
            f"🛡️ *Stop Loss:* `${sl:.2f}`\n"
            f"⚠️ Risk: {sl_progress:.1f}%\n\n"
            f"⏰ {datetime.now(pytz.timezone('Asia/Jakarta')).strftime('%H:%M:%S WIB')}"
        )
        
        return msg
    
    @staticmethod
    def trade_exit(exit_data: dict) -> str:
        """Format pesan trade exit"""
        result = exit_data['result']
        signal_type = exit_data['signal_type']
        entry = exit_data['entry_price']
        exit_price = exit_data['exit_price']
        pl = exit_data['actual_pl']
        reason = exit_data.get('reason', 'CLOSED')
        
        result_icon = "✅" if result == 'WIN' else "❌"
        result_text = "PROFIT" if result == 'WIN' else "LOSS"
        
        direction_icon = "🟢" if signal_type == 'BUY' else "🔴"
        
        price_change = exit_price - entry
        price_change_pct = (price_change / entry) * 100
        
        pl_text = f"+${pl:.2f}" if pl >= 0 else f"-${abs(pl):.2f}"
        
        reason_text = {
            'TP_HIT': 'Target Tercapai 🎯',
            'SL_HIT': 'Stop Loss Hit 🛡️',
            'DYNAMIC_SL_HIT': 'Dynamic SL Triggered 🔄',
            'MANUAL_CLOSE': 'Manual Close 👤',
            'CLOSED': 'Posisi Ditutup'
        }.get(reason, reason)
        
        msg = (
            f"{result_icon} *{result_text}* {direction_icon}\n"
            f"{'━' * 32}\n\n"
            f"📍 *Entry:* `${entry:.2f}`\n"
            f"🏁 *Exit:* `${exit_price:.2f}` ({price_change_pct:+.3f}%)\n"
            f"💰 *P/L:* `{pl_text}`\n\n"
            f"📋 *Status:* {reason_text}\n"
            f"⏰ {datetime.now(pytz.timezone('Asia/Jakarta')).strftime('%H:%M:%S WIB')}"
        )
        
        return msg
    
    @staticmethod
    def waiting_for_signal(signal_source: str = 'auto') -> str:
        """Format pesan menunggu sinyal"""
        icon = "🤖" if signal_source == 'auto' else "👤"
        mode = "otomatis" if signal_source == 'auto' else "manual"
        
        return (
            f"{icon} *Monitoring Aktif*\n"
            f"{'━' * 32}\n\n"
            f"⏳ Menunggu sinyal {mode}...\n"
            f"📊 Menganalisis market XAUUSD\n\n"
            f"⏰ {datetime.now(pytz.timezone('Asia/Jakarta')).strftime('%H:%M:%S WIB')}"
        )
    
    @staticmethod
    def session_blocked(active_source: str, requested_source: str) -> str:
        """Format pesan ketika sinyal diblok karena ada sesi aktif"""
        active_icon = "🤖" if active_source == 'auto' else "👤"
        requested_icon = "🤖" if requested_source == 'auto' else "👤"
        
        return (
            f"⛔ *Sinyal Diblok*\n"
            f"{'━' * 32}\n\n"
            f"{active_icon} Ada sinyal *{active_source.upper()}* yang masih aktif!\n\n"
            f"{requested_icon} Tidak bisa buat sinyal *{requested_source.upper()}* sekarang.\n\n"
            f"⏳ Tunggu sampai posisi selesai dulu\n"
            f"(TP/SL tercapai)\n\n"
            f"💡 Cek status: /status"
        )
    
    @staticmethod
    def no_active_position() -> str:
        """Format pesan tidak ada posisi aktif"""
        return (
            f"ℹ️ *Status Posisi*\n"
            f"{'━' * 32}\n\n"
            f"📭 Tidak ada posisi aktif\n\n"
            f"💡 Gunakan:\n"
            f"  • /monitor - Monitoring otomatis\n"
            f"  • /getsignal - Sinyal manual\n\n"
            f"⏰ {datetime.now(pytz.timezone('Asia/Jakarta')).strftime('%H:%M:%S WIB')}"
        )
    
    @staticmethod
    def statistics_summary(stats: dict) -> str:
        """Format statistik trading"""
        total_trades = stats.get('total_trades', 0)
        wins = stats.get('wins', 0)
        losses = stats.get('losses', 0)
        total_profit = stats.get('total_profit', 0)
        win_rate = stats.get('win_rate', 0)
        
        win_rate_emoji = "🔥" if win_rate >= 70 else "💪" if win_rate >= 50 else "📊"
        pl_emoji = "💰" if total_profit >= 0 else "📉"
        pl_text = f"+${total_profit:.2f}" if total_profit >= 0 else f"-${abs(total_profit):.2f}"
        
        return (
            f"📊 *Statistik Trading*\n"
            f"{'━' * 32}\n\n"
            f"📈 *Total Trades:* {total_trades}\n"
            f"✅ *Wins:* {wins}\n"
            f"❌ *Losses:* {losses}\n"
            f"{win_rate_emoji} *Win Rate:* {win_rate:.1f}%\n\n"
            f"{pl_emoji} *Total P/L:* `{pl_text}`\n\n"
            f"⏰ {datetime.now(pytz.timezone('Asia/Jakarta')).strftime('%Y-%m-%d %H:%M WIB')}"
        )
    
    @staticmethod
    def error_message(error_text: str, context: str = "") -> str:
        """Format pesan error"""
        return (
            f"⚠️ *Error*\n"
            f"{'━' * 32}\n\n"
            f"{error_text}\n\n"
            f"{f'Context: {context}' if context else ''}"
            f"⏰ {datetime.now(pytz.timezone('Asia/Jakarta')).strftime('%H:%M:%S WIB')}"
        )
