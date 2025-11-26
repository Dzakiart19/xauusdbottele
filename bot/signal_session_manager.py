"""
Signal Session Manager
Mengelola sesi sinyal untuk mencegah duplikasi dan konflik antara mode auto & manual
"""
import asyncio
import weakref
from datetime import datetime
from typing import Optional, Dict, Callable, List
from dataclasses import dataclass, field
import pytz
from bot.logger import setup_logger

logger = setup_logger('SignalSessionManager')

@dataclass
class SignalSession:
    """Representasi sesi sinyal aktif"""
    user_id: int
    signal_id: str
    signal_source: str
    signal_type: str
    entry_price: float
    stop_loss: float
    take_profit: float
    position_id: Optional[int]
    trade_id: Optional[int]
    started_at: datetime
    chart_path: Optional[str] = None
    photo_sent: bool = False

class SignalSessionManager:
    """
    Manager untuk mengelola sesi sinyal secara global
    Mencegah sinyal duplikat dan konflik antara auto/manual mode
    
    Thread-safe dengan proper locking untuk mencegah race conditions
    """
    
    def __init__(self):
        self.active_sessions: Dict[int, SignalSession] = {}
        self._session_lock = asyncio.Lock()
        self._event_lock = asyncio.Lock()
        self._event_handlers: Dict[str, List[Callable]] = {
            'on_session_start': [],
            'on_session_end': [],
            'on_session_update': []
        }
        self._pending_events: List[tuple] = []
        logger.info("Signal Session Manager initialized with enhanced locking")
    
    def register_event_handler(self, event: str, handler: Callable):
        """Daftarkan event handler untuk lifecycle events"""
        if event in self._event_handlers:
            self._event_handlers[event].append(handler)
            logger.debug(f"Event handler registered for: {event}")
    
    async def _emit_event_outside_lock(self, event: str, session: SignalSession):
        """
        Emit event ke semua registered handlers.
        PENTING: Method ini harus dipanggil DILUAR session_lock untuk mencegah deadlock.
        """
        handlers = []
        async with self._event_lock:
            handlers = list(self._event_handlers.get(event, []))
        
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(session)
                else:
                    handler(session)
            except Exception as e:
                logger.error(f"Error in event handler for {event}: {e}")
    
    async def can_create_signal(self, user_id: int, signal_source: str) -> tuple[bool, Optional[str]]:
        """
        Cek apakah user bisa membuat sinyal baru
        
        Returns:
            (can_create, rejection_reason)
        """
        async with self._session_lock:
            if user_id in self.active_sessions:
                active = self.active_sessions[user_id]
                
                if active.signal_source == signal_source:
                    reason = f"⚠️ Sinyal {signal_source} sudah aktif! Tunggu sampai posisi selesai."
                    return False, reason
                else:
                    reason = (
                        f"⚠️ Ada sinyal {active.signal_source} yang masih aktif!\n"
                        f"Tidak bisa buat sinyal {signal_source} sekarang.\n"
                        f"Tunggu posisi selesai dulu."
                    )
                    return False, reason
            
            return True, None
    
    async def create_session(self, user_id: int, signal_id: str, signal_source: str,
                            signal_type: str, entry_price: float, stop_loss: float,
                            take_profit: float) -> SignalSession:
        """Buat sesi sinyal baru"""
        async with self._session_lock:
            if user_id in self.active_sessions:
                old_session = self.active_sessions[user_id]
                logger.warning(f"Overwriting active session for user {user_id}: {old_session.signal_id}")
            
            session = SignalSession(
                user_id=user_id,
                signal_id=signal_id,
                signal_source=signal_source,
                signal_type=signal_type,
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                position_id=None,
                trade_id=None,
                started_at=datetime.now(pytz.UTC)
            )
            
            self.active_sessions[user_id] = session
            
            icon = "🤖" if signal_source == "auto" else "👤"
            logger.info(f"✅ Signal session created - User:{user_id} {icon} {signal_source.upper()} {signal_type}")
            
        await self._emit_event_outside_lock('on_session_start', session)
        
        return session
    
    async def update_session(self, user_id: int, **kwargs):
        """Update sesi yang sedang aktif"""
        async with self._session_lock:
            if user_id not in self.active_sessions:
                logger.warning(f"Attempting to update non-existent session for user {user_id}")
                return False
            
            session = self.active_sessions[user_id]
            
            for key, value in kwargs.items():
                if hasattr(session, key):
                    setattr(session, key, value)
            
        await self._emit_event_outside_lock('on_session_update', session)
        
        return True
    
    async def end_session(self, user_id: int, reason: str = "closed"):
        """Akhiri sesi sinyal dan cleanup chart jika ada - thread safe"""
        session = None
        chart_path = None
        
        async with self._session_lock:
            if user_id not in self.active_sessions:
                logger.debug(f"No active session to end for user {user_id}")
                return None
            
            session = self.active_sessions[user_id]
            chart_path = session.chart_path
            
            del self.active_sessions[user_id]
            
            duration = (datetime.now(pytz.UTC) - session.started_at).total_seconds()
            icon = "🤖" if session.signal_source == "auto" else "👤"
            
            logger.info(
                f"🏁 Signal session ended - User:{user_id} {icon} {session.signal_source.upper()} "
                f"Reason:{reason} Duration:{duration:.1f}s"
            )
        
        if chart_path:
            await self._cleanup_chart_file(chart_path)
        
        await self._emit_event_outside_lock('on_session_end', session)
        
        return session
    
    async def _cleanup_chart_file(self, chart_path: str):
        """Cleanup chart file secara async dan atomic"""
        try:
            import os
            if chart_path and os.path.exists(chart_path):
                os.remove(chart_path)
                logger.info(f"🗑️ Cleaned up session chart: {chart_path}")
                
                if os.path.exists(chart_path):
                    logger.warning(f"Chart file still exists after deletion: {chart_path}")
        except FileNotFoundError:
            logger.debug(f"Chart file already deleted: {chart_path}")
        except Exception as e:
            logger.warning(f"Failed to cleanup chart {chart_path}: {e}")
    
    async def clear_all_sessions(self, reason: str = "system_reset") -> int:
        """
        Hapus semua sesi sinyal aktif (untuk system reset) - thread safe
        
        Returns:
            int: Jumlah sesi yang dibersihkan
        """
        sessions_to_end = []
        chart_paths = []
        
        async with self._session_lock:
            session_count = len(self.active_sessions)
            
            if session_count == 0:
                logger.info("No active sessions to clear")
                return 0
            
            logger.info(f"Clearing all {session_count} active signal sessions...")
            
            sessions_to_end = list(self.active_sessions.values())
            chart_paths = [s.chart_path for s in sessions_to_end if s.chart_path]
            
            for session in sessions_to_end:
                duration = (datetime.now(pytz.UTC) - session.started_at).total_seconds()
                icon = "🤖" if session.signal_source == "auto" else "👤"
                
                logger.info(
                    f"🏁 Clearing session - User:{session.user_id} {icon} {session.signal_source.upper()} "
                    f"Type:{session.signal_type} Reason:{reason} Duration:{duration:.1f}s"
                )
            
            self.active_sessions.clear()
        
        for chart_path in chart_paths:
            await self._cleanup_chart_file(chart_path)
        
        for session in sessions_to_end:
            try:
                await self._emit_event_outside_lock('on_session_end', session)
            except Exception as e:
                logger.error(f"Error emitting end event for session {session.signal_id}: {e}")
        
        logger.info(f"✅ All {session_count} signal sessions cleared successfully")
        
        return session_count
    
    def get_active_session(self, user_id: int) -> Optional[SignalSession]:
        """Ambil sesi aktif untuk user"""
        return self.active_sessions.get(user_id)
    
    def has_active_session(self, user_id: int) -> bool:
        """Cek apakah user punya sesi aktif"""
        return user_id in self.active_sessions
    
    def get_all_active_sessions(self) -> Dict[int, SignalSession]:
        """Ambil semua sesi yang aktif"""
        return self.active_sessions.copy()
    
    def get_session_count(self) -> int:
        """Hitung jumlah sesi aktif"""
        return len(self.active_sessions)
    
    def get_stats(self) -> dict:
        """Dapatkan statistik sesi"""
        auto_count = sum(1 for s in self.active_sessions.values() if s.signal_source == 'auto')
        manual_count = sum(1 for s in self.active_sessions.values() if s.signal_source == 'manual')
        
        return {
            'total_sessions': len(self.active_sessions),
            'auto_sessions': auto_count,
            'manual_sessions': manual_count
        }
