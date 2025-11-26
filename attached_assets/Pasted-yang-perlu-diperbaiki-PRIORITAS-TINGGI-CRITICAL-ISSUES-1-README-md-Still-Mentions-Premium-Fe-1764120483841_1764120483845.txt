yang perlu diperbaiki:

🔴 PRIORITAS TINGGI - CRITICAL ISSUES
1. README.md Still Mentions Premium Features
File: README.md (baris 14, 81-84, 200-214)
Masalah:
Masih ada referensi "Premium Subscription" (✅ Premium Subscription)
Command /premium, /beli, /langganan, /addpremium masih di dokumentasi
Mention "Paket Premium: Weekly & Monthly"
Harus diperbaiki: Hapus semua referensi premium/subscription, update command list
2. Race Condition - Signal Session Cleanup
File: bot/signal_session_manager.py & bot/telegram_bot.py
Masalah: Session cleanup di _on_session_end_handler bisa race dengan incoming signal checks
Dampak: Bisa crash jika handler modifies session during iteration
Solusi: Implement proper locking mechanism untuk active_sessions dictionary
Database Transaction Isolation for Concurrent Users
File: bot/database.py & bot/risk_manager.py
Masalah:
Trade/Position Creation: Database commit sebelum _confirm_signal_sent - bisa divergence jika Telegram send gagal
Tidak ada transaction-level locking antara berbagai users
Dampak: Data inconsistency jika 2+ users trading bersamaan
Solusi: Implement SQLAlchemy transaction isolation level
🟡 PRIORITAS MEDIUM - IMPORTANT
4. Memory Leak - Thread Pool Executor
File: bot/chart_generator.py
Masalah: Thread pool executor tidak ada graceful shutdown - resource leak on bot restart
Dampak: Memory leak setiap kali bot di-restart
Solusi: Add executor shutdown dalam cleanup routine
5. Memory Leak - Active Dashboards Dict
File: bot/telegram_bot.py
Masalah: Active dashboards dict tidak auto-cleanup jika dashboard task fails - accumulate dead entries
Dampak: Memory accumulation over time
Solusi: Implement auto-cleanup atau weak references untuk failed tasks
Retry Mechanism for Failed Telegram Sends
File: bot/telegram_bot.py
Masalah: Signal send failure rollback sudah ada, tapi tidak ada retry mechanism untuk failed sends
Dampak: Sinyal hilang jika network unstable
Solusi: Implement exponential backoff retry (3-5 retries)
7. Signal Cache Cleanup
File: bot/telegram_bot.py
Masalah: Expired entries cleared only on new signal check - inactive users akan keep expired entries
Dampak: Memory leak untuk inactive users
Solusi: Add background task untuk cleanup expired cache
8. Chart Orphan Files
File: bot/chart_generator.py & bot/signal_session_manager.py
Masalah: Chart deletion tidak atomic dengan session end - orphan files bisa tertinggal
Dampak: Disk space leak
Solusi: Implement atomic deletion atau verify file deletion
PRIORITAS RENDAH - OPTIMIZATION
9. Performance Monitoring
File: bot/performance_monitor.py
Masalah: Sentry Integration hanya pada critical errors, tidak ada performance metrics tracking
Solusi: Add performance metrics tracking untuk signals/trades
10. Health Check Enhancement
File: main.py & bot/telegram_bot.py
Masalah: Basic status only, tidak monitor cache health atau position count
Solusi: Expand health check untuk include cache stats, position count
11. Log Rate Limiting
File: bot/logger.py
Masalah: Tidak ada rate-limiting untuk logs - bisa spam disk pada high-frequency signals
Solusi: Implement log rotation atau rate limiting
12. Position Tracker Optimization
File: bot/position_tracker.py
Masalah: Monitoring loop menggunakan asyncio.sleep(2) secara global - bisa dioptimalkan dengan event-based notification
Dampak: Tidak critical, tapi CPU usage bisa kurang optimal
Solusi: Implement event-driven model
Chart Generator Bottleneck
File: bot/chart_generator.py
Masalah: MAX_WORKERS=1 di FREE_TIER_MODE - akan bottleneck jika multi-user trading aktif
Dampak: Chart generation slow untuk multi-user
Solusi: Dynamic worker scaling berdasarkan queue size
14. Graceful Degradation for OOM
File: config.py & main.py
Masalah: FREE_TIER_MODE=true tapi tidak ada graceful degradation jika OOM
Solusi: Add fallback ketika memory critical
REKOMENDASI URUTAN PERBAIKAN
Segera: Hapus Premium dari README.md (ini documentation issue)
Fix race conditions & database transactions (data critical)
Memory leaks & cleanup issues
Performance optimization & monitoring enhancements