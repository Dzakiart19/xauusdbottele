# XAUUSD Trading Bot Pro V2

## Overview
This project is an automated XAUUSD trading bot accessible via Telegram. It provides real-time trading signals, automatic position tracking, and trade outcome notifications. The bot offers 24/7 unlimited signals, robust risk management, and a database for performance tracking. It includes a premium subscription system and features advanced chart generation with technical indicators. The bot employs a refined dual-mode strategy (Auto/Manual signals) incorporating advanced filtering with Twin Range Filter (TRF) and Market Bias CEREBR, along with a Trend-Plus-Pullback strategy for enhanced precision and opportunity, aiming to be a professional and informative trading assistant for XAUUSD.

## User Preferences
- Bahasa komunikasi: **Bahasa Indonesia** (100% tidak ada bahasa Inggris)
- Data source: **Deriv WebSocket** (gratis, tanpa API key)
- Trading pair: **XAUUSD** (Gold)
- Notifikasi: **Telegram** dengan foto chart + indikator
- Tracking: **Real-time** sampai TP/SL
- Mode: **24/7 unlimited** untuk admin/premium
- Akurasi: Strategi multi-indicator dengan validasi ketat
- Chart: Menampilkan indikator EMA, RSI, Stochastic (tidak polos)
- Sistem Premium: Paket mingguan (Rp 15.000) dan bulanan (Rp 30.000)
- Kontak untuk langganan: @dzeckyete

## System Architecture
The bot's architecture is modular, designed for scalability and maintainability.

**Core Components:**
- **Orchestrator:** Manages bot components.
- **Market Data:** Handles Deriv WebSocket connection and OHLC candle construction, including persistence.
- **Strategy:** Implements a dual-mode signal detection (Auto/Manual) using multiple indicators, including Twin Range Filter, Market Bias CEREBR, EMA 50 for trend filtering, and RSI for pullback confirmation. Auto mode requires a minimum score of 5 and minimum trend strength of 0.3+.
- **Position Tracker:** Monitors real-time trade positions per user.
- **Telegram Bot:** Manages command handling and notifications.
- **Chart Generator:** Creates professional charts with integrated technical indicators.
- **Risk Manager:** Calculates lot sizes, P/L, and enforces per-user risk limits (e.g., fixed SL, dynamic TP, daily loss limit, signal cooldown).
- **Database:** SQLite for persistent data, with PostgreSQL support and auto-migration.
- **User Manager:** Handles user authentication, subscription, and premium access control.
- **Resilience:** Implements CircuitBreaker for WebSocket, global rate limiting for Telegram API, and retry mechanisms.
- **System Health:** Includes port conflict detection, bot instance locking, and Sentry integration.

**UI/UX Decisions:**
- Telegram serves as the primary user interface.
- Signal messages are enriched with icons, source labels, and confidence reasons.
- Charts display candlesticks, volume, EMA, RSI, Stochastic, TRF bands, and CEREBR in a multi-panel layout.
- Exit notifications (WIN/LOSE) send TEXT ONLY without photo chart; photos are only sent when a signal is first active.
- All timestamps are displayed in WIB (Asia/Jakarta) timezone.

**Technical Implementations & Feature Specifications:**
- **Indicators:** EMA (5, 10, 20, 50), RSI (14 with 20-bar history), Stochastic (K=14, D=3), ATR (14), MACD (12,26,9), Volume, Twin Range Filter, Market Bias CEREBR.
- **Risk Management:** Fixed SL ($1 per trade), dynamic TP (1.45x-2.50x R:R), max spread (5 pips), signal cooldown (120s per user), daily loss limit (3% per user), risk per trade (0.5%). Includes dynamic SL tightening and trailing stop activation.
- **Subscription System:** Weekly and Monthly premium packages with automatic expiry.
- **Admin Commands:** `/riset`, `/addpremium`, `/status`, `/tasks`, `/analytics`, `/systemhealth`.
- **User Commands:** `/premium`, `/beli`, `/langganan`.
- **Anti-Duplicate Protection:** Two-phase cache pattern for race-condition-safe signal deduplication:
  - **Phase 1 (pending):** Atomically check duplicate and set pending status with `_cache_lock`
  - **Phase 2 (confirmed):** Upgrade to confirmed status only after Telegram send succeeds
  - **Rollback:** Remove pending entry on failure via `_rollback_signal_cache`
  - Hash-based tracking: `_generate_signal_hash` creates unique identifiers (user_id + direction + price_bucket)
  - Thread-safe: Single `asyncio.Lock()` protects all cache operations
  - Session cleanup: Cache cleared via `_clear_signal_cache` when signal session ends
  - Expiry: 120 seconds, 10 pips price tolerance for similar prices
- **Candle Data Persistence:** Stores M1 and M5 candles in the database for instant bot readiness on restart.
- **Chart Generation:** Uses `mplfinance` and `matplotlib` for multi-panel charts, configured for headless operation. Charts are automatically deleted upon signal session end, with aggressive cleanup task every 5 minutes (max 10 files, charts older than 30 minutes auto-deleted for Koyeb free tier optimization).
- **Multi-User Support:** Implements per-user position tracking and risk management.
- **Deployment:** Optimized for Koyeb and Replit, featuring an HTTP server for health checks and webhooks. Includes `FREE_TIER_MODE` for reduced resource usage and aggressive startup timeout.
- **Performance Optimization:** Global signal cooldown (3.0s), tick throttling (3.0s), position monitoring early exit, and optimized Telegram timeout handling. Dashboard update interval is 5 seconds. Exit notifications send TEXT ONLY for faster delivery.
- **Photo Deduplication:** A `photo_sent` flag in `SignalSession` prevents duplicate photo sending during retries/timeouts.

## External Dependencies
- **Deriv WebSocket API:** For real-time XAUUSD market data.
- **Telegram Bot API (`python-telegram-bot`):** For all Telegram interactions.
- **SQLAlchemy:** ORM for database interactions (SQLite, PostgreSQL).
- **Pandas & NumPy:** For data manipulation and numerical operations.
- **mplfinance & Matplotlib:** For generating financial charts.
- **pytz:** For timezone handling.
- **aiohttp:** For asynchronous HTTP server and client operations.
- **python-dotenv:** For managing environment variables.
- **Sentry:** For advanced error tracking and monitoring (optional).

## Recent Changes (2025-11-26)
- **Two-Phase Anti-Duplicate Signal Cache:** Implemented race-condition-safe signal deduplication with pending→confirmed status transitions and automatic rollback
- **Optimized Chart Cleanup for Koyeb:** Reduced max charts from 30 to 10, cleanup interval from 10 to 5 minutes, auto-delete charts older than 30 minutes
- **Async Lock Protection:** Single `asyncio.Lock()` guards all cache operations (`_check_and_set_pending`, `_confirm_signal_sent`, `_rollback_signal_cache`, `_clear_signal_cache`)
- **Session End Cleanup:** Signal cache automatically cleared via async method when signal session ends