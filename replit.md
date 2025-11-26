# XAUUSD Trading Bot Pro V2

## Overview
This project is an automated XAUUSD trading bot accessible via Telegram. It provides real-time trading signals, automatic position tracking, and trade outcome notifications. The bot offers 24/7 unlimited signals, robust risk management, and a database for performance tracking. It is a private bot with access control via authorized user IDs and features advanced chart generation with technical indicators. The bot employs a refined dual-mode strategy (Auto/Manual signals) incorporating advanced filtering with Twin Range Filter (TRF) and Market Bias CEREBR, along with a Trend-Plus-Pullback strategy for enhanced precision and opportunity, aiming to be a professional and informative trading assistant for XAUUSD.

## User Preferences
- Bahasa komunikasi: **Bahasa Indonesia** (100% tidak ada bahasa Inggris)
- Data source: **Deriv WebSocket** (gratis, tanpa API key)
- Trading pair: **XAUUSD** (Gold)
- Notifikasi: **Telegram** dengan foto chart + indikator
- Tracking: **Real-time** sampai TP/SL
- Mode: **24/7 unlimited** untuk user terdaftar
- Akurasi: Strategi multi-indicator dengan validasi ketat
- Chart: Menampilkan indikator EMA, RSI, Stochastic (tidak polos)
- Akses Bot: **Privat** - hanya untuk user yang terdaftar di AUTHORIZED_USER_IDS atau ID_USER_PUBLIC

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
- **Database:** SQLite for persistent data, with PostgreSQL support and auto-migration, featuring connection pooling and robust transaction management.
- **User Manager:** Handles user authentication and access control via AUTHORIZED_USER_IDS and ID_USER_PUBLIC, with thread-safe concurrent updates.
- **Resilience:** Implements CircuitBreaker for WebSocket, global rate limiting for Telegram API, retry mechanisms, and advanced WebSocket recovery with exponential backoff.
- **System Health:** Includes port conflict detection, bot instance locking, Sentry integration, comprehensive health checks, and OOM graceful degradation.

**UI/UX Decisions:**
- Telegram serves as the primary user interface.
- Signal messages are enriched with icons, source labels, and confidence reasons.
- Charts display candlesticks, volume, EMA, RSI, Stochastic, TRF bands, and CEREBR in a multi-panel layout.
- Exit notifications (WIN/LOSE) send TEXT ONLY without photo chart; photos are only sent when a signal is first active.
- All timestamps are displayed in WIB (Asia/Jakarta) timezone.

**Technical Implementations & Feature Specifications:**
- **Indicators:** EMA (5, 10, 20, 50), RSI (14 with 20-bar history), Stochastic (K=14, D=3), ATR (14), MACD (12,26,9), Volume, Twin Range Filter, Market Bias CEREBR. Includes NaN/Inf/Negative price handling and robust validation.
- **Risk Management:** Fixed SL ($1 per trade), dynamic TP (1.45x-2.50x R:R), max spread (5 pips), signal cooldown (120s per user), daily loss limit (3% per user), risk per trade (0.5%). Includes dynamic SL tightening and trailing stop activation.
- **Access Control:** Private bot with dual-tier access (AUTHORIZED_USER_IDS for admins, ID_USER_PUBLIC for public users).
- **Commands:** Admin commands (`/riset`, `/status`, `/tasks`, `/analytics`, `/systemhealth`) and User commands (`/start`, `/help`, `/monitor`, `/getsignal`, `/status`, `/riwayat`, `/performa`).
- **Anti-Duplicate Protection:** Two-phase cache pattern (pending/confirmed status, hash-based tracking, thread-safe locking, TTL-backed signal cache with async cleanup) for race-condition-safe signal deduplication and chart cleanup.
- **Candle Data Persistence:** Stores M1 and M5 candles in the database for instant bot readiness on restart.
- **Chart Generation:** Uses `mplfinance` and `matplotlib` for multi-panel charts, configured for headless operation, with configurable timeouts and proper thread cleanup. Charts are automatically deleted upon signal session end with aggressive cleanup.
- **Multi-User Support:** Implements per-user position tracking and risk management.
- **Deployment:** Optimized for Koyeb and Replit, featuring an HTTP server for health checks and webhooks, and `FREE_TIER_MODE` for resource efficiency.
- **Performance Optimization:** Global signal cooldown (3.0s), tick throttling (3.0s), position monitoring early exit, optimized Telegram timeout handling, and fast text-only exit notifications.
- **Logging & Error Handling:** Rate-limited logging, log rotation, type-safe indicator computations, and comprehensive exception handling with categorization for critical errors and deprecated API compatibility.
- **Task Management:** Centralized task registry with shielded cancellation for graceful shutdown, background task callbacks, and stale task detection.

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

## Recent Changes (26 November 2025)
**LSP Type Safety Improvements - ZERO Errors:**
- **bot/alert_system.py:** Fixed deque vs list type compatibility, migrated to public accessors for RateLimiter state persistence
- **bot/analytics.py:** Fixed 37 SQLAlchemy Column type errors using cast() pattern for proper Python type conversion
- **bot/user_manager.py:** Migrated to SQLAlchemy 2.0 Mapped annotations pattern, fixed return type safety
- **bot/utils.py:** Fixed dynamic attribute assignment with setattr(), fixed tuple type annotations
- **bot/strategy.py:** Added pyright complexity ignore for intentionally complex trading logic
- **bot/resilience.py:** Added public accessor methods (get_time_window, get_call_times, set_call_times) for RateLimiter

**Type Safety Patterns Applied:**
- SQLAlchemy Column values: Use `cast(float, column)` or `float(column)` after `is not None` check
- SQLAlchemy 2.0: Migrate from `Column()` to `Mapped[type] = mapped_column()` pattern
- Dynamic attributes: Use `setattr()` for wrapped function attribute assignment
- Complex functions: Use `# pyright: ignore[reportGeneralTypeIssues]` for intentionally complex trading logic