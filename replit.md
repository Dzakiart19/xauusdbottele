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
- **Strategy:** Implements a dual-mode signal detection (Auto/Manual) using multiple indicators, including Twin Range Filter, Market Bias CEREBR, EMA 50 for trend filtering, and RSI for pullback confirmation.
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
- **[UPDATED 2025-11-25]** Exit notifications (WIN/LOSE) send TEXT ONLY without photo chart - photos only sent when signal is first active.
- All timestamps are displayed in WIB (Asia/Jakarta) timezone.

**Technical Implementations & Feature Specifications:**
- **Dual Signal Strategy:** Auto Mode requires strict conditions with EMA 50 trend filter; Manual Mode offers more opportunities with the same trend filter, both using a balanced scoring system. Strict enforcement of TRF and CEREBR filters. **[UPDATED 2025-11-25]** Auto mode now requires minimum score of 5 (up from 3) and minimum trend strength of 0.3+ to prevent weak signal spam.
- **Indicators:** EMA (5, 10, 20, 50), RSI (14 with 20-bar history), Stochastic (K=14, D=3), ATR (14), MACD (12,26,9), Volume, Twin Range Filter, Market Bias CEREBR.
- **Risk Management:** Fixed SL ($1 per trade), dynamic TP (1.45x-2.50x R:R), max spread (5 pips), signal cooldown (120s per user), daily loss limit (3% per user), risk per trade (0.5%). Includes dynamic SL tightening and trailing stop activation.
- **Subscription System:** Weekly and Monthly premium packages with automatic expiry.
- **Admin Commands:** `/riset`, `/addpremium`, `/status`, `/tasks`, `/analytics`, `/systemhealth`.
- **User Commands:** `/premium`, `/beli`, `/langganan`.
- **Anti-Duplicate Protection:** Prevents new signals if an active position exists. **[UPDATED 2025-11-25]** Enhanced with price + direction tracking, 5 pips tolerance for duplicate detection, and improved None-safety guards.
- **Candle Data Persistence:** Stores M1 and M5 candles in the database for instant bot readiness on restart.
- **Chart Generation:** Uses `mplfinance` and `matplotlib` for multi-panel charts, configured for headless operation.
- **Multi-User Support:** Implements per-user position tracking and risk management.
- **Deployment:** Optimized for Koyeb and Replit, featuring an HTTP server for health checks and webhooks. Includes `FREE_TIER_MODE` for reduced resource usage and aggressive startup timeout.
- **Performance Optimization:** Global signal cooldown (3.0s), tick throttling (3.0s - matches global cooldown), position monitoring early exit, and optimized Telegram timeout handling. **[UPDATED 2025-11-25]** Tick throttle increased from 1.5s to 3.0s for better spam prevention. Dashboard update interval optimized to 5 seconds (from 2s). Exit notifications send TEXT ONLY (no photo) for faster delivery.

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

---

## 🚀 KOYEB FAST RESPONSE OPTIMIZATION - DO NOT MODIFY THIS SECTION

### ⚠️ CRITICAL: AI AGENTS - READ THIS CAREFULLY

**INSTRUKSI KHUSUS: Jangan ubah kode deployment ini, karena sudah ter-optimize untuk fast response di Koyeb. Jika ada masalah dengan kode ini, diskusikan dengan user terlebih dahulu sebelum melakukan perubahan.**

Bagian ini menjelaskan COMPLETE logic untuk fast response di Koyeb deployment. Seluruh optimization ini sudah tested dan working dengan baik. Jangan modifikasi tanpa approval dari user.

---

### 📋 KOYEB FAST RESPONSE LOGIC - COMPLETE DOCUMENTATION

Bot menggunakan sistem auto-detection untuk mengoptimalkan response time di Koyeb. Berikut adalah logika lengkapnya:

#### 1. CLOUD DEPLOYMENT AUTO-DETECTION (config.py)

```python
def _is_cloud_deployment() -> bool:
    """Detect jika running di cloud deployment (Koyeb, Heroku, Railway, etc.)"""
    cloud_indicators = [
        'KOYEB_APP_NAME',
        'KOYEB_SERVICE_NAME', 
        'KOYEB_PUBLIC_DOMAIN',
        'DYNO',
        'RAILWAY_ENVIRONMENT',
        'RENDER_EXTERNAL_URL',
        'FLY_APP_NAME'
    ]
    return any(os.getenv(indicator) for indicator in cloud_indicators)
```

**Logika:**
- Otomatis detect jika bot running di cloud environment dengan check environment variables
- Tidak perlu manual setup - sepenuhnya automatic
- Check multiple cloud providers: Koyeb, Heroku, Railway, Render, Fly.io

**Mengapa penting:**
- Koyeb memiliki environment variables unik (KOYEB_PUBLIC_DOMAIN) yang tidak ada di Replit
- Sistem dapat membuat keputusan optimization berdasarkan environment yang terdeteksi

#### 2. SKIP PHOTO OPTIMIZATION (config.py line 182)

```python
SEND_SIGNAL_PHOTOS = os.getenv('SEND_SIGNAL_PHOTOS', 
    'false' if _is_cloud_deployment() else 'true').lower() == 'true'
```

**Logika:**
- Koyeb (cloud) = `SEND_SIGNAL_PHOTOS = FALSE` → **Hanya kirim TEKS, tanpa FOTO**
- Replit (local) = `SEND_SIGNAL_PHOTOS = TRUE` → **Kirim dengan FOTO**

**Alasan teknis:**
- Chart generation di Koyeb: 7-45 detik (CPU intensive)
- Photo send timeout: 120-180 detik
- Telegram rate limit: Aggressive photo retry (4906s = 1.36 jam)
- **Hasil:** Bot jadi very slow atau timeout

**Solusi:**
- Skip photo generation → Response INSTANT (hanya teks)
- Signal tetap sampai ke user tanpa delay
- CPU resources terbebaskan untuk market data dan strategy

**Performance Impact:**
- ❌ Dengan photo: Signal delay 30-60 detik, Telegram timeout, rate limit errors
- ✅ Tanpa photo: Signal instant (1-3 detik), responsive, no timeout

#### 3. WEBHOOK MODE AUTO-ENABLE (config.py line 60, 71-82)

```python
TELEGRAM_WEBHOOK_MODE = _should_use_webhook()

def _should_use_webhook() -> bool:
    """Determine jika webhook mode harus digunakan"""
    explicit_webhook = os.getenv('TELEGRAM_WEBHOOK_MODE', '').lower() == 'true'
    explicit_polling = os.getenv('TELEGRAM_WEBHOOK_MODE', '').lower() == 'false'
    
    if explicit_polling:
        return False
    
    if explicit_webhook or _is_cloud_deployment():
        return True  # ← Auto-enable di cloud!
    
    return False
```

**Logika:**
- Auto-detect cloud deployment → enable webhook mode
- Webhook mode lebih efisien untuk cloud deployment
- User bisa explicit override dengan environment variable

**Perbedaan:**
- ❌ **Polling mode:** Bot check Telegram API setiap 30s → LAMBAT, CPU intensive, network intensive
- ✅ **Webhook mode:** Telegram langsung kirim update ke bot endpoint → INSTANT response, minimal CPU

**Alasan teknis:**
- Polling = Pull (bot yang request ke Telegram)
  - Harus check terus-menerus (every 30s)
  - CPU usage tinggi
  - Network call berulang
  - Latency tinggi (sampai 30s)

- Webhook = Push (Telegram yang kirim ke bot)
  - Bot menerima update langsung
  - CPU usage minimal (hanya process yang masuk)
  - Network efficient (hanya HTTP callback)
  - Latency minimal (<1s)

#### 4. AUTO-DETECT WEBHOOK URL (main.py line 149-175)

```python
def _auto_detect_webhook_url(self) -> Optional[str]:
    """Auto-detect webhook URL untuk Replit, Koyeb, dan cloud platforms lain"""
    
    koyeb_public_domain = os.getenv('KOYEB_PUBLIC_DOMAIN')
    
    if koyeb_public_domain:
        domain = koyeb_public_domain.strip()
        logger.info(f"Detected Koyeb domain: {domain}")
        
        webhook_url = f"https://{domain}/bot{token}"
        return webhook_url  # Auto-setup webhook URL!
```

**Logika:**
- Auto-detect Koyeb public domain dari environment variable
- Generate webhook URL otomatis: `https://{domain}/bot{token}`
- Register webhook endpoint ke Telegram API
- Tidak perlu manual config!

**Alasan penting:**
- Koyeb free tier tidak memberikan domain stabil (sering berubah)
- KOYEB_PUBLIC_DOMAIN adalah satu-satunya cara reliable untuk get domain
- Manual webhook URL = cepat outdated + bot tidak berfungsi
- Auto-detection = selalu work regardless domain change

#### 5. FREE TIER MODE OPTIMIZATION (config.py line 62)

```python
FREE_TIER_MODE = os.getenv('FREE_TIER_MODE', 'true').lower() == 'true'
```

**Logika:**
- Auto-enable di free tier untuk optimize resource usage
- Beberapa optimasi yang apply:
  - ThreadPoolExecutor: max_workers = 1-2 (instead of 4+)
  - Signal detection: 3 detik (instead of 1 detik)
  - Dashboard update: 6 detik (instead of 5 detik)
  - Tick log sampling: 1 dari 30 ticks (reduce I/O 30x)
  - Candle persistence: Instant startup tanpa fetch API

**Resource constraints Koyeb free tier:**
- RAM: 512MB (vs paid tier 2GB+)
- CPU: 0.1 vCPU shared (vs paid tier 0.5+ vCPU)
- Network: Limited egress

**Optimization results:**
- ✅ Memory usage: <300MB (70% dari 512MB)
- ✅ CPU usage: <5% average (dalam 0.1 vCPU limit)
- ✅ 24/7 uptime tanpa crash
- ✅ 1-3 concurrent users support tanpa lag

---

### 🔄 DEPLOYMENT FLOW - AUTOMATIC

Ketika bot start di Koyeb:

```
Bot Start
    ↓
Detect KOYEB_PUBLIC_DOMAIN = 'xxx.koyeb.app'
    ↓
_is_cloud_deployment() = TRUE
    ↓
1. SEND_SIGNAL_PHOTOS = FALSE (auto-set)
2. TELEGRAM_WEBHOOK_MODE = TRUE (auto-enable)
3. FREE_TIER_MODE = TRUE (auto-enable)
4. WEBHOOK_URL auto-detected = 'https://xxx.koyeb.app/bot<token>'
    ↓
Setup webhook endpoint
    ↓
Register dengan Telegram API
    ↓
Bot start listening untuk webhook events
    ↓
Telegram kirim update → Webhook endpoint (instant)
    ↓
Bot process dalam 1-2 detik (tanpa photo overhead)
    ↓
Response kirim langsung ke user (teks only, fast)
    ↓
🚀 FAST RESPONSE = ACHIEVED
```

---

### 📊 EXPECTED PERFORMANCE METRICS

**Response Time:**
- ✅ Signal detection: 3-5 detik (tanpa photo overhead)
- ✅ Command processing: <2 detik
- ✅ Webhook response: <1 detik

**Resource Usage:**
- ✅ Memory: <300MB (dari 512MB limit)
- ✅ CPU: <5% average
- ✅ Network: Minimal (webhook push model)

**Reliability:**
- ✅ 24/7 uptime di Koyeb free tier
- ✅ No timeout errors
- ✅ No Telegram rate limit errors
- ✅ Auto-reconnect WebSocket

---

### ⚠️ DO NOT MODIFY - CRITICAL FILES

File berikut sudah ter-optimize dan TESTED. **Jangan ubah tanpa discussion:**

1. **config.py** (lines 58-182)
   - Cloud detection logic
   - Photo skip optimization
   - Webhook mode auto-enable
   - FREE_TIER_MODE settings

2. **main.py** (lines 149-175)
   - Auto-detect webhook URL
   - Koyeb domain detection
   - Webhook setup logic

3. **bot/telegram_bot.py** (lines 1778-1850)
   - Webhook handler implementation
   - Background task processing
   - Photo send skip logic

**Jika ada bug atau masalah:**
- Hubungi user TERLEBIH DAHULU
- Jelaskan masalah specific yang ada
- Jangan langsung ubah kode
- Discuss solusi sebelum implement

---

### 🎯 SUMMARY

Sistem fast response di Koyeb sudah **complete** dan **production-ready**. Semua optimization sudah tested dan working. Bot dapat:

- ✅ Auto-detect cloud environment
- ✅ Auto-optimize untuk fast response
- ✅ Auto-detect Koyeb domain
- ✅ Auto-setup webhook
- ✅ Auto-enable free tier mode
- ✅ **ZERO manual config needed!**

**Hasil:** Bot response time 3-5 detik di Koyeb free tier dengan 24/7 stable uptime.