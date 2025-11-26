# 🤖 XAUUSD Trading Bot Pro V2.3

Bot trading otomatis untuk XAUUSD (Gold) dengan Telegram integration, dual-mode signal strategy, dan auto-monitoring 24/7.

## ✨ Fitur Utama

- ✅ **Dual Signal Mode** - Auto (🤖 strict) & Manual (👤 relaxed) dengan logic terpisah
- ✅ **Enhanced Scalping Strategy** - RSI crossover + EMA trend + Stochastic + Volume
- ✅ **Real-time Market Data** - Streaming dari Deriv WebSocket (gratis, tanpa API key)
- ✅ **Auto Position Tracking** - Monitor posisi sampai TP/SL tercapai
- ✅ **Chart Generation** - Setiap sinyal dengan chart + indikator lengkap
- ✅ **Risk Management** - Dynamic SL/TP, spread filter, daily loss limit
- ✅ **Signal Source Tracking** - Database track auto vs manual terpisah
- ✅ **Admin Commands** - User management & database control
- ✅ **24/7 Monitoring** - Auto-start untuk authorized users
- ✅ **Auto-Migration** - Database schema updates tanpa data loss

## 🎯 Dual Signal Strategy (V2.3)

### 🤖 Auto Mode (Strict - High Precision)
**Logic:** AND (semua kondisi harus terpenuhi)
- ✅ EMA trend alignment (5 > 10 > 20 untuk BUY)
- ✅ RSI > 50 untuk BUY, < 50 untuk SELL
- ✅ Stochastic K/D crossover confirmation
- ✅ Volume > 0.5x average

**Keuntungan:** Akurasi tinggi, sinyal berkualitas
**Kekurangan:** Lebih jarang muncul

### 👤 Manual Mode (Relaxed - More Opportunities)
**Logic:** OR (flexible conditions)
- ✅ EMA trend OR EMA crossover
- ✅ RSI crossover zone OR bullish/bearish
- ✅ Stochastic & Volume opsional

**Keuntungan:** Lebih banyak peluang trading
**Kekurangan:** Perlu validasi manual

**Fallback:** Gracefully handle missing historical data (rsi_prev, stoch_prev)

## 📊 Indicators

- **EMA:** 5, 10, 20 (trend & momentum)
- **RSI:** 14 (overbought/oversold + crossover)
- **Stochastic:** K=14, D=3 (momentum confirmation)
- **ATR:** 14 (volatility for SL/TP)
- **Volume:** 0.5x average threshold

## 🛡️ Risk Management

- **Stop Loss:** 1.0x ATR (min 20 pips)
- **Take Profit:** 1.5x R:R (min 30 pips)
- **Max Spread:** 10 pips
- **Signal Cooldown:** 30 detik (auto mode)
- **Daily Loss Limit:** 3% dari balance
- **Risk per Trade:** 0.5% dari balance

## 📱 Telegram Commands

```
/start       - Menu utama + status subscription
/help        - Bantuan lengkap semua command

📊 TRADING
/monitor     - Mulai monitoring sinyal otomatis (🤖)
/stopmonitor - Stop monitoring
/getsignal   - Generate sinyal manual sekarang (👤)
/status      - Status posisi aktif & monitoring

📈 ANALISIS
/riwayat     - Riwayat trading (WIN/LOSE)
/performa    - Statistik & performa bot
/analytics   - Comprehensive analytics (30 hari)
/settings    - Lihat konfigurasi indikator

🔍 SYSTEM
/systemhealth - Status sistem (CPU, Memory, WebSocket)
/tasks        - Lihat scheduled tasks

🔧 ADMIN ONLY
/riset       - Reset database trading
```

## 🚀 Quick Start

### 1. Environment Variables

Buat file `.env` (lihat `.env.example` untuk template):

```bash
# WAJIB
TELEGRAM_BOT_TOKEN=your_bot_token_from_botfather
AUTHORIZED_USER_IDS=123456789,987654321

# OPTIONAL (sudah ada default bagus)
SIGNAL_COOLDOWN_SECONDS=30
MAX_SPREAD_PIPS=10.0
SL_ATR_MULTIPLIER=1.0
TP_RR_RATIO=1.5
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run Bot

```bash
python main.py
```

## 🐳 Deploy ke Koyeb

Lihat panduan lengkap di **[DEPLOYMENT_KOYEB.md](DEPLOYMENT_KOYEB.md)**

**Highlight:**
- ✅ Dockerfile sudah optimized untuk Debian Trixie
- ✅ Auto-migration database on startup
- ✅ Health check endpoint (/health:8080)
- ✅ Zero API key untuk market data
- ✅ Free tier ready

## 📂 Project Structure

```
├── main.py                 # Orchestrator (entry point)
├── config.py               # Konfigurasi & environment vars
├── requirements.txt        # Python dependencies
├── Dockerfile              # Container config (Koyeb ready)
├── .env.example            # Template environment variables
│
├── bot/                    # Core modules
│   ├── market_data.py      # Deriv WebSocket client
│   ├── strategy.py         # Signal detection (dual mode)
│   ├── indicators.py       # Technical indicators
│   ├── telegram_bot.py     # Telegram integration
│   ├── position_tracker.py # Real-time position monitoring
│   ├── chart_generator.py  # Chart dengan indikator
│   ├── risk_manager.py     # SL/TP & risk calculation
│   ├── database.py         # SQLite ORM (auto-migration)
│   ├── user_manager.py     # Subscription & access control
│   ├── alert_system.py     # Telegram notifications
│   ├── task_scheduler.py   # Background jobs
│   └── error_handler.py    # Error logging & recovery
│
├── data/                   # Database files (auto-created)
├── logs/                   # Application logs (auto-created)
├── charts/                 # Generated charts (auto-cleanup)
│
├── README.md               # Dokumentasi utama (file ini)
├── replit.md               # System architecture & changelog
├── DEPLOYMENT_KOYEB.md     # Panduan deploy
└── TRADING_STRATEGY.md     # Penjelasan strategi detail
```

## 🔧 Configuration

Semua parameter bisa diubah via environment variables. Default values sudah optimal untuk M1-M5 scalping.

**Recommended Settings:**
- `SIGNAL_COOLDOWN_SECONDS=30` - Balance antara spam & opportunity
- `MAX_SPREAD_PIPS=10.0` - Filter spread terlalu lebar
- `TP_RR_RATIO=1.5` - Risk:Reward 1:1.5 (realistis)
- `DAILY_LOSS_PERCENT=3.0` - Stop trading jika loss 3%

Lihat `.env.example` untuk daftar lengkap.

## 📊 Database Schema

Bot menggunakan SQLite dengan auto-migration:

- **trades** - Riwayat trade dengan result (WIN/LOSE)
- **signal_logs** - Log semua sinyal (termasuk yang ditolak)
- **positions** - Posisi aktif untuk tracking
- **performance** - Statistik harian
- **users** - User subscription & access control

**Auto-Migration:** Saat restart, bot otomatis detect & add kolom baru tanpa data loss.

## 🎨 Chart Features

Setiap sinyal disertai chart profesional:
- **Candlestick** dengan volume bar
- **EMA 5, 10, 20** untuk trend
- **RSI panel** dengan level overbought/oversold
- **Stochastic panel** dengan K/D lines
- **Entry/SL/TP markers** (untuk exit charts)

Auto-cleanup setelah 60 menit untuk hemat storage.

## 🔧 Admin Commands

Bot menyediakan command khusus untuk admin untuk mengelola user dan database:

### /riset - Reset Database Trading

Command ini akan mereset seluruh database trading dan menghentikan semua aktivitas monitoring.

**Yang direset:**
- ✅ Semua riwayat trading (trades)
- ✅ Posisi aktif (positions)
- ✅ Data performa (performance)
- ✅ Monitoring aktif dihentikan
- ✅ Sinyal aktif dibatalkan

**Contoh penggunaan:**
```
/riset
```

**Catatan:** Command ini hanya bisa digunakan oleh admin dan akan membersihkan semua data trading. Gunakan dengan hati-hati!

## 📈 Performance Tracking

Bot track performa auto vs manual terpisah:

```sql
SELECT 
    signal_source, 
    COUNT(*) as total,
    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
    ROUND(AVG(actual_pl), 2) as avg_profit
FROM trades 
GROUP BY signal_source;
```

Gunakan `/performa` di Telegram untuk statistik lengkap.

## 🔍 Troubleshooting

### Bot Tidak Respond
- Check `TELEGRAM_BOT_TOKEN` benar
- Check `AUTHORIZED_USER_IDS` match dengan user ID Anda
- Lihat logs untuk error: `tail -f logs/main.log`

### Database Error
- Check file `data/bot.db` tidak corrupt
- Hapus file `data/bot.db*` untuk reset (DANGER: data hilang!)
- Auto-migration akan handle schema updates

### No Signals
- Auto mode strict, perlu semua kondisi terpenuhi
- Gunakan `/getsignal` untuk manual mode (lebih banyak peluang)
- Check market buka (XAUUSD trading 24/5, tutup weekend)

### Docker Build Failed (Koyeb)
- ✅ SUDAH FIXED di V2.3
- Dockerfile menggunakan `libgl1` untuk Debian Trixie
- Build sekarang berjalan lancar

## 📚 Documentation

- **[TRADING_STRATEGY.md](TRADING_STRATEGY.md)** - Strategi scalping detail
- **[DEPLOYMENT_KOYEB.md](DEPLOYMENT_KOYEB.md)** - Deploy guide
- **[replit.md](replit.md)** - System architecture & recent changes

## 🔄 Recent Changes (V2.3)

**Date:** November 18, 2025

1. ✅ Fixed Koyeb deployment error (libgl1-mesa-glx → libgl1)
2. ✅ Dual-mode signal strategy (auto strict + manual relaxed)
3. ✅ Enhanced scalping strategy (RSI crossover + EMA + Volume)
4. ✅ Database schema update (signal_source field)
5. ✅ Auto-migration system (backward compatible)
6. ✅ Manual signal bug fix (graceful fallback for missing data)
7. ✅ Enhanced signal messages (source icons + confidence reasons)

## ⚠️ Disclaimer

**PENTING:** Bot ini untuk informasi dan edukasi trading saja. TIDAK ada eksekusi trading otomatis ke broker. User bertanggung jawab penuh atas semua keputusan trading berdasarkan sinyal bot.

Trading forex/gold berisiko tinggi. Gunakan dengan bijak dan risk management yang baik.

## 📄 License

MIT License - Free to use and modify

## 🤝 Support

- **Telegram:** @dzeckyete
- **Issues:** Open issue di GitHub repository
- **Subscription:** Hubungi @dzeckyete

## 🔗 Links

- **Deriv API:** https://api.deriv.com
- **Telegram Bot API:** https://core.telegram.org/bots/api
- **XAUUSD Info:** https://www.investing.com/currencies/xau-usd

---

**Made with ❤️ for XAUUSD traders**
**Version 2.3 - Enhanced Strategy & Signal Separation**
