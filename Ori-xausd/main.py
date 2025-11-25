import asyncio
import signal
import sys
import os
from aiohttp import web
from typing import Optional
from sqlalchemy import text

from config import Config
from bot.logger import setup_logger, mask_token, sanitize_log_message
from bot.database import DatabaseManager
from bot.sentry_integration import initialize_sentry, get_sentry_manager
from bot.backup import DatabaseBackupManager
from bot.market_data import MarketDataClient
from bot.strategy import TradingStrategy
from bot.risk_manager import RiskManager
from bot.position_tracker import PositionTracker
from bot.chart_generator import ChartGenerator
from bot.telegram_bot import TradingBot
from bot.alert_system import AlertSystem
from bot.error_handler import ErrorHandler
from bot.user_manager import UserManager
from bot.task_scheduler import TaskScheduler, setup_default_tasks
from bot.signal_session_manager import SignalSessionManager

logger = setup_logger('Main')

class TradingBotOrchestrator:
    def __init__(self):
        self.config = Config()
        self.config_valid = False
        
        # Initialize Sentry for error tracking (optional - fallback to local logging if not configured)
        sentry_dsn = os.getenv('SENTRY_DSN')
        environment = os.getenv('ENVIRONMENT', 'production')
        initialize_sentry(sentry_dsn, environment)
        logger.info(f"Sentry error tracking initialized (environment: {environment})")
        
        logger.info("Validating configuration...")
        try:
            self.config.validate()
            logger.info("✅ Configuration validated successfully")
            self.config_valid = True
        except Exception as e:
            logger.warning(f"⚠️ Configuration validation issues: {e}")
            logger.warning("Bot will start in limited mode - health check will be available")
            logger.warning("Set missing environment variables and restart to enable full functionality")
            self.config_valid = False
            
            # Capture initialization errors to Sentry
            sentry = get_sentry_manager()
            sentry.capture_exception(e, {'context': 'Configuration validation'})
        
        self.running = False
        self.shutdown_in_progress = False
        self.shutdown_event = asyncio.Event()
        self.health_server = None
        self.tracked_tasks = []
        
        self.db_manager = DatabaseManager(
            db_path=self.config.DATABASE_PATH,
            database_url=self.config.DATABASE_URL
        )
        logger.info("Database initialized")
        
        # Initialize database backup manager
        self.backup_manager = DatabaseBackupManager(
            db_path=self.config.DATABASE_PATH,
            backup_dir='backups',
            max_backups=7
        )
        if self.config.DATABASE_URL:
            self.backup_manager.configure_postgres(self.config.DATABASE_URL)
        logger.info("Database backup manager initialized")
        
        self.task_scheduler = TaskScheduler(self.config)
        logger.info("Task scheduler initialized (available in all modes)")
        
        if not self.config_valid:
            logger.warning("Skipping full component initialization - running in limited mode")
            self.error_handler = None
            self.user_manager = None
            self.market_data = None
            self.strategy = None
            self.risk_manager = None
            self.chart_generator = None
            self.alert_system = None
            self.position_tracker = None
            self.telegram_bot = None
            logger.info("Limited mode: Only database, task scheduler, and health server will be initialized")
            return
        
        logger.info("Initializing Trading Bot components...")
        
        self.error_handler = ErrorHandler(self.config)
        logger.info("Error handler initialized")
        
        self.user_manager = UserManager(self.config)
        logger.info("User manager initialized")
        
        self.market_data = MarketDataClient(self.config)
        logger.info("Market data client initialized")
        
        self.strategy = TradingStrategy(self.config)
        logger.info("Trading strategy initialized")
        
        self.risk_manager = RiskManager(self.config, self.db_manager)
        logger.info("Risk manager initialized")
        
        self.chart_generator = ChartGenerator(self.config)
        logger.info("Chart generator initialized")
        
        self.alert_system = AlertSystem(self.config, self.db_manager)
        logger.info("Alert system initialized")
        
        self.signal_session_manager = SignalSessionManager()
        logger.info("Signal session manager initialized")
        
        self.position_tracker = PositionTracker(
            self.config, 
            self.db_manager, 
            self.risk_manager,
            self.alert_system,
            self.user_manager,
            self.chart_generator,
            self.market_data,
            signal_session_manager=self.signal_session_manager
        )
        logger.info("Position tracker initialized")
        
        self.telegram_bot = TradingBot(
            self.config,
            self.db_manager,
            self.strategy,
            self.risk_manager,
            self.market_data,
            self.position_tracker,
            self.chart_generator,
            self.alert_system,
            self.error_handler,
            self.user_manager,
            self.signal_session_manager,
            self.task_scheduler
        )
        logger.info("Telegram bot initialized")
        
        logger.info("All components initialized successfully")
    
    def _auto_detect_webhook_url(self) -> Optional[str]:
        """Auto-detect webhook URL for Replit, Koyeb, and other cloud platforms
        
        Returns:
            str: Auto-detected webhook URL or None if not detected
        """
        if self.config.WEBHOOK_URL and self.config.WEBHOOK_URL.strip():
            return None
        
        import json
        from urllib.parse import urlparse
        
        domain = None
        
        koyeb_app_name = os.getenv('KOYEB_APP_NAME')
        koyeb_service_name = os.getenv('KOYEB_SERVICE_NAME')
        koyeb_public_domain = os.getenv('KOYEB_PUBLIC_DOMAIN')
        
        if koyeb_public_domain:
            domain = koyeb_public_domain.strip()
            logger.info(f"Detected Koyeb domain from KOYEB_PUBLIC_DOMAIN: {domain}")
        elif koyeb_app_name or koyeb_service_name:
            app_name = koyeb_app_name or koyeb_service_name
            domain = f"{app_name}.koyeb.app"
            logger.info(f"Constructed Koyeb domain from app/service name: {domain}")
        
        if not domain:
            replit_domains = os.getenv('REPLIT_DOMAINS')
            replit_dev_domain = os.getenv('REPLIT_DEV_DOMAIN')
            
            if replit_domains:
                try:
                    domains_list = json.loads(replit_domains)
                    if isinstance(domains_list, list) and len(domains_list) > 0:
                        domain = str(domains_list[0]).strip()
                        logger.info(f"Detected Replit deployment domain from REPLIT_DOMAINS: {domain}")
                    else:
                        logger.warning(f"REPLIT_DOMAINS is not a valid array: {replit_domains}")
                except json.JSONDecodeError:
                    domain = replit_domains.strip().strip('[]"\'').split(',')[0].strip().strip('"\'')
                    logger.warning(f"Failed to parse REPLIT_DOMAINS as JSON, using fallback: {domain}")
                except Exception as e:
                    logger.error(f"Error parsing REPLIT_DOMAINS: {e}")
            
            if not domain and replit_dev_domain:
                domain = replit_dev_domain.strip()
                logger.info(f"Detected Replit dev domain from REPLIT_DEV_DOMAIN: {domain}")
        
        if domain:
            domain = domain.strip().strip('"\'')
            
            if not domain or domain.startswith('[') or domain.startswith('{') or '"' in domain or "'" in domain or '://' in domain:
                logger.error(f"Invalid domain detected after parsing: {domain}")
                return None
            
            try:
                test_url = f"https://{domain}"
                parsed = urlparse(test_url)
                if not parsed.netloc or parsed.netloc != domain:
                    logger.error(f"Domain validation failed - invalid structure: {domain}")
                    return None
            except Exception as e:
                logger.error(f"Domain validation error: {e}")
                return None
            
            bot_token = self.config.TELEGRAM_BOT_TOKEN
            webhook_url = f"https://{domain}/bot{bot_token}"
            
            logger.info(f"✅ Auto-constructed webhook URL: {webhook_url}")
            return webhook_url
        
        logger.warning("Could not auto-detect webhook URL - no Koyeb/Replit domain found")
        logger.warning("Set WEBHOOK_URL environment variable manually or KOYEB_PUBLIC_DOMAIN")
        return None
        
    async def start_health_server(self):
        try:
            import socket
            
            def is_port_in_use(port: int) -> bool:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    return s.connect_ex(('localhost', port)) == 0
            
            port = self.config.HEALTH_CHECK_PORT
            max_port_attempts = 5
            
            for attempt in range(max_port_attempts):
                if is_port_in_use(port):
                    logger.warning(f"Port {port} is already in use (attempt {attempt + 1}/{max_port_attempts})")
                    port += 1
                    logger.info(f"Trying alternative port: {port}")
                else:
                    logger.info(f"✅ Port {port} is available")
                    self.config.HEALTH_CHECK_PORT = port
                    break
            else:
                logger.error(f"Could not find available port after {max_port_attempts} attempts")
                raise Exception(f"All ports from {self.config.HEALTH_CHECK_PORT} to {port} are in use")
            
            async def health_check(request):
                missing_config = []
                if not self.config.TELEGRAM_BOT_TOKEN:
                    missing_config.append('TELEGRAM_BOT_TOKEN')
                if not self.config.AUTHORIZED_USER_IDS:
                    missing_config.append('AUTHORIZED_USER_IDS')
                
                market_status = 'not_initialized' if not self.config_valid else (self.market_data.get_status() if self.market_data else 'not_initialized')
                
                db_status = 'unknown'
                try:
                    session = self.db_manager.get_session()
                    result = session.execute(text('SELECT 1'))
                    result.fetchone()
                    session.close()
                    db_status = 'connected'
                except Exception as e:
                    db_status = f'error: {str(e)[:50]}'
                    logger.error(f"Database health check failed: {e}")
                
                mode = 'full' if self.config_valid else 'limited'
                overall_status = 'healthy' if self.config_valid and self.running else 'limited' if not self.config_valid else 'stopped'
                
                health_status = {
                    'status': overall_status,
                    'mode': mode,
                    'config_valid': self.config_valid,
                    'missing_config': missing_config,
                    'market_data': market_status,
                    'telegram_bot': 'running' if self.config_valid and self.telegram_bot and self.telegram_bot.app else 'not_initialized',
                    'scheduler': 'running' if self.config_valid and self.task_scheduler and self.task_scheduler.running else 'not_initialized',
                    'database': db_status,
                    'webhook_mode': self.config.TELEGRAM_WEBHOOK_MODE if self.config_valid else False,
                    'message': 'Bot running in limited mode - set missing environment variables to enable full functionality' if not self.config_valid else 'Bot running normally'
                }
                
                status_code = 200 if self.config_valid and self.running else 503
                
                return web.json_response(health_status, status=status_code)
            
            async def telegram_webhook(request):
                if not self.config.TELEGRAM_WEBHOOK_MODE:
                    logger.warning("⚠️ Webhook endpoint called but webhook mode is disabled")
                    return web.json_response({'error': 'Webhook mode is disabled'}, status=400)
                
                if not self.telegram_bot or not self.telegram_bot.app:
                    logger.error("❌ Webhook called but telegram bot not initialized (running in limited mode?)")
                    logger.error("Check if TELEGRAM_BOT_TOKEN and AUTHORIZED_USER_IDS are set in environment variables")
                    return web.json_response({'error': 'Bot not initialized'}, status=503)
                
                try:
                    update_data = await request.json()
                    update_id = update_data.get('update_id', 'unknown')
                    message_text = update_data.get('message', {}).get('text', 'no text')
                    user_id = update_data.get('message', {}).get('from', {}).get('id', 'unknown')
                    
                    logger.info(f"📨 Webhook received: update_id={update_id}, user={user_id}, message='{message_text}'")
                    
                    await self.telegram_bot.process_update(update_data)
                    
                    logger.info(f"✅ Webhook processed successfully: update_id={update_id}")
                    return web.json_response({'ok': True})
                    
                except Exception as e:
                    logger.error(f"❌ Error processing webhook request: {e}")
                    logger.error(f"Request path: {request.path}, Method: {request.method}")
                    if self.error_handler:
                        self.error_handler.log_exception(e, "webhook_endpoint")
                    return web.json_response({'error': str(e)}, status=500)
            
            app = web.Application()
            app.router.add_get('/health', health_check)
            app.router.add_get('/', health_check)
            
            webhook_path = None
            if self.config_valid and self.config.TELEGRAM_BOT_TOKEN:
                webhook_path = f"/bot{self.config.TELEGRAM_BOT_TOKEN}"
                app.router.add_post(webhook_path, telegram_webhook)
                logger.info(f"Webhook route registered: {webhook_path}")
            else:
                logger.info("Webhook route not registered - limited mode or missing bot token")
            
            runner = web.AppRunner(app)
            await runner.setup()
            
            site = web.TCPSite(runner, '0.0.0.0', self.config.HEALTH_CHECK_PORT)
            await site.start()
            
            self.health_server = runner
            logger.info(f"Health check server started on port {self.config.HEALTH_CHECK_PORT}")
            if self.config.TELEGRAM_WEBHOOK_MODE and webhook_path:
                logger.info(f"Webhook endpoint available at: http://0.0.0.0:{self.config.HEALTH_CHECK_PORT}{webhook_path}")
            elif self.config.TELEGRAM_WEBHOOK_MODE:
                logger.info("Webhook mode enabled but endpoint not available (limited mode)")
            
        except Exception as e:
            logger.error(f"Failed to start health server: {e}")
    
    async def setup_scheduled_tasks(self):
        if not self.config_valid or not self.task_scheduler:
            logger.warning("Skipping scheduled tasks setup - limited mode or scheduler not initialized")
            return
            
        bot_components = {
            'chart_generator': self.chart_generator,
            'alert_system': self.alert_system,
            'db_manager': self.db_manager,
            'market_data': self.market_data,
            'position_tracker': self.position_tracker
        }
        
        setup_default_tasks(self.task_scheduler, bot_components)
        logger.info("Scheduled tasks configured")
    
    async def start(self):
        logger.info("=" * 60)
        logger.info("XAUUSD TRADING BOT STARTING")
        logger.info("=" * 60)
        logger.info(f"Mode: {'DRY RUN (Simulation)' if self.config.DRY_RUN else 'LIVE'}")
        logger.info(f"Config Valid: {'YES ✅' if self.config_valid else 'NO ⚠️ (Limited Mode)'}")
        
        if self.config.TELEGRAM_BOT_TOKEN:
            logger.info(f"Telegram Bot Token: Configured ({self.config.get_masked_token()})")
            
            if ':' in self.config.TELEGRAM_BOT_TOKEN and len(self.config.TELEGRAM_BOT_TOKEN) > 40:
                logger.warning("⚠️ Bot token detected - ensure it's never logged in plain text")
        else:
            logger.warning("Telegram Bot Token: NOT CONFIGURED ⚠️")
        
        logger.info(f"Authorized Users: {len(self.config.AUTHORIZED_USER_IDS)}")
        
        if self.config.TELEGRAM_WEBHOOK_MODE:
            webhook_url = self._auto_detect_webhook_url()
            if webhook_url:
                self.config.WEBHOOK_URL = webhook_url
                logger.info(f"Webhook URL auto-detected: {webhook_url}")
            else:
                logger.info(f"Webhook mode enabled with URL: {self.config.WEBHOOK_URL}")
        
        logger.info("=" * 60)
        
        if not self.config_valid:
            logger.warning("=" * 60)
            logger.warning("RUNNING IN LIMITED MODE")
            logger.warning("=" * 60)
            logger.warning("Bot functionality will be limited due to missing configuration.")
            logger.warning("")
            logger.warning("To enable full functionality, set these environment variables:")
            if not self.config.TELEGRAM_BOT_TOKEN:
                logger.warning("  - TELEGRAM_BOT_TOKEN (get from @BotFather on Telegram)")
            if not self.config.AUTHORIZED_USER_IDS:
                logger.warning("  - AUTHORIZED_USER_IDS (your Telegram user ID)")
            logger.warning("")
            logger.warning("Health check endpoint will remain available at /health")
            logger.warning("=" * 60)
            
            logger.info("Starting health check server only...")
            await self.start_health_server()
            
            logger.info("=" * 60)
            logger.info("BOT RUNNING IN LIMITED MODE - HEALTH CHECK AVAILABLE")
            logger.info("=" * 60)
            logger.info("Set environment variables and restart to enable trading functionality")
            
            await self.shutdown_event.wait()
            return
        
        self.running = True
        
        assert self.market_data is not None, "Market data should be initialized in full mode"
        assert self.telegram_bot is not None, "Telegram bot should be initialized in full mode"
        assert self.task_scheduler is not None, "Task scheduler should be initialized early (available in all modes)"
        assert self.position_tracker is not None, "Position tracker should be initialized in full mode"
        assert self.alert_system is not None, "Alert system should be initialized in full mode"
        
        try:
            logger.info("Starting health check server...")
            await self.start_health_server()
            
            logger.info("Loading candles from database...")
            await self.market_data.load_candles_from_db(self.db_manager)
            
            logger.info("Connecting to market data feed...")
            market_task = asyncio.create_task(self.market_data.connect_websocket())
            self.tracked_tasks.append(market_task)
            
            logger.info("Waiting for initial market data (max 10s)...")
            for i in range(10):
                await asyncio.sleep(1)
                if self.market_data.is_connected():
                    logger.info("✅ Market data connection established")
                    break
                if i % 3 == 0:
                    logger.info(f"Connecting to market data... ({i}s)")
            
            if not self.market_data.is_connected():
                logger.warning("⚠️ Market data not connected - using cached candles or simulator mode")
            
            logger.info("Setting up scheduled tasks...")
            await self.setup_scheduled_tasks()
            
            logger.info("Starting task scheduler...")
            await self.task_scheduler.start()
            
            logger.info("Starting position tracker...")
            position_task = asyncio.create_task(
                self.position_tracker.monitor_positions(self.market_data)
            )
            self.tracked_tasks.append(position_task)
            
            logger.info("Initializing Telegram bot...")
            bot_initialized = await self.telegram_bot.initialize()
            
            if not bot_initialized:
                logger.error("Failed to initialize Telegram bot!")
                return
            
            if self.telegram_bot.app and self.config.AUTHORIZED_USER_IDS:
                self.alert_system.set_telegram_app(
                    self.telegram_bot.app,
                    self.config.AUTHORIZED_USER_IDS,
                    send_message_callback=self.telegram_bot._send_telegram_message
                )
                self.alert_system.telegram_app = self.telegram_bot.app
                self.position_tracker.telegram_app = self.telegram_bot.app
                logger.info("Telegram app set for alert system and position tracker with rate-limited callback")
            
            if self.config.TELEGRAM_WEBHOOK_MODE:
                if self.config.WEBHOOK_URL:
                    logger.info(f"Setting up webhook: {self.config.WEBHOOK_URL}")
                    try:
                        success = await self.telegram_bot.setup_webhook(self.config.WEBHOOK_URL)
                        if success:
                            logger.info("✅ Webhook setup completed successfully")
                        else:
                            logger.error("❌ Webhook setup failed!")
                    except Exception as e:
                        logger.error(f"❌ Failed to setup webhook: {e}")
                        if self.error_handler:
                            self.error_handler.log_exception(e, "webhook_setup")
                else:
                    logger.error("=" * 60)
                    logger.error("⚠️ WEBHOOK MODE ENABLED BUT NO WEBHOOK_URL!")
                    logger.error("=" * 60)
                    logger.error("Webhook mode is enabled but WEBHOOK_URL is not set.")
                    logger.error("This means bot CANNOT receive Telegram updates!")
                    logger.error("")
                    logger.error("To fix this:")
                    logger.error("1. Set WEBHOOK_URL environment variable in Koyeb, OR")
                    logger.error("2. Set KOYEB_PUBLIC_DOMAIN environment variable, OR")
                    logger.error("3. Run this command to set webhook manually:")
                    logger.error("   python3 fix_webhook.py")
                    logger.error("")
                    logger.error("Bot will continue but WILL NOT respond to commands!")
                    logger.error("=" * 60)
            
            logger.info("Starting Telegram bot polling...")
            bot_task = asyncio.create_task(self.telegram_bot.run())
            self.tracked_tasks.append(bot_task)
            
            logger.info("Waiting for candles to build (minimal 30 candles, max 20s)...")
            candle_ready = False
            for i in range(20):
                await asyncio.sleep(1)
                try:
                    df_check = await asyncio.wait_for(
                        self.market_data.get_historical_data('M1', 100),
                        timeout=3.0
                    )
                    if df_check is not None and len(df_check) >= 30:
                        logger.info(f"✅ Got {len(df_check)} candles, ready for trading!")
                        candle_ready = True
                        break
                except asyncio.TimeoutError:
                    pass
                if i % 5 == 0 and i > 0:
                    logger.info(f"Building candles... {i}s elapsed")
            
            if not candle_ready:
                logger.warning("⚠️ Candles not fully built yet, but continuing - bot will use available data")
            
            if self.telegram_bot.app and self.config.AUTHORIZED_USER_IDS:
                startup_msg = (
                    "🤖 *Bot Started Successfully*\n\n"
                    f"Mode: {'DRY RUN' if self.config.DRY_RUN else 'LIVE'}\n"
                    f"Market: {'Connected' if self.market_data.is_connected() else 'Connecting...'}\n"
                    f"Status: Auto-monitoring AKTIF ✅\n\n"
                    "Bot akan otomatis mendeteksi sinyal trading.\n"
                    "Gunakan /help untuk list command"
                )
                
                valid_user_ids = []
                for user_id in self.config.AUTHORIZED_USER_IDS:
                    try:
                        chat_info = await asyncio.wait_for(
                            self.telegram_bot.app.bot.get_chat(user_id),
                            timeout=5.0
                        )
                        if chat_info.type == 'bot':
                            logger.warning(f"Skipping bot ID {user_id} - cannot send messages to bots")
                            continue
                        
                        valid_user_ids.append(user_id)
                        await asyncio.wait_for(
                            self.telegram_bot.app.bot.send_message(
                                chat_id=user_id,
                                text=startup_msg,
                                parse_mode='Markdown'
                            ),
                            timeout=5.0
                        )
                        logger.debug(f"Startup message sent successfully to user {user_id}")
                    except Exception as telegram_error:
                        error_type = type(telegram_error).__name__
                        error_msg = str(telegram_error).lower()
                        
                        if 'bot' in error_msg and 'forbidden' in error_msg:
                            logger.warning(f"Skipping bot ID {user_id} - Telegram bots cannot receive messages")
                        else:
                            logger.error(f"Failed to send startup message to user {user_id}: [{error_type}] {telegram_error}")
                            if self.error_handler:
                                self.error_handler.log_exception(telegram_error, f"startup_message_user_{user_id}")
                
                if valid_user_ids:
                    logger.info(f"Auto-starting monitoring for {len(valid_user_ids)} valid users...")
                    await self.telegram_bot.auto_start_monitoring(valid_user_ids)
                else:
                    logger.warning("No valid user IDs found - all IDs are either bots or invalid")
            
            logger.info("=" * 60)
            logger.info("BOT IS NOW RUNNING")
            logger.info("=" * 60)
            logger.info("Press Ctrl+C to stop")
            
            await self.shutdown_event.wait()
            
        except asyncio.CancelledError:
            logger.info("Bot tasks cancelled")
        except Exception as e:
            logger.error(f"Error during bot operation: {e}")
            if self.error_handler:
                self.error_handler.log_exception(e, "main_loop")
            if self.alert_system:
                await self.alert_system.send_system_error(f"Bot crashed: {str(e)}")
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        if not self.running or self.shutdown_in_progress:
            logger.debug("Shutdown already in progress or bot not running, skipping.")
            return
        
        self.shutdown_in_progress = True
        logger.info("=" * 60)
        logger.info("SHUTTING DOWN BOT...")
        logger.info("=" * 60)
        
        self.running = False
        shutdown_timeout = 28
        
        loop = asyncio.get_running_loop()
        shutdown_start_time = loop.time()
        
        try:
            logger.info("Cancelling tracked async tasks...")
            cancelled_count = 0
            for task in self.tracked_tasks:
                if not task.done():
                    task.cancel()
                    cancelled_count += 1
            
            if cancelled_count > 0:
                logger.info(f"Cancelled {cancelled_count} tasks, waiting for completion...")
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*self.tracked_tasks, return_exceptions=True),
                        timeout=8
                    )
                    logger.info("All tracked tasks completed")
                except asyncio.TimeoutError:
                    logger.warning("Some tasks did not complete within timeout")
            
            logger.info("Stopping Telegram bot...")
            if self.telegram_bot:
                try:
                    await asyncio.wait_for(self.telegram_bot.stop(), timeout=8)
                except asyncio.TimeoutError:
                    logger.warning(f"Telegram bot shutdown timed out after 8s")
                except Exception as e:
                    logger.error(f"Error stopping Telegram bot: {e}")
            
            logger.info("Stopping position tracker...")
            if self.position_tracker:
                try:
                    self.position_tracker.stop_monitoring()
                except Exception as e:
                    logger.error(f"Error stopping position tracker: {e}")
            
            logger.info("Stopping task scheduler...")
            if self.task_scheduler:
                try:
                    await asyncio.wait_for(self.task_scheduler.stop(), timeout=5)
                except asyncio.TimeoutError:
                    logger.warning(f"Task scheduler shutdown timed out after 5s")
                except Exception as e:
                    logger.error(f"Error stopping task scheduler: {e}")
            
            logger.info("Stopping chart generator...")
            if self.chart_generator:
                try:
                    self.chart_generator.shutdown()
                except Exception as e:
                    logger.error(f"Error shutting down chart generator: {e}")
            
            logger.info("Saving candles to database...")
            if self.market_data:
                try:
                    await self.market_data.save_candles_to_db(self.db_manager)
                except Exception as e:
                    logger.error(f"Error saving candles: {e}")
            
            logger.info("Stopping market data connection...")
            if self.market_data:
                try:
                    self.market_data.disconnect()
                except Exception as e:
                    logger.error(f"Error disconnecting market data: {e}")
            
            logger.info("Closing database connections...")
            if self.db_manager:
                try:
                    self.db_manager.close()
                except Exception as e:
                    logger.error(f"Error closing database: {e}")
            
            logger.info("Stopping health server...")
            if self.health_server:
                try:
                    await asyncio.wait_for(self.health_server.cleanup(), timeout=5)
                except asyncio.TimeoutError:
                    logger.warning(f"Health server shutdown timed out after 5s")
                except Exception as e:
                    logger.error(f"Error stopping health server: {e}")
            
            shutdown_duration = loop.time() - shutdown_start_time
            logger.info(f"Shutdown completed in {shutdown_duration:.2f}s")
            
            if shutdown_duration > shutdown_timeout:
                logger.warning(f"Graceful shutdown exceeded timeout ({shutdown_duration:.2f}s > {shutdown_timeout}s)")
            
            logger.info("=" * 60)
            logger.info("BOT SHUTDOWN COMPLETE")
            logger.info("=" * 60)
            
            import logging
            logging.shutdown()
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            import logging
            logging.shutdown()
            raise
        finally:
            self.shutdown_in_progress = False

async def main():
    orchestrator = TradingBotOrchestrator()
    loop = asyncio.get_running_loop()
    
    def signal_handler(sig, frame):
        signame = signal.Signals(sig).name
        logger.info(f"Received signal {signame} ({sig})")
        try:
            loop.call_soon_threadsafe(orchestrator.shutdown_event.set)
        except RuntimeError:
            pass
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await orchestrator.start()
        return 0
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received")
        return 0
    except Exception as e:
        logger.error(f"Unhandled exception in main: {e}")
        return 1
    finally:
        if not orchestrator.shutdown_in_progress:
            await orchestrator.shutdown()

if __name__ == "__main__":
    exit_code = 1
    try:
        exit_code = asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        exit_code = 0
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        exit_code = 1
    
    sys.exit(exit_code)
