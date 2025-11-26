import asyncio
import signal
import sys
import os
from aiohttp import web
from typing import Optional, Dict, Set, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
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


class TaskPriority(Enum):
    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass
class TaskInfo:
    name: str
    task: asyncio.Task
    priority: TaskPriority = TaskPriority.NORMAL
    critical: bool = False
    created_at: datetime = field(default_factory=datetime.now)
    status: TaskStatus = TaskStatus.RUNNING
    cancel_timeout: float = 5.0
    
    def is_done(self) -> bool:
        return self.task.done()
    
    def is_cancelled(self) -> bool:
        return self.task.cancelled()


class TradingBotOrchestrator:
    SHUTDOWN_TOTAL_TIMEOUT = 30
    SHUTDOWN_PHASE_TIMEOUT = 8
    TASK_CANCEL_TIMEOUT = 5
    
    def __init__(self):
        self.config = Config()
        self.config_valid = False
        
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
            
            sentry = get_sentry_manager()
            sentry.capture_exception(e, {'context': 'Configuration validation'})
        
        self.running = False
        self._shutdown_in_progress = False
        self._shutdown_count = 0
        self._shutdown_lock = asyncio.Lock()
        self.shutdown_event = asyncio.Event()
        self.health_server = None
        
        self._task_registry: Dict[str, TaskInfo] = {}
        self._task_registry_lock = asyncio.Lock()
        self._completed_tasks: Set[str] = set()
        
        self.db_manager = DatabaseManager(
            db_path=self.config.DATABASE_PATH,
            database_url=self.config.DATABASE_URL
        )
        logger.info("Database initialized")
        
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
    
    @property
    def shutdown_in_progress(self) -> bool:
        return self._shutdown_in_progress
    
    async def register_task(
        self,
        name: str,
        task: asyncio.Task,
        priority: TaskPriority = TaskPriority.NORMAL,
        critical: bool = False,
        cancel_timeout: float = 5.0
    ) -> TaskInfo:
        async with self._task_registry_lock:
            if name in self._task_registry:
                old_task = self._task_registry[name]
                if not old_task.is_done():
                    logger.warning(f"Replacing existing running task: {name}")
                    old_task.task.cancel()
            
            task_info = TaskInfo(
                name=name,
                task=task,
                priority=priority,
                critical=critical,
                cancel_timeout=cancel_timeout
            )
            self._task_registry[name] = task_info
            logger.debug(f"Task registered: {name} (priority={priority.name}, critical={critical})")
            return task_info
    
    async def unregister_task(self, name: str, cancel: bool = False) -> bool:
        async with self._task_registry_lock:
            if name not in self._task_registry:
                logger.warning(f"Task not found for unregister: {name}")
                return False
            
            task_info = self._task_registry[name]
            
            if cancel and not task_info.is_done():
                task_info.task.cancel()
                try:
                    await asyncio.wait_for(
                        asyncio.shield(task_info.task),
                        timeout=task_info.cancel_timeout
                    )
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            
            task_info.status = TaskStatus.COMPLETED if task_info.is_done() else TaskStatus.CANCELLED
            self._completed_tasks.add(name)
            del self._task_registry[name]
            logger.debug(f"Task unregistered: {name} (status={task_info.status.value})")
            return True
    
    def get_task_status(self, name: str) -> Optional[TaskStatus]:
        if name in self._task_registry:
            task_info = self._task_registry[name]
            if task_info.is_done():
                if task_info.is_cancelled():
                    return TaskStatus.CANCELLED
                elif task_info.task.exception():
                    return TaskStatus.FAILED
                return TaskStatus.COMPLETED
            return TaskStatus.RUNNING
        elif name in self._completed_tasks:
            return TaskStatus.COMPLETED
        return None
    
    def get_registered_tasks(self) -> Dict[str, Dict[str, Any]]:
        result = {}
        for name, info in self._task_registry.items():
            result[name] = {
                'priority': info.priority.name,
                'critical': info.critical,
                'created_at': info.created_at.isoformat(),
                'is_done': info.is_done(),
                'is_cancelled': info.is_cancelled(),
                'status': self.get_task_status(name).value if self.get_task_status(name) else 'unknown'
            }
        return result
    
    async def _cancel_task_with_shield(
        self,
        task_info: TaskInfo,
        timeout: float
    ) -> bool:
        if task_info.is_done():
            return True
        
        name = task_info.name
        
        if task_info.critical:
            logger.info(f"[SHUTDOWN] Shielding critical task: {name}")
            try:
                await asyncio.wait_for(
                    asyncio.shield(task_info.task),
                    timeout=timeout
                )
                logger.info(f"[SHUTDOWN] Critical task completed: {name}")
                return True
            except asyncio.TimeoutError:
                logger.warning(f"[SHUTDOWN] Critical task {name} timeout after {timeout}s, forcing cancel")
                task_info.task.cancel()
                try:
                    await asyncio.wait_for(task_info.task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                return False
            except asyncio.CancelledError:
                logger.info(f"[SHUTDOWN] Critical task {name} cancelled")
                return True
        else:
            logger.debug(f"[SHUTDOWN] Cancelling task: {name}")
            task_info.task.cancel()
            try:
                await asyncio.wait_for(task_info.task, timeout=timeout)
                return True
            except (asyncio.CancelledError, asyncio.TimeoutError):
                return False
    
    async def _cancel_all_registered_tasks(self, timeout: float = 10.0) -> int:
        async with self._task_registry_lock:
            if not self._task_registry:
                return 0
            
            sorted_tasks = sorted(
                self._task_registry.values(),
                key=lambda t: (t.critical, t.priority.value),
                reverse=True
            )
            
            cancelled_count = 0
            logger.info(f"[SHUTDOWN] Cancelling {len(sorted_tasks)} registered tasks...")
            
            for task_info in sorted_tasks:
                if not task_info.is_done():
                    per_task_timeout = min(timeout / len(sorted_tasks), task_info.cancel_timeout)
                    success = await self._cancel_task_with_shield(task_info, per_task_timeout)
                    if success:
                        cancelled_count += 1
                        task_info.status = TaskStatus.COMPLETED
                    else:
                        task_info.status = TaskStatus.CANCELLED
            
            return cancelled_count
    
    def _auto_detect_webhook_url(self) -> Optional[str]:
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
                position_count = 0
                try:
                    session = self.db_manager.get_session()
                    result = session.execute(text('SELECT 1'))
                    result.fetchone()
                    
                    try:
                        count_result = session.execute(text("SELECT COUNT(*) FROM positions WHERE status = 'open'"))
                        position_count = count_result.scalar() or 0
                    except Exception:
                        position_count = 0
                    
                    session.close()
                    db_status = 'connected'
                except Exception as e:
                    db_status = f'error: {str(e)[:50]}'
                    logger.error(f"Database health check failed: {e}")
                
                memory_status = self.config.check_memory_status()
                
                cache_stats = {}
                chart_stats = {}
                if self.config_valid and self.telegram_bot:
                    try:
                        cache_stats = self.telegram_bot.get_cache_stats()
                    except Exception:
                        cache_stats = {'error': 'unavailable'}
                
                if self.config_valid and self.chart_generator:
                    try:
                        chart_stats = self.chart_generator.get_stats()
                    except Exception:
                        chart_stats = {'error': 'unavailable'}
                
                mode = 'full' if self.config_valid else 'limited'
                
                is_degraded = self.config.should_degrade_gracefully()
                if is_degraded:
                    mode = 'degraded'
                
                overall_status = 'healthy' if self.config_valid and self.running and not is_degraded else 'degraded' if is_degraded else 'limited' if not self.config_valid else 'stopped'
                
                task_registry_info = self.get_registered_tasks()
                
                health_status = {
                    'status': overall_status,
                    'mode': mode,
                    'config_valid': self.config_valid,
                    'missing_config': missing_config,
                    'market_data': market_status,
                    'telegram_bot': 'running' if self.config_valid and self.telegram_bot and self.telegram_bot.app else 'not_initialized',
                    'scheduler': 'running' if self.config_valid and self.task_scheduler and self.task_scheduler.running else 'not_initialized',
                    'database': db_status,
                    'open_positions': position_count,
                    'webhook_mode': self.config.TELEGRAM_WEBHOOK_MODE if self.config_valid else False,
                    'memory': memory_status,
                    'cache_stats': cache_stats,
                    'chart_stats': chart_stats,
                    'registered_tasks': len(task_registry_info),
                    'task_details': task_registry_info,
                    'shutdown_in_progress': self._shutdown_in_progress,
                    'free_tier_mode': self.config.FREE_TIER_MODE,
                    'message': 'Bot running in degraded mode - memory critical' if is_degraded else 'Bot running in limited mode - set missing environment variables to enable full functionality' if not self.config_valid else 'Bot running normally'
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
            await self.register_task(
                name="market_data_websocket",
                task=market_task,
                priority=TaskPriority.CRITICAL,
                critical=True,
                cancel_timeout=10.0
            )
            
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
            await self.register_task(
                name="position_tracker",
                task=position_task,
                priority=TaskPriority.HIGH,
                critical=False,
                cancel_timeout=5.0
            )
            
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
            
            logger.info("Starting background cleanup tasks...")
            await self.telegram_bot.start_background_cleanup_tasks()
            
            logger.info("Starting Telegram bot polling...")
            bot_task = asyncio.create_task(self.telegram_bot.run())
            await self.register_task(
                name="telegram_bot",
                task=bot_task,
                priority=TaskPriority.HIGH,
                critical=False,
                cancel_timeout=8.0
            )
            
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
            logger.info(f"Registered tasks: {list(self._task_registry.keys())}")
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
        async with self._shutdown_lock:
            if self._shutdown_in_progress:
                self._shutdown_count += 1
                logger.warning(f"[SHUTDOWN] Shutdown already in progress (signal count: {self._shutdown_count})")
                if self._shutdown_count >= 3:
                    logger.error("[SHUTDOWN] Received 3+ shutdown signals, forcing immediate exit")
                    sys.exit(1)
                return
            
            self._shutdown_in_progress = True
        
        if not self.running and not self.health_server:
            logger.debug("[SHUTDOWN] Bot not running and no health server, skipping shutdown")
            self._shutdown_in_progress = False
            return
        
        logger.info("=" * 60)
        logger.info("[SHUTDOWN] GRACEFUL SHUTDOWN INITIATED")
        logger.info("=" * 60)
        
        self.running = False
        loop = asyncio.get_running_loop()
        shutdown_start_time = loop.time()
        
        def log_progress(phase: str, status: str = "started"):
            elapsed = loop.time() - shutdown_start_time
            logger.info(f"[SHUTDOWN] [{elapsed:.1f}s] Phase: {phase} - {status}")
        
        try:
            log_progress("MarketData", "saving candles and disconnecting")
            if self.market_data:
                try:
                    await asyncio.wait_for(
                        self.market_data.save_candles_to_db(self.db_manager),
                        timeout=self.SHUTDOWN_PHASE_TIMEOUT
                    )
                    log_progress("MarketData", "candles saved")
                except asyncio.TimeoutError:
                    logger.warning("[SHUTDOWN] Market data save timed out")
                except Exception as e:
                    logger.error(f"[SHUTDOWN] Error saving candles: {e}")
                
                try:
                    self.market_data.disconnect()
                    log_progress("MarketData", "disconnected ✓")
                except Exception as e:
                    logger.error(f"[SHUTDOWN] Error disconnecting market data: {e}")
            
            log_progress("Telegram", "stopping bot")
            if self.telegram_bot:
                try:
                    await asyncio.wait_for(
                        self.telegram_bot.stop_background_cleanup_tasks(),
                        timeout=5
                    )
                except asyncio.TimeoutError:
                    logger.warning("[SHUTDOWN] Background cleanup tasks shutdown timed out")
                except Exception as e:
                    logger.error(f"[SHUTDOWN] Error stopping background cleanup tasks: {e}")
                
                try:
                    await asyncio.wait_for(
                        self.telegram_bot.stop(),
                        timeout=self.SHUTDOWN_PHASE_TIMEOUT
                    )
                    log_progress("Telegram", "stopped ✓")
                except asyncio.TimeoutError:
                    logger.warning("[SHUTDOWN] Telegram bot shutdown timed out")
                except Exception as e:
                    logger.error(f"[SHUTDOWN] Error stopping Telegram bot: {e}")
            
            log_progress("TaskScheduler", "stopping")
            if self.task_scheduler:
                try:
                    await asyncio.wait_for(
                        self.task_scheduler.stop(),
                        timeout=self.SHUTDOWN_PHASE_TIMEOUT
                    )
                    log_progress("TaskScheduler", "stopped ✓")
                except asyncio.TimeoutError:
                    logger.warning("[SHUTDOWN] Task scheduler shutdown timed out")
                except Exception as e:
                    logger.error(f"[SHUTDOWN] Error stopping task scheduler: {e}")
            
            log_progress("PositionTracker", "stopping")
            if self.position_tracker:
                try:
                    self.position_tracker.stop_monitoring()
                    log_progress("PositionTracker", "stopped ✓")
                except Exception as e:
                    logger.error(f"[SHUTDOWN] Error stopping position tracker: {e}")
            
            log_progress("RegisteredTasks", "cancelling all")
            cancelled = await self._cancel_all_registered_tasks(timeout=10.0)
            log_progress("RegisteredTasks", f"cancelled {cancelled} tasks ✓")
            
            log_progress("ChartGenerator", "shutting down")
            if self.chart_generator:
                try:
                    self.chart_generator.shutdown()
                    log_progress("ChartGenerator", "shutdown ✓")
                except Exception as e:
                    logger.error(f"[SHUTDOWN] Error shutting down chart generator: {e}")
            
            log_progress("HTTPServer", "stopping health server")
            if self.health_server:
                try:
                    await asyncio.wait_for(
                        self.health_server.cleanup(),
                        timeout=5
                    )
                    log_progress("HTTPServer", "stopped ✓")
                except asyncio.TimeoutError:
                    logger.warning("[SHUTDOWN] Health server shutdown timed out")
                except Exception as e:
                    logger.error(f"[SHUTDOWN] Error stopping health server: {e}")
            
            log_progress("Database", "closing connections")
            if self.db_manager:
                try:
                    self.db_manager.close()
                    log_progress("Database", "closed ✓")
                except Exception as e:
                    logger.error(f"[SHUTDOWN] Error closing database: {e}")
            
            shutdown_duration = loop.time() - shutdown_start_time
            
            logger.info("=" * 60)
            logger.info(f"[SHUTDOWN] COMPLETE in {shutdown_duration:.2f}s")
            if shutdown_duration > self.SHUTDOWN_TOTAL_TIMEOUT:
                logger.warning(f"[SHUTDOWN] Exceeded timeout ({shutdown_duration:.2f}s > {self.SHUTDOWN_TOTAL_TIMEOUT}s)")
            logger.info("=" * 60)
            
            import logging
            logging.shutdown()
            
        except Exception as e:
            logger.error(f"[SHUTDOWN] Error during shutdown: {e}")
            import logging
            logging.shutdown()
            raise
        finally:
            self._shutdown_in_progress = False


async def main():
    orchestrator = TradingBotOrchestrator()
    loop = asyncio.get_running_loop()
    
    shutdown_requested = False
    
    def signal_handler(sig, frame):
        nonlocal shutdown_requested
        signame = signal.Signals(sig).name
        
        if shutdown_requested:
            orchestrator._shutdown_count += 1
            logger.warning(f"[SIGNAL] Received {signame} again (count: {orchestrator._shutdown_count})")
            if orchestrator._shutdown_count >= 3:
                logger.error("[SIGNAL] Forced exit after 3 signals")
                sys.exit(1)
            return
        
        shutdown_requested = True
        logger.info(f"[SIGNAL] Received {signame} ({sig}), initiating graceful shutdown...")
        
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
        logger.info("[SIGNAL] KeyboardInterrupt received")
        return 0
    except Exception as e:
        logger.error(f"[MAIN] Unhandled exception: {e}")
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
