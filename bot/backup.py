"""Database backup and recovery utilities with support for SQLite and PostgreSQL"""

import logging
import os
import shutil
import subprocess
import gzip
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List
import json

logger = logging.getLogger('DatabaseBackup')


class DatabaseBackupManager:
    """Manages database backups with automatic rotation and recovery"""
    
    def __init__(self, db_path: str = 'data/bot.db', backup_dir: str = 'backups', max_backups: int = 7):
        """Initialize backup manager
        
        Args:
            db_path: Path to database file
            backup_dir: Directory to store backups
            max_backups: Maximum number of backups to retain
        """
        self.db_path = db_path
        self.backup_dir = Path(backup_dir)
        self.max_backups = max_backups
        self.is_postgres = False
        
        self.backup_dir.mkdir(exist_ok=True)
        logger.info(f"Backup manager initialized: {self.backup_dir}")
    
    def configure_postgres(self, database_url: str):
        """Configure PostgreSQL backup parameters
        
        Args:
            database_url: PostgreSQL connection string
        """
        if database_url and ('postgresql://' in database_url or 'postgres://' in database_url):
            self.is_postgres = True
            self.database_url = database_url
            logger.info("PostgreSQL backup mode enabled")
    
    def backup_sqlite(self) -> str:
        """Backup SQLite database with compression
        
        Returns:
            Path to backup file
        """
        if not os.path.exists(self.db_path):
            logger.warning(f"SQLite database not found: {self.db_path}")
            return ""
        
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_file = self.backup_dir / f"bot_sqlite_{timestamp}.db.gz"
        
        try:
            with open(self.db_path, 'rb') as f_in:
                with gzip.open(backup_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            size_mb = backup_file.stat().st_size / (1024 * 1024)
            logger.info(f"✅ SQLite backup created: {backup_file.name} ({size_mb:.2f}MB)")
            
            self._rotate_backups()
            return str(backup_file)
            
        except Exception as e:
            logger.error(f"SQLite backup failed: {e}")
            return ""
    
    def backup_postgres(self) -> str:
        """Backup PostgreSQL database using pg_dump
        
        Returns:
            Path to backup file
        """
        if not self.is_postgres:
            logger.warning("PostgreSQL not configured")
            return ""
        
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_file = self.backup_dir / f"bot_postgres_{timestamp}.sql.gz"
        
        try:
            from urllib.parse import urlparse
            parsed = urlparse(self.database_url)
            
            env = os.environ.copy()
            if parsed.password:
                env['PGPASSWORD'] = parsed.password
            
            cmd = [
                'pg_dump',
                '-h', parsed.hostname or 'localhost',
                '-U', parsed.username or 'postgres',
                '-d', parsed.path.lstrip('/'),
                '-F', 'c',
            ]
            
            with open(backup_file, 'wb') as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, env=env, check=False, timeout=300)
                if result.returncode != 0:
                    raise RuntimeError(f"pg_dump failed: {result.stderr.decode()}")
            
            size_mb = backup_file.stat().st_size / (1024 * 1024)
            logger.info(f"✅ PostgreSQL backup created: {backup_file.name} ({size_mb:.2f}MB)")
            
            self._rotate_backups()
            return str(backup_file)
            
        except subprocess.TimeoutExpired:
            logger.error("PostgreSQL backup timeout (300s)")
            return ""
        except Exception as e:
            logger.error(f"PostgreSQL backup failed: {e}")
            return ""
    
    def backup(self) -> str:
        """Create automatic backup based on database type
        
        Returns:
            Path to backup file
        """
        if self.is_postgres:
            return self.backup_postgres()
        else:
            return self.backup_sqlite()
    
    def restore_sqlite(self, backup_file: str) -> bool:
        """Restore SQLite database from compressed backup
        
        Args:
            backup_file: Path to backup file
            
        Returns:
            True if successful
        """
        if not os.path.exists(backup_file):
            logger.error(f"Backup file not found: {backup_file}")
            return False
        
        try:
            # Create backup of current database
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            current_backup = Path(self.db_path).with_name(f"{Path(self.db_path).stem}_before_restore_{timestamp}{Path(self.db_path).suffix}")
            
            if os.path.exists(self.db_path):
                shutil.copy2(self.db_path, current_backup)
                logger.info(f"Current database backed up to: {current_backup}")
            
            # Restore from backup
            with gzip.open(backup_file, 'rb') as f_in:
                with open(self.db_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            logger.info(f"✅ SQLite database restored from: {backup_file}")
            return True
            
        except Exception as e:
            logger.error(f"SQLite restore failed: {e}")
            return False
    
    def _rotate_backups(self):
        """Keep only the most recent max_backups backups"""
        try:
            backups = sorted(self.backup_dir.glob("bot_*.gz"), key=lambda p: p.stat().st_mtime, reverse=True)
            
            for old_backup in backups[self.max_backups:]:
                old_backup.unlink()
                logger.info(f"Deleted old backup: {old_backup.name}")
                
        except Exception as e:
            logger.warning(f"Backup rotation failed: {e}")
    
    def list_backups(self) -> List[Dict]:
        """List all available backups
        
        Returns:
            List of backup info dictionaries
        """
        backups = []
        try:
            for backup in sorted(self.backup_dir.glob("bot_*.gz"), key=lambda p: p.stat().st_mtime, reverse=True):
                size_mb = backup.stat().st_size / (1024 * 1024)
                mtime = datetime.fromtimestamp(backup.stat().st_mtime)
                
                backups.append({
                    'file': backup.name,
                    'path': str(backup),
                    'size_mb': round(size_mb, 2),
                    'created': mtime.isoformat(),
                    'type': 'postgres' if 'postgres' in backup.name else 'sqlite'
                })
        except Exception as e:
            logger.error(f"Failed to list backups: {e}")
        
        return backups
    
    def get_backup_status(self) -> Dict:
        """Get comprehensive backup status
        
        Returns:
            Status dictionary
        """
        backups = self.list_backups()
        latest_backup = backups[0] if backups else None
        
        status = {
            'total_backups': len(backups),
            'max_backups': self.max_backups,
            'backup_dir': str(self.backup_dir),
            'latest_backup': latest_backup,
            'all_backups': backups,
            'db_type': 'PostgreSQL' if self.is_postgres else 'SQLite'
        }
        
        return status
