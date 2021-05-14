import os

from utils import logger_app

PATH_BACKUP_DATABASES = '/mnt/data/backups/databases'
PATH_CURRENT_DATABASES = '/mnt/mysql'
PATH_BACKUP_SYSTEM = '/mnt/data/backups/system'
CONFIG_FILE = 'config.ini'

BACKUP_SQL_HOST = os.getenv('BACKUP_SQL_HOST')
BACKUP_SQL_USER = os.getenv('BACKUP_SQL_USER')
BACKUP_SQL_PASS = os.getenv('BACKUP_SQL_PASS')

log = logger_app.get_logger(__name__)


def sql(request: str, db: str) -> str:
    if not BACKUP_SQL_HOST or not BACKUP_SQL_USER or not BACKUP_SQL_PASS:
        log.info('Не все environments настроены. Выход.')
        exit(1)
    command = f'/usr/bin/mysql -h {BACKUP_SQL_HOST} -D {db} -u {BACKUP_SQL_USER} -p{BACKUP_SQL_PASS} -N -e "{request};" 2> /dev/null'
    return os.popen(command).read()


def mysqldump(db: str, table: str, path: str):
    command = f'/usr/bin/mysqldump {db} --skip-lock-tables --skip-add-locks --add-drop-table --result-file="{path}/{table}.sql" -u{BACKUP_SQL_USER} -h{BACKUP_SQL_HOST} -p{BACKUP_SQL_PASS} {table} 2>/dev/null'
    os.popen(command).read()


def compress(table: str, path: str):
    command = f'/usr/bin/xz -T6 {path}/{table}.sql'
    os.popen(command).read()
