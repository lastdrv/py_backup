import configparser
import datetime
import os
from datetime import datetime
from typing import List

from utils import logger_app
from utils.utils_func import PATH_BACKUP_DATABASES, PATH_CURRENT_DATABASES, CONFIG_FILE

log = logger_app.get_logger(__name__)


def get_list_backup_dirs(db: str) -> List:
    """
    Возвращет список каталогов с бакапами для конкретной БД
    """
    if os.path.isdir(f'{PATH_BACKUP_DATABASES}/{db}'):
        return [(i, j, y) for (i, j, y) in os.walk(f'{PATH_BACKUP_DATABASES}/{db}')][0][1]
    return []


class BaseStatus:
    def __init__(self):
        self.bases = []  # список баз
        self.exclude_bases = []  # список исключаемых баз
        self.data = {}  # основной набор данных - файлы с датами изменений
        self.exclude_tables = {}  # словарь исключённых таблиц
        self.backups = {}  # словарь директорий бакапов

    def create_from_config(self):
        log.info('читаем файл конфигурации')
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.bases = config.sections()
        for section in config.sections():
            dict_tables = {}
            for item in config.items(section):
                if item[0] == 'exclude_base' and item[1] == 'on':
                    self.exclude_bases.append(section)
                    continue
                if item[0] == 'exclude_tables' and item[1]:
                    self.exclude_tables.update({section: item[1].split(', ')})
                    continue
                dict_tables.update({item[0]: item[1]})
            self.data.update({section: dict_tables})

    def create_from_db(self, dbs: List):
        """

        :param dbs: список каталогов где предположительно лежат базы
        """
        # выкидываем каталоги в которых нет БД
        self.bases = [db for db in dbs if os.path.isfile(f'{PATH_CURRENT_DATABASES}/{db}/db.opt')]
        log.info(f'список текущих БД {sorted(self.bases)}')
        for db in self.bases:
            dict_files = {}
            for file in os.scandir(f'{PATH_CURRENT_DATABASES}/{db}'):
                if file.name != 'db.opt':
                    dict_files.update(
                        {file.name: datetime.fromtimestamp(file.stat().st_mtime).strftime('%Y-%m-%d %H:%M')})
                else:
                    continue
            self.data.update({db: dict_files})
            self.backups.update({db: get_list_backup_dirs(db)})

    def save_config(self):
        config = configparser.ConfigParser()
        for base in sorted(self.bases):
            config[base] = {}
            if base in self.exclude_bases:
                config[base]['exclude_base'] = 'on'
            else:
                config[base]['exclude_base'] = 'off'
            if self.exclude_tables and self.exclude_tables.get(base):
                config[base]['exclude_tables'] = ', '.join(self.exclude_tables[base])
            else:
                config[base]['exclude_tables'] = ''
            for file in sorted(self.data.get(base).keys()):
                config[base][file] = self.data.get(base).get(file)
        with open(CONFIG_FILE, 'w') as config_file:
            config_file.write(f'# Автогенерация {datetime.now()}\n')
            config.write(config_file)
