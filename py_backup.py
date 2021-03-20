import os
import sys
from datetime import datetime

from base_status import BaseStatus
from utils import log, PATH_CURRENT_DATABASES, PATH_BACKUP_DATABASES, sql, mysqldump, compress


def backup_db():
    log.info('*** Запуск бакапа баз данных ***')
    # сканируем каталоги БД и заполняем текущее состояние в объекте BaseStatus
    status_from_files = scan_files_db()
    # сканируем конфигурационный файл и заполняем прошлое состояние в объекте BaseStatus
    status_from_conf = scan_config_db()
    # для каждой существующей базы и таблицы в ней принимаем решение о бакапе
    # и за одно вносим правким в status_from_files, который потом запишем в новый CONFIG_FILE
    for base in status_from_files.bases:
        if base in status_from_conf.exclude_bases:
            status_from_files.exclude_bases.append(base)
            status_from_files.exclude_tables.update({base: ''})
            log.info(f'база {base} полностью исключена из бакапов')
            continue
        # если количество бакапов у базы больше одного, то сверяем даты изменений у каждого файла
        if len(status_from_files.backups.get(base)) > 1:
            were_there_any_changes(status_from_files, status_from_conf, base)
        # иначе бакапим потаблично
        else:
            backup_by_tables(status_from_files, status_from_conf, base)
    # генерим новый конфиг
    status_from_files.save_config()
    log.info('*** Закончили бакап баз данных ***')


def were_there_any_changes(status_from_files: BaseStatus, status_from_conf: BaseStatus, base: str):
    """
    Ищем изменения с последнего бакапа
    """
    for file in status_from_files.data.get(base):
        # если такой базы в конфиге вообще нет
        if not status_from_conf.data.get(base):
            backup_by_tables(status_from_files, status_from_conf, base)
            return
        # если файла в конфиге нет
        if not status_from_conf.data.get(base).get(file):
            backup_by_tables(status_from_files, status_from_conf, base)
            return
        # если дата в конфиге равна дате файла
        if status_from_conf.data.get(base).get(file) == status_from_files.data.get(base).get(file):
            continue
        else:
            backup_by_tables(status_from_files, status_from_conf, base)
            return
    status_from_files.exclude_tables.update({base: status_from_conf.exclude_tables.get(base)})


def backup_by_tables(status_from_files: BaseStatus, status_from_conf: BaseStatus, base: str):
    """
    Бакапим БД, за исключением BaseStatus.exclude_tables
    """
    # создаём каталог для бакапа
    path_backup_db = f'{PATH_BACKUP_DATABASES}/{base}'
    path_backup_db_data = f'{path_backup_db}/{datetime.now().strftime("%Y-%m-%d")}'
    if not os.path.isdir(path_backup_db):
        os.mkdir(path_backup_db)
    if not os.path.isdir(path_backup_db_data):
        os.mkdir(path_backup_db_data)
    # ставляем список таблиц
    tables = sql(f'show tables from {base}', base).split('\n')[:-1]
    #print(f'tables - {tables}')
    list_exclude_tables_for_save = []
    # исключаем те, что в status_from_conf.exclude_tables и сохраняем этот список исключений в status_from_files
    for table in tables:
        # если таблица в списке исключений
        if status_from_conf.exclude_tables.get(base) and table in status_from_conf.exclude_tables.get(base):
            list_exclude_tables_for_save.append(table)
            log.info(f'исключили {base}.{table}')
            continue
        backup_table(base, table, path_backup_db_data)
    status_from_files.exclude_tables.update({base: list_exclude_tables_for_save})


def backup_table(base: str, table: str, path: str):
    log.info(f'дампим {base}.{table}...')
    mysqldump(base, table, path)
    log.info(f'архивируем {base}.{table}...')
    compress(table, path)


def scan_files_db() -> BaseStatus:
    list_current_db = [(i, j, y) for (i, j, y) in os.walk(PATH_CURRENT_DATABASES)][0][1]
    status_from_db = BaseStatus()
    status_from_db.create_from_db(list_current_db)
    return status_from_db


def scan_config_db() -> BaseStatus:
    status_from_conf = BaseStatus()
    status_from_conf.create_from_config()
    return status_from_conf


if __name__ == '__main__':
    if len(sys.argv) != 2:
        log.info('Параметры запуска python3 py_backup.py < db | system >')
        exit(1)
    if sys.argv[1] == 'db':
        backup_db()
        exit(0)
    if sys.argv[1] == 'system':
        pass
        exit(0)
    log.info('Неверные параметры запуска. Используйте python3 py_backup.py < db | system >')
    exit(1)
