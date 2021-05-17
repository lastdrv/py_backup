import os
from datetime import datetime

from base_status import BaseStatus
from utils import logger_app
from utils.utils_func import PATH_CURRENT_DATABASES, mysqldump, compress, PATH_BACKUP_DATABASES, sql

log = logger_app.get_logger(__name__)


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
        if not status_from_conf.data.get(base):
            log.info(f'{base} такой базы в конфиге вообще нет')
            backup_by_tables(status_from_files, status_from_conf, base)
            return
        if not status_from_conf.data.get(base).get(file):
            log.info(f'{file} файла в конфиге нет')
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
    # ставляем список таблиц
    tables = sql(f'show tables from {base}', base).split('\n')[:-1]
    list_exclude_tables_for_save = []
    # исключаем те, что в status_from_conf.exclude_tables и сохраняем этот список исключений в status_from_files
    for table in tables:
        # если таблица в списке исключений
        if status_from_conf.exclude_tables.get(base) and table in status_from_conf.exclude_tables.get(base):
            list_exclude_tables_for_save.append(table)
            log.info(f'исключили {base}.{table}')
            continue
        backup_table(base, table)
    status_from_files.exclude_tables.update({base: list_exclude_tables_for_save})


def create_dir_if_need(base: str) -> str:
    path_backup_db = f'{PATH_BACKUP_DATABASES}/{base}'
    path_backup_db_data = f'{path_backup_db}/{datetime.now().strftime("%Y-%m-%d")}'
    if not os.path.isdir(path_backup_db):
        os.mkdir(path_backup_db)
    if not os.path.isdir(path_backup_db_data):
        os.mkdir(path_backup_db_data)
    return path_backup_db_data


def backup_table(base: str, table: str):
    path = create_dir_if_need(base)
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


def backup_system():
    pass


def remove_old():
    """
    Если в каталоге с бакапами конкретной базы больше 3х непустых каталога,
    тогда оставляем последний
        если последний совпадает с первым в этом месяце, то оставляем первый в прошлом и позапрошлом месяце (если они были)
        если послендний не совпадает с первым в этом месяце, то оставляем их оба и первый в прошлом месяце
    остальное удаляем
    """
    bases = [(i, j, y) for (i, j, y) in os.walk(PATH_BACKUP_DATABASES)][0][1]
    for base in bases:
        sub_folders = [(i, j, y) for (i, j, y) in os.walk(f'{PATH_BACKUP_DATABASES}/{base}')][0][1]
        if len(sub_folders) <= 3:
            continue
        # для каждого каталога берём дату создания
        dict_dates = {}
        for data_dir in sub_folders:
            dict_dates.update({f'{PATH_BACKUP_DATABASES}/{base}/{data_dir}': datetime.fromtimestamp(os.path.getmtime(f'{PATH_BACKUP_DATABASES}/{base}/{data_dir}')).strftime('%Y-%m-%d')})
        # удалим из словаря все каталоги которые нам нужны


if __name__ == '__main__':
    backup_db()
    # backup_system()
    # remove_old()
