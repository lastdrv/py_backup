import logging


def get_logger(name):
    log_format = f'%(asctime)s - [%(levelname)s] - %(name)s - %(message)s'
    logging.basicConfig(filename='py_backup.log', format=log_format, level=logging.INFO)
    return logging.getLogger(name)
