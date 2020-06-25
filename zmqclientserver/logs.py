import logging
import logging.handlers

BASE_NAME = 'zmq'


# Define main logger from where all other logs will depend
log = logging.getLogger(BASE_NAME)
log.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

nh = logging.NullHandler()
log.addHandler(nh)


def log_add_stream_handler(level=logging.DEBUG):
    global log
    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(formatter)
    log.addHandler(sh)


def log_add_file_handler(file_path, level=logging.DEBUG):
    global log
    global formatter
    fh = logging.handlers.RotatingFileHandler(filename=file_path, maxBytes=5 * 1024 * 1024, backupCount=5)
    #    fh = logging.FileHandler(filename=file_path)
    fh.setLevel(level=level)
    # create formatter and add it to the handlers
    fh.setFormatter(formatter)
    log.addHandler(fh)


def get_logger(logger_name):
    return log.getChild(logger_name)
