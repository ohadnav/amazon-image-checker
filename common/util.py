import logging
import sys

LOGGING_FORMAT = ('%(filename)s: '
                  '%(levelname)s: '
                  '%(funcName)s(): '
                  '%(lineno)d:\t'
                  '%(message)s')


def set_default_logging_config():
    logging.basicConfig(
        format=LOGGING_FORMAT, level=logging.INFO if sys.gettrace() else logging.WARNING, stream=sys.stderr)


run_log_init_once = False


def initialize_logging(logging_level=logging.INFO):
    global run_log_init_once
    if not run_log_init_once:
        logger = logging.getLogger()
        logger.setLevel(logging_level)
        set_logging_stream_to_stdout(logger)
    run_log_init_once = True


def set_logging_stream_to_stdout(logger: logging.Logger):
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(LOGGING_FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
