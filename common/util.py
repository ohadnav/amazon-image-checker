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


def initialize_debug_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    set_logging_stream_to_stdout(logger)


def set_logging_stream_to_stdout(logger: logging.Logger):
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(LOGGING_FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
