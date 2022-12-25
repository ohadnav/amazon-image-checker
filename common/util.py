import logging
import sys

LOGGING_FORMAT = ('%(filename)s: '
                  '%(levelname)s: '
                  '%(funcName)s(): '
                  '%(lineno)d:\t'
                  '%(message)s')


def set_default_logging_format():
    logging.basicConfig(
        format=LOGGING_FORMAT, level=logging.INFO if sys.gettrace() else logging.WARNING, stream=sys.stderr)


def set_default_logging_level_to_debug():
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.DEBUG, stream=sys.stdout)
