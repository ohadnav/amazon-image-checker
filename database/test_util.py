import logging
from unittest import TestCase


def log_everything_to_stdout():
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s [%(levelname)8.8s] %(message)s", handlers=[logging.StreamHandler()])


class BaseTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        log_everything_to_stdout()
        super(BaseTestCase, cls).setUpClass()
