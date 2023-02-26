from common.test_util import BaseTestCase
from database import config
from database.local_mysql_connector import LocalMySQLConnector


class TestLocalMySQLConnectorTestCase(BaseTestCase):
    def setUp(self) -> None:
        super(BaseTestCase, self).setUp()
        self.local_connector = LocalMySQLConnector()

    def tearDown(self) -> None:
        self.local_connector.kill_all()

    def test_create_test_tables(self):
        expected_tables = {config.PRODUCT_READ_HISTORY_TABLE, config.PRODUCT_READ_CHANGES_TABLE,
                           config.AB_TEST_RUNS_TABLE}
        actual_tables = self.local_connector.run_query(f'SELECT table_name FROM information_schema.tables '
                                                       f'WHERE table_schema = "{self.local_connector.database}";')
        actual_tables = {table['TABLE_NAME'] for table in actual_tables}
        self.assertSetEqual(expected_tables, actual_tables)
