import os
from datetime import datetime, timedelta

from amazon_sp_api.images_api import ImageVariation
from common.test_util import BaseTestCase
from database import config
from database.connector import MySQLConnector, ProductRead, ProductReadDiff


class MockMySQLConnector(MySQLConnector):
    def __init__(self):
        self.set_test_environment_variables()
        super().__init__()
        self.drop_test_database()
        self.create_test_database_if_not_exists()
        self.create_test_tables()

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        self.drop_test_tables()
        self.drop_test_database()

    def set_test_environment_variables(self):
        os.environ['DB_HOST'] =  'localhost'
        os.environ['DB_DATABASE'] =  'test_database'
        os.environ['USER_ID'] =  '1'

    def create_images_table_query(self, table_name: str) -> str:
        return f'CREATE TABLE IF NOT EXISTS {table_name} ' \
               f'(`{config.ASIN_FIELD}` VARCHAR(10) NOT NULL, ' \
               f'`{config.READ_TIME_FIELD}` DATETIME NOT NULL, ' \
               f'`{config.IMAGE_VARIATIONS_FIELD}` JSON, ' \
               f'`{config.USER_ID_FIELD}` INT NOT NULL, ' \
               f'PRIMARY KEY (`{config.ASIN_FIELD}`, `{config.READ_TIME_FIELD}`))'

    def create_schedule_table_query(self) -> str:
        return f'CREATE TABLE IF NOT EXISTS {config.SCHEDULE_TABLE} ' \
               f'(`temp_id` INT AUTO_INCREMENT PRIMARY KEY, ' \
               f'`{config.ASIN_FIELD}` VARCHAR(100) NOT NULL, ' \
               f'`{config.START_TIME_FIELD}` VARCHAR(100) NOT NULL, ' \
               f'`{config.START_DATE_FIELD}` VARCHAR(100) NOT NULL, ' \
               f'`{config.END_TIME_FIELD}` VARCHAR(100) NOT NULL, ' \
               f'`{config.END_DATE_FIELD}` VARCHAR(100) NOT NULL, ' \
               f'`{config.USER_ID_FIELD}` INT NOT NULL)'

    def drop_test_tables(self):
        self.run_query(f'DROP TABLE IF EXISTS {config.SCHEDULE_TABLE}')
        self.run_query(f'DROP TABLE IF EXISTS {config.IMAGES_HISTORY_TABLE}')
        self.run_query(f'DROP TABLE IF EXISTS {config.IMAGES_CHANGES_TABLE}')

    def drop_test_database(self):
        drop_db_query = 'DROP DATABASE IF EXISTS test_database;'
        self.run_query(drop_db_query, with_db=False)

    def create_test_database_if_not_exists(self):
        create_db_query = 'CREATE DATABASE IF NOT EXISTS test_database;'
        self.run_query(create_db_query, with_db=False)

    def create_test_tables(self):
        self.run_query(self.create_images_table_query(config.IMAGES_HISTORY_TABLE))
        self.run_query(self.create_images_table_query(config.IMAGES_CHANGES_TABLE))
        self.run_query(self.create_schedule_table_query())

    def kill_all(self):
        self.drop_test_tables()
        self.drop_test_database()


class BaseConnectorTestCase(BaseTestCase):
    def setUp(self) -> None:
        self.mock_connector = MockMySQLConnector()
        self.define_const()

    def tearDown(self) -> None:
        self.mock_connector.kill_all()

    def define_const(self):
        self.today = datetime.today().replace(microsecond=0)
        self.yesterday = self.today - timedelta(days=1)
        self.two_days_ago_date = self.today - timedelta(days=2)
        self.two_days_ago_str = self.two_days_ago_date.strftime('%Y-%m-%d')
        self.two_days_from_now_str = self.today.replace(day=self.today.day + 2).strftime('%Y-%m-%d')
        self.start_time = '00:00:00'
        self.end_time = '23:59:59'
        self.asin_active = 'B01N4J6L3I'
        self.asin_inactive = 'B07JQZQZ4Z'
        self.image_variations_of_active_asin = [
            ImageVariation('MAIN', 'https://m.media-amazon.com/images/I/51Zy9Z9Z1a.jpg', 2500, 2500),
            ImageVariation('PT01', 'https://m.media-amazon.com/images/I/51Zy9Z9Z1b.jpg', 500, 500),
            ImageVariation('PT02', 'https://m.media-amazon.com/images/I/51Zy9Z9Z1c.jpg', 500, 500)]
        self.image_variations2 = [
            ImageVariation('MAIN', 'https://m.media-amazon.com/images/I/51Zy9Z9Z1a.jpg', 2500, 2500),
            ImageVariation('PT01', 'https://m.media-amazon.com/images/I/51Zy9Z9Z1B.jpg', 500, 500),
            ImageVariation('PT02', 'https://m.media-amazon.com/images/I/51Zy9Z9Z1C.jpg', 500, 500)]
        self.product_read_today = ProductRead(self.asin_active, self.today, self.image_variations_of_active_asin)
        self.product_read_yesterday = ProductRead(self.asin_active, self.two_days_ago_date, self.image_variations2)
        self.product_read_diff = ProductReadDiff(self.product_read_today, self.product_read_yesterday)

    def insert_ab_test(self, is_active=True):
        query = f'INSERT INTO {config.SCHEDULE_TABLE} ' \
                f'(`{config.ASIN_FIELD}`, `{config.START_TIME_FIELD}`, `{config.START_DATE_FIELD}`, ' \
                f'`{config.END_TIME_FIELD}`, `{config.END_DATE_FIELD}`, `{config.USER_ID_FIELD}`) ' \
                f'VALUES ("{self.asin_active}", "{self.start_time}", "{self.two_days_ago_str}", ' \
                f'"{self.end_time}", "{self.two_days_from_now_str if is_active else self.two_days_ago_str}", ' \
                f'{os.environ["USER_ID"]});'
        self.mock_connector.run_query(query)


class TestMySQLConnector(BaseConnectorTestCase):
    def test_get_asins_with_active_ab_test(self):
        self.insert_ab_test()
        self.insert_ab_test(is_active=False)
        asins_with_active_ab_test = self.mock_connector.get_asins_with_active_ab_test()
        self.assertEqual(asins_with_active_ab_test, [self.asin_active])

    def test_get_last_product_read_empty_table(self):
        last_product_read = self.mock_connector.get_last_product_read(self.asin_active)
        self.assertEqual(last_product_read, None)

    def test_get_last_product_read(self):
        self.mock_connector.insert_product_read(self.product_read_yesterday)
        self.assertEqual(self.mock_connector.get_last_product_read(self.asin_active), self.product_read_yesterday)
        self.mock_connector.insert_product_read(self.product_read_today)
        self.assertEqual(self.mock_connector.get_last_product_read(self.asin_active), self.product_read_today)

    def test_insert_images_changes(self):
        self.mock_connector.insert_images_changes(self.product_read_diff)
        self.assertEqual(self.product_read_diff, self.mock_connector.get_last_images_changes(self.asin_active))
