from __future__ import annotations

import copy
import os
from datetime import datetime, timedelta

import airtable.config
from airtable.reader import ABTestRecord
from amazon_sp_api.amazon_api import ImageVariation
from common.test_util import BaseTestCase, TEST_FLATFILE_A, TEST_FLATFILE_B
from database import config
from database.connector import MySQLConnector, ProductRead, ProductReadDiff, ABTestRun


class LocalMySQLConnector(MySQLConnector):
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
        os.environ['DB_HOST'] = 'localhost'
        os.environ['DB_DATABASE'] = 'test_database'
        os.environ['USER_ID'] = '1'

    def create_images_table_query(self, table_name: str) -> str:
        return f'CREATE TABLE IF NOT EXISTS {table_name} ' \
               f'(`{config.ASIN_FIELD}` VARCHAR(10) NOT NULL, ' \
               f'`{config.READ_TIME_FIELD}` DATETIME NOT NULL, ' \
               f'`{config.IMAGE_VARIATIONS_FIELD}` JSON, ' \
               f'`{config.USER_ID_FIELD}` INT NOT NULL, ' \
               f'PRIMARY KEY (`{config.ASIN_FIELD}`, `{config.READ_TIME_FIELD}`))'

    def create_ab_test_runs_table_query(self) -> str:
        return f'CREATE TABLE IF NOT EXISTS {config.AB_TEST_RUNS_TABLE} ' \
               f'(`{config.RUN_ID_FIELD}` int NOT NULL AUTO_INCREMENT, ' \
               f'`{config.AB_TEST_ID_FIELD}` INT NOT NULL, ' \
               f'`{config.RUN_TIME_FIELD}` DATETIME NOT NULL, ' \
               f'`{config.FEED_ID_FIELD}` INT, ' \
               f'`{config.VARIATION_FIELD}` VARCHAR(10), ' \
               f'`{config.USER_ID_FIELD}` INT NOT NULL, ' \
               f'PRIMARY KEY (run_id))'

    def drop_test_tables(self):
        self.run_query(f'DROP TABLE IF EXISTS {config.IMAGES_HISTORY_TABLE}')
        self.run_query(f'DROP TABLE IF EXISTS {config.IMAGES_CHANGES_TABLE}')
        self.run_query(f'DROP TABLE IF EXISTS {config.AB_TEST_RUNS_TABLE}')

    def drop_test_database(self):
        drop_db_query = 'DROP DATABASE IF EXISTS test_database;'
        self.run_query(drop_db_query, with_db=False)

    def create_test_database_if_not_exists(self):
        create_db_query = 'CREATE DATABASE IF NOT EXISTS test_database;'
        self.run_query(create_db_query, with_db=False)

    def create_test_tables(self):
        self.run_query(self.create_images_table_query(config.IMAGES_HISTORY_TABLE))
        self.run_query(self.create_images_table_query(config.IMAGES_CHANGES_TABLE))
        self.run_query(self.create_ab_test_runs_table_query())

    def kill_all(self):
        self.drop_test_tables()
        self.drop_test_database()


class BaseConnectorTestCase(BaseTestCase):
    def setUp(self) -> None:
        super(BaseConnectorTestCase, self).setUp()
        self.local_connector = LocalMySQLConnector()
        self.define_const()

    def tearDown(self) -> None:
        self.local_connector.kill_all()

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
        self.ab_test_record1 = ABTestRecord(
            {
                airtable.config.TEST_ID_FIELD: 1,
                airtable.config.START_DATE_FIELD: self.two_days_ago_date.strftime(airtable.config.AIRTABLE_TIME_FORMAT),
                airtable.config.END_DATE_FIELD: self.today.strftime(airtable.config.AIRTABLE_TIME_FORMAT),
                airtable.config.ROTATION_FIELD: 24,
                airtable.config.FLATFILE_FIELD['A']: [{
                    'filename': os.path.basename(TEST_FLATFILE_A),
                    'url': f'test.com/{os.path.basename(TEST_FLATFILE_A)}'
                }],
                airtable.config.FLATFILE_FIELD['B']: [{
                    'filename': os.path.basename(TEST_FLATFILE_B),
                    'url': f'test.com/{os.path.basename(TEST_FLATFILE_B)}'
                }]
            })
        self.ab_test_record2 = copy.deepcopy(self.ab_test_record1)
        self.ab_test_record2.fields[airtable.config.TEST_ID_FIELD] = 2
        self.ab_test_run1a = ABTestRun(
            self.ab_test_record1.fields[airtable.config.TEST_ID_FIELD], self.two_days_ago_date, 'A', 1, 1)
        self.ab_test_run1b = ABTestRun(
            self.ab_test_record1.fields[airtable.config.TEST_ID_FIELD], self.yesterday, 'B', 2, 2)
        self.ab_test_run2a = ABTestRun(
            self.ab_test_record2.fields[airtable.config.TEST_ID_FIELD], self.two_days_ago_date, 'A', 3, 3)
        self.ab_test_run2b = ABTestRun(
            self.ab_test_record2.fields[airtable.config.TEST_ID_FIELD], self.yesterday, 'B', 4, 4)


class TestMySQLConnector(BaseConnectorTestCase):
    def test_get_last_product_read_empty_table(self):
        last_product_read = self.local_connector.get_last_product_read(self.asin_active)
        self.assertEqual(last_product_read, None)

    def test_get_last_product_read(self):
        self.local_connector.insert_product_read(self.product_read_yesterday)
        self.assertEqual(self.local_connector.get_last_product_read(self.asin_active), self.product_read_yesterday)
        self.local_connector.insert_product_read(self.product_read_today)
        self.assertEqual(self.local_connector.get_last_product_read(self.asin_active), self.product_read_today)

    def test_insert_images_changes(self):
        self.local_connector.insert_images_changes(self.product_read_diff)
        self.assertEqual(self.product_read_diff, self.local_connector.get_last_images_changes(self.asin_active))

    def get_feed_id_by_run_id(self, run_id: int) -> int | None:
        query = f'SELECT {config.FEED_ID_FIELD} FROM {config.AB_TEST_RUNS_TABLE} ' \
                f'WHERE {config.RUN_ID_FIELD} = "{run_id}"'
        result = self.local_connector.run_query(query)
        if result:
            return result[0][config.FEED_ID_FIELD]
        return None

    def test_update_feed_id(self):
        self.local_connector.insert_ab_test_run(self.ab_test_run1a)
        self.assertIsNone(self.local_connector.get_last_run_for_ab_test(self.ab_test_record1).feed_id)
        self.local_connector.update_feed_id(self.ab_test_record1, self.ab_test_run1a.feed_id)
        self.assertEqual(self.get_feed_id_by_run_id(self.ab_test_run1a.run_id), self.ab_test_run1a.feed_id)
        self.local_connector.insert_ab_test_run(self.ab_test_run1b)
        self.local_connector.insert_ab_test_run(self.ab_test_run2a)
        self.assertIsNone(self.local_connector.get_last_run_for_ab_test(self.ab_test_record1).feed_id)
        self.local_connector.update_feed_id(self.ab_test_record1, self.ab_test_run1b.feed_id)
        self.assertEqual(self.get_feed_id_by_run_id(self.ab_test_run1b.run_id), self.ab_test_run1b.feed_id)
        self.assertEqual(self.get_feed_id_by_run_id(self.ab_test_run1a.run_id), self.ab_test_run1a.feed_id)
        self.assertIsNone(self.local_connector.get_last_run_for_ab_test(self.ab_test_record2).feed_id)

    def test_get_last_run_for_ab_test(self):
        self.local_connector.insert_ab_test_run(self.ab_test_run1a)
        self.ab_test_run1a.feed_id = None
        self.assertEqual(self.local_connector.get_last_run_for_ab_test(self.ab_test_record1), self.ab_test_run1a)
        self.local_connector.insert_ab_test_run(self.ab_test_run1b)
        self.ab_test_run1b.feed_id = None
        self.assertEqual(self.local_connector.get_last_run_for_ab_test(self.ab_test_record1), self.ab_test_run1b)

    def get_last_run_for_ab_test_no_runs(self):
        self.local_connector.insert_ab_test_run(self.ab_test_run1a)
        self.assertEqual(self.local_connector.get_last_run_for_ab_test(self.ab_test_record2), None)
