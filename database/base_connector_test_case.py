from __future__ import annotations

import copy
import os
from datetime import datetime, timedelta

import airtable.config
from airtable.reader import ABTestRecord
from amazon_sp_api.amazon_api import ImageVariation
from common.test_util import BaseTestCase, TEST_FLATFILE_A, TEST_FLATFILE_B
from database import config
from database.database_api import DatabaseApi, ProductRead, ProductReadDiff, ABTestRun
from database.local_mysql_connector import LocalMySQLConnector
from database.mysql_connector import MySQLConnector


class BaseConnectorTestCase(BaseTestCase):
    def setUp(self) -> None:
        super(BaseConnectorTestCase, self).setUp()
        self.local_connector = LocalMySQLConnector()
        self.database_api = DatabaseApi()
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
        self.listing_price1 = 1.0
        self.listing_price2 = 2.0
        self.image_variations_of_active_asin = [
            ImageVariation('MAIN', 'https://m.media-amazon.com/images/I/51Zy9Z9Z1a.jpg', 2500, 2500),
            ImageVariation('PT01', 'https://m.media-amazon.com/images/I/51Zy9Z9Z1b.jpg', 500, 500),
            ImageVariation('PT02', 'https://m.media-amazon.com/images/I/51Zy9Z9Z1c.jpg', 500, 500)]
        self.image_variations2 = [
            ImageVariation('MAIN', 'https://m.media-amazon.com/images/I/51Zy9Z9Z1a.jpg', 2500, 2500),
            ImageVariation('PT01', 'https://m.media-amazon.com/images/I/51Zy9Z9Z1B.jpg', 500, 500),
            ImageVariation('PT02', 'https://m.media-amazon.com/images/I/51Zy9Z9Z1C.jpg', 500, 500)]
        self.product_read_today = ProductRead(
            self.asin_active, self.today, self.image_variations_of_active_asin, self.listing_price1)
        self.product_read_yesterday = ProductRead(
            self.asin_active, self.two_days_ago_date, self.image_variations2, self.listing_price2)
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

    def get_feed_id_by_run_id(self, run_id: int) -> int | None:
        query = f'SELECT {config.FEED_ID_FIELD} FROM {config.AB_TEST_RUNS_TABLE} ' \
                f'WHERE {config.RUN_ID_FIELD} = "{run_id}"'
        result = self.local_connector.run_query(query)
        if result:
            return result[0][config.FEED_ID_FIELD]
        return None