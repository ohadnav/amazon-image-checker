from __future__ import annotations

import copy
import os
from datetime import datetime, timedelta

import airtable.config
from airtable.reader import ABTestRecord
from amazon_sp_api.amazon_api import ImageVariation
from common.test_util import BaseTestCase, TEST_FLATFILE_A, TEST_FLATFILE_B
from database import config
from database.base_connector_test_case import BaseConnectorTestCase
from database.database_api import DatabaseApi, ProductRead, ProductReadDiff, ABTestRun
from database.mysql_connector import MySQLConnector


class TestDatabaseApi(BaseConnectorTestCase):
    def test_get_last_product_read_empty_table(self):
        last_product_read = self.database_api.get_last_product_read(self.asin_active)
        self.assertEqual(last_product_read, None)

    def test_get_last_product_read(self):
        self.database_api.insert_product_read(self.product_read_yesterday)
        self.assertEqual(self.database_api.get_last_product_read(self.asin_active), self.product_read_yesterday)
        self.database_api.insert_product_read(self.product_read_today)
        self.assertEqual(self.database_api.get_last_product_read(self.asin_active), self.product_read_today)

    def test_insert_product_read_changes(self):
        self.database_api.insert_product_read_changes(self.product_read_diff)
        changes_from_db = self.database_api.get_last_product_read_changes(self.asin_active)
        self.assertEqual(self.product_read_diff, changes_from_db)

    def test_update_feed_id(self):
        self.database_api.insert_ab_test_run(self.ab_test_run1a)
        self.assertIsNone(self.database_api.get_last_run_for_ab_test(self.ab_test_record1).feed_id)
        self.database_api.update_feed_id(self.ab_test_run1a)
        self.assertEqual(self.get_feed_id_by_run_id(self.ab_test_run1a.run_id), self.ab_test_run1a.feed_id)
        self.database_api.insert_ab_test_run(self.ab_test_run1b)
        self.database_api.insert_ab_test_run(self.ab_test_run2a)
        self.assertIsNone(self.database_api.get_last_run_for_ab_test(self.ab_test_record1).feed_id)
        self.database_api.update_feed_id(self.ab_test_run1b)
        self.assertEqual(self.get_feed_id_by_run_id(self.ab_test_run1b.run_id), self.ab_test_run1b.feed_id)
        self.assertEqual(self.get_feed_id_by_run_id(self.ab_test_run1a.run_id), self.ab_test_run1a.feed_id)
        self.assertIsNone(self.database_api.get_last_run_for_ab_test(self.ab_test_record2).feed_id)

    def test_get_last_run_for_ab_test(self):
        inserted_ab_test_run = self.database_api.insert_ab_test_run(self.ab_test_run1a)
        self.assertEqual(self.ab_test_run1a.run_id, inserted_ab_test_run.run_id)
        self.ab_test_run1a.feed_id = None
        self.assertEqual(self.database_api.get_last_run_for_ab_test(self.ab_test_record1), self.ab_test_run1a)
        self.database_api.insert_ab_test_run(self.ab_test_run1b)
        self.ab_test_run1b.feed_id = None
        self.assertEqual(self.database_api.get_last_run_for_ab_test(self.ab_test_record1), self.ab_test_run1b)

    def get_last_run_for_ab_test_no_runs(self):
        self.database_api.insert_ab_test_run(self.ab_test_run1a)
        self.assertEqual(self.database_api.get_last_run_for_ab_test(self.ab_test_record2), None)
