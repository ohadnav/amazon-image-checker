import os
from dataclasses import replace
from datetime import timedelta
from unittest.mock import MagicMock

from database.test_database_api import BaseConnectorTestCase
from scheduler.product_read_changes_task import ProductReadChangesTask


class TestProductReadChangesTask(BaseConnectorTestCase):
    def setUp(self) -> None:
        super(TestProductReadChangesTask, self).setUp()
        self.product_read_changes_task = ProductReadChangesTask()
        self.product_read_changes_task.database_api = self.local_connector
        self.product_read_changes_task.amazon_api = MagicMock()

    def test_is_valid_change(self):
        max_hours = int(os.environ['MAX_TIME_DIFFERENCE_IN_HOURS'])
        self.product_read_yesterday.read_time = self.product_read_today.read_time - timedelta(hours=2 * max_hours)
        self.assertIsNone(
            self.product_read_changes_task.is_valid_change(self.product_read_today, self.product_read_yesterday))
        half_max_hours = self.product_read_today.read_time - timedelta(hours=0.5 * max_hours)
        product_read_no_diff_last_hour = replace(self.product_read_today, read_time=half_max_hours)
        self.assertIsNone(
            self.product_read_changes_task.is_valid_change(self.product_read_today, product_read_no_diff_last_hour))
        self.product_read_yesterday.read_time = half_max_hours
        self.assertDeepAlmostEqual(
            self.product_read_diff,
            self.product_read_changes_task.is_valid_change(self.product_read_today, self.product_read_yesterday))

    def test_process_asin(self):
        self.product_read_changes_task.database_api.get_last_product_read = MagicMock(return_value=None)

        self.product_read_changes_task.insert_new_product_read = MagicMock(return_value=self.product_read_today)
        self.product_read_changes_task.process_product_read_changed = MagicMock()
        self.product_read_changes_task.is_valid_change = MagicMock()
        self.product_read_changes_task.process_asin(self.asin_active)
        self.product_read_changes_task.is_valid_change.assert_not_called()
        self.product_read_changes_task.process_product_read_changed.assert_not_called()

        self.product_read_changes_task.database_api.get_last_product_read = MagicMock(
            return_value=self.product_read_yesterday)
        self.product_read_changes_task.is_valid_change = MagicMock(return_value=None)
        self.product_read_changes_task.process_asin(self.asin_active)
        self.product_read_changes_task.process_product_read_changed.assert_not_called()

        self.product_read_changes_task.is_valid_change = MagicMock(return_value=self.product_read_diff)
        self.product_read_changes_task.process_asin(self.asin_active)
        self.product_read_changes_task.process_product_read_changed.assert_called()
