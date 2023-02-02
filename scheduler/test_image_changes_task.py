import os
from dataclasses import replace
from datetime import timedelta
from unittest.mock import MagicMock

from database.test_connector import BaseConnectorTestCase
from scheduler.image_changes_task import ImageChangesTask


class TestImageChangesTask(BaseConnectorTestCase):
    def setUp(self) -> None:
        super(TestImageChangesTask, self).setUp()
        self.image_changes_task = ImageChangesTask()
        self.image_changes_task.connector = self.local_connector
        self.image_changes_task.amazon_api = MagicMock()

    def test_is_valid_image_change(self):
        max_hours = int(os.environ['MAX_TIME_DIFFERENCE_IN_HOURS'])
        self.product_read_yesterday.read_time = self.product_read_today.read_time - timedelta(hours=2 * max_hours)
        self.assertIsNone(
            self.image_changes_task.is_valid_image_change(self.product_read_today, self.product_read_yesterday))
        half_max_hours = self.product_read_today.read_time - timedelta(hours=0.5 * max_hours)
        product_read_no_diff_last_hour = replace(self.product_read_today, read_time=half_max_hours)
        self.assertIsNone(
            self.image_changes_task.is_valid_image_change(self.product_read_today, product_read_no_diff_last_hour))
        self.product_read_yesterday.read_time = half_max_hours
        self.assertDeepAlmostEqual(
            self.product_read_diff,
            self.image_changes_task.is_valid_image_change(self.product_read_today, self.product_read_yesterday))

    def test_process_asin(self):
        self.image_changes_task.connector.get_last_product_read = MagicMock(return_value=None)

        self.image_changes_task.insert_new_product_read = MagicMock(return_value=self.product_read_today)
        self.image_changes_task.apply_product_images_changed = MagicMock()
        self.image_changes_task.is_valid_image_change = MagicMock()
        self.image_changes_task.process_asin(self.asin_active)
        self.image_changes_task.is_valid_image_change.assert_not_called()
        self.image_changes_task.apply_product_images_changed.assert_not_called()

        self.image_changes_task.connector.get_last_product_read = MagicMock(return_value=self.product_read_yesterday)
        self.image_changes_task.is_valid_image_change = MagicMock(return_value=None)
        self.image_changes_task.process_asin(self.asin_active)
        self.image_changes_task.apply_product_images_changed.assert_not_called()

        self.image_changes_task.is_valid_image_change = MagicMock(return_value=self.product_read_diff)
        self.image_changes_task.process_asin(self.asin_active)
        self.image_changes_task.apply_product_images_changed.assert_called()
