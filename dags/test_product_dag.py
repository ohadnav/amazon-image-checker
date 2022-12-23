from datetime import timedelta
from unittest.mock import MagicMock

from common import env_var
from dags import product_dag
from database.test_connector import BaseConnectorTestCase


class TestProductDag(BaseConnectorTestCase):
    def test_is_valid_image_change(self):
        max_hours = int(env_var.get('MAX_TIME_DIFFERENCE_IN_HOURS'))
        self.product_read_yesterday.read_time = self.product_read_today.read_time - timedelta(hours=2 * max_hours)
        self.assertIsNone(product_dag.is_valid_image_change(self.product_read_today, self.product_read_yesterday))
        self.product_read_yesterday.read_time = self.product_read_today.read_time - timedelta(hours=0.5 * max_hours)
        self.assertDeepAlmostEqual(
            self.product_read_diff,
            product_dag.is_valid_image_change(self.product_read_today, self.product_read_yesterday))

    def test_process_asin(self):
        mock_images_api = MagicMock()
        self.mock_connector.get_last_product_read = MagicMock(return_value=None)
        product_dag.insert_new_product_read = MagicMock(return_value=self.product_read_today)
        product_dag.apply_product_images_changed = MagicMock()
        product_dag.is_valid_image_change = MagicMock()
        product_dag.process_asin(self.asin_active, self.mock_connector, mock_images_api)
        product_dag.is_valid_image_change.assert_not_called()
        product_dag.apply_product_images_changed.assert_not_called()

        self.mock_connector.get_last_product_read = MagicMock(return_value=self.product_read_yesterday)
        product_dag.is_valid_image_change = MagicMock(return_value=None)
        product_dag.process_asin(self.asin_active, self.mock_connector, mock_images_api)
        product_dag.apply_product_images_changed.assert_not_called()

        product_dag.is_valid_image_change = MagicMock(return_value=self.product_read_diff)
        product_dag.process_asin(self.asin_active, self.mock_connector, mock_images_api)
        product_dag.apply_product_images_changed.assert_called()
