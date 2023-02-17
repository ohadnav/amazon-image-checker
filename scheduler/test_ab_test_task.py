from unittest.mock import MagicMock, patch

import airtable.config
import airtable.reader
from database.test_connector import BaseConnectorTestCase
from scheduler.ab_test_task import ABTestTask


class ABTestTaskTestCase(BaseConnectorTestCase):

    def setUp(self) -> None:
        super(ABTestTaskTestCase, self).setUp()
        self.ab_test_task = ABTestTask()
        self.ab_test_task.connector = self.local_connector
        self.ab_test_task.amazon_api = MagicMock()
        self.ab_test_task.airtable_reader = MagicMock()

    @patch('airtable.reader.AirtableReader.current_variation', autospec=True)
    def test_should_not_run_ab_test(self, current_variation_mock):
        self.ab_test_task.connector.get_last_run_for_ab_test = MagicMock(return_value=None)
        current_variation_mock.return_value = 'A'
        self.assertFalse(self.ab_test_task.should_run_ab_test(self.ab_test_record1))

        current_variation_mock.return_value = 'B'
        self.assertTrue(self.ab_test_task.should_run_ab_test(self.ab_test_record1))

        current_variation_mock.return_value = 'A'
        self.ab_test_task.connector.get_last_run_for_ab_test.return_value = self.ab_test_run1a
        self.assertFalse(self.ab_test_task.should_run_ab_test(self.ab_test_record1))

        current_variation_mock.return_value = self.ab_test_run1b.variation
        self.assertTrue(self.ab_test_task.should_run_ab_test(self.ab_test_record1))

    def test_task(self):
        self.ab_test_task.airtable_reader.get_active_ab_test_records = MagicMock(
            return_value={self.ab_test_record1.fields[airtable.config.TEST_ID_FIELD]: self.ab_test_record1})
        self.ab_test_task.should_run_ab_test = MagicMock(return_value=False)
        self.ab_test_task.amazon_api.post_feed = MagicMock()
        self.ab_test_task.connector.insert_ab_test_run = MagicMock()
        self.ab_test_task.task()
        self.ab_test_task.amazon_api.post_feed.assert_not_called()
        self.ab_test_task.connector.insert_ab_test_run.assert_not_called()
        self.ab_test_task.should_run_ab_test.return_value = True
        self.ab_test_task.amazon_api.post_feed.return_value = self.ab_test_run1a.feed_id
        self.ab_test_task.connector.update_feed_id = MagicMock()
        self.ab_test_task.task()
        self.ab_test_task.connector.update_feed_id.assert_called_once_with(
            self.ab_test_record1, self.ab_test_run1a.feed_id)
        self.ab_test_task.connector.insert_ab_test_run.assert_called_once()
