from datetime import datetime
from unittest.mock import MagicMock, patch

from freezegun import freeze_time
from pytz import timezone

import airtable.config
from airtable.test_reader import ACTIVE_TEST_ID
from database.test_database_api import BaseConnectorTestCase
from scheduler.ab_test_task import ABTestTask


class ABTestTaskTestCase(BaseConnectorTestCase):

    def setUp(self) -> None:
        super(ABTestTaskTestCase, self).setUp()
        self.ab_test_task = ABTestTask()
        self.ab_test_task.database_api = self.database_api
        self.ab_test_task.amazon_api = MagicMock()
        self.ab_test_task.airtable_reader = MagicMock()

    @patch('airtable.reader.AirtableReader.current_variation', autospec=True)
    def test_should_not_run_ab_test(self, current_variation_mock):
        self.ab_test_task.database_api.get_last_run_for_ab_test = MagicMock(return_value=None)
        current_variation_mock.return_value = 'A'
        self.assertFalse(self.ab_test_task.should_run_ab_test(self.ab_test_record1))

        current_variation_mock.return_value = 'B'
        self.assertTrue(self.ab_test_task.should_run_ab_test(self.ab_test_record1))

        current_variation_mock.return_value = 'A'
        self.ab_test_task.database_api.get_last_run_for_ab_test.return_value = self.ab_test_run1a
        self.assertFalse(self.ab_test_task.should_run_ab_test(self.ab_test_record1))

        current_variation_mock.return_value = self.ab_test_run1b.variation
        self.assertTrue(self.ab_test_task.should_run_ab_test(self.ab_test_record1))

    def test_task(self):
        self.ab_test_task.airtable_reader.get_active_ab_test_records = MagicMock(
            return_value={self.ab_test_record1.fields[airtable.config.TEST_ID_FIELD]: self.ab_test_record1})
        self.ab_test_task.should_run_ab_test = MagicMock(return_value=False)
        self.ab_test_task.amazon_api.post_feed = MagicMock()
        self.ab_test_task.database_api.insert_ab_test_run = MagicMock()
        self.ab_test_task.task()
        self.ab_test_task.amazon_api.post_feed.assert_not_called()
        self.ab_test_task.database_api.insert_ab_test_run.assert_not_called()
        self.ab_test_task.should_run_ab_test.return_value = True
        self.ab_test_task.amazon_api.post_feed.return_value = self.ab_test_run1a.feed_id
        self.ab_test_task.database_api.update_feed_id = MagicMock()
        self.ab_test_task.database_api.insert_ab_test_run.return_value = self.ab_test_run1a
        self.ab_test_task.task()
        self.ab_test_task.database_api.update_feed_id.assert_called_once_with(self.ab_test_run1a)
        self.ab_test_task.database_api.insert_ab_test_run.assert_called_once()

    def test_task_integration(self):
        self.ab_test_task = ABTestTask()
        self.ab_test_task.should_run_ab_test = MagicMock(return_value=True)
        with freeze_time(
                datetime.strptime('2023-01-16 00:01', airtable.config.PYTHON_TIME_FORMAT).astimezone(
                    timezone(airtable.config.TIMEZONE))):
            active_ab_test_records = self.ab_test_task.airtable_reader.get_active_ab_test_records()
            active_ab_test = active_ab_test_records[ACTIVE_TEST_ID]
        self.assertIsNone(self.ab_test_task.database_api.get_last_run_for_ab_test(active_ab_test))
        self.ab_test_task.airtable_reader = MagicMock()
        self.ab_test_task.airtable_reader.get_active_ab_test_records = MagicMock(return_value=active_ab_test_records)
        self.ab_test_task.run()
        last_run = self.ab_test_task.database_api.get_last_run_for_ab_test(active_ab_test)
        self.assertEqual(last_run.test_id, active_ab_test.fields[airtable.config.TEST_ID_FIELD])
        self.assertIsInstance(last_run.feed_id, int)
        self.assertEqual(last_run.feed_id, self.get_feed_id_by_run_id(last_run.run_id))
