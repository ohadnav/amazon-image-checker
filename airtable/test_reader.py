import os
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from freezegun import freeze_time
from pytz import timezone

from airtable import config
from airtable.reader import AirtableReader, ABTestRecord
from common import test_util
from common.test_util import BaseTestCase

ACTIVE_TEST_ID = 1


@freeze_time(datetime.strptime('2023-01-16 00:01', config.PYTHON_TIME_FORMAT).astimezone(timezone(config.TIMEZONE)))
class BaseAirtableReaderTestCase(BaseTestCase):
    def setUp(self) -> None:
        super(BaseAirtableReaderTestCase, self).setUp()
        self.airtable_reader = AirtableReader()

    def test_get_active_ab_tests(self):
        active_ab_tests = self.airtable_reader.get_active_ab_test_records()
        self.assertEqual(len(active_ab_tests), 1)
        self.assertEqual(active_ab_tests[ACTIVE_TEST_ID].fields[config.TEST_ID_FIELD], ACTIVE_TEST_ID)

    def test_calculate_variation(self):
        self.assertEqual(
            AirtableReader.current_variation(
                ABTestRecord(
                    {
                        config.ROTATION_FIELD: 24,
                        config.START_DATE_FIELD: datetime.strftime(
                            datetime.now() + timedelta(hours=23), config.AIRTABLE_TIME_FORMAT)})),
            'A')
        self.assertEqual(
            AirtableReader.current_variation(
                ABTestRecord(
                    {
                        config.ROTATION_FIELD: 12,
                        config.START_DATE_FIELD: datetime.strftime(
                            datetime.now() + timedelta(hours=23), config.AIRTABLE_TIME_FORMAT)})),
            'B')
        self.assertEqual(
            AirtableReader.current_variation(
                ABTestRecord(
                    {
                        config.ROTATION_FIELD: 24,
                        config.START_DATE_FIELD: datetime.strftime(
                            datetime.now() + timedelta(hours=25), config.AIRTABLE_TIME_FORMAT)})),
            'B')

    def test_get_flatfile_for_record(self):
        record = self.airtable_reader.get_active_ab_test_records()[ACTIVE_TEST_ID]
        AirtableReader.current_variation = MagicMock(return_value='A')
        flatfile_for_record = AirtableReader.get_flatfile_for_record(record)
        self.assertEqual(os.path.basename(test_util.TEST_FLATFILE_A), flatfile_for_record['filename'])

    def test_get_asins_with_active_ab_test(self):
        AirtableReader.current_variation = MagicMock(return_value='A')
        self.assertListEqual(
            self.airtable_reader.get_asins_of_active_ab_test(), ['B08LKMP5QX'])
