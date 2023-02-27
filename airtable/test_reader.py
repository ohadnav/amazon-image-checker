import os
from datetime import datetime
from unittest.mock import MagicMock

from freezegun import freeze_time
from pytz import timezone

from airtable import config
from airtable.reader import AirtableReader
from common import test_util
from common.test_util import BaseTestCase

ACTIVE_TEST_ID = 1
TEST_ASIN = 'B08LKMP5QX'


@freeze_time(datetime.strptime('2023-01-16 00:01', config.PYTHON_TIME_FORMAT).astimezone(timezone(config.TIMEZONE)))
class BaseAirtableReaderTestCase(BaseTestCase):
    def setUp(self) -> None:
        super(BaseAirtableReaderTestCase, self).setUp()
        self.airtable_reader = AirtableReader()

    def test_get_active_ab_tests(self):
        active_ab_tests = self.airtable_reader.get_active_ab_test_records()
        self.assertEqual(len(active_ab_tests), 1)
        self.assertEqual(active_ab_tests[ACTIVE_TEST_ID].fields[config.TEST_ID_FIELD], ACTIVE_TEST_ID)

    def test_get_flatfile_for_record(self):
        record = self.airtable_reader.get_active_ab_test_records()[ACTIVE_TEST_ID]
        AirtableReader.current_variation = MagicMock(return_value='A')
        flatfile_for_record = record.get_flatfile_for_record()
        self.assertEqual(os.path.basename(test_util.TEST_FLATFILE_A), flatfile_for_record['filename'])

    def test_get_asins_with_active_ab_test(self):
        AirtableReader.current_variation = MagicMock(return_value='A')
        active_ab_tests = self.airtable_reader.get_active_ab_test_records()
        self.assertDictEqual(
            self.airtable_reader.get_asins_to_active_ab_test(), {TEST_ASIN: active_ab_tests[ACTIVE_TEST_ID]})
