from datetime import datetime, timedelta

from airtable import config
from airtable.ab_test_record import ABTestRecord
from common.test_util import BaseTestCase


class TestABTestRecordTestCase(BaseTestCase):
    def test_current_variation(self):
        record1 = ABTestRecord({config.ROTATION_FIELD: 24,
                                config.START_DATE_FIELD: datetime.strftime(datetime.now() + timedelta(hours=23),
                                                                           config.AIRTABLE_TIME_FORMAT)})
        self.assertEqual(record1.current_variation(), 'A')
        record2 = ABTestRecord({config.ROTATION_FIELD: 12,
                                config.START_DATE_FIELD: datetime.strftime(datetime.now() + timedelta(hours=23),
                                                                           config.AIRTABLE_TIME_FORMAT)})
        self.assertEqual(record2.current_variation(), 'B')
        record3 = ABTestRecord({config.ROTATION_FIELD: 24,
                                config.START_DATE_FIELD: datetime.strftime(datetime.now() + timedelta(hours=25),
                                                                           config.AIRTABLE_TIME_FORMAT)})
        self.assertEqual(record3.current_variation(), 'B')
