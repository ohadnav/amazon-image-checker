import os
from dataclasses import dataclass
from datetime import datetime
from typing import List, Mapping

import pandas as pd
from pyairtable import Table

from airtable import config

ABTestId = int


@dataclass
class ABTestRecord:
    fields: dict


class AirtableReader():
    def __init__(self):
        self.table = Table(os.environ['AIRTABLE_API_KEY'], os.environ['AIRTABLE_BASE_ID'], os.environ['AIRTABLE_TABLE'])

    def _get_raw_record_id(self, record: dict) -> ABTestId:
        return self._get_raw_record_fields(record)[config.TEST_ID_FIELD]

    def get_active_ab_test_records(self) -> Mapping[ABTestId, ABTestRecord]:
        active_ab_tests = self.table.all(formula=AirtableReader._active_ab_test_formula())
        test_id_to_records = {self._get_raw_record_id(raw_record): ABTestRecord(self._get_raw_record_fields(raw_record))
            for raw_record in active_ab_tests}
        return test_id_to_records

    @staticmethod
    def _parse_start_date(record: ABTestRecord) -> datetime:
        return datetime.strptime(record.fields[config.START_DATE_FIELD], config.AIRTABLE_TIME_FORMAT)

    @staticmethod
    def current_variation(record: ABTestRecord) -> str:
        hours_diff = AirtableReader._calculate_hours_since_start(record)
        rotations = int(hours_diff / record.fields[config.ROTATION_FIELD])
        return 'B' if rotations % 2 else 'A'

    @staticmethod
    def _calculate_hours_since_start(record: ABTestRecord) -> float:
        return (datetime.now() - AirtableReader._parse_start_date(record)).total_seconds() / 3600

    @staticmethod
    def get_flatfile_for_record(record: ABTestRecord) -> dict:
        flatfile = record.fields[config.FLATFILE_FIELD[AirtableReader.current_variation(record)]][0]
        return flatfile

    @staticmethod
    def get_flatfile_url_for_record(record: ABTestRecord) -> dict:
        return AirtableReader.get_flatfile_for_record(record)['url']

    def get_asins_of_active_ab_test(self) -> List[str]:
        all_asins = []
        for ab_test_record in self.get_active_ab_test_records().values():
            flatfile_url = self.get_flatfile_url_for_record(ab_test_record)
            ab_test_df = pd.read_excel(flatfile_url, sheet_name=config.FLATFILE_SHEET_NAME, skiprows=[0, 2]).dropna(
                how='all')
            ab_test_asins = ab_test_df[config.FLATFILE_ASIN_COLUMN].dropna().tolist()
            all_asins.extend(ab_test_asins)
        return all_asins

    @staticmethod
    def _get_raw_record_fields(record: dict) -> dict:
        return record['fields']

    @staticmethod
    def _active_ab_test_formula() -> str:
        now_str = f'DATETIME_PARSE(\"{datetime.now().strftime(config.PYTHON_TIME_FORMAT)}\", \"YYYY-MM-DD hh:mm\")'
        now_after_start_formula = f'IS_AFTER({now_str}, {{{config.START_DATE_FIELD}}})'
        now_before_end_formula = f'IS_BEFORE({now_str}, {{{config.END_DATE_FIELD}}})'
        between_dates_formula = f'AND({now_after_start_formula},{now_before_end_formula})'
        active_status_formula = f'{{{config.STATUS_FIELD}}} = \"Active\"'
        final_formula = f'AND({between_dates_formula},{active_status_formula})'
        return final_formula
