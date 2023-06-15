from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pytz import timezone

from airtable import config


@dataclass
class ABTestRecord:
    fields: dict
    amazon_timezone = timezone(config.AMAZON_TIMEZONE_NAME)

    def _start_date_to_datetime(self) -> datetime:
        return datetime.strptime(
            f'{self.fields[config.START_DATE_FIELD]} {config.AMAZON_TIMEZONE}',
            config.AIRTABLE_DATETIME_TIMEZONE_FORMAT)

    def _calculate_hours_since_start(self) -> float:
        return (datetime.utcnow().astimezone(
            ABTestRecord.amazon_timezone) - self._start_date_to_datetime()).total_seconds() / 3600

    def current_variation(self) -> str:
        hours_diff = self._calculate_hours_since_start()
        rotations = int(hours_diff / self.fields[config.ROTATION_FIELD])
        return 'B' if rotations % 2 else 'A'

    def get_flatfile_for_record(self) -> dict:
        flatfile = self.fields[config.FLATFILE_FIELD[self.current_variation()]][0]
        return flatfile

    def get_flatfile_url_for_record(self) -> str:
        return self.get_flatfile_for_record()['url']


ABTestId = int
