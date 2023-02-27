from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from airtable import config


@dataclass
class ABTestRecord:
    fields: dict

    def _parse_start_date(self) -> datetime:
        return datetime.strptime(self.fields[config.START_DATE_FIELD], config.AIRTABLE_TIME_FORMAT)

    def current_variation(self) -> str:
        hours_diff = self._calculate_hours_since_start()
        rotations = int(hours_diff / self.fields[config.ROTATION_FIELD])
        return 'B' if rotations % 2 else 'A'

    def _calculate_hours_since_start(self) -> float:
        return (datetime.now() - self._parse_start_date()).total_seconds() / 3600

    def get_flatfile_for_record(self) -> dict:
        flatfile = self.fields[config.FLATFILE_FIELD[self.current_variation()]][0]
        return flatfile

    def get_flatfile_url_for_record(self) -> str:
        return self.get_flatfile_for_record()['url']


ABTestId = int
