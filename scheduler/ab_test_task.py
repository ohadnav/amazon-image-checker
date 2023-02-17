import logging

from airtable.reader import ABTestRecord, AirtableReader
from database.connector import ABTestRun
from scheduler.base_task import BaseTask


class ABTestTask(BaseTask):
    def should_run_ab_test(self, ab_test_record: ABTestRecord) -> bool:
        last_ab_test_run = self.connector.get_last_run_for_ab_test(ab_test_record)
        current_variation = AirtableReader.current_variation(ab_test_record)
        if not last_ab_test_run and current_variation == 'B':
            return True
        return last_ab_test_run and last_ab_test_run.variation != current_variation

    def task(self):
        active_ab_test_records = self.airtable_reader.get_active_ab_test_records()
        for ab_test_record in active_ab_test_records.values():
            if self.should_run_ab_test(ab_test_record):
                ab_test_run = ABTestRun.from_record(ab_test_record)
                logging.debug(f'Running AB test: {ab_test_run}')
                self.connector.insert_ab_test_run(ab_test_run)
                feed_id = self.amazon_api.post_feed(AirtableReader.get_flatfile_url_for_record(ab_test_record))
                self.connector.update_feed_id(ab_test_record, feed_id)
                logging.debug(f'Finished running AB test: {ab_test_run}')


if __name__ == '__main__':
    ABTestTask().run()
