import logging
import os
from datetime import datetime, timedelta
from time import strptime
from typing import Optional

import airtable.config
from airtable.ab_test_record import ABTestRecord
from common import util
from database.data_model import ASIN, ProductRead, ProductReadDiff
from notify.slack_notifier import notify
from scheduler.base_task import BaseTask


class ProductReadChangesTask(BaseTask):
    def __init__(self):
        super(ProductReadChangesTask, self).__init__()

    def is_valid_change(self, current: ProductRead, last: ProductRead) -> Optional[ProductReadDiff]:
        product_read_diff = ProductReadDiff(current, last)
        max_diff = strptime(os.environ['MAX_TIME_DIFFERENCE_IN_HOURS'], '%H')
        # convert to timedelta
        max_delta = timedelta(hours=max_diff.tm_hour)
        # diff between current and last read time
        diff = current.read_time - last.read_time
        # if diff is greater than max diff
        if diff > max_delta or not product_read_diff.has_diff():
            return None
        return product_read_diff

    def task(self):
        asins_to_active_ab_test = self.airtable_reader.get_asins_to_active_ab_test()
        for asin, ab_test_record in asins_to_active_ab_test.items():
            self.process_asin(asin, ab_test_record)

    def process_asin(self, asin: ASIN, ab_test_record: ABTestRecord):
        logging.debug('Processing asin {}'.format(asin))
        last_product_read = self.database_api.get_last_product_read(asin, ab_test_record)
        new_product_read = self.insert_new_product_read(asin, ab_test_record)
        if last_product_read:
            product_read_diff = self.is_valid_change(new_product_read, last_product_read)
            if product_read_diff:
                self.process_product_read_changed(product_read_diff)

    def process_product_read_changed(self, product_read_diff: ProductReadDiff):
        images_changed = set([image_variation.variant for image_variation in product_read_diff.image_variations])
        logging.info(
            f'Found product read changes for asin {product_read_diff.asin}'
            f'{" in images " if images_changed else ""}{", ".join(images_changed)}'
            f'{" in listing price " + str(product_read_diff.listing_price) if product_read_diff.listing_price else ""}'
            f'{" in is_active " + str(product_read_diff.is_active) if product_read_diff.is_active is not None else ""}')
        self.database_api.insert_product_read_changes(product_read_diff)
        notify(product_read_diff)

    def insert_new_product_read(self, asin: ASIN, ab_test_record: ABTestRecord) -> ProductRead:
        read_time = datetime.now()
        images = self.amazon_api.get_images(asin, ab_test_record)
        listing_price = self.amazon_api.get_listing_price(asin, ab_test_record)
        is_active = self.amazon_api.is_active(asin, ab_test_record)
        new_product_read = ProductRead(
            asin, read_time, images, listing_price,
            ab_test_record.fields[airtable.config.MERCHANT_FIELD], is_active)
        self.database_api.insert_product_read(new_product_read)
        return new_product_read


if __name__ == '__main__':
    util.initialize_logging(logging_level=logging.DEBUG)
    ProductReadChangesTask().run()
