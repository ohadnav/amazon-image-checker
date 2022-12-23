import os
from datetime import datetime, timedelta
from time import strptime
from typing import Optional

from amazon_sp_api.images_api import ImagesApi
from database.connector import MySQLConnector, ProductRead, ProductReadDiff
from notify.slack_notifier import notify


def is_valid_image_change(current: ProductRead, last: ProductRead) -> Optional[ProductReadDiff]:
    product_read_diff = ProductReadDiff(current, last)
    max_diff = strptime(os.environ['MAX_TIME_DIFFERENCE_IN_HOURS'], '%H')
    # convert to timedelta
    max_delta = timedelta(hours=max_diff.tm_hour)
    # diff between current and last read time
    diff = current.read_time - last.read_time
    # if diff is greater than max diff
    if diff > max_delta:
        return None
    return product_read_diff


def product_images_task():
    connector = MySQLConnector()
    images_api = ImagesApi()
    asins_with_active_ab_test = connector.get_asins_with_active_ab_test()
    for asin in asins_with_active_ab_test:
        process_asin(asin, connector, images_api)


def process_asin(asin: str, connector: MySQLConnector, images_api: ImagesApi):
    last_product_read = connector.get_last_product_read(asin)
    new_product_read = insert_new_product_read(asin, connector, images_api)
    if last_product_read:
        product_read_diff = is_valid_image_change(new_product_read, last_product_read)
        if product_read_diff:
            apply_product_images_changed(product_read_diff, connector)


def apply_product_images_changed(product_read_diff: ProductReadDiff, connector: MySQLConnector):
    connector.insert_images_changes(product_read_diff)
    notify(product_read_diff)


def insert_new_product_read(asin: str, connector: MySQLConnector, images_api: ImagesApi) -> ProductRead:
    read_time = datetime.now()
    images = images_api.get_images(asin)
    new_product_read = ProductRead(asin, read_time, images)
    connector.insert_product_read(new_product_read)
    return new_product_read


if __name__ == '__main__':
    print('Starting image changes task')
    product_images_task()
