import os
from time import strptime

import cronitor

import db_connector
import notifier
import scraper
from db_connector import ProductRead


@cronitor.job('amazon-image-scraper')
def run_scraper():
    amazon_spider = scraper.AmazonSpider()
    asins_with_active_ab_test = db_connector.get_asins_with_active_ab_test()
    for asin in asins_with_active_ab_test:
        amazon_spider.start_request_for_asin(asin)


def is_image_changed(current: ProductRead, last: ProductRead) -> bool:
    max_diff = strptime(os.environ['MAX_TIME_DIFFERENCE_IN_SECONDS'], '%H:%M:%S')
    if current.read_time > last.read_time + max_diff:
        return False
    return set(last.image_urls) != set(current.image_urls)


def handle_scraper_response(product_read: db_connector.ProductRead):
    db_connector.insert_product_read(product_read)
    last_read = db_connector.get_last_product_read(product_read.asin)
    if is_image_changed(product_read, last_read):
        notifier.notify_asin(product_read.asin)
