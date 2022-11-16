# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import os
from datetime import datetime, timedelta
from time import strptime

# useful for handling different item types with a single interface
from scrapy.exceptions import DropItem

from amazon_scrapy.items import AmazonScrapyItem
from amazon_scrapy.spiders.amazon_spider import AmazonSpider
from app import notifier
from database import config
from database.connector import ProductRead, MySQLConnector


class AmazonScrapyPipeline:
    def __init__(self):
        self.connector = MySQLConnector()

    def is_image_changed(self, current: ProductRead, last: ProductRead) -> bool:
        max_diff = strptime(os.environ['MAX_TIME_DIFFERENCE_IN_HOURS'], '%H')
        # convert to timedelta
        max_delta = timedelta(hours=max_diff.tm_hour)
        # diff between current and last read time
        diff = current.read_time - last.read_time
        # if diff is greater than max diff
        if diff > max_delta:
            return False
        return set(last.image_urls) != set(current.image_urls)

    def process_item(self, item: AmazonScrapyItem, spider: AmazonSpider):
        if not item[config.IMAGE_URLS_FIELD] and item[config.ASIN_FIELD]:
            raise DropItem('Missing images for asin: ' + item[config.ASIN_FIELD])
        elif not item[config.ASIN_FIELD]:
            raise DropItem('Missing asin')
        read_time = datetime.now()
        product_read = ProductRead(item[config.ASIN_FIELD], read_time, item[config.IMAGE_URLS_FIELD])
        self.connector.insert_product_read(product_read)
        self.notify_if_needed(product_read)
        return item

    def notify_if_needed(self, current_product_read: ProductRead):
        last_read = self.connector.get_last_product_read(current_product_read.asin)
        if last_read:
            if self.is_image_changed(current_product_read, last_read):
                notifier.notify(current_product_read.asin)
