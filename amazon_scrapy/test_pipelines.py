import os
from copy import deepcopy
from datetime import datetime, timedelta
from unittest import TestCase, mock

from scrapy.exceptions import DropItem

from amazon_scrapy.items import AmazonScrapyItem
from amazon_scrapy.pipelines import AmazonScrapyPipeline
from amazon_scrapy.spiders.amazon_spider import AmazonSpider
from database import config
from database.connector import ProductRead
from database.test_connector import MockMySQLConnector


class TestAmazonScrapyPipeline(TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.pipeline = AmazonScrapyPipeline()
        self.pipeline.connector = MockMySQLConnector()
        self.image_urls = ['https://m.media-amazon.com/images/I/51rvrsfUhZL._AC_.jpg',
            'https://m.media-amazon.com/images/I/51OVx5NEYVL._AC_.jpg',
            'https://m.media-amazon.com/images/I/51o5Wr83HXL._AC_.jpg',
            'https://m.media-amazon.com/images/I/51hVxG9bKTL._AC_.jpg',
            'https://m.media-amazon.com/images/I/41hxnxAcihL._AC_.jpg']
        self.asin = 'B07X9FSTWW'
        self.item = AmazonScrapyItem({config.ASIN_FIELD: self.asin, config.IMAGE_URLS_FIELD: self.image_urls})
        self.current = ProductRead(self.asin, datetime.now(), self.image_urls)
        self.last = deepcopy(self.current)
        self.spider = AmazonSpider()
        self.pipeline.connector.insert_product_read = mock.MagicMock()
        self.pipeline.connector.get_last_product_read = mock.MagicMock()

    def test_is_image_changed(self):
        self.assertFalse(self.pipeline.is_image_changed(self.current, self.last))
        self.last.image_urls = self.image_urls[1:]
        self.assertTrue(self.pipeline.is_image_changed(self.current, self.last))
        self.last.image_urls = deepcopy(self.image_urls)
        self.last.image_urls[0] += '1'
        self.assertTrue(self.pipeline.is_image_changed(self.current, self.last))
        self.last.image_urls = deepcopy(self.image_urls)
        self.last.read_time - timedelta(hours=float(os.environ['MAX_TIME_DIFFERENCE_IN_HOURS']) * 2)
        self.assertFalse(self.pipeline.is_image_changed(self.current, self.last))

    def test_process_item(self):
        self.pipeline.notify_if_needed = mock.MagicMock()
        self.pipeline.process_item(self.item, self.spider)
        self.pipeline.notify_if_needed.assert_called_once()
        self.pipeline.connector.insert_product_read.assert_called_once()

    def test_process_item_missing_data(self):
        self.item[config.IMAGE_URLS_FIELD] = None
        self.pipeline.notify_if_needed = mock.MagicMock()
        with self.assertRaises(DropItem):
            self.pipeline.process_item(self.item, self.spider)
            self.pipeline.connector.insert_product_read.assert_not_called()
            self.pipeline.notify_if_needed.assert_not_called()
        self.item[config.IMAGE_URLS_FIELD] = self.image_urls
        self.item[config.ASIN_FIELD] = None
        with self.assertRaises(DropItem):
            self.pipeline.process_item(self.item, self.spider)
            self.pipeline.connector.insert_product_read.assert_not_called()
            self.pipeline.notify_if_needed.assert_not_called()

    @mock.patch('app.notifier.notify')
    def test_notify_if_needed_missing_last(self, mock_notify):
        self.pipeline.is_image_changed = mock.MagicMock(return_value=True)
        self.pipeline.connector.get_last_product_read.return_value = None
        self.pipeline.notify_if_needed(self.current)
        mock_notify.assert_not_called()

    @mock.patch('app.notifier.notify')
    def test_notify_if_needed_notified(self, mock_notify):
        self.pipeline.is_image_changed = mock.MagicMock(return_value=True)
        self.pipeline.connector.get_last_product_read.return_value = self.last
        self.pipeline.notify_if_needed(self.current)
        mock_notify.assert_called_once_with(self.asin)

    @mock.patch('app.notifier.notify')
    def test_notify_if_needed_image_unchanged(self, mock_notify):
        self.pipeline.is_image_changed = mock.MagicMock(return_value=False)
        self.pipeline.connector.get_last_product_read.return_value = self.last
        self.pipeline.notify_if_needed(self.current)
        mock_notify.assert_not_called()
