from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from amazon_scrapy.spiders.amazon_spider import AmazonSpider
from database import config
from database.test_util import BaseTestCase


class TestAmazonSpider(BaseTestCase):
    def setUp(self) -> None:
        self.asin = 'B07X9FSTWW'
        self.spider = AmazonSpider()
        self.spider.add_asin('B07X9FSTWW')
        settings = get_project_settings()
        settings['ITEM_PIPELINES']['amazon_scrapy.spiders.integration_test_amazon_spider.ScrapyTestPipeline'] = 999
        self.process = CrawlerProcess(settings)
        self.process.crawl(AmazonSpider)
        global scraped_items
        scraped_items = []

    def test_parse(self):
        self.process.start()
        product_read = scraped_items[0]
        expected_image_urls = ['https://m.media-amazon.com/images/I/51rvrsfUhZL._AC_.jpg',
            'https://m.media-amazon.com/images/I/51OVx5NEYVL._AC_.jpg',
            'https://m.media-amazon.com/images/I/51o5Wr83HXL._AC_.jpg',
            'https://m.media-amazon.com/images/I/51hVxG9bKTL._AC_.jpg',
            'https://m.media-amazon.com/images/I/41hxnxAcihL._AC_.jpg']
        self.assertEqual(product_read[config.IMAGE_URLS_FIELD], expected_image_urls)


scraped_items = []


class ScrapyTestPipeline:
    def process_item(self, item, spider):
        global scraped_items
        scraped_items.append(item)
        return item
