import cronitor
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from amazon_scrapy.spiders.amazon_spider import AmazonSpider
from database.connector import MySQLConnector


@cronitor.job('amazon-image-scraper')
def run_scraper():
    amazon_spider = AmazonSpider()
    connector = MySQLConnector()
    process = CrawlerProcess(get_project_settings())
    process.crawl(AmazonSpider)
    asins_with_active_ab_test = connector.get_asins_with_active_ab_test()
    for asin in asins_with_active_ab_test:
        amazon_spider.add_asin(asin)
    process.start()
