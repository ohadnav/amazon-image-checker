import json
import re

import scrapy
from scrapy.http import Response

from amazon_scrapy.items import AmazonScrapyItem


class AmazonSpider(scrapy.Spider):
    name = 'amazon-images-spider'
    allowed_domains = ['amazon.com']
    start_urls = []

    def add_asin(self, asin: str):
        self.start_urls.append(self.get_url_from_asin(asin))

    def extract_asin_from_url(self, url: str) -> str:
        asin = re.search(r'/dp/(.*)/?', url).group(1)
        return asin

    def parse(self, response: Response, **kwargs):
        image_urls = self.find_images_urls_for_asin(response)
        asin = self.extract_asin_from_url(response.request.url)
        yield AmazonScrapyItem(asin=asin, image_urls=image_urls)

    def get_url_from_asin(self, asin: str) -> str:
        url = 'https://www.amazon.com/dp/' + asin
        return url

    def find_images_urls_for_asin(self, response: Response) -> list[str]:
        images_json_object = self.extract_images_json(response)
        # pick the large image from each image in the json object
        large_images = [image['large'] for image in images_json_object]
        return large_images

    def extract_images_json(self, response: Response) -> dict:
        images_script = self.extract_images_script(response)
        # load the color images into a json object
        images_json_text = re.search(r'colorImages\':\s(\{.*})', images_script).group(1)
        # fix json commas into double ones
        images_json_text = images_json_text.replace("'", '"')
        images_json_object = json.loads(images_json_text)['initial']
        return images_json_object

    def extract_images_script(self, response: Response) -> str:
        images_script = response.xpath(
            '//div[@id="imageBlock_feature_div"]/script[contains(.,colorImages)]/text()').get()
        return images_script
