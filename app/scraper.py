# This is a sample Python script.
import json
import logging
import re
import webbrowser
from datetime import datetime

import scrapy
from scrapy.http import Response


# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


class AmazonSpider(scrapy.Spider):
    def start_request_for_asin(self, asin: str):
        yield scrapy.Request(url=self.get_url_from_asin(asin), callback=self.parse)

    def extract_asin_from_url(self, url: str) -> str:
        asin = re.search(r'/dp/(.*)/', url).group(1)
        return asin

    def parse(self, response: Response, **kwargs):
        image_urls = self.find_images_urls_for_asin(response)
        read_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if __debug__:
            open_list_of_urls_in_browser(image_urls)
        asin = self.extract_asin_from_url(response.url)
        logging.info(f'images for {asin} are {", ".join(image_urls)}')

    def get_url_from_asin(self, asin: str) -> str:
        url = 'https://www.amazon.com/dp/' + asin
        return url

    def get_url_of_landing_image_from_amazon(self, response: Response) -> str:
        # find the landing image url in the scrapy response
        landing_image_url = response.css('#landingImage::attr(src)').get()
        return landing_image_url

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
        # find the images div in the scrapy response
        images_div = response.css('#imageBlock_feature_div')
        # find all the html elements with the tag script in the images div
        all_scripts_in_div = images_div.css('script')
        # get the inner html of each script element
        scripts_inner_html = [script.get() for script in all_scripts_in_div]
        landing_image_url = self.get_url_of_landing_image_from_amazon(response)
        images_script = [script for script in scripts_inner_html if landing_image_url in script][0]
        return images_script


# open url in a browser
def open_url_in_new_tab_in_browser(url: str):
    webbrowser.open_new_tab(url)


def open_list_of_urls_in_browser(urls: list[str]):
    for url in urls:
        open_url_in_new_tab_in_browser(url)
