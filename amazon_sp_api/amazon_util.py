import re

from amazon_sp_api.amazon_api import ASIN


class AmazonUtil():
    @staticmethod
    def extract_asin_from_url(url: str) -> str:
        asin = re.search(r'/dp/(.*)/?', url).group(1)
        return asin

    @staticmethod
    def get_url_from_asin(asin: ASIN) -> str:
        url = 'https://www.amazon.com/dp/' + asin
        return url
