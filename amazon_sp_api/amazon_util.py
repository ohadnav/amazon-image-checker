import re


class AmazonUtil():
    @staticmethod
    def extract_asin_from_url(url: str) -> str:
        asin = re.search(r'/dp/(.*)/?', url).group(1)
        return asin

    @staticmethod
    def get_url_from_asin(asin: str) -> str:
        url = 'https://www.amazon.com/dp/' + asin
        return url
