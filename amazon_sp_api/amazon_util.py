import re


class AmazonUtil():
    def extract_asin_from_url(self, url: str) -> str:
        asin = re.search(r'/dp/(.*)/?', url).group(1)
        return asin

    def get_url_from_asin(self, asin: str) -> str:
        url = 'https://www.amazon.com/dp/' + asin
        return url
