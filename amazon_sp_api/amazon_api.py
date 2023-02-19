import logging
import os
import time
import urllib.request
from dataclasses import dataclass, asdict
from io import FileIO, BytesIO
from typing import List

from dacite import from_dict
from sp_api.api import CatalogItems, Feeds, Products
from sp_api.base import FeedType
from sp_api.util import throttle_retry


@dataclass
class ImageVariation:
    variant: str
    link: str
    height: int
    width: int

    # implement hash to allow for set operations
    def __hash__(self):
        # convert dataclass to string
        self_str = str(asdict(self))
        return hash(self_str)


class AmazonApi():
    @throttle_retry()
    def get_images(self, asin: str) -> List[ImageVariation]:
        logging.debug(f'Getting images for asin: {asin}')
        response = CatalogItems().get_catalog_item(asin, includedData=['images'])
        images_response = response.payload['images'][0]['images']
        # convert dict to ImageVariation
        images = []
        for image in images_response:
            images.append(ImageVariation(image['variant'], image['link'], image['height'], image['width']))
        image_variations = [from_dict(ImageVariation, image) for image in images_response]
        unique_image_variations = set([image_variation.variant for image_variation in image_variations])
        logging.debug(f'Got {unique_image_variations} images for asin: {asin}')
        return image_variations

    @throttle_retry()
    def get_price(self, asin: str) -> float:
        logging.debug(f'Getting price for asin: {asin}')
        response = Products().get_competitive_pricing_for_asins([asin])
        return response.payload[0]['Product']['CompetitivePricing']['CompetitivePrices'][0]['Price']['ListingPrice'][
            'Amount']

    @throttle_retry()
    def post_feed(self, feed_url: str) -> int:
        logging.debug(f'Posting feed: {feed_url}')
        temp_file_path = self.read_xlsm_url_into_temp_file(feed_url)
        try:
            response = Feeds().submit_feed(
                FeedType.POST_FLAT_FILE_LISTINGS_DATA, FileIO(temp_file_path),
                content_type='application/vnd.ms-excel.sheet.macroEnabled.12')
        except Exception as e:
            os.remove(temp_file_path)
            raise e
        finally:
            os.remove(temp_file_path)
        payload_feed_id = int(response[1].payload['feedId'])
        logging.debug(f'Got feed_id: {payload_feed_id}')
        return payload_feed_id

    @staticmethod
    def read_xlsm_url_into_temp_file(feed_url: str) -> str:
        if feed_url.startswith('http'):
            with urllib.request.urlopen(feed_url) as url_response:
                url_content = url_response.read()
            bytes_io = BytesIO(url_content)
        else:
            with open(feed_url, "rb") as local_file:
                bytes_io = BytesIO(local_file.read())
        temp_file_path = f'{int(time.time())}.xlsm'
        with open(temp_file_path, 'wb') as temp_file:
            temp_file.write(bytes_io.read())
        return temp_file_path
