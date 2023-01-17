import logging
from dataclasses import dataclass, asdict
from io import FileIO
from typing import List

from dacite import from_dict
from sp_api.api import CatalogItems, Feeds
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
    def post_feed(self, feed_filename: str):
        response = Feeds().submit_feed(
            FeedType.POST_FLAT_FILE_LISTINGS_DATA, FileIO(feed_filename),
            content_type='application/vnd.ms-excel.sheet.macroEnabled.12')
        return response
