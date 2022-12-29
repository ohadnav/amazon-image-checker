import logging
import time
from dataclasses import dataclass, asdict
from typing import List

from dacite import from_dict
from sp_api.api import CatalogItems
from sp_api.base import SellingApiRequestThrottledException


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


class ImagesApi():
    def get_images(self, asin: str, retries=0) -> List[ImageVariation]:
        try:
            catalog = CatalogItems()
            logging.debug(f'Getting images for asin: {asin}')
            response = catalog.get_catalog_item(asin, includedData=['images'])
            images_response = response.payload['images'][0]['images']
            # convert dict to ImageVariation
            images = []
            for image in images_response:
                images.append(ImageVariation(image['variant'], image['link'], image['height'], image['width']))
            image_variations = [from_dict(ImageVariation, image) for image in images_response]
            unique_image_variations = set([image_variation.variant for image_variation in image_variations])
            logging.debug(f'Got {unique_image_variations} images for asin: {asin}')
            return image_variations
        except SellingApiRequestThrottledException as e:
            if retries < 10:
                logging.info(f'Got throttled, retrying in a minute. Retries: {retries}')
                time.sleep(60)
                return self.get_images(asin, retries + 1)
            else:
                logging.warning(f'Max retires for throttling reached. Asin: {asin}')
                raise e
        except Exception as e:
            logging.error(f'Error getting images for asin {asin}: {e}')
            raise e
