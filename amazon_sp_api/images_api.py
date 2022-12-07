import logging
from typing import List

from sp_api.api import CatalogItems


class ImagesApi():
    def get_images(self, asin: str) -> List[dict]:
        try:
            catalog = CatalogItems()
            response = catalog.get_catalog_item(asin, includedData=['images'])
            images = response.payload['images'][0]['images']
            return images
        except Exception as e:
            logging.error(f'Error getting images for asin {asin}: {e}')
            raise e
