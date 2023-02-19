from amazon_sp_api.amazon_api import AmazonApi
from common import test_util
from common.test_util import BaseTestCase

TEST_ASIN = 'B07Z95MG3S'
TEST_PRICE = 11.99

# TEST_SKU = 'TF-W-THIN-7P8-07'


class TestAmazonApi(BaseTestCase):
    def setUp(self) -> None:
        super(TestAmazonApi, self).setUp()
        self.amazon_api = AmazonApi()

    def test_get_images(self):
        images = self.amazon_api.get_images(TEST_ASIN)
        self.assertTrue(len(images))
        self.assertTrue(images[0].link.startswith('https://m.media-amazon.com/images/I/'))

    def test_submit_feed(self):
        self.assertIsInstance(self.amazon_api.post_feed(test_util.TEST_FLATFILE_A), int)

    def test_get_listing_price(self):
        self.assertAlmostEqual(self.amazon_api.get_listing_price(TEST_ASIN), TEST_PRICE)
