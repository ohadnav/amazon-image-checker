from amazon_sp_api.images_api import ImagesApi, ImageVariation
from common.test_util import BaseTestCase

TEST_ASIN = 'B07Z95MG3S'
EXPECTED_IMAGE_VARIATION = [
    ImageVariation(variant='MAIN', link='https://m.media-amazon.com/images/I/712IylmFunL.jpg', height=2500, width=2500),
    ImageVariation(variant='MAIN', link='https://m.media-amazon.com/images/I/41GEGFWswNL.jpg', height=500, width=500),
    ImageVariation(
        variant='MAIN', link='https://m.media-amazon.com/images/I/41GEGFWswNL._SL75_.jpg', height=75, width=75),
    ImageVariation(variant='PT01', link='https://m.media-amazon.com/images/I/71VzRBGHBtL.jpg', height=2000, width=2001),
    ImageVariation(variant='PT01', link='https://m.media-amazon.com/images/I/41hJcEDYdWL.jpg', height=500, width=500),
    ImageVariation(
        variant='PT01', link='https://m.media-amazon.com/images/I/41hJcEDYdWL._SL75_.jpg', height=75, width=75),
    ImageVariation(variant='PT02', link='https://m.media-amazon.com/images/I/91blEU2pflL.jpg', height=2560, width=2560),
    ImageVariation(variant='PT02', link='https://m.media-amazon.com/images/I/51-bSsN0RiL.jpg', height=500, width=500),
    ImageVariation(
        variant='PT02', link='https://m.media-amazon.com/images/I/51-bSsN0RiL._SL75_.jpg', height=75, width=75),
    ImageVariation(variant='PT03', link='https://m.media-amazon.com/images/I/81db+btNWIL.jpg', height=2560, width=2560),
    ImageVariation(variant='PT03', link='https://m.media-amazon.com/images/I/4121TmWXDTL.jpg', height=500, width=500),
    ImageVariation(
        variant='PT03', link='https://m.media-amazon.com/images/I/4121TmWXDTL._SL75_.jpg', height=75, width=75),
    ImageVariation(variant='PT04', link='https://m.media-amazon.com/images/I/81Sn3rgSL3L.jpg', height=2560, width=2560),
    ImageVariation(variant='PT04', link='https://m.media-amazon.com/images/I/41t4uhbt40L.jpg', height=500, width=500),
    ImageVariation(
        variant='PT04', link='https://m.media-amazon.com/images/I/41t4uhbt40L._SL75_.jpg', height=75, width=75),
    ImageVariation(variant='PT05', link='https://m.media-amazon.com/images/I/81HZM2xEjSL.jpg', height=2560, width=2560),
    ImageVariation(variant='PT05', link='https://m.media-amazon.com/images/I/41DmHXE64tL.jpg', height=500, width=500),
    ImageVariation(
        variant='PT05', link='https://m.media-amazon.com/images/I/41DmHXE64tL._SL75_.jpg', height=75, width=75),
    ImageVariation(variant='PT06', link='https://m.media-amazon.com/images/I/81SQc3UIzSL.jpg', height=2500, width=2500),
    ImageVariation(variant='PT06', link='https://m.media-amazon.com/images/I/51ModeeSUYL.jpg', height=500, width=500),
    ImageVariation(
        variant='PT06', link='https://m.media-amazon.com/images/I/51ModeeSUYL._SL75_.jpg', height=75, width=75)
]


class TestImagesApi(BaseTestCase):
    def setUp(self) -> None:
        self.images_api = ImagesApi()

    def test_get_images(self):
        images = self.images_api.get_images(TEST_ASIN)
        self.assertEqual(set(images), set(EXPECTED_IMAGE_VARIATION))