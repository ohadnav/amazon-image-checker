'''
calculate whether the last image url from the database is different from the current image url
'''
import json
import logging
import os
import sys

import requests

from amazon_sp_api.amazon_util import AmazonUtil
from database.connector import ProductReadDiff


def notify(product_read_diff: ProductReadDiff):
    message = create_message_for_product_read_changes(product_read_diff)
    logging.debug(f'Notifying for asin {product_read_diff.asin}: {message}')
    send_message_to_slack(message)


def create_message_for_product_read_changes(product_read_diff: ProductReadDiff) -> str:
    asin_url = AmazonUtil.get_url_from_asin(product_read_diff.asin)
    change_message = f'Changes in {asin_url}:'
    if product_read_diff.image_variations:
        variations_with_diff = set([image_variation.variant for image_variation in product_read_diff.image_variations])
        change_message += f' changed image variations {", ".join(variations_with_diff)}'
    if product_read_diff.listing_price:
        change_message += f' listing price changed to {product_read_diff.listing_price}'
    return change_message


def send_message_to_slack(message: str):
    webhook_url = os.environ['SLACK_WEBHOOK_URL']
    slack_data = {'text': message}
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    response = requests.post(webhook_url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        logging.error(f'{response.status_code} - {response.text}')
