'''
calculate whether the last image url from the database is different from the current image url
'''
import json
import sys

import requests

from common import env_var
from database.connector import ProductReadDiff


def notify(product_read_diff: ProductReadDiff):
    message = create_message_for_changed_image(product_read_diff)
    send_message_to_slack(message)


def create_message_for_changed_image(product_read_diff: ProductReadDiff) -> str:
    asin_url = f'https://www.amazon.com/dp/{product_read_diff.asin}'
    variations_with_diff = set([image_variation.variant for image_variation in product_read_diff.image_variations])
    change_message = f'Image of {asin_url} has changed in variations {", ".join(variations_with_diff)}'
    return change_message


def send_message_to_slack(message: str):
    webhook_url = env_var.get('SLACK_WEBHOOK_URL')
    slack_data = {'text': message}
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    response = requests.post(webhook_url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        print(f'{response.status_code} - {response.text}')
