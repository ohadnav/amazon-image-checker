'''
calculate whether the last image url from the database is different from the current image url
'''
import json
import os
import sys

import requests


def notify(asin: str):
    message = create_message_for_changed_image(asin)
    send_message_to_slack(message)


def create_message_for_changed_image(asin: str):
    asin_url = f'https://www.amazon.com/dp/{asin}'
    change_message = f'Image of {asin_url} has changed'
    return change_message


def send_message_to_slack(message: str):
    webhook_url = os.environ['SLACK_WEBHOOK_URL']
    slack_data = {'text': message}
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    response = requests.post(webhook_url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        print(f'{response.status_code} - {response.text}')
