import logging
import os
from dataclasses import dataclass
from datetime import datetime

import mysql.connector

import config


@dataclass
class ProductRead:
    asin: str
    read_time: datetime
    image_urls: list[str]


'''
Connect to mysql database with credentials from environment variables
'''


def connect():
    return mysql.connector.connect(
        host=os.environ['DB_HOST'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        database=os.environ['DB_DATABASE']
    )


'''
get asin from database with active AB test
'''


def get_asins_with_active_ab_test():
    connection = connect()
    cursor = connection.cursor()
    query = select_active_ab_test_query()
    logging.info(f'query: {query}')
    cursor.execute(query)
    asins = cursor.fetchall()
    cursor.close()
    connection.close()
    return asins


def select_active_ab_test_query():
    query = f'SELECT asin FROM {config.SCHEDULE_TABLE}'
    # calculate date from start_date and start_time in sql
    query += 'WHERE cast(@start_date as datetime) + cast(@start_time as datetime) <= CURDATE()'
    query += 'AND cast(@end_date as datetime) + cast(@end_time as datetime) >= CURDATE()'
    query += f'AND user_id = {os.environ["USER_ID"]}'
    return query


def select_last_product_read_query(asin):
    query = f'SELECT image_url, asin, MAX(read_time) FROM {config.IMAGES_HISTORY_TABLE}'
    query += f'WHERE asin  = {asin}'
    # add filter so that all images are from the latest read_time
    query += ' AND read_time >= (SELECT MAX(read_time) FROM {config.IMAGES_HISTORY_TABLE} WHERE asin = {asin})'
    query += f' AND user_id = {os.environ["USER_ID"]}'
    return query


'''
read the last image url from database
'''


def get_last_product_read(asin: str) -> ProductRead:
    connection = connect()
    cursor = connection.cursor()
    query = select_last_product_read_query(asin)
    logging.info(f'query: {query}')
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    image_urls = results['image_url'].tolist()
    last_product_read = ProductRead(asin=asin, read_time=results[0][2], image_urls=image_urls)
    return last_product_read


def insert_product_read_query(product_read: ProductRead):
    query = f'INSERT INTO {config.IMAGES_HISTORY_TABLE} (asin, image_url, read_time, user_id)'
    query += 'VALUES '
    for image_url in product_read.image_urls:
        query += f'({product_read.asin}, {image_url}, {product_read.read_time}, {os.environ["USER_ID"]}),'
    return query[:-1]


def insert_product_read(product_read: ProductRead):
    connection = connect()
    cursor = connection.cursor()
    query = insert_product_read_query(product_read)
    logging.info(f'query: {query}')
    cursor.execute(query)
    connection.commit()
    cursor.close()
    connection.close()
