from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import mysql.connector
from mysql.connector import errorcode

from database import config


@dataclass
class ProductRead:
    asin: str
    read_time: datetime
    image_urls: list[str]


class MySQLConnector:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def __enter__(self):
        return self.connection.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def connect_without_db(self):
        self.connection = mysql.connector.connect(
            host=os.environ['DB_HOST'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD']
        )
        self.cursor = self.connection.cursor(buffered=True)

    def connect(self):
        if not self.connection or not self.connection.is_connected():
            self.connect_without_db()
        if self.connection:
            self.connection.database = os.environ['DB_DATABASE']

    def run_query(self, query: str, with_db=True) -> list[Any]:
        results = []
        try:
            if with_db:
                self.connect()
            else:
                self.connect_without_db()
            self.cursor.execute(query)
            logging.info(f'query: {query}')
            self.connection.commit()
            if self.cursor.rowcount:
                results = self.cursor.fetchall()
        except mysql.connector.Error as error:
            self.handle_error(error, query)
            raise error
        finally:
            if self.connection.is_connected():
                self.close_connection()
            return results

    def close_connection(self):
        self.cursor.close()
        self.connection.close()

    def handle_error(self, error, query):
        logging.error(f'Failed to execute query: {query} with error: {error}')
        if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error("Something is wrong with your user name or password")
        elif error.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error(f'Database error: {error}')

    def get_asins_with_active_ab_test(self) -> list[str]:
        """
        :return: asins from database with active AB test
        """
        results = self.run_query(self.select_active_ab_test_query())
        return [result[0] for result in results]

    def select_active_ab_test_query(self) -> str:
        query = f'SELECT {config.ASIN_FIELD} FROM {config.SCHEDULE_TABLE} '
        # calculate date from start_date and start_time in sql
        query += f'WHERE NOW() BETWEEN' \
                 f' CAST(CAST({config.START_DATE_FIELD} as DATETIME) + ' \
                 f' STR_TO_DATE({config.START_TIME_FIELD}, "%H:%i:%S") AS DATETIME)' \
                 f' AND CAST(CAST({config.END_DATE_FIELD} as DATETIME) + ' \
                 f' STR_TO_DATE({config.END_TIME_FIELD}, "%H:%i:%S") as DATETIME)' \
                 f' AND {config.USER_ID_FIELD} = {os.environ["USER_ID"]}'
        return query

    def select_last_product_read_query(self, asin: str) -> str:
        query = f'SELECT {config.IMAGE_URLS_FIELD}, {config.ASIN_FIELD}, {config.READ_TIME_FIELD} FROM ' \
                f'{config.IMAGES_HISTORY_TABLE} '
        query += f'WHERE {config.ASIN_FIELD}  = "{asin}"'
        # add filter so that all images are from the latest read_time
        query += f' AND {config.READ_TIME_FIELD} >= (SELECT MAX({config.READ_TIME_FIELD}) FROM ' \
                 f'{config.IMAGES_HISTORY_TABLE} WHERE {config.ASIN_FIELD} = "{asin}") '
        query += f' AND {config.USER_ID_FIELD} = {os.environ["USER_ID"]}'
        return query

    def get_last_product_read(self, asin: str) -> ProductRead | None:
        """
        :return: last image urls from database
        :param asin: asin of the product
        """
        results = self.run_query(self.select_last_product_read_query(asin))
        if results:
            # parse image_urls from str to list
            image_urls = results[0][0].replace('[', '').replace(']', '').replace('"', '').replace(' ', '').split(',')
            last_product_read = ProductRead(asin=asin, read_time=results[0][2], image_urls=image_urls)
            return last_product_read
        return None

    def insert_product_read_query(self, product_read: ProductRead):
        image_urls_json_query = 'JSON_ARRAY(' + ','.join(
            [f'"{image_url}"' for image_url in product_read.image_urls]) + ')'
        read_time = product_read.read_time
        query = f'INSERT INTO {config.IMAGES_HISTORY_TABLE} (`{config.ASIN_FIELD}`, ' \
                f'`{config.IMAGE_URLS_FIELD}`, `{config.READ_TIME_FIELD}`, `{config.USER_ID_FIELD}`) ' \
                f'VALUES ("{product_read.asin}", {image_urls_json_query}, "{read_time}", ' \
                f'{os.environ["USER_ID"]})'
        return query

    def insert_product_read(self, product_read: ProductRead):
        self.run_query(self.insert_product_read_query(product_read))
