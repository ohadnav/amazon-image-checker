from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, List

import mysql.connector
from mysql.connector import errorcode

from amazon_sp_api.images_api import ImageVariation
from database import config


@dataclass
class ProductRead:
    asin: str
    read_time: datetime
    image_variations: List[ImageVariation]


@dataclass
class ProductReadDiff(ProductRead):
    def __init__(self, current: ProductRead, last: ProductRead | None = None):
        if last:
            assert current.asin == last.asin and current.read_time > last.read_time
        self.asin = current.asin
        self.read_time = current.read_time
        self.image_variations = self._calculate_variants_with_diff(current, last) if last else current.image_variations

    def _calculate_variants_with_diff(self, current: ProductRead, last: ProductRead) -> List[ImageVariation]:
        return [variant for variant in current.image_variations if variant not in last.image_variations]


class MySQLConnector:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    def connect_without_db(self):
        self.connection = mysql.connector.connect(
            host=os.environ['DB_HOST'],
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD']
        )
        self.cursor = self.connection.cursor(buffered=True, dictionary=True)

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
            logging.info(f'running query: {query}')
            self.connection.commit()
            if self.cursor.rowcount:
                results = self.cursor.fetchall()
        except mysql.connector.Error as error:
            self.handle_error(error, query)
            raise error
        finally:
            if self.connection.is_connected():
                self.close_connection()
            logging.debug(f'got results {results} for {query}')
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
        return [result[config.ASIN_FIELD] for result in results]

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

    def _select_last_product_read_query(self, asin: str, table_name: str) -> str:
        query = f'SELECT {config.IMAGE_VARIATIONS_FIELD}, {config.ASIN_FIELD}, {config.READ_TIME_FIELD} FROM ' \
                f'{table_name} '
        query += f'WHERE {config.ASIN_FIELD}  = "{asin}"'
        query += f' AND {config.USER_ID_FIELD} = {os.environ["USER_ID"]}'
        query += f' ORDER BY {config.READ_TIME_FIELD} DESC LIMIT 1'
        return query

    def get_last_product_read(self, asin: str, table_name: str = None) -> ProductRead | None:
        table_name = table_name if table_name else config.IMAGES_HISTORY_TABLE
        results = self.run_query(self._select_last_product_read_query(asin, table_name))
        if results:
            last_product_read = self._parse_query_result_row_to_product_read(results[0])
            return last_product_read
        return None

    def _parse_query_result_row_to_product_read(self, result_row: dict) -> ProductRead:
        image_variations = json.loads(result_row[config.IMAGE_VARIATIONS_FIELD])
        image_variations_list = [ImageVariation(**image_variation) for image_variation in image_variations]
        last_product_read = ProductRead(
            asin=result_row[config.ASIN_FIELD], read_time=result_row[config.READ_TIME_FIELD],
            image_variations=image_variations_list)
        return last_product_read

    def _insert_product_read_query(self, product_read: ProductRead, table_name: str):
        # convert image_variations from list to str matching the format in the database
        image_variations_json = self._prepare_json_to_sql_insert_query(product_read)
        query = f'INSERT INTO `{table_name}` (`{config.ASIN_FIELD}`, ' \
                f'`{config.IMAGE_VARIATIONS_FIELD}`, `{config.READ_TIME_FIELD}`, `{config.USER_ID_FIELD}`) ' \
                f'VALUES ("{product_read.asin}", "{image_variations_json}", "{product_read.read_time}", ' \
                f'{os.environ["USER_ID"]})'
        # write query for inserting image_variations to mysql database
        return query

    def _prepare_json_to_sql_insert_query(self, product_read: ProductRead) -> str:
        return str(asdict(product_read)['image_variations']).replace("'", '\\"')

    def insert_product_read(self, product_read: ProductRead, table_name: str = None):
        table_name = table_name or config.IMAGES_HISTORY_TABLE
        self.run_query(self._insert_product_read_query(product_read, table_name))

    def insert_images_changes(self, product_read_diff: ProductReadDiff):
        self.insert_product_read(product_read_diff, config.IMAGES_CHANGES_TABLE)

    def get_last_images_changes(self, asin: str) -> ProductReadDiff | None:
        product_read_maybe = self.get_last_product_read(asin, config.IMAGES_CHANGES_TABLE)
        if product_read_maybe:
            return ProductReadDiff(product_read_maybe)
        return None
