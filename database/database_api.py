from __future__ import annotations

import copy
import json
import logging
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, List, Optional

import mysql.connector
from mysql.connector import errorcode

import airtable.config
from airtable.reader import ABTestRecord, AirtableReader
from amazon_sp_api.amazon_api import ImageVariation
from database import config
from database.mysql_connector import MySQLConnector, SQLQuery


@dataclass
class ProductRead:
    asin: str
    read_time: datetime
    image_variations: List[ImageVariation]
    listing_price: Optional[float]


@dataclass
class ABTestRun:
    test_id: int
    run_time: datetime
    variation: str
    run_id: int = None
    feed_id: int = None

    @staticmethod
    def from_record(ab_test_record: ABTestRecord):
        return ABTestRun(
            test_id=ab_test_record.fields[airtable.config.TEST_ID_FIELD], run_time=datetime.now(),
            variation=AirtableReader.current_variation(ab_test_record))


@dataclass
class ProductReadDiff(ProductRead):
    def __init__(self, current: ProductRead, last: ProductRead | None = None):
        if last:
            assert current.asin == last.asin and current.read_time > last.read_time
        self.asin = current.asin
        self.read_time = current.read_time
        self.image_variations = ProductReadDiff._calculate_variants_with_diff(
            current, last) if last else current.image_variations
        self.listing_price = current.listing_price if not last or current.listing_price != \
                                                      last.listing_price else None

    @staticmethod
    def _calculate_variants_with_diff(current: ProductRead, last: ProductRead) -> List[ImageVariation]:
        return [variant for variant in current.image_variations if variant not in last.image_variations]

    def has_diff(self) -> bool:
        return bool(self.image_variations) or bool(self.listing_price)


class DatabaseApi:
    def __init__(self):
        self.connector = MySQLConnector()
        
    def _last_product_read_query(self, asin: str, table_name: str) -> SQLQuery:
        query = f'SELECT {config.IMAGE_VARIATIONS_FIELD}, {config.ASIN_FIELD}, {config.READ_TIME_FIELD}, ' \
                f'{config.LISTING_PRICE_FIELD} ' \
                f'FROM {table_name} ' \
                f'WHERE {config.ASIN_FIELD}  = "{asin}" AND {config.USER_ID_FIELD} = {os.environ["USER_ID"]} ' \
                f'ORDER BY {config.READ_TIME_FIELD} DESC LIMIT 1'
        return query

    def get_last_product_read(self, asin: str, table_name: str = None) -> ProductRead | None:
        table_name = table_name if table_name else config.PRODUCT_READ_HISTORY_TABLE
        results = self.connector.run_query(self._last_product_read_query(asin, table_name))
        if results:
            last_product_read = self._parse_query_result_row_to_product_read(results[0])
            return last_product_read
        return None

    def _parse_query_result_row_to_product_read(self, result_row: dict) -> ProductRead:
        image_variations = json.loads(result_row[config.IMAGE_VARIATIONS_FIELD])
        image_variations_list = [ImageVariation(**image_variation) for image_variation in image_variations]
        product_read = ProductRead(
            asin=result_row[config.ASIN_FIELD], read_time=result_row[config.READ_TIME_FIELD],
            image_variations=image_variations_list, listing_price=result_row[config.LISTING_PRICE_FIELD])
        return product_read

    def _insert_product_read_query(self, product_read: ProductRead, table_name: str):
        # convert image_variations from list to str matching the format in the database
        image_variations_json = self._prepare_json_to_sql_insert_query(product_read)
        query = f'INSERT INTO `{table_name}` ' \
                f'(`{config.ASIN_FIELD}`, `{config.IMAGE_VARIATIONS_FIELD}`, `{config.READ_TIME_FIELD}`, ' \
                f'`{config.LISTING_PRICE_FIELD}`, `{config.USER_ID_FIELD}`) ' \
                f'VALUES ("{product_read.asin}", "{image_variations_json}", "{product_read.read_time}", ' \
                f'{product_read.listing_price}, {os.environ["USER_ID"]})'
        return query

    def _prepare_json_to_sql_insert_query(self, product_read: ProductRead) -> SQLQuery:
        return str(asdict(product_read)['image_variations']).replace("'", '\\"')

    def insert_product_read(self, product_read: ProductRead, table_name: str = None):
        table_name = table_name or config.PRODUCT_READ_HISTORY_TABLE
        self.connector.run_query(self._insert_product_read_query(product_read, table_name))

    def insert_product_read_changes(self, product_read_diff: ProductReadDiff):
        assert product_read_diff.has_diff()
        self.insert_product_read(product_read_diff, config.PRODUCT_READ_CHANGES_TABLE)

    def get_last_product_read_changes(self, asin: str) -> ProductReadDiff | None:
        product_read_maybe = self.get_last_product_read(asin, config.PRODUCT_READ_CHANGES_TABLE)
        if product_read_maybe:
            return ProductReadDiff(product_read_maybe)
        return None

    def _insert_ab_test_run_query(self, ab_test_run: ABTestRun) -> SQLQuery:
        query = f'INSERT INTO `{config.AB_TEST_RUNS_TABLE}` ' \
                f'(`{config.AB_TEST_ID_FIELD}`, `{config.RUN_TIME_FIELD}`, `{config.VARIATION_FIELD}`, ' \
                f'`{config.USER_ID_FIELD}`) ' \
                f'VALUES ("{ab_test_run.test_id}", "{ab_test_run.run_time}", "{ab_test_run.variation}", ' \
                f'{os.environ["USER_ID"]})'
        return query

    def _get_inserted_ab_test_run_id_query(self, ab_test_run: ABTestRun) -> SQLQuery:
        query = f'SELECT `{config.RUN_ID_FIELD}` FROM `{config.AB_TEST_RUNS_TABLE}` ' \
                f'WHERE {config.AB_TEST_ID_FIELD} = "{ab_test_run.test_id}" ' \
                f'ORDER BY {config.RUN_ID_FIELD} DESC LIMIT 1 '
        return query

    def insert_ab_test_run(self, ab_test_run: ABTestRun) -> ABTestRun:
        self.connector.run_query(self._insert_ab_test_run_query(ab_test_run))
        inserted_run_id = self.connector.run_query(self._get_inserted_ab_test_run_id_query(ab_test_run))[0][config.RUN_ID_FIELD]
        inserted_ab_test_run = copy.deepcopy(ab_test_run)
        inserted_ab_test_run.run_id = inserted_run_id
        return inserted_ab_test_run

    def _last_run_for_ab_test_query(self, ab_test_record: ABTestRecord) -> SQLQuery:
        query = f'SELECT {config.AB_TEST_ID_FIELD}, {config.AB_TEST_ID_FIELD}, {config.RUN_TIME_FIELD},' \
                f' {config.VARIATION_FIELD}, {config.FEED_ID_FIELD}, {config.RUN_ID_FIELD} ' \
                f'FROM {config.AB_TEST_RUNS_TABLE} ' \
                f'WHERE {config.AB_TEST_ID_FIELD}  = "{ab_test_record.fields[airtable.config.TEST_ID_FIELD]}"' \
                f' AND {config.USER_ID_FIELD} = {os.environ["USER_ID"]} ' \
                f'ORDER BY {config.RUN_ID_FIELD} DESC LIMIT 1'
        return query

    def _parse_query_result_row_to_ab_test_run(self, result_row: dict) -> ABTestRun:
        ab_test_run = ABTestRun(
            test_id=result_row[config.AB_TEST_ID_FIELD], run_time=result_row[config.RUN_TIME_FIELD],
            variation=result_row[config.VARIATION_FIELD], run_id=result_row[config.RUN_ID_FIELD],
            feed_id=result_row[config.FEED_ID_FIELD])
        return ab_test_run

    def get_last_run_for_ab_test(self, ab_test_record: ABTestRecord) -> ABTestRun | None:
        results = self.connector.run_query(self._last_run_for_ab_test_query(ab_test_record))
        if results:
            last_ab_test_run = self._parse_query_result_row_to_ab_test_run(results[0])
            return last_ab_test_run
        return None

    def _update_feed_id_query(self, ab_test_run: ABTestRun) -> SQLQuery:
        query = f'UPDATE {config.AB_TEST_RUNS_TABLE} ' \
                f'SET {config.FEED_ID_FIELD} = "{ab_test_run.feed_id}" ' \
                f'WHERE {config.AB_TEST_ID_FIELD} = "{ab_test_run.test_id}" ' \
                f'ORDER BY {config.RUN_ID_FIELD} DESC LIMIT 1'
        return query

    def update_feed_id(self, ab_test_run: ABTestRun):
        self.connector.run_query(self._update_feed_id_query(ab_test_run))
