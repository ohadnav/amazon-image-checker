from __future__ import annotations

import logging
import os

import psycopg2
from psycopg2.extras import RealDictCursor

from database import config
from database.base_connector import BaseConnector, SQLQuery


class PostgresConnector(BaseConnector):
    def create_new_connection(self) -> object:
        pg_connect = psycopg2.connect(os.environ['DATABASE_URL'])
        pg_connect.set_session(autocommit=True)
        return pg_connect

    def init_cursor(self):
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)

    def is_connected(self) -> bool:
        return self.connection is not None and not self.connection.closed

    def handle_error(self, error: psycopg2.Error, query: SQLQuery):
        logging.error(f'ERROR {error.pgcode} {error}: {query}')

    def create_product_read_table_query(self, table_name: str) -> str:
        return f'CREATE TABLE IF NOT EXISTS {table_name} ' \
               f'({config.ASIN_FIELD} TEXT NOT NULL, ' \
               f'{config.READ_TIME_FIELD} TIMESTAMP NOT NULL, ' \
               f'{config.IMAGE_VARIATIONS_FIELD} JSON, ' \
               f'{config.LISTING_PRICE_FIELD} FLOAT, ' \
               f'{config.MERCHANT_FIELD} TEXT NOT NULL, ' \
               f'PRIMARY KEY ({config.ASIN_FIELD}, {config.READ_TIME_FIELD}))'

    def create_ab_test_runs_table_query(self) -> str:
        return f'CREATE TABLE IF NOT EXISTS {config.AB_TEST_RUNS_TABLE} ' \
               f'({config.RUN_ID_FIELD} SERIAL PRIMARY KEY NOT NULL, ' \
               f'{config.AB_TEST_ID_FIELD} INT NOT NULL, ' \
               f'{config.RUN_TIME_FIELD} TIMESTAMP NOT NULL, ' \
               f'{config.FEED_ID_FIELD} BIGINT, ' \
               f'{config.VARIATION_FIELD} TEXT, ' \
               f'{config.MERCHANT_FIELD} TEXT NOT NULL)'

    def create_merchants_table_query(self) -> str:
        return f'CREATE TABLE IF NOT EXISTS {config.MERCHANTS_TABLE} ' \
               f'({config.MERCHANT_ID_FIELD} SERIAL PRIMARY KEY NOT NULL, ' \
               f'{config.LWA_CLIENT_SECRET_FIELD} TEXT NOT NULL, ' \
               f'{config.LWA_APP_ID_FIELD} TEXT NOT NULL, ' \
               f'{config.MERCHANT_FIELD} TEXT)'
