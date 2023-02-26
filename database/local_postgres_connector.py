from __future__ import annotations

from database import config
from database.postgres_connector import PostgresConnector


class LocalPostgresConnector(PostgresConnector):
    def __init__(self):
        super(LocalPostgresConnector, self).__init__()
        self.create_test_database()
        self.create_test_tables()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.kill_all()
        super(LocalPostgresConnector, self).__exit__(exc_type, exc_val, exc_tb)

    def create_product_read_table_query(self, table_name: str) -> str:
        return f'CREATE TABLE IF NOT EXISTS {table_name} ' \
               f'({config.ASIN_FIELD} VARCHAR(10) NOT NULL, ' \
               f'{config.READ_TIME_FIELD} TIMESTAMP NOT NULL, ' \
               f'{config.IMAGE_VARIATIONS_FIELD} JSON, ' \
               f'{config.LISTING_PRICE_FIELD} FLOAT, ' \
               f'{config.USER_ID_FIELD} INT NOT NULL, ' \
               f'PRIMARY KEY ({config.ASIN_FIELD}, {config.READ_TIME_FIELD}))'

    def create_ab_test_runs_table_query(self) -> str:
        return f'CREATE TABLE IF NOT EXISTS {config.AB_TEST_RUNS_TABLE} ' \
               f'({config.RUN_ID_FIELD} SERIAL PRIMARY KEY NOT NULL, ' \
               f'{config.AB_TEST_ID_FIELD} INT NOT NULL, ' \
               f'{config.RUN_TIME_FIELD} TIMESTAMP NOT NULL, ' \
               f'{config.FEED_ID_FIELD} BIGINT, ' \
               f'{config.VARIATION_FIELD} VARCHAR(10), ' \
               f'{config.USER_ID_FIELD} INT NOT NULL)'

    def drop_test_tables(self):
        self.run_query(f'DROP TABLE IF EXISTS {config.PRODUCT_READ_HISTORY_TABLE}')
        self.run_query(f'DROP TABLE IF EXISTS {config.PRODUCT_READ_CHANGES_TABLE}')
        self.run_query(f'DROP TABLE IF EXISTS {config.AB_TEST_RUNS_TABLE}')

    def drop_test_database(self):
        drop_db_query = 'DROP DATABASE test_database;'
        self.run_query(drop_db_query, with_db=False)

    def create_test_database(self):
        create_db_query = 'CREATE DATABASE test_database;'
        self.run_query(create_db_query, with_db=False)

    def create_test_tables(self):
        self.run_query(self.create_product_read_table_query(config.PRODUCT_READ_HISTORY_TABLE))
        self.run_query(self.create_product_read_table_query(config.PRODUCT_READ_CHANGES_TABLE))
        self.run_query(self.create_ab_test_runs_table_query())

    def kill_all(self):
        self.drop_test_tables()
        self.drop_test_database()
