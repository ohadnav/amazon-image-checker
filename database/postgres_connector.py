from __future__ import annotations

import logging
import os

import psycopg2
from psycopg2.extras import RealDictCursor

from database.base_connector import BaseConnector, SQLQuery


class PostgresConnector(BaseConnector):
    def __init__(self):
        super(PostgresConnector, self).__init__(os.environ['POSTGRES_DATABASE'])

    def create_new_connection(self, with_db: bool) -> object:
        pg_connect = psycopg2.connect(host=os.environ['POSTGRES_HOST'], user=os.environ['POSTGRES_USER'],
                                      password=os.environ['POSTGRES_PASSWORD'],
                                      database=os.environ['POSTGRES_DATABASE'] if with_db else None)
        pg_connect.set_session(autocommit=True)
        return pg_connect

    def init_cursor(self):
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)

    def is_connected(self) -> bool:
        return self.connection is not None and not self.connection.closed

    def handle_error(self, error: psycopg2.Error, query: SQLQuery):
        logging.error(f'ERROR {error.pgcode} {error}: {query}')
