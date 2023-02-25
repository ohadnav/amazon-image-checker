from __future__ import annotations

import logging
import os
from typing import Any

import mysql.connector
from mysql.connector import errorcode

from database.base_connector import BaseConnector

SQLQuery = str


class MySQLConnector(BaseConnector):
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
        super(MySQLConnector, self).handle_error(error, query)
        if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error("Something is wrong with your user name or password")
        elif error.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error(f'Database error: {error}')
