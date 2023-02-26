from __future__ import annotations

import logging
import os

from mysql import connector
from mysql.connector import MySQLConnection

from database.base_connector import BaseConnector, SQLQuery


class MySQLConnector(BaseConnector):
    def __init__(self):
        super(MySQLConnector, self).__init__(os.environ['MYSQL_DATABASE'])

    def create_new_connection(self, with_db: bool) -> MySQLConnection:
        mysql_connector_connect = connector.connect(host=os.environ['MYSQL_HOST'], user=os.environ['MYSQL_USER'],
                                                    password=os.environ['MYSQL_PASSWORD'])
        if with_db:
            mysql_connector_connect.database = self.database
        return mysql_connector_connect

    def init_cursor(self):
        self.cursor = self.connection.cursor(buffered=True, dictionary=True)

    def handle_error(self, error: connector.Error, query: SQLQuery):
        logging.error(f'Failed to execute query: {query} ERROR {error.errno}: {error}')

    def is_connected(self) -> bool:
        return self.connection and self.connection.is_connected()
