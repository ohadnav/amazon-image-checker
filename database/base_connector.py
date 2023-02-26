from __future__ import annotations

import logging
from abc import abstractmethod
from typing import Any

SQLQuery = str


class BaseConnector:
    def __init__(self, database: str):
        self.connection = None
        self.cursor = None
        self.database = database

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    def run_query(self, query: SQLQuery, with_db=True) -> list[Any]:
        results = []
        try:
            self.connect(with_db)
            logging.info(f'running query: {query}')
            self.cursor.execute(query)
            self.connection.commit()
            if self.should_fetch(with_db, query):
                results = self.cursor.fetchall()
        except Exception as error:
            self.handle_error(error, query)
            raise error
        finally:
            if self.is_connected():
                self.close_connection()
            logging.debug(f'got results {results} for {query}')
            return results

    def should_fetch(self, with_db: bool, query: SQLQuery) -> bool:
        return with_db and query.lower().startswith('select') and self.cursor.rowcount > 0

    @abstractmethod
    def handle_error(self, error: Exception, query: SQLQuery):
        pass

    @abstractmethod
    def create_new_connection(self, with_db: bool) -> object:
        pass

    def init_cursor(self):
        self.cursor = self.connection.cursor()

    def connect(self, with_db=True):
        if not self.is_connected():
            self.connection = self.create_new_connection(with_db)
        self.init_cursor()

    @abstractmethod
    def is_connected(self) -> bool:
        pass

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
