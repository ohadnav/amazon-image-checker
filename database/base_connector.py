from __future__ import annotations

import logging
from abc import abstractmethod
from typing import Any

SQLQuery = str


class BaseConnector:
    @abstractmethod
    def run_query(self, query: SQLQuery) -> list[Any]:
        pass

    def handle_error(self, error, query):
        logging.error(f'Failed to execute query: {query} with error: {error}')
