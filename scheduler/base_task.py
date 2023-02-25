import logging
from datetime import datetime

from airtable.reader import AirtableReader
from amazon_sp_api.amazon_api import AmazonApi
from database.database_api import DatabaseApi
from database.mysql_connector import MySQLConnector


class BaseTask:
    def __init__(self):
        self.database_api = DatabaseApi(MySQLConnector())
        self.airtable_reader = AirtableReader()
        self.amazon_api = AmazonApi()

    def task(self):
        pass

    def run(self):
        logging.info(f'Running {self.__class__.__name__}  time is {datetime.now()}')
        self.task()
