import logging
import os

from apscheduler.schedulers.blocking import BlockingScheduler

from common import util
from scheduler.ab_test_task import ABTestTask
from scheduler.product_read_changes_task import ProductReadChangesTask

scheduler = BlockingScheduler()


@scheduler.scheduled_job('interval', minutes=15)
def product_read_changes_task_scheduler():
    ProductReadChangesTask().run()


@scheduler.scheduled_job('interval', hours=int(os.environ['SCHEDULER_INTERVAL_HOURS']))
def ab_test_task_scheduler():
    ABTestTask().run()


if __name__ == '__main__':
    util.initialize_logging()
    logging.info('Starting scheduler')
    scheduler.start()
