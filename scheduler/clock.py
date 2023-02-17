import logging
import os

from apscheduler.schedulers.blocking import BlockingScheduler

from common import util
from scheduler.ab_test_task import ABTestTask
from scheduler.image_changes_task import ImageChangesTask

scheduler = BlockingScheduler()


@scheduler.scheduled_job('interval', hours=int(os.environ['SCHEDULER_INTERVAL_HOURS']), minutes=7)
def image_changes_task_scheduler():
    ImageChangesTask().run()


@scheduler.scheduled_job('interval', hours=int(os.environ['SCHEDULER_INTERVAL_HOURS']))
def ab_test_task_scheduler():
    ABTestTask().run()


if __name__ == '__main__':
    util.initialize_debug_logging()
    logging.info('Starting scheduler')
    scheduler.start()
