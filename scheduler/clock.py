import logging
import os

from apscheduler.schedulers.blocking import BlockingScheduler

from common import util
from scheduler.image_changes_task import ImageChangesTask

scheduler = BlockingScheduler()


@scheduler.scheduled_job('interval', hours=int(os.environ['SCHEDULER_INTERVAL_HOURS']))
def image_changes_task_scheduler():
    ImageChangesTask().run()


if __name__ == '__main__':
    util.initialize_debug_logging()
    logging.info(f'Starting scheduler {os.path.basename(__file__)}')
    scheduler.start()
