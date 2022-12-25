import logging
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from common import util
from image_changes import image_changes_task

scheduler = BlockingScheduler()

@scheduler.scheduled_job('interval', hours=1)
def hourly_image_changes_task():
    util.initialize_debug_logging()
    logging.debug('Running hourly image changes task time is {}'.format(datetime.now()))
    image_changes_task.product_images_task()


if __name__ == '__main__':
    util.initialize_debug_logging()
    logging.info('Starting scheduler')
    scheduler.start()
