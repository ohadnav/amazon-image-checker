from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from image_changes import image_changes_task

scheduler = BlockingScheduler()

@scheduler.scheduled_job('interval', hours=1)
def hourly_image_changes_task():
    print('Running hourly image changes task time is {}'.format(datetime.now()))
    image_changes_task.product_images_task()

scheduler.start()