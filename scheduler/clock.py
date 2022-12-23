from apscheduler.schedulers.blocking import BlockingScheduler

from image_changes import image_changes_task

scheduler = BlockingScheduler()

@scheduler.scheduled_job('interval', hours=1)
def hourly_image_changes_task():
    image_changes_task.product_images_task()

scheduler.start()