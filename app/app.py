from crontab import CronTab


def main():
    cron = CronTab(user='root')
    job = cron.new(command='python3 /image_changes/image_changes_task.py')
    job.hour.every(1)
    cron.write()


if __name__ == '__main__':
    main()
