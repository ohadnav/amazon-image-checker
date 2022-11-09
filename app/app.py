from crontab import CronTab


def main():
    cron = CronTab(user='root')
    job = cron.new(command='python3 /app/scheduler.py')
    job.hour.every(1)


if __name__ == '__main__':
    main()
