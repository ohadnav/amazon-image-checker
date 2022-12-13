from datetime import timedelta

from airflow.utils.dates import days_ago

default_args = {
    'owner': 'ohad',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['ohadnav@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PRODUCT_DAG_ID = 'product_dag'
PRODUCT_DAG_TAG = 'product'
READ_PRODUCT_IMAGES_TASK_ID = 'read_product_images'
