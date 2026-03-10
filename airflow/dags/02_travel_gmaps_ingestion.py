from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'khang_data_guy',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2, # Nếu rớt mạng hoặc Google chặn, thử lại 2 lần
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    '02_travel_gmaps_ingestion',
    default_args=default_args,
    description='Automated Selenium Crawler for Google Maps (Travel Module)',
    schedule_interval='0 8,14 * * *', # Chạy 2 lần 1 ngày (8h sáng, 2h chiều) như bạn mong muốn
    start_date=datetime(2024, 3, 1),
    catchup=False,
    tags=['social-trend', 'travel', 'selenium', 'ingestion'],
) as dag:

    # Task chạy script Python có chứa Selenium
    run_gmaps_crawler = BashOperator(
        task_id='run_selenium_gmaps_crawler',
        bash_command='python /opt/airflow/src/ingestion/travel/crawl_google_maps.py',
    )

    run_gmaps_crawler