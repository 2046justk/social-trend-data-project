from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Cấu hình mặc định cho DAG
default_args = {
    'owner': 'khang_data_guy',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Khởi tạo DAG
with DAG(
    '03_mock_social_ingestion',
    default_args=default_args,
    description='Smart Mock Ingestion for Facebook and TikTok (All Modules)',
    schedule_interval='@hourly', # Chạy mỗi 1 tiếng để update Buzz
    start_date=datetime(2024, 3, 1),
    catchup=False,
    tags=['social-trend', 'mock', 'facebook', 'tiktok', 'ingestion'],
) as dag:

    # Task 1: In thông báo
    start_mock_ingestion = BashOperator(
        task_id='start_mock_ingestion',
        bash_command='echo "🚀 Bắt đầu sinh dữ liệu FB/TikTok cho toàn bộ các Passion Points..."',
    )

    # Task 2: Chạy script Python Smart Mock
    run_smart_mock = BashOperator(
        task_id='run_smart_mock_script',
        bash_command='python /opt/airflow/src/ingestion/mock_social_ingestion.py',
    )

    # Thiết lập luồng chạy
    start_mock_ingestion >> run_smart_mock