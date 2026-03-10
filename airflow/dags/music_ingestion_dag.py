from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Cấu hình mặc định cho DAG
default_args = {
    'owner': 'khang_data_guy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Khởi tạo DAG
with DAG(
    '01_music_ingestion_realtime', # Tên hiển thị trên Web UI
    default_args=default_args,
    description='Crawl YouTube Data for Music Trends every hour',
    schedule_interval='@hourly',   # Chạy mỗi 1 tiếng (Đúng chuẩn Smart Micro-batch)
    start_date=datetime(2024, 1, 1),
    catchup=False,                 # Không chạy bù các ngày quá khứ (chỉ chạy từ bây giờ)
    tags=['social-trend', 'music', 'ingestion'],
) as dag:

    # Task 1: In ra thông báo bắt đầu (để debug cho dễ)
    start_task = BashOperator(
        task_id='start_ingestion',
        bash_command='echo "🚀 Bắt đầu quy trình thu thập dữ liệu Music..."',
    )

    # Task 2: Chạy file Python Crawler mà chúng ta đã viết và test thành công
    # Lưu ý: File code nằm ở /opt/airflow/src/... (do chúng ta đã mount volume)
    run_crawler = BashOperator(
        task_id='crawl_youtube_data',
        bash_command='python /opt/airflow/src/ingestion/music/ingest_youtube.py',
    )

    # Thiết lập thứ tự chạy
    start_task >> run_crawler