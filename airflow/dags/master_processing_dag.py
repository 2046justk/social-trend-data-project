from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Cấu hình mặc định cho DAG
default_args = {
    'owner': 'khang_data_guy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2), # Giảm thời gian chờ xuống 2 phút cho nhanh
}

# Khởi tạo DAG
with DAG(
    '01_master_processing_pipeline', 
    default_args=default_args,
    description='Automated Data Processing for Music & Travel',
    schedule_interval='@daily',
    start_date=datetime(2024, 3, 5),
    catchup=False,
    tags=['social-trend', 'processing', 'lakehouse'],
) as dag:

    start_pipeline = BashOperator(
        task_id='start_pipeline',
        bash_command='echo "🚀 Bắt đầu chuỗi xử lý Data Lakehouse..."',
    )

    # 💡 SENIOR TRICK: Dùng docker exec thay vì SparkSubmitOperator
    # Lưu ý: Vì Airflow chạy trong Docker, nó không thể gọi "docker exec" trực tiếp dễ dàng.
    # Thay vào đó, ta gọi trực tiếp script spark-submit TẠI CHỖ, nhưng ép nó chạy ở chế độ LOCAL
    # để không cần thông qua Spark Master phức tạp (dữ liệu của chúng ta hiện tại đủ nhỏ để chạy Local)

    process_travel = BashOperator(
        task_id='process_travel_data',
        bash_command='''
        export SPARK_HOME=/home/airflow/.local/lib/python3.12/site-packages/pyspark &&
        python /opt/airflow/src/processing/travel/process_reviews_spark.py
        '''
    )

    process_music = BashOperator(
        task_id='process_music_lifecycle',
        bash_command='''
        export SPARK_HOME=/home/airflow/.local/lib/python3.12/site-packages/pyspark &&
        python /opt/airflow/src/processing/music/process_music_spark.py
        '''
    )

    end_pipeline = BashOperator(
        task_id='end_pipeline',
        bash_command='echo "✅ Hoàn tất toàn bộ chuỗi xử lý!"',
    )

    # Thiết lập luồng chạy
    start_pipeline >> process_travel >> process_music >> end_pipeline