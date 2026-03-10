FROM apache/airflow:2.10.5

# Cài đặt Java với quyền Root
USER root

# 1. Cài đặt các gói hệ thống (System Dependencies)
# CẬP NHẬT: Thêm chromium và chromium-driver để chạy Selenium
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    openjdk-17-jdk-headless \
    chromium \
    chromium-driver \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 2. Chuyển sang user airflow
USER airflow

# 3. [QUAN TRỌNG NHẤT] Thiết lập cứng đường dẫn PATH
# Dòng này đảm bảo hệ thống LUÔN tìm thấy lệnh airflow trong .local/bin
ENV PATH="/home/airflow/.local/bin:${PATH}"

# 4. Cài đặt thư viện Python
RUN pip install --no-cache-dir "apache-airflow==2.10.5" \
    "apache-airflow-providers-openlineage>=1.8.0" \
    "apache-airflow-providers-apache-spark" \
    "apache-airflow-providers-amazon" \
    "apache-airflow-providers-postgres" \
    "pyspark==3.5.3" \
    "minio" \
    "pandas" \
    "numpy" \
    "requests" \
    "beautifulsoup4" \
    "selenium" \
    "webdriver-manager"