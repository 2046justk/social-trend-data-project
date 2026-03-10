# Sử dụng image Spark chính chủ (phiên bản mới nhất)
FROM apache/spark:3.5.3

USER root

# 1. Cài đặt các thư viện hệ thống cần thiết
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    build-essential \
    && apt-get clean

# 2. Cài đặt các thư viện Python chuyên dụng cho Data Engineer
# - delta-spark: Để làm việc với Delta Lake
# - underthesea: Thư viện NLP tiếng Việt cực xịn
# - boto3: Để làm việc với MinIO/S3
RUN pip install --no-cache-dir \
    delta-spark \
    underthesea \
    boto3 \
    pandas \
    numpy

# 3. Tải sẵn các file JAR cần thiết cho S3 và Delta Lake
RUN curl -sL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -sL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar && \
    curl -sL https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar && \
    curl -sL https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar -o /opt/spark/jars/delta-storage-3.2.0.jar

USER spark