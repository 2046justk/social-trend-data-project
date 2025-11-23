# Sử dụng image gốc
FROM apache/superset:latest

# Chuyển sang quyền root để cài thư viện hệ thống
USER root

# 1. Cài đặt các công cụ biên dịch và thư viện hệ thống
RUN apt-get update && \
    apt-get install -y \
        build-essential \
        gcc \
        g++ \
        libsasl2-dev \
        libsasl2-modules \
        libssl-dev && \
    apt-get clean

# 2. Cài đặt Driver vào môi trường Python hiện hành
# Lưu ý: Dùng 'pip' trực tiếp, không cần đường dẫn tuyệt đối
RUN pip install --no-cache-dir \
    thrift==0.16.0 \
    sasl \
    thrift_sasl \
    pyhive \
    impyla

# Chuyển lại quyền user superset
USER superset