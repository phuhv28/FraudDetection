# Sử dụng Flink Image chính thức (đã có Java 11)
FROM flink:1.20-java11

# 1. Cài đặt Python 3.12 và các công cụ build
RUN apt-get update -y && \
    apt-get install -y python3.12 python3.12-dev python3-pip && \
    ln -s /usr/bin/python3.12 /usr/bin/python && \
    ln -s /usr/bin/pip3 /usr/bin/pip

# 2. Tạo thư mục ứng dụng
WORKDIR /app

# 3. Copy source code và model
COPY . /app

# 4. Cài đặt các thư viện Python
RUN pip install --no-cache-dir -r requirements.txt

# 5. Cấp quyền cho user flink (để tránh lỗi permission trên K8s)
RUN chown -R flink:flink /app

USER flink