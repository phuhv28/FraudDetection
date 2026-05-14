# Sử dụng Python image nhẹ
FROM python:3.9-slim

# Cài đặt các thư viện hệ thống cần thiết cho confluent-kafka
RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Tạo thư mục làm việc
WORKDIR /app

# Copy file requirements và cài đặt thư viện Python
# Bạn nên tạo file requirements.txt chứa: pandas, confluent-kafka
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy mã nguồn và dữ liệu vào image
COPY producer_fraud.py .
COPY fraud_data.csv . 

# Lệnh chạy khi container khởi động
CMD ["python", "producer_fraud.py"]

