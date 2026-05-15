import pandas as pd
import json
import time
from confluent_kafka import Producer

# Cấu hình Kafka
conf = {
    'bootstrap.servers': 'kafka.default.svc.cluster.local:29092',
    'client.id': 'fraud-csv-producer',
    'acks': '1' # Gửi nhanh, không cần đợi xác nhận từ tất cả broker
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Gửi lỗi: {err}")
    else:
        # Giải mã key để log ra màn hình
        print(f"✅ Đã gửi giao dịch của thẻ: {msg.key().decode('utf-8')}")

def stream_csv(file_path):
    print(f"🚀 Bắt đầu đẩy dữ liệu vào Kafka theo từng chunk...")

    # Đọc mỗi lần 1000 dòng
    chunk_size = 1000
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        for index, row in chunk.iterrows():
            payload = row.to_dict()
            key_value = str(payload['cc_num'])

            producer.produce(
                topic='transaction',
                key=key_value,
                value=json.dumps(payload).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)
            time.sleep(1)

        # Flush sau mỗi chunk để giải phóng bộ nhớ đệm của Kafka Producer
        producer.flush()
        print(f"✅ Đã xử lý xong một cụm {chunk_size} dòng.")

    print("🏁 Hoàn thành.")

if __name__ == "__main__":
    while True: # Thêm dòng này để chạy liên tục
        stream_csv('fraud_data.csv')
        print("🔄 Đã hết file, bắt đầu lại từ đầu...")
        time.sleep(5) # Nghỉ 5s trước khi lặp lại

