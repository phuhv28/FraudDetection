import pandas as pd
import json
import time
from confluent_kafka import Producer

# Cấu hình Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'fraud-csv-producer',
    'acks': '1'
}

producer = Producer(conf)

COLUMNS = [
    "trans_date_trans_time",
    "cc_num",
    "merchant",
    "category",
    "amt",
    "trans_num",
    "merch_lat",
    "merch_long"
]

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Gửi lỗi: {err}")
    else:
        print(f"✅ Đã gửi giao dịch của thẻ: {msg.key().decode('utf-8')}")

def stream_csv(file_path):
    print("🚀 Bắt đầu đẩy dữ liệu vào Kafka...")

    chunk_size = 1000

    for chunk in pd.read_csv(file_path, chunksize=chunk_size, usecols=COLUMNS):

        for _, row in chunk.iterrows():
            payload = row.to_dict()

            key_value = str(payload["cc_num"])

            producer.produce(
                topic="transaction",
                key=key_value,
                value=json.dumps(payload).encode("utf-8"),
                callback=delivery_report
            )

            producer.poll(0)
            time.sleep(1)

        producer.flush()
        print(f"✅ Xong chunk {chunk_size} dòng")

    print("🏁 Hoàn thành")

if __name__ == "__main__":
    stream_csv("fraud_data.csv")