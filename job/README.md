# 🚀 Hướng dẫn Cấu hình Kafka và Task Manager

### Pre-requisite
* File .jar đã package
* Đã docker compose up thành công

### 1. Khởi tạo Kafka Topic

Sử dụng lệnh sau để tạo topic `transactions` bên trong container Kafka:

```bash
docker exec kafka-kraft kafka-topics \
  --create \
  --topic transactions \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

```

### 2. Cài đặt Thư viện cho Task Manager

Truy cập vào terminal của Task Manager và cài đặt thư viện `libgomp1` để hỗ trợ các tính năng tính toán song song:

```bash
apt-get update && apt-get install -y --no-install-recommends libgomp1

```

### 3. Cấu hình Job

Thiết lập các thông số sau khi chạy job:

* **Entry Class:** `[Tên Entry Class của bạn]`
* **Program Arguments:** `--bootstrap.servers kafka:29092`

### 4. Kiểm tra luồng dữ liệu (Testing)

Sử dụng Kafka Console Producer để đẩy thử một dữ liệu mẫu (event) vào hệ thống:

```bash
docker exec -it kafka-kraft kafka-console-producer \
  --topic transactions \
  --bootstrap-server localhost:9092

```

**Dữ liệu mẫu (JSON):**
Copy và dán nội dung sau vào terminal sau khi chạy lệnh trên:

```json
{
  "trans_date_trans_time": "2020-06-26 23:18:46",
  "dob": "1982-02-08",
  "amt": 949.88,
  "lat": 41.55,
  "long": -87.4569,
  "merch_lat": 41.618135,
  "merch_long": -87.55474699999999,
  "category": "shopping_net",
  "gender": "M",
  "state": "IN",
  "city_pop": 23727,
  "trans_count_24h": 1,
  "amt_sum_24h": 949.88,
  "trans_count_7d": 1,
  "amt_sum_7d": 949.88
}

```

### 5. Xác nhận kết quả

Kết quả xử lý sẽ được hiển thị trực tiếp tại **logs** của container **TaskManager**.

