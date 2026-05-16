# 🛡️ Fraud Detection on Kubernetes — Hướng dẫn triển khai

Hệ thống phát hiện gian lận thời gian thực sử dụng **Apache Flink**, **Kafka**, **MinIO**, và **Redis** chạy trên cụm **Kubernetes**.

---

## ✅ Tiền điều kiện

Đảm bảo máy bạn đã cài đặt đầy đủ các công cụ sau trước khi bắt đầu:

| Công cụ | Mục đích | Kiểm tra |
|---|---|---|
| [Docker](https://docs.docker.com/get-docker/) | Chạy container | `docker --version` |
| [kind](https://kind.sigs.k8s.io/docs/user/quick-start/) | Tạo cụm K8s cục bộ | `kind --version` |
| [kubectl](https://kubernetes.io/docs/tasks/tools/) | Quản lý tài nguyên K8s | `kubectl version --client` |
| [Helm](https://helm.sh/docs/intro/install/) | Cài Flink Operator | `helm version` |
| [netcat (nc)](https://netcat.sourceforge.net/) | Kiểm tra kết nối Kafka | `nc -h` |
| [File JAR job](https://drive.google.com/drive/folders/1b8qV_gkCCGQCGpOEJMyXYMTlhWPheEVH?usp=sharing)|||


> **Lưu ý:** Lưu ý: File JAR phải được đặt trong folder ./flink/models

---

## 🚀 Cách 1 — Dùng script tự động `setup.sh`

Đây là cách nhanh nhất để dựng toàn bộ hệ thống chỉ với một lệnh.

### Bước 1 — Cấp quyền thực thi cho script

```bash
chmod +x setup.sh
```

### Bước 2 — Chạy script

```bash
./setup.sh
```

Script sẽ tự động thực hiện toàn bộ các bước bên dưới theo thứ tự và dừng lại nếu có bất kỳ lỗi nào xảy ra (`set -e`).

### Bước 3 — Theo dõi logs

Sau khi triển khai xong, script sẽ tự động stream logs từ Flink TaskManager:

```bash
kubectl logs -l component=taskmanager -f
```

Nhấn `Ctrl + C` để thoát khỏi chế độ xem logs khi cần.

---

## 🔧 Cách 2 — Thực hiện thủ công từng bước

Thực hiện theo đúng thứ tự các bước sau.

---

### Bước 1 — Tạo cụm Kubernetes với kind

```bash
kind create cluster --name fraud --config kind-config.yaml
```

---

### Bước 2 — Cài Flink Operator

**2.1. Cài cert-manager (dependency của Flink Operator):**

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
```

**2.2. Thêm Helm repo và cài Flink Kubernetes Operator:**

```bash
helm repo add flink-operator-repo \
  https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.12.0/
helm repo update
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --set webhook.create=false
```

---

### Bước 3 — Deploy Kafka

```bash
kubectl apply -f kafka.yaml
```

### Bước 4 — Tạo Kafka Topics

```bash
# Tạo topic transactions
kubectl exec $KAFKA_POD -- kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic transactions \
  --partitions 3 \
  --replication-factor 1

# Tạo topic fraud-alerts
kubectl exec $KAFKA_POD -- kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic fraud-alerts \
  --partitions 3 \
  --replication-factor 1

# Kiểm tra danh sách topics
kubectl exec $KAFKA_POD -- kafka-topics \
  --list --bootstrap-server kafka:29092
```

---

### Bước 5 — Deploy MinIO và Redis

**5.1. Apply manifests:**

```bash
kubectl apply -f minio.yaml
kubectl apply -f redis.yaml
```

**5.2. Khởi tạo MinIO Bucket cho Flink Data Lake**

**5.2.1. Lấy tên MinIO Pod:**

```bash
MINIO_POD=$(kubectl get pod -l app=minio -o jsonpath='{.items[0].metadata.name}')
```

**5.2.2. Tạo bucket `fraud-data-lake` bên trong Pod:**

```bash
kubectl exec $MINIO_POD -- /bin/sh -c "
  mc alias set root_local http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1

  if mc ls root_local/fraud-data-lake > /dev/null 2>&1; then
    echo 'Bucket đã tồn tại, bỏ qua.'
  else
    echo 'Đang tạo bucket fraud-data-lake...'
    mc mb root_local/fraud-data-lake && echo 'Tạo bucket thành công!'
  fi
"
```

> Tên bucket `fraud-data-lake` phải khớp với biến `minioBucket` được khai báo trong code Flink.

---

### Bước 6 — Deploy Flink Job

```bash
kubectl apply -f flink-operator.yaml
```
---

### Bước 7 — Deploy Producer

```bash
kubectl apply -f producer-deployment.yaml
```

---

### Bước 8 — Xem logs Flink TaskManager

```bash
kubectl logs -l component=taskmanager -f
```

Nhấn `Ctrl + C` để thoát.

---

## 📋 Kiểm tra trạng thái hệ thống

Sau khi triển khai xong, bạn có thể dùng các lệnh sau để kiểm tra tổng quan:

```bash
# Xem toàn bộ pods
kubectl get pods

# Xem logs Kafka
kubectl logs -l app=kafka

# Xem logs Producer
kubectl logs -l app=fraud-producer

# Xem logs Flink JobManager
kubectl logs -l component=jobmanager
```

---

## 🗑️ Dọn dẹp

Để xóa toàn bộ cụm và giải phóng tài nguyên:

```bash
chmod +x clean.sh
./clean.sh
```