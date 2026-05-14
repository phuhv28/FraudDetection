## 📌 Tổng quan kiến trúc

Hệ thống sử dụng **Strimzi Operator** để quản lý Kafka. Cụm Kafka được cấu hình với chế độ **KRaft** (không dùng Zookeeper) và phân tách vai trò (Roles) để tối ưu hóa tài nguyên trên 3 máy Worker.

---

## 🛠 Yêu cầu hệ thống

- **Kubernetes Cluster**: 1 Master, 3 Workers.
- **Storage Class**: `local-path`.
- **Namespace**: `kafka`.

---

## Cài đặt Storage Class

### Bước 1: Cài đặt Local Path Provisioner (Rancher)

Chạy lệnh trên máy master:

```bash
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/master/deploy/local-path-storage.yaml
```

### Bước 2: Kiểm tra StorageClass

Sau khi chạy lệnh trên, hãy kiểm tra xem hệ thống đã nhận diện được "kho lưu trữ" mới chưa:

```bash
kubectl get sc
```

Bạn sẽ thấy một StorageClass tên là local-path.

### Bước 3: Thiết lập làm Storage mặc định (Default)

Để khi triển khai Kafka (hoặc các ứng dụng khác) không cần phải chỉ định tên StorageClass thủ công, hãy đặt local-path làm mặc định:

```bash
kubectl patch storageclass local-path -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'
```

Bây giờ, khi chạy lại lệnh "kubectl get sc", sẽ thấy local-path (default).

## 🏗 Các bước cài đặt

### Bước 1: Khởi tạo Namespace

Tạo không gian tên riêng biệt để quản lý các tài nguyên liên quan đến Kafka:

```bash
kubectl create namespace kafka
```

### Bước 2: Cài đặt Strimzi Cluster Operator

Cài đặt "bộ não" điều phối Kafka. Lệnh này sẽ định nghĩa các Custom Resource (CRDs) và tạo Operator Pod.

```bash
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
```

Kiểm tra trạng thái Operator:

```bash
kubectl get pods -n kafka -w
```

Lưu ý: Đợi cho đến khi Pod strimzi-cluster-operator-... ở trạng thái Running trước khi sang bước tiếp theo.

### Bước 3: Triển khai Kafka Cluster (KRaft Mode)

Sử dụng file cấu hình kafka.yaml (bao gồm KafkaNodePool và Kafka) để khởi tạo 3 Broker.

```bash
kubectl apply -f kafka.yaml -n kafka
```

### Bước 4: Kiểm tra trạng thái cụm

Sau khi apply, Strimzi sẽ tự động tạo các Pod cho Broker. Quá trình này có thể mất 3-5 phút.

```bash
# Kiểm tra các Pod Broker
kubectl get pods -n kafka -o wide

# Kiểm tra trạng thái Ready của Kafka Resource
kubectl get kafka -n kafka
```

### Bước 5: Tạo topic

Tạo topic transaction.raw để xử lý các event từ producer

```bash
kubectl apply -f transaction-raw-topic.yaml
```

## Deploy fraud_producer

```bash
kubectl apply -f producer-deployment.yaml
```
