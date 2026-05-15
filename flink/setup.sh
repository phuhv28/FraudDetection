#!/bin/bash
set -e

# 1. Tạo cụm Kubernetes với kind
kind create cluster --name fraud --config kind-config.yaml

# 2. Cài Flink Operator (Giữ nguyên các bước của bạn)
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
sleep 15
kubectl wait --for=condition=ready pod -l app.kubernetes.io/instance=cert-manager -n cert-manager --timeout=120s 

helm repo add flink-operator-repo https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.12.0/
helm repo update
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator --set webhook.create=false

# 3. Deploy Kafka
kubectl apply -f kafka.yaml
echo "Waiting for Kafka pod to be ready..."
kubectl wait --for=condition=ready pod -l app=kafka --timeout=300s

# ĐỢI KAFKA BROKER THỰC SỰ LẮNG NGHE (Tránh lỗi AdminClient)
echo "Waiting for Kafka broker to start listening on 29092..."
KAFKA_POD=$(kubectl get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
until kubectl exec $KAFKA_POD -- bash -c "nc -z localhost 29092" 2>/dev/null; do
  sleep 2
done

# 4. Tạo Kafka Topic
kubectl exec $KAFKA_POD -- kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic transactions --partitions 3 --replication-factor 1
kubectl exec $KAFKA_POD -- kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic fraud-alerts --partitions 3 --replication-factor 1

kubectl exec $KAFKA_POD -- kafka-topics --list --bootstrap-server kafka:29092

# 5. Deploy MinIO và Redis
echo "Deploying MinIO and Redis..."
kubectl apply -f minio.yaml
kubectl apply -f redis.yaml
kubectl wait --for=condition=ready pod -l app=minio --timeout=300s
kubectl wait --for=condition=ready pod -l app=redis --timeout=300s

# =====================================================================
# 5.5. TỰ ĐỘNG KHỞI TẠO MINIO BUCKET CHO FLINK DATA LAKE
# =====================================================================
echo "=== Đang tự động cấu hình MinIO Bucket ==="
MINIO_POD=$(kubectl get pod -l app=minio -o jsonpath='{.items[0].metadata.name}')
BUCKET_NAME="fraud-data-lake" # Thay đổi tên bucket khớp với biến 'minioBucket' trong code Flink của bạn
MINIO_USER="minioadmin"
MINIO_PASS="minioadmin"

echo "Đang khởi tạo lệnh tạo bucket trong Pod: $MINIO_POD"
kubectl exec $MINIO_POD -- /bin/sh -c "
  # Định nghĩa alias quyền root tối cao nội bộ
  mc alias set root_local http://localhost:9000 $MINIO_USER $MINIO_PASS > /dev/null 2>&1

  # Kiểm tra và tạo bucket nếu chưa tồn tại
  if mc ls root_local/$BUCKET_NAME > /dev/null 2>&1; then
    echo '>>> Bucket \"$BUCKET_NAME\" đã tồn tại rồi, bỏ qua.'
  else
    echo '>>> Bucket \"$BUCKET_NAME\" chưa có. Đang tiến hành tạo mới...'
    if mc mb root_local/$BUCKET_NAME; then
      echo '>>> Tạo Bucket thành công!'
    else
      echo '>>> LỖI: Không thể tạo Bucket.'
      exit 1
    fi
  fi
"
# =====================================================================

# 6. Deploy Flink Job
kubectl apply -f flink-operator.yaml

# ĐỢI TASKMANAGER XUẤT HIỆN TRƯỚC KHI WAIT
echo "Waiting for Flink TaskManager to be created..."
until kubectl get pods -l component=taskmanager 2>/dev/null | grep -q "taskmanager"; do
  sleep 5
done

echo "TaskManager found. Waiting for it to be READY..."
kubectl wait --for=condition=ready pod -l component=taskmanager --timeout=300s

# 7. Deploy producer
kubectl apply -f producer-deployment.yaml
kubectl wait --for=condition=ready pod -l app=fraud-producer --timeout=300s

# 8. Xem logs
kubectl logs -l component=taskmanager -f