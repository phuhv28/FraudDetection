# Real-time Fraud Detection với Flink + Kafka + XGBoost trên Kubernetes (kind)

Pipeline phát hiện gian lận theo thời gian thực: Kafka nhận transaction → Flink xử lý → XGBoost dự đoán.

---

## Kiến trúc

```
Producer → Kafka (transactions) → Flink Job → XGBoost Model → Fraud Alert
```

## Yêu cầu

- Docker
- kind
- kubectl
- Flink Kubernetes Operator
- Docker Hub account

---

## 1. Tạo cụm Kubernetes với kind

```bash
kind create cluster --name fraud --config kind-config.yaml
```

---

## 2. Cài Flink Kubernetes Operator

```bash
# Cài cert-manager (dependency của Operator)
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
kubectl wait --for=condition=ready pod -l app=cert-manager -n cert-manager --timeout=120s 

# Cài Flink Operator qua Helm
helm repo add flink-operator-repo https://archive.apache.org/dist/flink/flink-kubernetes-operator-1.12.0/
helm repo update
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --set webhook.create=false
```

---

## 3. Deploy Kafka (KRaft mode — không cần Zookeeper)

```bash
kubectl apply -f kafka.yaml
```

> **Lưu ý về LISTENERS vs ADVERTISED_LISTENERS:**
> - `KAFKA_LISTENERS`: Kafka lắng nghe thực tế trên `0.0.0.0` (tất cả interface)
> - `KAFKA_ADVERTISED_LISTENERS`: Địa chỉ Kafka thông báo cho client kết nối lại
>   - `INTERNAL://kafka:29092` → cho các service trong Docker network
>   - `EXTERNAL://localhost:9092` → cho client trên máy host

---

## 4. Tạo Kafka Topic

```bash
kubectl exec -it $(kubectl get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}') -- bash -c "kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic transactions --partitions 3 --replication-factor 1"
```

Kiểm tra topic đã tạo:

```bash
kubectl exec -it \
  $(kubectl get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}') \
  -- bash -c "kafka-topics --list --bootstrap-server kafka:29092"
```

> Dùng `kafka:29092` (INTERNAL) vì đang exec **bên trong** Kubernetes network.

---

## 5. Deploy Flink Job

Chuẩn bị:
- File `.jar` của job: `realtime-fraud-detection-1.0-SNAPSHOT.jar`
- XGBoost model: `best_fraud_model_096.json`
- Label encoders: `label_encoders.json`

Đặt tất cả vào `./models/` trên máy host.

```bash
kubectl apply -f flink-operator.yaml
kubectl get pods -w
```
Doi den khi cac pods READY het roi chay buoc tiep theo.

---

## 6. Test

### Push event vào Kafka

```bash
  kubectl exec -it   $(kubectl get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')   -- bash -c "kafka-console-producer --bootstrap-server kafka:29092 --topic transactions"
```

Roi paste:

```
{"trans_date_trans_time": "2020-06-26 23:18:46", "dob": "1982-02-08", "amt": 949.88, "lat": 41.55, "long": -87.4569, "merch_lat": 41.618135, "merch_long": -87.55474699999999, "category": "shopping_net", "gender": "M", "state": "IN", "city_pop": 23727, "trans_count_24h": 1, "amt_sum_24h": 949.88, "trans_count_7d": 1, "amt_sum_7d": 949.88}
```


### Xem kết quả

```bash
kubectl logs -l component=taskmanager -f
```

Kết quả khi phát hiện gian lận:

```
Fraud Alert! Score: 0.8234
```