# 🎯 Kafka Consumer + Flink Pipeline - Deployment Guide

Xử lý real-time fraud detection từ Kafka message streams.

## 📚 Cấu trúc

```
kafka/
├── kafka_consumer.py          # Standalone Kafka consumer
├── producer_fraud.py          # Data producer để test
├── requirements.txt           # Dependencies
└── kafka.yaml                 # Kafka K8s config

flink/
├── flink_fraud_pipeline.py    # Flink pipeline job (advanced)
└── docker-entrypoint.sh       # Entrypoint cho Docker

Dockerfiles:
├── Dockerfile.kafka-consumer  # Consumer image
└── Dockerfile.flink           # Flink cluster image

k8s/
├── namespace-rbac.yaml        # Namespace + RBAC setup
├── fraud-detector-consumer-deployment.yaml
└── flink-deployment.yaml      # Flink Job Manager + Task Manager
```

## 🚀 Quick Start - Kafka Consumer (Recommended for Start)

### 📦 Local Setup

```bash
cd /home/papaba333/Documents/FraudDetection

# 1. Install dependencies
pip install confluent-kafka xgboost numpy pandas shap google-genai

# 2. Start consumer
python kafka/kafka_consumer.py \
  --bootstrap-servers my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --group-id fraud-detector-consumer \
  --topics transactions.raw
```

### ☸️ Deploy on Kubernetes

```bash
# Step 1: Create namespace + RBAC
kubectl apply -f k8s/namespace-rbac.yaml

# Step 2: Create ConfigMap + Secret
kubectl apply -f k8s/fraud-detector-consumer-deployment.yaml

# Step 3: Update Gemini API Key
kubectl -n fraud-detection edit secret fraud-secrets
# Then replace YOUR_GEMINI_API_KEY_HERE with actual key

# Step 4: Check deployment
kubectl -n fraud-detection get pods -w
kubectl -n fraud-detection logs -f deployment/fraud-detector-consumer

# Step 5: View metrics
kubectl -n fraud-detection get hpa -w
```

## 🔥 Advanced - Flink Pipeline (Distributed Processing)

Flink pipeline sử dụng cho high-throughput, low-latency scenarios.

### 📦 Prerequisites

```bash
# Install PyFlink locally (for testing)
pip install apache-flink==1.18.0
```

### 🐳 Build Docker Images

```bash
cd /home/papaba333/Documents/FraudDetection

# Build consumer image
docker build -f Dockerfile.kafka-consumer -t fraud-detection:latest .

# Build Flink image (requires Flink base)
docker build -f Dockerfile.flink -t fraud-detection-flink:latest .

# Load to Kind (if using local K8s)
kind load docker-image fraud-detection:latest --name fraud-cluster
kind load docker-image fraud-detection-flink:latest --name fraud-cluster
```

### ☸️ Deploy Flink on Kubernetes

```bash
# Deploy Flink cluster
kubectl apply -f k8s/flink-deployment.yaml

# Check deployment
kubectl -n fraud-detection get pods
kubectl -n fraud-detection get svc

# Port-forward to Flink UI
kubectl -n fraud-detection port-forward svc/flink-jobmanager 8081:8081

# Access UI: http://localhost:8081
```

## 📊 Data Flow

### Input Topic: `transactions.raw`

```json
{
  "cc_num": "4532015112830366",
  "amt": 123.45,
  "trans_date_trans_time": "2023-05-12 10:30:00",
  "merchant": "Shell Oil",
  "category": "Gas",
  "lat": 40.7128,
  "long": -74.0060,
  "merch_lat": 40.7589,
  "merch_long": -73.9851,
  "city_pop": 8000000,
  "dob": "1990-01-01",
  "gender": "M",
  "state": "NY",
  "trans_count_24h": 5,
  "amt_sum_24h": 500,
  "trans_count_7d": 25,
  "amt_sum_7d": 2000
}
```

### Output Topics

#### 1. `transactions.fraud` - Fraud Detected ⚠️

```json
{
  "status": "SUCCESS",
  "fraud_score": 0.85,
  "is_fraud": 1,
  "action": "BLOCK",
  "cc_num": "4532015112830366",
  "amt": 123.45,
  "timestamp": "2023-05-12T10:30:00.123456",
  "shap_explanation": [
    {
      "feature": "distance",
      "value": 100.5,
      "impact_score": 0.3456,
      "direction": "TĂNG RỦI RO"
    }
  ],
  "llm_alert": "Giao dịch cách vị trí đã đăng ký 100km với số tiền lớn bất thường..."
}
```

#### 2. `transactions.clean` - Legitimate ✅

```json
{
  "status": "SUCCESS",
  "fraud_score": 0.15,
  "is_fraud": 0,
  "action": "ALLOW",
  "cc_num": "4532015112830366",
  "timestamp": "2023-05-12T10:30:00.123456"
}
```

#### 3. `transactions.error` - Processing Error ❌

```json
{
  "status": "ERROR",
  "message": "Missing required field: amt",
  "timestamp": "2023-05-12T10:30:00.123456"
}
```

## 🧪 Testing

### Produce Test Data

```bash
# Create test data stream
python kafka/producer_fraud.py --count 100

# Or manually using kafka-console-producer
kafka-console-producer --broker-list localhost:9092 \
  --topic transactions.raw \
  --property "parse.key=true" \
  --property "key.separator=:" < /tmp/test-data.txt
```

### Monitor Output

```bash
# Watch fraud transactions
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic transactions.fraud \
  --from-beginning

# Watch clean transactions
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic transactions.clean \
  --from-beginning

# Watch errors
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic transactions.error \
  --from-beginning
```

### Check Consumer Lag

```bash
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group fraud-detector-consumer \
  --describe
```

## 🔧 Configuration

### Environment Variables

```bash
# Kafka connection
KAFKA_BOOTSTRAP_SERVERS=my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092

# Fraud detection threshold (0-1)
FRAUD_THRESHOLD=0.2426

# Gemini API for LLM alerts (REQUIRED)
GEMINI_API_KEY=your_api_key_here

# Logging
LOG_LEVEL=INFO
```

### Consumer Configuration

**Kafka Consumer Config:**
- `auto.offset.reset`: earliest (start from beginning if no offset)
- `enable.auto.commit`: true (auto-commit offset)
- `session.timeout.ms`: 30000
- `group.id`: fraud-detector-consumer

### Flink Configuration

**Parallelism**: 2 (match task manager slots)
**Task Slots**: 2 per task manager
**Memory**: 1.6GB job manager, 1.7GB per task manager

## 🚨 Troubleshooting

### Consumer không kết nối Kafka

```bash
# Check Kafka service DNS
kubectl -n kafka get svc
kubectl -n kafka get pods

# Test connectivity
kubectl -n fraud-detection run -it --rm debug \
  --image=busybox --restart=Never -- \
  nc -zv my-cluster-kafka-bootstrap.kafka.svc.cluster.local 9092

# Check consumer logs
kubectl -n fraud-detection logs -f deployment/fraud-detector-consumer
```

### Flink Job không chạy

```bash
# Check Job Manager status
kubectl -n fraud-detection describe pod -l component=jobmanager

# View Job Manager logs
kubectl -n fraud-detection logs -f -l component=jobmanager

# Restart deployment
kubectl -n fraud-detection rollout restart deployment/flink-jobmanager
kubectl -n fraud-detection rollout restart deployment/flink-taskmanager

# Check Task Manager logs
kubectl -n fraud-detection logs -f -l component=taskmanager
```

### High memory usage

```bash
# Check current resource usage
kubectl -n fraud-detection top pods

# If OOMKilled, increase limits in deployment YAML:
resources:
  limits:
    memory: "4Gi"  # Increase from 2Gi
```

### Consumer lag growing

```bash
# Check lag details
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group fraud-detector-consumer \
  --members --verbose

# Solutions:
# 1. Scale up replicas: kubectl -n fraud-detection scale deployment/fraud-detector-consumer --replicas=3
# 2. Increase batch.size (Kafka config)
# 3. Check fraud detector performance
```

## 📈 Performance Tuning

### Kafka Consumer
- **Batch size**: 32KB (increase if lag high)
- **Linger time**: 10ms (batching)
- **Fetch size**: 256KB

### Flink Pipeline
- **Parallelism**: Match number of cores
- **Task slots**: 2-4 per task manager
- **Checkpoints**: Every 60 seconds

## 🔐 Security Best Practices

1. **API Keys**: Store in K8s Secrets, not in code
2. **TLS**: Enable TLS for Kafka (production)
3. **Authentication**: SASL/SCRAM for Kafka
4. **Network Policies**: Restrict inter-pod traffic
5. **RBAC**: Limited service account permissions

## 📊 Monitoring & Alerting

### Metrics to track

- `kafka_consumer_lag_sum`: Consumer lag
- `flink_taskmanager_job_task_operator_*`: Task metrics
- `container_memory_usage_bytes`: Memory usage
- `container_cpu_usage_seconds_total`: CPU usage

### Setup Prometheus (optional)

```yaml
# Add to consumer deployment
serviceMonitor:
  enabled: true
  interval: 30s
  path: /metrics
```

## 🔄 Scaling Strategy

**Vertical Scaling:**
```bash
# Increase resource limits
kubectl -n fraud-detection set resources deployment/fraud-detector-consumer \
  --limits=cpu=2000m,memory=2Gi
```

**Horizontal Scaling:**
```bash
# Manual scaling
kubectl -n fraud-detection scale deployment/fraud-detector-consumer --replicas=5

# Auto-scaling (via HPA)
# Already configured in deployment YAML
kubectl -n fraud-detection get hpa
```

## 🎯 Next Steps

1. ✅ Setup K8s namespace + RBAC
2. ✅ Deploy Kafka Consumer
3. ✅ Monitor logs + metrics
4. ✅ Produce test data
5. ✅ Verify output topics
6. ⏳ Optional: Deploy Flink for advanced use
7. ⏳ Setup alerting + monitoring
8. ⏳ Production hardening

## 📚 References

- [Kafka Consumer API](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Flink Documentation](https://nightlies.apache.org/flink/flink-docs-stable/)
- [Strimzi Kafka on K8s](https://strimzi.io/)
- [XGBoost Model](../fraud_detector/README.md)

---

**Support & Questions**: Check fraud_detector/README.md for AI model details
