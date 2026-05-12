# 🔥 Flink Pipeline

Advanced real-time fraud detection using Apache Flink for distributed stream processing.

## 📌 Overview

Flink provides:
- **Low-latency** processing (sub-millisecond)
- **Exactly-once semantics** (no duplicate processing)
- **Scalability** (horizontal - add more task managers)
- **State management** (maintain statistics across events)
- **Complex event processing** (windowing, joins)

## 🏗 Architecture

```
Kafka Source (transactions.raw)
         ↓
    [Deserialize JSON]
         ↓
[FraudDetectionMapFunction]
    (Process via AI Model)
         ↓
   [Routing Logic]
         ↓
Kafka Sinks:
  - transactions.fraud
  - transactions.clean
  - transactions.error
```

## 🚀 Getting Started Locally

### Install Flink

```bash
# Download Flink
wget https://archive.apache.org/dist/flink/flink-1.18.0/flink-1.18.0-bin-scala_2.12.tgz
tar -xzf flink-1.18.0-bin-scala_2.12.tgz
export PATH=$PATH:$(pwd)/flink-1.18.0/bin

# Verify installation
flink --version
```

### Install PyFlink

```bash
pip install apache-flink==1.18.0
```

### Run Locally (Standalone)

```bash
# Start Flink cluster
cd flink-1.18.0
./bin/start-cluster.sh

# Submit job
cd /home/papaba333/Documents/FraudDetection
flink run -py flink/flink_fraud_pipeline.py

# Access Web UI: http://localhost:8081

# Stop cluster
./bin/stop-cluster.sh
```

## ☸️ Deploy on Kubernetes

### Prerequisites

```bash
# Build Docker image
docker build -f Dockerfile.flink -t fraud-detection-flink:latest .

# Load to Kind
kind load docker-image fraud-detection-flink:latest --name fraud-cluster
```

### Deploy

```bash
# Apply deployment
kubectl apply -f k8s/flink-deployment.yaml

# Check status
kubectl -n fraud-detection get pods

# Port-forward to UI
kubectl -n fraud-detection port-forward svc/flink-jobmanager 8081:8081

# View UI: http://localhost:8081
```

## 🔍 Understanding the Pipeline

### 1. Data Source (Kafka Consumer)

```python
kafka_stream = env.add_source(kafka_consumer)
```

Reads raw transactions from Kafka topic `transactions.raw`

### 2. Fraud Detection Map Function

```python
fraud_results = kafka_stream.map(FraudDetectionMapFunction())
```

For each transaction:
1. Load the XGBoost model
2. Extract features
3. Calculate SHAP explanations
4. Call Gemini AI for alert messages
5. Output: JSON with fraud_score, is_fraud, explanations

### 3. Routing (Split Stream)

```python
fraud_stream = fraud_results.filter(...)
clean_stream = fraud_results.filter(...)
error_stream = fraud_results.filter(...)
```

Route messages to appropriate Kafka topics based on prediction

### 4. Kafka Sink

```python
fraud_stream.add_sink(kafka_producer)
```

Output results to Kafka topics for downstream processing

## 📊 Monitoring

### Flink Web UI

Access http://localhost:8081 (after port-forward on K8s)

**Key Metrics:**
- **TaskManager**: Number of task slots, memory usage
- **Job**: Running/Failed/Cancelled jobs
- **Metrics**: Event throughput, processing time

### Kafka Metrics

```bash
# Check topic sizes
kafka-topics.sh --bootstrap-server localhost:9092 --describe

# Monitor consumer lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group flink-fraud-detector \
  --describe
```

## ⚙️ Configuration

### Flink Configuration (k8s/flink-deployment.yaml)

```yaml
parallelism.default: 2           # Global parallelism
taskmanager.numberOfTaskSlots: 2 # Slots per task manager
jobmanager.memory.process.size: 1600m
taskmanager.memory.process.size: 1728m
```

### Checkpoint Configuration

```yaml
state.backend: rocksdb                          # State storage
state.checkpoints.dir: file:///flink/checkpoints
state.backend.incremental: true
```

## 🔧 Troubleshooting

### Job fails on startup

```bash
# Check logs
kubectl -n fraud-detection logs -f deployment/flink-jobmanager
kubectl -n fraud-detection logs -f deployment/flink-taskmanager

# Common issues:
# - Kafka not accessible: Check service DNS
# - Out of memory: Increase memory limits
# - Missing dependencies: Rebuild Docker image
```

### High latency

```bash
# Check throughput in Flink UI
# Solutions:
# 1. Increase parallelism (if possible)
# 2. Increase task slots
# 3. Scale task managers horizontally
# 4. Check FraudDetector.process_and_predict() performance
```

### Duplicate processing

This is prevented by Flink's exactly-once semantics with checkpoints.
Verify checkpoints are working:

```bash
# Check checkpoint status
kubectl -n fraud-detection exec -it \
  $(kubectl get pod -n fraud-detection -l component=jobmanager -o jsonpath='{.items[0].metadata.name}') \
  -- cat /flink/checkpoints/info.log
```

## 📈 Performance Tuning

### Increase Throughput

```yaml
# In flink-deployment.yaml
parallelism.default: 4
taskmanager.numberOfTaskSlots: 4
replicas: 4  # for flink-taskmanager
```

### Reduce Latency

```yaml
# Disable batching
environment:
  - name: FLINK_PROPERTIES
    value: |
      taskmanager.memory.network.fraction: 0.2
```

### Memory Optimization

```yaml
# For large state
taskmanager.memory.process.size: 4Gi
state.backend.rocksdb.block.cache.size: 2g
```

## 🔐 Security

1. **TLS for Kafka**: Enable in Strimzi Kafka config
2. **Authentication**: SASL/SCRAM for Kafka
3. **Network Policies**: Restrict pod-to-pod communication
4. **RBAC**: Service account with limited permissions

## 📚 Advanced Topics

### Window Operations

```python
# Tumbling window (fraud detection per 1-minute batch)
keyed_stream = fraud_stream.key_by(lambda x: x['cc_num'])
windowed = keyed_stream.window(TumblingEventTimeWindow.of(60000))
```

### State Management

```python
# Rich map function with state
class StatefulFraudDetector(RichMapFunction):
    def open(self, runtime_context):
        self.state = runtime_context.get_state(...)
```

### Exactly-Once Processing

Guaranteed by default with:
- Checkpoints every 60 seconds
- Kafka transactions
- Idempotent operations

## 🚀 Scaling Example

```bash
# Start small
kubectl -n fraud-detection scale deployment/flink-taskmanager --replicas=2

# Monitor
kubectl -n fraud-detection top pods

# Scale up if needed
kubectl -n fraud-detection scale deployment/flink-taskmanager --replicas=5

# Check HPA
kubectl -n fraud-detection get hpa
```

## 📖 References

- [Flink Docs](https://nightlies.apache.org/flink/flink-docs-stable/)
- [Flink Kafka Connector](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/)
- [Flink Python API](https://nightlies.apache.org/flink/flink-docs-stable/api/python/)
- [Kubernetes on Flink](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/resource-providers/native_kubernetes/overview/)

---

**Next**: Compare Flink vs Kafka Consumer trade-offs in [DEPLOYMENT.md](../DEPLOYMENT.md)
