# 🔥 Flink Fraud Detection Pipeline - Deployment Guide

## Overview

**Complete 4-Layer Fraud Detection Pipeline** using Apache Flink 1.20:
- **Layer A**: Data Ingestion (Kafka)
- **Layer B**: Stream Processing (Redis enrichment + Rules + ML + Decision)
- **Layer C**: Storage & Routing (MinIO + 4 output topics)

**Key Features**:
- ✅ Exactly-once semantics with checkpointing
- ✅ Distributed processing (3+ parallelism)
- ✅ Real-time enrichment from Redis
- ✅ 4-topic routing (fraud/review/clean/error)
- ✅ Audit trail in MinIO
- ✅ Horizontal auto-scaling

---

## Prerequisites

### 1. Verify Kubernetes Cluster
```bash
# Check cluster status
kubectl cluster-info
kubectl get nodes

# Should show:
# - fraud-cluster-control-plane
# - fraud-cluster-worker
# - fraud-cluster-worker2
```

### 2. Verify Kafka
```bash
kubectl get pods -n kafka
# Should show:
# - my-cluster-dual-role-0        1/1 Running
# - my-cluster-dual-role-1        1/1 Running
# - my-cluster-dual-role-2        1/1 Running
# - strimzi-cluster-operator-xxx  1/1 Running
```

### 3. Verify Storage Components
```bash
# Redis
kubectl get pods -n storage | grep redis
# Should show: redis pod running

# MinIO
kubectl get pods -n storage | grep minio
# Should show: minio pod running
```

---

## Step 1: Prepare Docker Image

### Build Fraud Detection Docker Image
```bash
cd /home/papaba333/Documents/FraudDetection

# Build the complete Flink pipeline image
docker build -t fraud-detection:latest -f Dockerfile .

# Verify image was created
docker images | grep fraud-detection
```

### Load Image into Kind Cluster
```bash
# For Kind cluster, load the local image
kind load docker-image fraud-detection:latest --name fraud-cluster

# Verify image is available in cluster
docker exec fraud-cluster-control-plane crictl images | grep fraud-detection
```

---

## Step 2: Deploy Flink Cluster

### Deploy Flink JobManager + TaskManagers
```bash
# Apply Flink deployment manifest
kubectl apply -f kubernetes/flink-deployment.yaml

# Verify deployment
kubectl get all -n fraud-detection

# Should show:
# - flink-jobmanager pod (1/1 Running)
# - flink-taskmanager pods (3/3 Running)
# - flink-jobmanager service
```

### Wait for Flink to be Ready
```bash
# Watch deployment progress
kubectl get pods -n fraud-detection -w

# Once all pods are Running, check JobManager web UI
kubectl port-forward -n fraud-detection svc/flink-jobmanager 8081:8081 &
# Open browser: http://localhost:8081
```

---

## Step 3: Submit Fraud Detection Job

### Submit Job to Flink
```bash
# Option 1: Via Flink Job deployment
kubectl apply -f kubernetes/flink-deployment.yaml

# Option 2: Via Flink command-line
JOBMANAGER_POD=$(kubectl get pod -n fraud-detection -l component=jobmanager -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n fraud-detection $JOBMANAGER_POD -- \
  /opt/flink/bin/flink run \
  --jobmanager $JOBMANAGER_POD:6123 \
  --parallelism 3 \
  /app/flink/flink_fraud_pipeline_complete.py
```

### Monitor Job Status
```bash
# Check job status
kubectl get jobs -n fraud-detection -w

# View Job Manager logs
kubectl logs -n fraud-detection deployment/flink-jobmanager -f

# View TaskManager logs (example: first replica)
kubectl logs -n fraud-detection -l component=taskmanager -c taskmanager --tail=50 -f
```

---

## Step 4: Test the Pipeline

### Send Test Transactions
```bash
# Open terminal in your workspace
cd /home/papaba333/Documents/FraudDetection/kafka

# Start a producer (if you have one)
python producer_fraud.py --transaction-count 100 --rate 5

# Or use Kafka console producer
kubectl run -it --image=confluentinc/cp-kafka:7.5.0 --rm \
  --restart=Never --name kafka-producer -- \
  bash -c 'kafka-console-producer --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 --topic transactions.raw'
```

### Monitor Output Topics
```bash
# Terminal 1: Watch transactions.fraud (fraud decisions)
kubectl run -it --image=confluentinc/cp-kafka:7.5.0 --rm \
  --restart=Never --name fraud-consumer -- \
  kafka-console-consumer --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --topic transactions.fraud --from-beginning

# Terminal 2: Watch transactions.review (review decisions)
kubectl run -it --image=confluentinc/cp-kafka:7.5.0 --rm \
  --restart=Never --name review-consumer -- \
  kafka-console-consumer --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --topic transactions.review --from-beginning

# Terminal 3: Watch transactions.clean (clean decisions)
kubectl run -it --image=confluentinc/cp-kafka:7.5.0 --rm \
  --restart=Never --name clean-consumer -- \
  kafka-console-consumer --bootstrap-server my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --topic transactions.clean --from-beginning
```

### Monitor Flink Metrics
```bash
# Open Flink Web UI
kubectl port-forward -n fraud-detection svc/flink-jobmanager 8081:8081

# Visit: http://localhost:8081
# Check:
# - Task Managers tab (should show 3 active)
# - Running Jobs (should show fraud detection job)
# - Task Manager logs
# - Metrics (throughput, latency, etc.)
```

---

## Step 5: Monitor and Troubleshoot

### Check Flink Job Logs
```bash
# JobManager logs
kubectl logs -n fraud-detection deployment/flink-jobmanager --all-containers=true -f

# TaskManager logs (all)
kubectl logs -n fraud-detection -l component=taskmanager --all-containers=true -f

# Specific pod
POD_NAME=$(kubectl get pods -n fraud-detection -l component=taskmanager -o jsonpath='{.items[0].metadata.name}')
kubectl logs -n fraud-detection $POD_NAME -f
```

### Check Resource Usage
```bash
# CPU and Memory
kubectl top pods -n fraud-detection

# Expected:
# flink-jobmanager: 200-400m CPU, 800-1200Mi memory
# flink-taskmanager-*: 200-400m CPU, 800-1200Mi memory each
```

### Verify Redis Connection
```bash
# Check Redis is accessible from Flink pods
kubectl exec -n fraud-detection $(kubectl get pod -n fraud-detection -l component=taskmanager -o jsonpath='{.items[0].metadata.name}') -- \
  python -c "import redis; r = redis.Redis(host='redis.storage.svc.cluster.local'); print('Redis OK:', r.ping())"
```

### Verify MinIO Connection
```bash
# Check MinIO is accessible from Flink pods
kubectl exec -n fraud-detection $(kubectl get pod -n fraud-detection -l component=taskmanager -o jsonpath='{.items[0].metadata.name}') -- \
  python -c "from minio import Minio; m = Minio('minio.storage.svc.cluster.local:9000', access_key='minioadmin', secret_key='minioadmin'); print('MinIO OK:', m.bucket_exists('fraud-detection-lake'))"
```

---

## Scaling the Pipeline

### Increase Parallelism
```bash
# Edit deployment to change number of TaskManagers
kubectl patch deployment flink-taskmanager -n fraud-detection \
  -p '{"spec":{"replicas":5}}'

# Or scale via kubectl
kubectl scale deployment flink-taskmanager -n fraud-detection --replicas 5
```

### Check Auto-Scaling (HPA)
```bash
# View HPA status
kubectl get hpa -n fraud-detection

# Watch auto-scaling
kubectl get hpa -n fraud-detection -w
```

---

## Performance Tuning

### Configuration Options (in flink-deployment.yaml)

```yaml
# Increase heap memory
FLINK_JM_HEAP: "2048m"          # JobManager heap
FLINK_TM_HEAP: "2048m"          # TaskManager heap

# Increase parallelism
parallelism.default: 6           # Process 6 partitions in parallel

# Checkpointing
state.checkpoints.dir: ...       # Checkpoint interval
state.backend: filesystem        # State backend type
```

### Performance Targets
- **Latency**: <500ms end-to-end (per transaction)
- **Throughput**: 1000+ transactions/second
- **Checkpoint interval**: 10 seconds (for exactly-once)

---

## Troubleshooting

### Issue 1: Pods in Pending State
```bash
# Check pod events
kubectl describe pod -n fraud-detection <pod-name>

# Common causes:
# - Not enough CPU/memory resources
# - Node affinity constraints
# - PVC not bound

# Solution:
kubectl delete pod -n fraud-detection <pod-name>  # Restart pod
```

### Issue 2: Job Fails to Start
```bash
# Check JobManager logs
kubectl logs -n fraud-detection deployment/flink-jobmanager

# Common causes:
# - Redis not reachable
# - MinIO not reachable
# - Model files missing
# - Python dependencies not installed

# Solution: Rebuild Docker image or fix connection issues
```

### Issue 3: Low Throughput
```bash
# Check TaskManager metrics
kubectl top pods -n fraud-detection

# Common causes:
# - Low parallelism (increase TaskManager replicas)
# - High latency (check Redis/MinIO latency)
# - GC pauses (increase heap memory)

# Solution: Scale up TaskManagers or optimize code
```

### Issue 4: High Memory Usage
```bash
# Monitor memory
kubectl top pods -n fraud-detection | grep memory

# Common causes:
# - Large state backend
# - Memory leak in code
# - Too many concurrent transactions

# Solution:
# - Enable state compression
# - Optimize code
# - Reduce parallelism
```

---

## Cleanup

### Stop Flink Pipeline
```bash
# Delete Flink job
kubectl delete job -n fraud-detection flink-fraud-detection-job

# Or delete specific pod
kubectl delete pod -n fraud-detection <job-pod-name>
```

### Remove Flink Deployment
```bash
# Delete all Flink resources
kubectl delete all -n fraud-detection -l app=flink
```

### Full Cleanup
```bash
# Delete entire fraud-detection namespace
kubectl delete namespace fraud-detection
```

---

## Production Checklist

- [ ] Kafka cluster running with 3+ brokers
- [ ] Redis cluster configured with persistence
- [ ] MinIO with S3 compatibility enabled
- [ ] Flink cluster deployed with 3+ TaskManagers
- [ ] Docker image built and loaded into Kind
- [ ] Environment variables configured (REDIS_HOST, MINIO_HOST, GEMINI_API_KEY)
- [ ] Monitoring and logging configured
- [ ] Auto-scaling (HPA) enabled
- [ ] Checkpoint directory persistent
- [ ] SSL/TLS configured for production
- [ ] Resource quotas set
- [ ] Network policies configured

---

## Next Steps

1. **Deploy Feature Enrichment CronJob**
   - Hourly batch job to populate Redis with features
   - Enables low-latency enrichment during stream processing

2. **Build Frontend Dashboard**
   - Real-time fraud monitoring
   - Decision analytics
   - Rule performance tracking

3. **Setup Monitoring & Alerting**
   - Prometheus metrics
   - Grafana dashboards
   - Alert rules for anomalies

4. **Production Deployment**
   - Deploy to cloud Kubernetes (EKS, AKS, GKE)
   - Configure TLS/mutual authentication
   - Setup cross-region replication

---

**Status**: 🔥 **Ready for Production Deployment**

For questions or issues, check logs and ensure all prerequisites are met.
