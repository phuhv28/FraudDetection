# 🔥 Flink Pipeline - Quick Start

## What You Have

✅ **Complete Flink 4-Layer Pipeline** with:
- Data Ingestion (Kafka)
- Stream Processing (Redis + Rules + ML + Decision)
- Storage & Routing (MinIO + 4 output topics)

## Files Created

```
flink/
  ├── flink_fraud_pipeline_complete.py  ← Main pipeline (ALL 4 LAYERS)
  └── ...other files

kubernetes/
  └── flink-deployment.yaml  ← Complete K8s manifests (JobManager + TaskManagers)

Dockerfile  ← Updated with PyFlink + all dependencies

FLINK_DEPLOYMENT.md  ← Full deployment guide
```

## Quick Deploy (5 Steps)

### 1. Build Docker Image
```bash
cd /home/papaba333/Documents/FraudDetection
docker build -t fraud-detection:latest -f Dockerfile .
```

### 2. Load into Kind
```bash
kind load docker-image fraud-detection:latest --name fraud-cluster
```

### 3. Deploy Flink
```bash
kubectl apply -f kubernetes/flink-deployment.yaml
```

### 4. Wait for Ready
```bash
kubectl get pods -n fraud-detection -w
# Wait until all show "1/1 Running"
```

### 5. Monitor
```bash
# Watch logs
kubectl logs -n fraud-detection deployment/flink-jobmanager -f

# Open web UI
kubectl port-forward -n fraud-detection svc/flink-jobmanager 8081:8081
# Visit: http://localhost:8081
```

## Test

### Send Test Transactions
```bash
# In one terminal, send test data
python kafka/producer_fraud.py --transaction-count 100 --rate 5

# In other terminals, watch outputs
kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions.fraud
kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions.review
kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions.clean
```

## Expected Output

Each transaction gets a JSON with:
```json
{
  "final_decision": "BLOCK|REVIEW|ALLOW",
  "combined_score": 65.5,
  "rule_evaluation": {...},
  "ml_evaluation": {...},
  "alert_message": "Human-readable explanation",
  "routing_topic": "transactions.fraud/review/clean"
}
```

## Architecture Summary

```
Kafka Input (transactions.raw)
    ↓
Flink Map Function
    ├─ Step 1: Parse & Enrich (Redis)
    ├─ Step 2: Evaluate Rules (R1-R7)
    ├─ Step 3: ML Inference (XGBoost)
    ├─ Step 4: Decision (combine scores, generate alert)
    └─ Step 5: Store (MinIO) + Route
    ↓
Output Topics
    ├─ transactions.fraud  (combined_score ≥ 80)
    ├─ transactions.review (combined_score 50-79)
    ├─ transactions.clean  (combined_score < 50)
    └─ transactions.error  (processing failed)
```

## Scaling

### More Task Managers = Faster Processing
```bash
# Increase from 3 to 6 parallel workers
kubectl scale deployment flink-taskmanager -n fraud-detection --replicas 6
```

## Monitoring

```bash
# Job Manager logs
kubectl logs -n fraud-detection deployment/flink-jobmanager -f

# Task Managers logs
kubectl logs -n fraud-detection -l component=taskmanager -f

# CPU/Memory usage
kubectl top pods -n fraud-detection

# Flink web UI metrics
http://localhost:8081
```

## Troubleshooting

### Pods stuck in "Pending"
```bash
kubectl describe pod -n fraud-detection <pod-name>
# Check if it needs CPU/memory
```

### Job fails to start
```bash
kubectl logs -n fraud-detection deployment/flink-jobmanager
# Check for Redis/MinIO connection errors
```

### Slow processing
```bash
kubectl top pods -n fraud-detection
# Scale up if CPU not maxed, might be I/O bound
```

## Next Steps

1. ✅ Deploy Flink (you are here)
2. Send test transactions and verify output topics
3. Monitor performance and latency
4. Fine-tune rule thresholds if needed
5. Deploy Feature Enrichment CronJob (hourly)
6. Build dashboard for monitoring

---

**Status**: 🎯 **Ready to Deploy**

For full guide, see: `FLINK_DEPLOYMENT.md`
