# 🚀 Quick Start - 4-Layer Fraud Detection Pipeline

## One-Page Reference

### The Pipeline (Visual)
```
Kafka Input → Redis Enrichment → Rule Engine → ML Inference → Decision Service → MinIO → Output Topics
  Raw Txn       (User Data)      (7 Rules)     (XGBoost)      (Score+Alert)    (Audit)  (3 Topics)
```

### Key Numbers
- **Rule Score**: 0-100 (from R1-R7 rules)
- **ML Score**: 0-1 (fraud probability from XGBoost)
- **Combined Score**: 60% Rules + 40% ML
- **Decision Thresholds**:
  - **BLOCK** if combined_score ≥ 80
  - **REVIEW** if combined_score ≥ 50
  - **ALLOW** if combined_score < 50

### The 7 Rules (R1-R7)
| Rule | Detects | Weight |
|------|---------|--------|
| R1 | Single transaction >100M VND | 100 |
| R2 | Total spending >500M VND in 24h | 50 |
| R3 | >10 transactions in 1 minute | 75 |
| R4 | Impossible travel (>500km in 5min) | 75 |
| R5 | Transactions outside 6AM-11PM | 30 |
| R6 | New user at high-risk merchant | 60 |
| R7 | Amount >3σ from user average | 50 |

### Output Format (One Transaction)
```json
{
  "final_decision": "BLOCK",           # BLOCK/REVIEW/ALLOW
  "combined_score": 87.5,              # 0-100 (higher = more fraud)
  "rule_evaluation": {...},            # Which rules triggered
  "ml_evaluation": {...},              # ML confidence
  "alert_message": "..."               # Human-readable reason
}
```

### Output Topics
```
transactions.raw  (Input)
    ↓
    ├→ transactions.fraud   (BLOCK - immediate action)
    ├→ transactions.review  (REVIEW - manual queue)
    ├→ transactions.clean   (ALLOW - normal processing)
    └→ transactions.error   (ERROR - replay/debug)
```

### Deploy Checklist
```bash
✅ Check Redis running:    kubectl get pods -n storage | grep redis
✅ Check MinIO running:    kubectl get pods -n storage | grep minio
✅ Check Kafka running:    kubectl get pods -n kafka | grep kafka
✅ Set environment vars:   REDIS_HOST, MINIO_HOST, GEMINI_API_KEY
✅ Build container:        ./deploy.sh
✅ Check consumer logs:    kubectl logs -n fraud-detection -f <pod>
✅ Send test transaction:  python producer_fraud.py --count 1
✅ Check output topics:    kafka-console-consumer --topic transactions.fraud
```

### Key Files
- **Consumer Logic**: `kafka/kafka_consumer.py`
- **Rules**: `fraud_detector/rule_engine.py`
- **Redis Cache**: `fraud_detector/redis_feature_store.py`
- **Decision Maker**: `fraud_detector/decision_service.py`
- **Data Lake**: `fraud_detector/minio_data_lake.py`

### Environment Variables
```bash
REDIS_HOST=redis.storage.svc.cluster.local
MINIO_HOST=minio.storage.svc.cluster.local:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
GEMINI_API_KEY=your-api-key
KAFKA_BOOTSTRAP_SERVERS=my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
```

### Monitor (Every 100 transactions)
```
✅ Total Processed: 1000
🚨 Fraud (BLOCK):  30     (3%)
⚠️  Review:         80     (8%)
✔️  Clean (ALLOW):  890    (89%)
```

### Latency Targets
- Enrichment: <50ms (Redis)
- Rules: <50ms
- ML: <100ms
- Decision: <50ms
- **Total**: <300ms (target <500ms)

### Test Flow
1. Send transaction: `kafka-console-producer --topic transactions.raw`
2. Check consumer: `kubectl logs -n fraud-detection -f <pod>`
3. Monitor output: `kafka-console-consumer --topic transactions.fraud`
4. Verify MinIO: Check S3 bucket for raw + processed data
5. Check Redis: `redis-cli KEYS "user:*"`

### Troubleshoot
```bash
# Consumer won't start?
kubectl describe pod -n fraud-detection <pod>
kubectl logs -n fraud-detection <pod> --all-containers=true

# Too many errors?
Check transactions.error topic

# Wrong decisions?
Look at rule_evaluation and ml_evaluation in output

# Slow processing?
Monitor Redis connection, check MinIO latency
```

### Scale Up
```bash
# Increase consumer replicas
kubectl scale deployment fraud-detector-consumer -n fraud-detection --replicas 3

# Consumer group automatically load-balances
# Each pod processes different partitions of transactions.raw
```

### 📊 Success Indicators
- ✅ Consumer starts without errors
- ✅ Transactions flow: raw → processed (within 500ms)
- ✅ Decisions appear in correct output topics
- ✅ Raw data in MinIO bucket
- ✅ Stats show 0 errors
- ✅ Fraud rate 2-5% (typical)
- ✅ Review rate 5-10% (expected)

---

**Ready to deploy!** 🎉

For detailed guide, see: `INTEGRATION_SUMMARY.md` or `DEPLOYMENT_READY.md`
