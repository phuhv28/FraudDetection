# 🏗 Kiến Trúc Hệ Thống Fraud Detection - Chi Tiết

## 📐 Tổng Quan 4 Lớp

```
┌─────────────────────────────────────────────────────────────────┐
│  LỚPNGÂN A: DATA INGESTION                                      │
│  (Kafka) → Thu thập transaction events từ nhiều nguồn           │
│  Topic: transactions.raw                                        │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ↓
┌─────────────────────────────────────────────────────────────────┐
│  LỚPP B: STREAM PROCESSING                                      │
│  (Flink/Consumer)                                               │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 1. DATA ENRICHMENT                                       │  │
│  │    - Redis lookup: User history, avg transaction        │  │
│  │    - Geolocation enrichment                              │  │
│  │    - Real-time feature aggregation                       │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           ↓                                      │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 2. RULE-BASED ENGINE                                    │  │
│  │    - Amount check: >100M in 1 sec? BLOCK                │  │
│  │    - Velocity check: 2 transactions in 5 min far apart? │  │
│  │    - Geographic check: Impossible movement?             │  │
│  │    - Time check: Outside normal hours? Blacklist date? │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           ↓                                      │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 3. ML INFERENCE                                          │  │
│  │    - XGBoost model prediction                            │  │
│  │    - Fraud probability score                             │  │
│  │    - SHAP explanations                                   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           ↓                                      │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ 4. DECISION LOGIC                                        │  │
│  │    - Combine rule + ML scores                            │  │
│  │    - Final decision: ALLOW / BLOCK / REVIEW              │  │
│  │    - Generate alert via Gemini LLM                       │  │
│  └──────────────────────────────────────────────────────────┘  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ↓
┌─────────────────────────────────────────────────────────────────┐
│  LỚPPP C: SERVING & STORAGE                                     │
│                                                                 │
│  ┌─────────────────────┐        ┌──────────────────────────┐   │
│  │ Redis (Hot Cache)   │        │ MinIO (Data Lake)        │   │
│  ├─────────────────────┤        ├──────────────────────────┤   │
│  │ user:123:features   │        │ transactions/raw/        │   │
│  │ user:123:history    │        │ transactions/processed/  │   │
│  │ merchant:456:info   │        │ features/vectors/        │   │
│  │ cache:ttl:300s      │        │ models/versions/         │   │
│  └─────────────────────┘        │ analysis/reports/        │   │
│                                 └──────────────────────────┘   │
│                                                                 │
│  Output Kafka Topics:                                           │
│  - transactions.fraud     → cho dashboard                       │
│  - transactions.clean     → cho settlement                      │
│  - transactions.review    → cho manual review                   │
│  - features.updated       → cho downstream                      │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ↓
┌─────────────────────────────────────────────────────────────────┐
│  (Optional) LỚPPP D: ANALYTICS & MONITORING                     │
│  - Dashboard (Grafana)                                          │
│  - Alerting (AlertManager)                                      │
│  - Model monitoring (model drift detection)                     │
│  - User analytics (behavior profiling)                          │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔧 Thành Phần Chi Tiết

### **A. DATA INGESTION**

**Kafka Topics:**
```
transactions.raw
├─ partition: 0-2 (3 partitions for parallelism)
├─ retention: 7 days
├─ replication-factor: 3
└─ schema: JSON
   {
     "cc_num": "...",
     "amt": 123.45,
     "merchant": "...",
     "lat": 10.7,
     "long": 106.7,
     "trans_date_trans_time": "...",
     "dob": "...",
     "gender": "M",
     "state": "HN",
     ...
   }
```

**Sources (Future):**
- Mobile App (push to Kafka)
- Web platform (REST → Kafka)
- ATM network (batch producer)
- Third-party APIs

---

### **B. STREAM PROCESSING (Flink/Consumer)**

#### **1️⃣ Data Enrichment Service**

**Redis Keys để cache:**
```
user:{cc_num}:profile           → { "dob", "gender", "state", "preferred_merchant" }
user:{cc_num}:history:24h       → { "count", "total_amt", "merchants", "locations" }
user:{cc_num}:history:7d        → { "count", "total_amt", "avg_amt", "std_dev_amt" }
user:{cc_num}:locations:history → [ (lat, long, timestamp, count), ... ]
merchant:{merchant_id}:info     → { "category", "avg_transaction", "high_risk_flag" }
geohash:{geohash}:transactions  → count of transactions
fraud_cache:blacklist:users     → set of known fraud accounts
fraud_cache:whitelist:merchants → set of trusted merchants

TTL:
- Profile: 86400s (24 hours)
- History: 3600s (1 hour)
- Blacklist: 600s (10 minutes)
```

**Data Enrichment Steps:**
```python
def enrich_transaction(raw_event):
    # 1. Lookup user profile
    user_profile = redis.get(f"user:{cc_num}:profile")
    
    # 2. Lookup user history
    user_hist_24h = redis.get(f"user:{cc_num}:history:24h")
    user_hist_7d = redis.get(f"user:{cc_num}:history:7d")
    
    # 3. Geolocation lookup
    recent_locations = redis.lrange(f"user:{cc_num}:locations", 0, -1)
    
    # 4. Merchant info
    merchant_info = redis.get(f"merchant:{merchant_id}:info")
    
    # 5. Return enriched event
    return {
        **raw_event,
        "user_profile": user_profile,
        "user_history_24h": user_hist_24h,
        "user_history_7d": user_hist_7d,
        "recent_locations": recent_locations,
        "merchant_info": merchant_info
    }
```

#### **2️⃣ Rule-Based Engine**

**Rules (configurable via ConfigMap):**

| ID | Rule | Condition | Action |
|---|---|---|---|
| R1 | Amount Threshold | amt > 100M | IMMEDIATE_BLOCK |
| R2 | Velocity (Amount) | sum(24h) > 500M | REVIEW |
| R3 | Velocity (Count) | count(1min) > 10 | BLOCK |
| R4 | Geographic | distance > 500km in 5min | BLOCK |
| R5 | Time-based | trans outside 6AM-11PM | REVIEW |
| R6 | Merchant Risk | high_risk_merchant + new_user | REVIEW |
| R7 | Pattern Break | deviation > 3σ from user avg | REVIEW |

**Implementation:**
```python
def apply_rules(enriched_event):
    score = 0
    triggered_rules = []
    
    # R1: Amount threshold
    if enriched_event['amt'] > 100_000_000:
        score += 100
        triggered_rules.append('R1_AMOUNT_THRESHOLD')
    
    # R2: 24h velocity
    if enriched_event.get('user_history_24h', {}).get('total_amt', 0) > 500_000_000:
        score += 50
        triggered_rules.append('R2_VELOCITY_AMOUNT')
    
    # R4: Geographic check
    last_location = enriched_event.get('recent_locations', [{}])[0]
    if calculate_distance(...) > 500:  # km in 5min
        score += 75
        triggered_rules.append('R4_GEOGRAPHIC')
    
    # ... more rules
    
    return {
        "rule_score": score,
        "triggered_rules": triggered_rules,
        "rule_decision": "BLOCK" if score >= 100 else "REVIEW" if score >= 50 else "PASS"
    }
```

#### **3️⃣ ML Inference**

```python
def predict_fraud(enriched_event, rule_result):
    # 1. Extract features
    features = extract_features(enriched_event)
    
    # 2. XGBoost prediction
    fraud_prob = model.predict(features)
    shap_explanation = explainer.explain(features)
    
    # 3. Combine with rules
    combined_score = (
        rule_result['rule_score'] * 0.3 +  # 30% weight
        fraud_prob * 100 * 0.7               # 70% weight
    )
    
    return {
        "ml_fraud_prob": fraud_prob,
        "ml_fraud_score": fraud_prob * 100,
        "shap_explanation": shap_explanation,
        "combined_score": combined_score
    }
```

#### **4️⃣ Decision Logic**

```python
def make_decision(enriched_event, rule_result, ml_result):
    combined_score = ml_result['combined_score']
    
    if combined_score >= 80:
        action = "BLOCK"
        alert_level = "CRITICAL"
    elif combined_score >= 50:
        action = "REVIEW"
        alert_level = "WARNING"
    else:
        action = "ALLOW"
        alert_level = "INFO"
    
    # Generate LLM alert
    llm_alert = generate_alert_via_gemini(
        action, combined_score, rule_result, ml_result
    )
    
    return {
        "action": action,
        "combined_score": combined_score,
        "alert_level": alert_level,
        "llm_alert": llm_alert,
        "rule_details": rule_result['triggered_rules'],
        "ml_details": ml_result['shap_explanation']
    }
```

---

### **C. SERVING & STORAGE**

#### **Redis (Hot Cache)**

**Deployment:**
```yaml
# Helm chart
helm install my-redis oci://registry-1.docker.io/bitnamicharts/redis \
  -f redis-values.yaml \
  -n storage --create-namespace
```

**Capacity Planning:**
- Users: 100M → 10-50GB (depending on history depth)
- Partitioning: Redis Cluster or Sentinel for HA
- Persistence: AOF + RDB snapshots

**Update Strategy:**
```
Batch Job (hourly):
  1. Get all active users
  2. Aggregate transaction history
  3. Update Redis cache
  4. Set TTL
```

#### **MinIO (Data Lake)**

**Bucket Structure:**
```
fraud-detection-lake/
├── raw/
│   ├── transactions/2024-05-12/
│   └── events/2024-05-12/
├── processed/
│   ├── transactions-enriched/2024-05-12/
│   └── features-computed/2024-05-12/
├── models/
│   ├── fraud-model-v1/
│   ├── fraud-model-v2/
│   └── production/
├── analysis/
│   ├── fraud-patterns/
│   ├── user-profiles/
│   └── model-performance/
└── ml/
    ├── training-data/
    ├── test-data/
    └── holdout-data/
```

**Lifecycle Policy:**
- raw data: 30 days → delete (already in processed)
- processed: 1 year
- models: keep forever (versioning)

#### **Output Topics**

```
transactions.fraud
├─ Schema: Result + rule_details + ml_details
├─ Consumers: Dashboard, Alert system, Manual review queue
└─ Retention: 7 days

transactions.clean
├─ Schema: Minimal (just confirmation)
├─ Consumers: Settlement system, Analytics
└─ Retention: 1 day

transactions.review
├─ Schema: Full context + analyst notes (later)
├─ Consumers: Manual review platform
└─ Retention: 30 days

features.updated
├─ Schema: User features + timestamp
├─ Consumers: Feature store, ML training pipeline
└─ Retention: 90 days (for model retraining)
```

---

## 💾 Proposed Improvements (Cải tiến đề xuất)

### ❌ Issues with Current Design

1. **No feature cache** - Every transaction recalculates features
2. **No rule engine** - Just ML, missing business rules
3. **No data persistence** - Can't audit or retrain
4. **Single model** - No A/B testing or versioning

### ✅ Proposed Enhancements

1. **Feature Store (Redis)**
   - Pre-computed features
   - 300s TTL for real-time updates
   - Reduces ML inference latency

2. **Configurable Rules Engine**
   - Business teams can update without code changes
   - Rule versioning for audit trail
   - Performance impact quantification

3. **Data Lake (MinIO)**
   - All raw + processed data
   - Model versioning
   - Training data lineage

4. **Model Management**
   - A/B testing framework
   - Model drift detection
   - Automatic retraining triggers

5. **Monitoring Dashboard**
   - Real-time fraud rate
   - Model performance metrics
   - False positive/negative rates
   - Rule effectiveness

---

## 📊 Data Pipeline Diagram

```
[Raw Event] 
    ↓
[Kafka Topic: transactions.raw]
    ↓
[Consumer/Flink Intake]
    ├─ Parse JSON
    ├─ Basic validation
    └─ Enrich with Redis
         ├─ User profile
         ├─ History
         ├─ Merchant info
         └─ Location history
    ↓
[Rule Engine]
    ├─ Check R1-R7
    ├─ Calculate rule_score
    └─ Rule decision (BLOCK/REVIEW/PASS)
    ↓
[ML Model]
    ├─ Extract features
    ├─ XGBoost inference
    ├─ SHAP explanations
    └─ Calculate fraud_prob
    ↓
[Decision Logic]
    ├─ Combine scores (70% ML + 30% Rules)
    ├─ Final decision
    ├─ Gemini LLM alert
    └─ Route to output topic
    ↓
[Output Topics]
    ├─ transactions.fraud
    ├─ transactions.clean
    ├─ transactions.review
    └─ features.updated
         ↓
[Storage]
    ├─ Dashboard (Kafka sink)
    ├─ MinIO (batch writer)
    ├─ Redis (cache update)
    └─ External systems
```

---

## 🚀 Implementation Roadmap

### Phase 1: Core (Done) ✅
- [x] Kafka Consumer
- [x] ML Model
- [x] Basic decision logic

### Phase 2: Enhancement (Next - 2-3 weeks)
- [ ] Redis integration
- [ ] Rule engine
- [ ] Data enrichment
- [ ] MinIO integration

### Phase 3: Optimization (4-6 weeks)
- [ ] Feature store
- [ ] Model versioning
- [ ] A/B testing
- [ ] Monitoring dashboard

### Phase 4: Advanced (2+ months)
- [ ] Automated retraining
- [ ] Model drift detection
- [ ] User feedback loop
- [ ] Advanced analytics

---

## 📋 Deployment Architecture

```
K8s Cluster
├── Namespace: kafka
│   └── Kafka Cluster (Strimzi)
│
├── Namespace: fraud-detection
│   ├── Fraud Consumer (2-5 pods)
│   ├── Flink Job Manager (1 pod)
│   ├── Flink Task Manager (2-5 pods)
│   └── Feature Store Sync Job
│
├── Namespace: storage
│   ├── Redis (1 master + 2 replicas)
│   ├── MinIO (4 nodes distributed)
│   └── PostgreSQL (for feature metadata)
│
└── Namespace: monitoring
    ├── Prometheus
    ├── Grafana
    └── AlertManager
```

---

## 📚 Next Steps

1. Create Redis integration module
2. Implement Rule Engine
3. Create MinIO connector
4. Update Flink pipeline
5. Create K8s manifests (Redis + MinIO)
6. Add monitoring & dashboards
