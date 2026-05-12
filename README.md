# 🚨 Real-Time Fraud Detection System

An end-to-end fraud detection system using AI (XGBoost), Kafka, Kubernetes, and Flink/Kafka Consumer.

## 🏗 Architecture

```
┌──────────────────┐
│   Transaction    │
│      Data        │
└────────┬─────────┘
         │
    ┌────▼─────┐
    │   Kafka  │ (transactions.raw)
    └────┬─────┘
         │
    ┌────▼──────────────────────────┐
    │  Consumer/Flink Pipeline       │
    │  - Parse & Extract Features    │
    │  - XGBoost Fraud Detection     │
    │  - SHAP Explanations           │
    │  - LLM Alert Generation        │
    └────┬───────────────────────────┘
         │
    ┌────┴──────────────────────────┐
    │      Output Topics             │
    ├────────────────────────────────┤
    │ - transactions.fraud ⚠️        │
    │ - transactions.clean ✅        │
    │ - transactions.error ❌        │
    └────────────────────────────────┘
         │
    ┌────▼─────────────────┐
    │  Backend API / UI    │
    │  Dashboards          │
    │  Alerting System     │
    └──────────────────────┘
```

## 📦 Components

### 1. **Fraud Detector AI** (`fraud_detector/`)
   - XGBoost model (AP: 0.96)
   - Feature extraction & engineering
   - SHAP explanations (interpretable)
   - Gemini LLM integration (alert generation)
   - Python-based inference

### 2. **Data Pipeline** (`kafka/`)
   - **Kafka Consumer**: Simple, scalable event processor
   - **Producer**: Test data stream generator
   - **Configuration**: Kafka broker setup (Strimzi/KRaft)

### 3. **Advanced Pipeline** (`flink/`) - Optional
   - Apache Flink for distributed streaming
   - Low-latency real-time processing
   - State management & checkpointing
   - Horizontal scaling

### 4. **Kubernetes Deployment** (`k8s/`)
   - Consumer deployment (auto-scaling)
   - Flink Job Manager + Task Manager
   - ConfigMaps, Secrets, RBAC
   - Network policies

### 5. **Infrastructure** (`kind/`)
   - Kind cluster configuration
   - Local Kubernetes setup
   - Redis & Minio storage

## 🚀 Quick Start

### Prerequisites

```bash
# Install required tools
# - Docker
# - Kubernetes (kind, kubectl)
# - Helm (optional)
```

### 1. Setup Infrastructure

```bash
# Create Kind cluster
cd kind
kind create cluster --config kind-config.yaml --name fraud-cluster

# Verify cluster
kubectl get nodes

# Install Helm
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

### 2. Deploy Kafka (Strimzi)

```bash
# Create Kafka namespace
kubectl create namespace kafka

# Install Strimzi operator
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka

# Deploy Kafka cluster
cd kafka
kubectl apply -f kafka.yaml -n kafka

# Verify Kafka is running
kubectl get pods -n kafka -w
```

### 3. Deploy Fraud Detection Consumer

```bash
# Option A: Simple way - Kafka Consumer
bash deploy.sh deploy-consumer

# Option B: Manual way
kubectl apply -f k8s/namespace-rbac.yaml
kubectl apply -f k8s/fraud-detector-consumer-deployment.yaml

# Verify deployment
kubectl -n fraud-detection get pods
kubectl -n fraud-detection logs -f deployment/fraud-detector-consumer
```

### 4. (Optional) Deploy Flink

```bash
bash deploy.sh deploy-flink

# Access Flink UI
kubectl -n fraud-detection port-forward svc/flink-jobmanager 8081:8081
# Open http://localhost:8081
```

## 📁 Project Structure

```
FraudDetection/
├── fraud_detector/              # AI Model
│   ├── best_fraud_model_096.json
│   ├── label_encoders.json
│   ├── fraud_detector.py
│   ├── usage.py
│   ├── requirements.txt
│   └── README.md
├── kafka/                       # Event Streaming
│   ├── kafka_consumer.py        # Main consumer
│   ├── producer_fraud.py        # Test producer
│   ├── kafka.yaml               # Kafka config
│   ├── requirements.txt
│   └── README.md
├── flink/                       # Optional: Advanced Pipeline
│   ├── flink_fraud_pipeline.py
│   ├── docker-entrypoint.sh
│   └── README.md
├── kind/                        # Kubernetes Setup
│   ├── kind-config.yaml
│   ├── minio-values.yaml
│   └── redis-values.yaml
├── k8s/                         # K8s Manifests
│   ├── namespace-rbac.yaml
│   ├── fraud-detector-consumer-deployment.yaml
│   ├── flink-deployment.yaml
│   └── README.md
├── Dockerfile.kafka-consumer    # Container images
├── Dockerfile.flink
├── docker-compose.yml           # Local testing
├── deploy.sh                    # Deployment script
├── DEPLOYMENT.md                # Deployment guide
└── README.md                    # This file
```

## 📊 Data Flow

### Input: Raw Transaction
```json
{
  "cc_num": "4532015112830366",
  "amt": 123.45,
  "trans_date_trans_time": "2023-05-12 10:30:00",
  "merchant": "Shell Oil",
  "category": "Gas",
  "lat": 40.7128,
  "long": -74.0060,
  ...
}
```

### Output: Fraud Decision
```json
{
  "status": "SUCCESS",
  "fraud_score": 0.85,
  "is_fraud": 1,
  "action": "BLOCK",
  "shap_explanation": [...],
  "llm_alert": "高度异常交易..."
}
```

## 🧪 Testing Locally

### Using Docker Compose

```bash
# Start all services
docker-compose up -d

# Produce test data
docker-compose run --rm test-producer

# Monitor consumer
docker-compose logs -f fraud-consumer

# Access Kafka UI
# http://localhost:8080

# Stop all services
docker-compose down
```

### Using Local Python

```bash
cd fraud_detector

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Test fraud detector
python usage.py
```

## 📈 Deployment Options

| Option | Complexity | Throughput | Latency | Cost |
|--------|-----------|-----------|---------|------|
| **Kafka Consumer** | ⭐ Easy | Medium | ~100ms | Low |
| **Flink Pipeline** | ⭐⭐⭐ Complex | High | ~10ms | Medium |
| **Serverless (Azure Functions)** | ⭐⭐ Medium | Low | High | Variable |

**Recommendation**: Start with Kafka Consumer, upgrade to Flink if needed.

## 🔧 Configuration

### Environment Variables

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092

# AI Model
FRAUD_THRESHOLD=0.2426

# LLM (REQUIRED)
GEMINI_API_KEY=your_api_key_here

# Logging
LOG_LEVEL=INFO
```

### Update Secrets in K8s

```bash
kubectl -n fraud-detection edit secret fraud-secrets
# Update GEMINI_API_KEY
```

## 🚨 Monitoring & Troubleshooting

### Check Consumer Health

```bash
# View logs
kubectl -n fraud-detection logs -f deployment/fraud-detector-consumer

# Check resource usage
kubectl -n fraud-detection top pods

# Describe deployment
kubectl -n fraud-detection describe deployment fraud-detector-consumer
```

### Monitor Kafka Topics

```bash
# List topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Consumer group lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group fraud-detector-consumer \
  --describe

# View messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic transactions.fraud --from-beginning
```

### Common Issues

See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed troubleshooting.

## 🔐 Security

- ✅ Network policies (K8s)
- ✅ RBAC (Service accounts)
- ✅ Secret management (K8s Secrets)
- ✅ Resource limits (prevent DoS)
- ⏳ TLS for Kafka (production)
- ⏳ Authentication/Authorization

## 📚 Documentation

- [Fraud Detector README](fraud_detector/README.md) - AI model details
- [Kafka Consumer README](kafka/README.md) - Streaming setup
- [Flink README](flink/README.md) - Advanced pipeline
- [K8s README](k8s/README.md) - Deployment manifests
- [DEPLOYMENT.md](DEPLOYMENT.md) - Full deployment guide

## 🎯 Next Steps

1. ✅ Setup Kind cluster
2. ✅ Deploy Kafka
3. ✅ Deploy Fraud Detection Consumer
4. ⏳ Produce test data
5. ⏳ Verify output topics
6. ⏳ Deploy Frontend UI
7. ⏳ Setup monitoring + alerting
8. ⏳ Production optimization

## 💡 Key Features

- **AI-Powered**: XGBoost model with 96% AP
- **Interpretable**: SHAP explanations for every decision
- **Real-Time**: Sub-second detection latency
- **Scalable**: Horizontal scaling via Kubernetes
- **Cloud-Ready**: Designed for Azure/Kubernetes
- **Production-Grade**: Logging, monitoring, health checks

## 🤝 Support

For issues or questions:
1. Check [DEPLOYMENT.md](DEPLOYMENT.md) troubleshooting section
2. Review component README files
3. Check Kubernetes pod logs: `kubectl -n fraud-detection logs <pod>`

---

**Last Updated**: May 2026
**Status**: Production Ready ✅
