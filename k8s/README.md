# ☸️ Kubernetes Manifests for Fraud Detection

Production-ready Kubernetes deployments for the fraud detection pipeline.

## 📂 Files

```
k8s/
├── namespace-rbac.yaml                        # Namespace, RBAC, NetworkPolicy
├── fraud-detector-consumer-deployment.yaml    # Kafka Consumer deployment
└── flink-deployment.yaml                      # Flink Job Manager + Task Manager
```

## 🚀 Quick Deploy

```bash
# 1. Setup namespace + RBAC
kubectl apply -f k8s/namespace-rbac.yaml

# 2. Deploy Kafka Consumer
kubectl apply -f k8s/fraud-detector-consumer-deployment.yaml

# 3. (Optional) Deploy Flink
kubectl apply -f k8s/flink-deployment.yaml

# 4. Verify
kubectl -n fraud-detection get all
```

## 📋 Component Details

### 1. namespace-rbac.yaml

**Contains:**
- **Namespace**: `fraud-detection` (isolated environment)
- **ServiceAccount**: `fraud-detection-sa` (identity for pods)
- **ClusterRole**: Permissions for pods
- **ClusterRoleBinding**: Link role to service account
- **NetworkPolicy**: Control traffic between pods

**Key Settings:**
```yaml
# Allows intra-namespace communication
# Allows Kafka access
# Allows DNS for external APIs (Gemini)
```

**Apply:**
```bash
kubectl apply -f k8s/namespace-rbac.yaml
```

### 2. fraud-detector-consumer-deployment.yaml

**Contains:**
- **ConfigMap**: Environment configuration
- **Secret**: API keys (Gemini)
- **Deployment**: Kafka Consumer pods
- **Service**: Internal service for consumer
- **HPA**: Auto-scaling (2-5 replicas)

**Key Settings:**

| Setting | Value | Purpose |
|---------|-------|---------|
| Replicas | 2 | Start with 2 for HA |
| CPU | 500m req, 1000m limit | Fraud detection throughput |
| Memory | 512Mi req, 1Gi limit | Model + Kafka buffering |
| Liveness Probe | TCP port 9092 | Restart if can't reach Kafka |
| Readiness Probe | TCP port 9092 | Exclude from load balancer if fails |

**Scaling Rules (HPA):**
- Min replicas: 2
- Max replicas: 5
- Scale up: CPU > 70% or Memory > 80%
- Scale down: CPU < 70% for 5 minutes

**Deploy:**
```bash
# Create deployment
kubectl apply -f k8s/fraud-detector-consumer-deployment.yaml

# Watch rollout
kubectl -n fraud-detection rollout status deployment/fraud-detector-consumer

# Check HPA
kubectl -n fraud-detection get hpa -w
```

### 3. flink-deployment.yaml

**Contains:**
- **Flink Job Manager**: Coordinator (1 replica)
- **Flink Task Manager**: Workers (2-5 replicas)
- **Service**: Expose Job Manager
- **HPA**: Auto-scale task managers
- **ConfigMap**: Flink configuration

**Key Settings:**

| Component | Setting | Value |
|-----------|---------|-------|
| Job Manager | Memory | 1.6GB |
| Job Manager | CPU | 500m req, 1000m limit |
| Task Manager | Memory | 1.7GB |
| Task Manager | CPU | 500m req, 1000m limit |
| Task Manager | Slots | 2 |
| Parallelism | Default | 2 |

**Ports:**
- **6123**: Job Manager RPC
- **6124**: Task Manager RPC (Blob)
- **8081**: Job Manager Web UI

**Deploy:**
```bash
# Create deployment
kubectl apply -f k8s/flink-deployment.yaml

# Wait for startup
kubectl -n fraud-detection wait --for=condition=ready pod \
  -l component=jobmanager --timeout=300s

# Port-forward to UI
kubectl -n fraud-detection port-forward svc/flink-jobmanager 8081:8081

# Access: http://localhost:8081
```

## 🔧 Configuration Management

### ConfigMap (Shared Configuration)

```bash
# View current config
kubectl -n fraud-detection get configmap fraud-config -o yaml

# Edit config (auto-reload by deployment)
kubectl -n fraud-detection edit configmap fraud-config

# Key variables:
# - KAFKA_BOOTSTRAP_SERVERS
# - FRAUD_THRESHOLD
# - LOG_LEVEL
```

### Secret (Sensitive Data)

```bash
# Edit secret
kubectl -n fraud-detection edit secret fraud-secrets

# Key variables:
# - GEMINI_API_KEY (REQUIRED - update this!)
```

## 📊 Monitoring

### Pod Status

```bash
# Get all pods
kubectl -n fraud-detection get pods

# Watch pods
kubectl -n fraud-detection get pods -w

# Describe specific pod
kubectl -n fraud-detection describe pod <pod-name>

# Show resource usage
kubectl -n fraud-detection top pods
```

### Logs

```bash
# View logs
kubectl -n fraud-detection logs -f deployment/fraud-detector-consumer

# View logs from specific pod
kubectl -n fraud-detection logs <pod-name>

# View logs from all pods matching label
kubectl -n fraud-detection logs -l app=fraud-detector-consumer --all-containers=true

# Follow specific container
kubectl -n fraud-detection logs -f <pod-name> -c kafka-consumer
```

### Events

```bash
# Get namespace events
kubectl -n fraud-detection get events --sort-by='.lastTimestamp'

# Watch events
kubectl -n fraud-detection get events -w
```

## 🚨 Troubleshooting

### Pod not starting

```bash
# Check pod status
kubectl -n fraud-detection describe pod <pod-name>

# Common issues:
# - ImagePullBackOff: Docker image not found
# - CrashLoopBackOff: Application crashes
# - Pending: No resources available

# View logs
kubectl -n fraud-detection logs <pod-name> --previous
```

### Pod OutOfMemory (OOMKilled)

```bash
# Check memory usage
kubectl -n fraud-detection top pods

# Increase limits in deployment:
resources:
  limits:
    memory: "2Gi"  # Increase from 1Gi

# Apply change
kubectl apply -f k8s/fraud-detector-consumer-deployment.yaml
```

### Pod CrashLoopBackOff

```bash
# Check recent logs
kubectl -n fraud-detection logs <pod-name> --tail=50 --timestamps=true

# Check if Kafka is reachable
kubectl -n fraud-detection run debug --image=busybox -it --rm -- \
  nc -zv my-cluster-kafka-bootstrap.kafka.svc.cluster.local 9092

# Verify API keys
kubectl -n fraud-detection get secret fraud-secrets -o jsonpath='{.data.GEMINI_API_KEY}' | base64 -d
```

### HPA not scaling

```bash
# Check HPA status
kubectl -n fraud-detection get hpa -o wide

# Describe HPA
kubectl -n fraud-detection describe hpa fraud-detector-consumer-hpa

# Verify metrics-server is running
kubectl get deployment metrics-server -n kube-system

# Check metrics
kubectl -n fraud-detection get --raw /apis/metrics.k8s.io/v1beta1/nodes
```

## 🔄 Updates & Rollouts

### Update deployment

```bash
# Edit deployment directly
kubectl -n fraud-detection edit deployment fraud-detector-consumer

# Or apply updated YAML
kubectl apply -f k8s/fraud-detector-consumer-deployment.yaml

# Watch rollout
kubectl -n fraud-detection rollout status deployment/fraud-detector-consumer

# Rollback if needed
kubectl -n fraud-detection rollout undo deployment/fraud-detector-consumer
```

### Update Docker image

```bash
# Update deployment image
kubectl -n fraud-detection set image deployment/fraud-detector-consumer \
  kafka-consumer=fraud-detection:v2.0

# Verify rollout
kubectl -n fraud-detection rollout status deployment/fraud-detector-consumer
```

### Update ConfigMap (requires rollout restart)

```bash
# Edit config
kubectl -n fraud-detection edit configmap fraud-config

# Rollout restart to pick up changes
kubectl -n fraud-detection rollout restart deployment/fraud-detector-consumer
```

## 🔐 Security Best Practices

### Already Implemented

✅ Network Policy - restrict traffic
✅ RBAC - limited permissions
✅ Resource limits - prevent resource exhaustion
✅ Service Account - pod identity
✅ Health checks - liveness + readiness
✅ Graceful shutdown - 15s preStop

### Recommendations

```yaml
# 1. Pod Security Policy (PSP)
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  fsReadOnlyRootFilesystem: true

# 2. Network Policy - stricter
policyTypes:
- Ingress
- Egress
# Allow only Kafka and DNS

# 3. Resource Quotas
kubectl create quota fraud-quota -n fraud-detection \
  --hard=requests.cpu=4,requests.memory=4Gi,limits.cpu=8,limits.memory=8Gi

# 4. Audit logging
# Enable in kube-apiserver for compliance

# 5. Image scanning
# Scan Docker images for vulnerabilities before deploy
```

## 📈 Scaling Strategy

### Vertical Scaling (Resources)

```bash
# Increase resource limits
kubectl -n fraud-detection set resources deployment/fraud-detector-consumer \
  --limits=cpu=2000m,memory=2Gi \
  --requests=cpu=1000m,memory=1Gi
```

### Horizontal Scaling (Replicas)

```bash
# Manual
kubectl -n fraud-detection scale deployment/fraud-detector-consumer --replicas=5

# Auto (via HPA - already configured)
# HPA will automatically scale between minReplicas and maxReplicas
```

## 🧪 Testing Deployments

### Deploy to staging first

```bash
# Create staging namespace
kubectl create namespace fraud-detection-staging

# Deploy to staging
kubectl -n fraud-detection-staging apply -f k8s/

# Test thoroughly
kubectl -n fraud-detection-staging logs -f deployment/fraud-detector-consumer

# Promote to production
kubectl -n fraud-detection apply -f k8s/
```

### Load testing

```bash
# Generate test load
kubectl -n fraud-detection exec deployment/fraud-detector-consumer -- \
  python kafka/producer_fraud.py --count 10000

# Monitor consumer lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group fraud-detector-consumer \
  --describe
```

## 📚 References

- [Kubernetes Deployments](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/)
- [Service Accounts](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/)
- [Network Policies](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
- [HPA](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- [Resource Management](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)

---

**Next**: See [DEPLOYMENT.md](../DEPLOYMENT.md) for deployment instructions
