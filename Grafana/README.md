## Set up Grafana và K8s dashboard

### Bước 1: Cài đặt Prometheus và Grafana

Tạo namespace monitoring

```bash
kubectl create namespace monitoring
```

Cài đặt Prometheus

```bash
helm install prometheus prometheus-community/prometheus \
  -n monitoring \
  --set server.resources.requests.memory=128Mi \
  --set server.resources.limits.memory=256Mi \
  --set alertmanager.enabled=false \
  --set pushgateway.enabled=false \
  --set kube-state-metrics.enabled=false
```

Cài đặt Grafana

```bash
helm install grafana grafana/grafana \
  -n monitoring \
  --set resources.requests.memory=128Mi \
  --set resources.limits.memory=256Mi
```

### Bước 2: Lấy pw và port-forward Grafana

Lấy pw Grafana:

```bash
kubectl get secret grafana -n monitoring \
-o jsonpath="{.data.admin-password}" | base64 -d
```

Port-forward Grafana:

```bash
kubectl port-forward svc/grafana 3000:80 -n monitoring
```

Mở Grafana tại địa chỉ: http://localhost:3000

Đăng nhập:

- user: admin.
- pw đã lấy ở trên.

### Bước 3: Add prometheus datasource

- Vào Connections -> Data Sources
- Add Prometheus
- Nhập URL: http://prometheus-server
- Save & test

### Bước 4: Import k8s dashboard

- Vào dashboad, chọn New, chọn Import
- Chọn upload dashboard JSON file, chọn k8s.json

## Cài đặt Kafka Dashboard

### Bước 1: Cài đặt Kafka Exporter

```bash
kubectl apply -f kafka-exporter.yaml
```

Kiểm tra trạng thái exporter:

```bash
kubectl get pods
```

Lưu ý: Đợi cho đến khi Pod kafka-exporter-xxxxx ở trạng thái Running trước khi sang bước tiếp theo.

### Bước 3: Add scrape config vào Prometheus

Edit configmap:

```bash
kubectl edit configmap prometheus-server -n monitoring
```

Thêm job scrape
Trong: "scrape_configs:", thêm

```bash
- job_name: 'kafka-exporter'

  static_configs:
    - targets:
        - kafka-exporter.default.svc.cluster.local:9308
```

Restart Prometheus

```bash
kubectl rollout restart deployment prometheus-server -n monitoring
```

## Cài đặt Flink Dashboard

### Bước 1: Enable Prometheus metrics trong Flink

(Tôi sửa file sẵn rồi, ko cần apply lại) \
Thêm vào file flink-operator.yaml
Trong:

spec:

```bash
  flinkConfiguration:
```

thêm:

```bash
    metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
    metrics.reporter.prom.port: 9249
```

Apply lại.

### Bước 2: Add scrape config vào Prometheus

Edit configmap:

```bash
kubectl edit configmap prometheus-server -n monitoring
```

Thêm job scrape
Trong: "scrape_configs:", thêm

```bash
- job_name: 'flink'

  kubernetes_sd_configs:
    - role: pod

  relabel_configs:
    - source_labels: [__meta_kubernetes_pod_label_app]
      action: keep
      regex: flink-cluster

    - source_labels: [__meta_kubernetes_pod_ip]
      target_label: __address__
      replacement: $1:9249
```

Restart Prometheus

```bash
kubectl rollout restart deployment prometheus-server -n monitoring
```

### Bước 3: Import Flink Dashboard

- Vào dashboad, chọn New, chọn Import
- Chọn upload dashboard JSON file, chọn flink.json

## Cài đặt Fraud_detection Dashboard

### Bước 1: Add scrape config vào Prometheus

Edit configmap:

```bash
kubectl edit configmap prometheus-server -n monitoring
```

Thêm job scrape
Trong: "scrape_configs:", thêm

```bash
- job_name: 'fraud-detection'

  kubernetes_sd_configs:
    - role: pod

  relabel_configs:
    - source_labels: [__meta_kubernetes_pod_name]
      action: keep
      regex: flink-cluster-taskmanager.*

    - source_labels: [__meta_kubernetes_pod_ip]
      target_label: __address__
      replacement: $1:8000
```

Restart Prometheus

```bash
kubectl rollout restart deployment prometheus-server -n monitoring
```

### Bước 2: Create Fraud_Detection Dashboard

- Vào Dashboard --> chọn New --> New Dashboard
- Add Visuallization
- Thêm các query như mong muốn (fraud_transactions_total, normal_transactions_total, fraud_score)
