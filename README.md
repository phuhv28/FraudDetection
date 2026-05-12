# FraudDetection

Tao cluster:
cd kind
kind create cluster --name fraud-detection --config kind-config.yaml
kubectl create namespace data-layer
kubectl create namespace processing-layer
kubectl create namespace monitoring
kubectl create namespace kafka

Kiem tra:
kubectl get nodes

Cai helm:
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

A. Deploy Kafka
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl apply -f kafka.yaml -n kafka

B. Deploy Redis

helm install redis oci://registry-1.docker.io/bitnamicharts/redis \
  --namespace data-layer \
  -f redis-values.yaml

Kiem tra;
kubectl get pods -n storage