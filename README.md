# FraudDetection

Tao cluster:
cd kind
kind create cluster --config kind-config.yaml --name fraud-cluster

Kiem tra:
kubectl get nodes

Cai helm:
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

A. Deploy Kafka

B. Deploy Redis

helm install my-redis oci://registry-1.docker.io/bitnamicharts/redis -f redis-values.yaml -n storage --create-namespace

Kiem tra;
kubectl get pods -n storage

C. Deploy Minio
helm install minio bitnami/minio -f minio-values.yaml --namespace storage