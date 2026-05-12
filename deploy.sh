#!/bin/bash

# 🚀 Deployment Script for Fraud Detection Pipeline
# Usage: ./deploy.sh [command]
# Commands: setup, deploy-consumer, deploy-flink, cleanup, status

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

NAMESPACE="fraud-detection"
PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# Functions
print_header() {
    echo -e "${BLUE}================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================================${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl is not installed"
        exit 1
    fi
    print_success "kubectl found: $(kubectl version --client --short)"
    
    # Check cluster connection
    if ! kubectl cluster-info &> /dev/null; then
        print_error "Cannot connect to Kubernetes cluster"
        exit 1
    fi
    print_success "Connected to Kubernetes cluster"
    
    # Check Kafka namespace
    if ! kubectl get namespace kafka &> /dev/null; then
        print_error "Kafka namespace not found. Please setup Kafka first."
        exit 1
    fi
    print_success "Kafka namespace exists"
    
    # Check if Kafka service is running
    if kubectl -n kafka get svc my-cluster-kafka-bootstrap &> /dev/null; then
        print_success "Kafka service is running"
    else
        print_warning "Kafka service not found. Pipeline may not work."
    fi
}

# Setup namespace and RBAC
setup_namespace() {
    print_header "Setting up Namespace & RBAC"
    
    kubectl apply -f "${PROJECT_ROOT}/k8s/namespace-rbac.yaml"
    print_success "Namespace and RBAC created"
    
    # Wait for namespace to be ready
    kubectl wait --for condition=NamespaceActive namespace/${NAMESPACE} --timeout=30s || true
    print_success "Namespace is active"
}

# Setup ConfigMap and Secret
setup_config() {
    print_header "Setting up Configuration"
    
    # Check if secret already exists
    if kubectl -n ${NAMESPACE} get secret fraud-secrets &> /dev/null; then
        print_warning "Secret already exists. Skipping creation."
    else
        # Create dummy secret (user must update)
        kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: fraud-secrets
  namespace: ${NAMESPACE}
type: Opaque
stringData:
  GEMINI_API_KEY: "CHANGE_ME"
EOF
        print_warning "Secret created with placeholder value."
        print_warning "Update it: kubectl -n ${NAMESPACE} edit secret fraud-secrets"
    fi
    
    # Create ConfigMap
    kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: fraud-config
  namespace: ${NAMESPACE}
data:
  KAFKA_BOOTSTRAP_SERVERS: "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
  FRAUD_THRESHOLD: "0.2426"
  LOG_LEVEL: "INFO"
EOF
    print_success "ConfigMap created"
}

# Build Docker images
build_images() {
    print_header "Building Docker Images"
    
    cd "${PROJECT_ROOT}"
    
    # Build consumer image
    print_warning "Building fraud-detection:latest..."
    docker build -f Dockerfile.kafka-consumer -t fraud-detection:latest .
    print_success "Consumer image built"
    
    # Build Flink image
    print_warning "Building fraud-detection-flink:latest..."
    docker build -f Dockerfile.flink -t fraud-detection-flink:latest .
    print_success "Flink image built"
    
    # Load to Kind if applicable
    if command -v kind &> /dev/null; then
        print_warning "Loading images to Kind cluster..."
        kind load docker-image fraud-detection:latest --name fraud-cluster 2>/dev/null || true
        kind load docker-image fraud-detection-flink:latest --name fraud-cluster 2>/dev/null || true
        print_success "Images loaded to Kind"
    fi
}

# Deploy Kafka Consumer
deploy_consumer() {
    print_header "Deploying Kafka Consumer"
    
    # First setup namespace and config
    setup_namespace
    setup_config
    
    # Deploy consumer
    kubectl apply -f "${PROJECT_ROOT}/k8s/fraud-detector-consumer-deployment.yaml"
    print_success "Consumer deployment created"
    
    # Wait for rollout
    print_warning "Waiting for consumer pods to be ready..."
    kubectl -n ${NAMESPACE} rollout status deployment/fraud-detector-consumer --timeout=300s || true
    
    # Show status
    print_success "Consumer deployed!"
    echo ""
    kubectl -n ${NAMESPACE} get pods -l app=fraud-detector-consumer
}

# Deploy Flink Pipeline
deploy_flink() {
    print_header "Deploying Flink Pipeline"
    
    # First setup namespace and config
    setup_namespace
    setup_config
    
    # Deploy Flink
    kubectl apply -f "${PROJECT_ROOT}/k8s/flink-deployment.yaml"
    print_success "Flink deployment created"
    
    # Wait for rollout
    print_warning "Waiting for Flink pods to be ready..."
    kubectl -n ${NAMESPACE} rollout status deployment/flink-jobmanager --timeout=300s || true
    kubectl -n ${NAMESPACE} rollout status deployment/flink-taskmanager --timeout=300s || true
    
    # Port-forward to Flink UI
    print_success "Flink deployed!"
    print_warning "To access Flink UI:"
    print_warning "  kubectl -n ${NAMESPACE} port-forward svc/flink-jobmanager 8081:8081"
    print_warning "  Then open http://localhost:8081"
}

# Deploy both
deploy_all() {
    print_header "Full Deployment"
    
    build_images
    deploy_consumer
    
    echo ""
    print_warning "Deploy Flink as well? (y/n)"
    read -r response
    if [[ "$response" == "y" ]]; then
        deploy_flink
    fi
}

# Show status
show_status() {
    print_header "Deployment Status"
    
    echo -e "${BLUE}Namespace:${NC}"
    kubectl get namespace ${NAMESPACE} 2>/dev/null || echo "Not found"
    
    echo -e "\n${BLUE}Pods:${NC}"
    kubectl -n ${NAMESPACE} get pods 2>/dev/null || echo "No pods found"
    
    echo -e "\n${BLUE}Services:${NC}"
    kubectl -n ${NAMESPACE} get svc 2>/dev/null || echo "No services found"
    
    echo -e "\n${BLUE}HPA:${NC}"
    kubectl -n ${NAMESPACE} get hpa 2>/dev/null || echo "No HPA found"
    
    echo -e "\n${BLUE}Recent Logs:${NC}"
    kubectl -n ${NAMESPACE} logs -l app=fraud-detector-consumer --tail=10 2>/dev/null || echo "No logs found"
}

# Cleanup
cleanup() {
    print_header "Cleaning up Deployment"
    
    print_warning "This will delete all fraud-detection resources. Continue? (y/n)"
    read -r response
    if [[ "$response" == "y" ]]; then
        kubectl delete namespace ${NAMESPACE} --ignore-not-found
        print_success "Cleanup complete"
    else
        print_warning "Cleanup cancelled"
    fi
}

# Main
main() {
    local command="${1:-status}"
    
    case "$command" in
        setup)
            check_prerequisites
            setup_namespace
            setup_config
            ;;
        build)
            check_prerequisites
            build_images
            ;;
        deploy-consumer)
            check_prerequisites
            deploy_consumer
            ;;
        deploy-flink)
            check_prerequisites
            deploy_flink
            ;;
        deploy)
            check_prerequisites
            deploy_all
            ;;
        status)
            check_prerequisites
            show_status
            ;;
        cleanup)
            cleanup
            ;;
        *)
            echo "Usage: $0 [setup|build|deploy-consumer|deploy-flink|deploy|status|cleanup]"
            echo ""
            echo "Commands:"
            echo "  setup             - Create namespace and RBAC"
            echo "  build             - Build Docker images"
            echo "  deploy-consumer   - Deploy Kafka Consumer only"
            echo "  deploy-flink      - Deploy Flink Pipeline only"
            echo "  deploy            - Deploy everything"
            echo "  status            - Show deployment status"
            echo "  cleanup           - Delete all resources"
            exit 1
            ;;
    esac
}

main "$@"
