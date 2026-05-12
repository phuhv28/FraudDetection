#!/bin/bash

# Docker entrypoint script for Flink Fraud Detection Pipeline

set -e

echo "🚀 Starting Flink Fraud Detection Pipeline"

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
for i in {1..30}; do
    if echo > /dev/tcp/my-cluster-kafka-bootstrap.kafka.svc.cluster.local/9092 2>/dev/null; then
        echo "✅ Kafka is ready"
        break
    fi
    echo "⏳ Kafka not ready yet, retrying ($i/30)..."
    sleep 2
done

# Start Flink Job Manager (if this is the job manager)
if [ "$1" = "jobmanager" ]; then
    echo "Starting Flink Job Manager..."
    /docker-entrypoint.sh jobmanager &
    sleep 5
    
    # Submit PyFlink job
    echo "Submitting Fraud Detection Pipeline job..."
    $FLINK_HOME/bin/flink run \
        -py /opt/flink/flink/flink_fraud_pipeline.py \
        -pyarch fraud_detector.zip \
        -d
    
    # Keep container running
    wait

elif [ "$1" = "taskmanager" ]; then
    echo "Starting Flink Task Manager..."
    /docker-entrypoint.sh taskmanager

else
    # Standalone mode - run as single instance
    echo "Starting Flink in Standalone mode..."
    
    # Start Job Manager
    /opt/flink/bin/jobmanager.sh start
    
    sleep 3
    
    # Submit PyFlink job
    echo "Submitting Fraud Detection Pipeline job..."
    /opt/flink/bin/flink run \
        -py /opt/flink/flink/flink_fraud_pipeline.py \
        -d
    
    # Keep container alive
    sleep infinity
fi
