# 🔥 Apache Flink 1.20 with Python 3.11 - Fraud Detection Pipeline
FROM flink:1.20-java11

# ==================== SETUP ENVIRONMENT ====================

# 1. Install Python 3.11 and build tools
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-dev \
    python3-pip \
    build-essential \
    git \
    curl \
    && ln -sf /usr/bin/python3.11 /usr/bin/python \
    && ln -sf /usr/bin/python3.11 /usr/bin/python3 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# ==================== COPY APPLICATION ====================

# 3. Set working directory
WORKDIR /app

# 4. Copy only necessary files (flink pipeline, fraud detector models, requirements)
COPY flink/flink_fraud_pipeline_complete.py /app/flink/
COPY fraud_detector/ /app/fraud_detector/
COPY requirements.txt /app/

# 5. Create required directories
RUN mkdir -p /app/logs \
    && mkdir -p /app/checkpoints \
    && mkdir -p /app/data

# ==================== INSTALL DEPENDENCIES ====================

# 6. Install Python dependencies (including PyFlink)
RUN pip install --no-cache-dir -r /app/requirements.txt

# 7. Install Apache Flink Python package
RUN pip install --no-cache-dir apache-flink==1.20.0

# ==================== CONFIGURE PERMISSIONS ====================

# 8. Set permissions for flink user
RUN chown -R flink:flink /app && \
    chmod -R 755 /app

# 9. Change to flink user
USER flink

# ==================== ENTRY POINT ====================

# 10. Set environment variables for Python
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    FLINK_HOME=/opt/flink \
    PATH="${PATH}:/app/flink/bin"

# 11. Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8081/jars || exit 1

# 12. Default command - run the complete Flink fraud detection pipeline
CMD ["python", "/app/flink/flink_fraud_pipeline_complete.py"]