"""
🔥 FLINK REAL-TIME FRAUD DETECTION PIPELINE - 4-LAYER COMPLETE 🔥

Distributed stream processing với Apache Flink.
Exactly-once semantics + checkpoint + 3-topic routing.

Architecture:
┌─────────────────────────────────────────────────────────────────┐
│                    Kafka: transactions.raw                       │
└────────────────────────────┬──────────────────────────────────────┘
                             │
┌────────────────────────────▼──────────────────────────────────────┐
│              LAYER B: Stream Processing (Flink)                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1️⃣  Redis Enrichment      → User profiles, history, merchants  │
│  2️⃣  Rule Engine (R1-R7)    → Score 0-100                       │
│  3️⃣  ML Inference (XGBoost) → Fraud probability                 │
│  4️⃣  Decision Service       → BLOCK/REVIEW/ALLOW + Alert       │
│                                                                  │
└────────────────────────────┬──────────────────────────────────────┘
                             │
┌────────────────────────────▼──────────────────────────────────────┐
│              LAYER C: Storage & Routing                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  MinIO Data Lake          Kafka Output Topics                  │
│  ├─ Raw transactions  ──→  transactions.fraud (BLOCK)          │
│  ├─ Processed results  ──→  transactions.review (REVIEW)       │
│  └─ Feature vectors   ──→  transactions.clean (ALLOW)          │
│                     ──→  transactions.error (ERROR)          │
└─────────────────────────────────────────────────────────────────┘
"""

import json
import sys
import os
import time
from datetime import datetime
import logging
from typing import Dict, Any

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(name)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add fraud_detector module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'fraud_detector'))

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction, FilterFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.checkpoint_mode import CheckpointingMode
from pyflink.common.watermark_strategy import WatermarkStrategy

from fraud_detector import FraudDetector
from redis_feature_store import RedisFeatureStore
from rule_engine import RuleEngine
from decision_service import DecisionService, MLModelLoader
from minio_data_lake import MinIODataLake


class FourLayerFraudDetectionMapFunction(MapFunction):
    """
    🎯 4-Layer Fraud Detection Map Function
    
    A. Data Ingestion: Parse raw transaction
    B. Stream Processing: Enrich + Rules + ML + Decision
    C. Storage: Save to MinIO
    
    Initialization: Called ONCE per parallel instance (when job starts)
    Map: Called for EVERY transaction in the stream
    """
    
    def __init__(self):
        """Initialize components (lazy loading in open())"""
        self.fraud_detector = None
        self.feature_store = None
        self.rule_engine = None
        self.decision_service = None
        self.data_lake = None
        self.stats = {
            'processed': 0,
            'fraud': 0,
            'review': 0,
            'clean': 0,
            'errors': 0,
            'start_time': None
        }
    
    def open(self, runtime_context: RuntimeContext):
        """
        🔧 Initialize all 4-layer components
        Called ONCE per Flink Task instance
        """
        self.stats['start_time'] = time.time()
        
        try:
            # ==================== LAYER B: INITIALIZE ====================
            
            # 1. Initialize Fraud Detector (for backward compatibility + feature extraction)
            try:
                detector_path = os.path.join(
                    os.path.dirname(__file__), 
                    '..', 
                    'fraud_detector',
                    'best_fraud_model_096.json'
                )
                encoder_path = os.path.join(
                    os.path.dirname(__file__), 
                    '..', 
                    'fraud_detector',
                    'label_encoders.json'
                )
                self.fraud_detector = FraudDetector(
                    model_path=detector_path,
                    encoder_path=encoder_path,
                    threshold=0.2426
                )
                logger.info("✅ FraudDetector initialized")
            except Exception as e:
                logger.warning(f"⚠️  FraudDetector initialization failed: {str(e)}")
            
            # 2. Initialize Redis Feature Store
            try:
                redis_host = os.getenv('REDIS_HOST', 'redis.storage.svc.cluster.local')
                self.feature_store = RedisFeatureStore(host=redis_host)
                logger.info("✅ Redis Feature Store connected")
            except Exception as e:
                logger.warning(f"⚠️  Redis connection failed: {str(e)} - continuing without cache")
                self.feature_store = None
            
            # 3. Initialize Rule Engine
            try:
                self.rule_engine = RuleEngine()
                logger.info("✅ Rule Engine initialized")
            except Exception as e:
                logger.error(f"❌ Rule Engine initialization failed: {str(e)}")
                raise
            
            # 4. Initialize Decision Service
            try:
                self.ml_loader = MLModelLoader()
                self.decision_service = DecisionService(
                    self.rule_engine,
                    self.ml_loader,
                    gemini_api_key=os.getenv('GEMINI_API_KEY')
                )
                logger.info("✅ Decision Service initialized")
            except Exception as e:
                logger.error(f"❌ Decision Service initialization failed: {str(e)}")
                raise
            
            # 5. Initialize MinIO Data Lake
            try:
                minio_host = os.getenv('MINIO_HOST', 'minio.storage.svc.cluster.local:9000')
                self.data_lake = MinIODataLake(endpoint=minio_host)
                logger.info("✅ MinIO Data Lake connected")
            except Exception as e:
                logger.warning(f"⚠️  MinIO connection failed: {str(e)} - continuing without data lake")
                self.data_lake = None
            
            logger.info("🚀 4-Layer Pipeline initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ FATAL: Initialization failed: {str(e)}", exc_info=True)
            raise
    
    def map(self, transaction_json: str) -> str:
        """
        🎯 Process transaction through 4-layer pipeline
        
        Input: Raw transaction JSON from Kafka
        Output: Decision result JSON with routing information
        """
        try:
            # ==================== A. DATA INGESTION ====================
            transaction = json.loads(transaction_json)
            cc_num = transaction.get('cc_num', 'UNKNOWN')
            
            # ==================== B. STREAM PROCESSING ====================
            
            # Step 1: DATA ENRICHMENT (Redis)
            enriched_event = transaction.copy()
            
            if self.feature_store:
                try:
                    # Get user profile
                    user_profile = self.feature_store.get_user_profile(cc_num)
                    if user_profile:
                        enriched_event['user_profile'] = user_profile
                    
                    # Get user history
                    history_24h = self.feature_store.get_user_history(cc_num, '24h')
                    if history_24h:
                        enriched_event['user_history_24h'] = history_24h
                    
                    history_7d = self.feature_store.get_user_history(cc_num, '7d')
                    if history_7d:
                        enriched_event['user_history_7d'] = history_7d
                    
                    # Get location history
                    locations = self.feature_store.get_location_history(cc_num, 10)
                    if locations:
                        enriched_event['recent_locations'] = locations
                    
                    # Get merchant info
                    merchant_id = transaction.get('merchant_id', transaction.get('merchant', ''))
                    merchant_info = self.feature_store.get_merchant_info(merchant_id)
                    if merchant_info:
                        enriched_event['merchant_info'] = merchant_info
                    
                    # Check blacklist
                    if self.feature_store.is_user_blacklisted(cc_num):
                        enriched_event['is_blacklisted'] = True
                
                except Exception as e:
                    logger.debug(f"⚠️  Enrichment error: {str(e)}")
            
            # Step 2: MAKE DECISION (Rules + ML + LLM)
            decision_result = self.decision_service.decide(enriched_event)
            
            # Step 3: COMBINE RESULTS
            final_result = {
                'status': 'SUCCESS',
                'transaction_id': transaction.get('transaction_id', 'unknown'),
                'cc_num': cc_num,
                'amt': transaction.get('amt', 0),
                'merchant': transaction.get('merchant', 'UNKNOWN'),
                'timestamp_transaction': transaction.get('trans_date_trans_time', ''),
                
                # Decision components
                'rule_evaluation': decision_result.get('rule_evaluation', {}),
                'ml_evaluation': decision_result.get('ml_evaluation', {}),
                
                'combined_score': decision_result.get('combined_score', 0),
                'final_decision': decision_result.get('final_decision', 'ALLOW'),
                'confidence': decision_result.get('confidence', 0),
                
                'alert_message': decision_result.get('alert_message', ''),
                
                'timestamp_decision': decision_result.get('timestamp_decision', ''),
                
                # Flink metadata
                'flink_instance': runtime_context.get_task_name() if hasattr(self, '_runtime_context') else 'unknown',
                'processing_timestamp': datetime.utcnow().isoformat()
            }
            
            # ==================== C. STORAGE & ROUTING ====================
            
            # Save to MinIO data lake
            if self.data_lake:
                try:
                    self.data_lake.save_raw_transaction(transaction)
                    self.data_lake.save_processed_transaction(final_result)
                except Exception as e:
                    logger.debug(f"⚠️  MinIO save error: {str(e)}")
            
            # Update stats
            self.stats['processed'] += 1
            decision = final_result['final_decision']
            if decision == 'BLOCK':
                self.stats['fraud'] += 1
            elif decision == 'REVIEW':
                self.stats['review'] += 1
            else:
                self.stats['clean'] += 1
            
            # Log sample transactions (every 100th)
            if self.stats['processed'] % 100 == 0:
                elapsed = time.time() - self.stats['start_time']
                throughput = self.stats['processed'] / elapsed if elapsed > 0 else 0
                logger.info(
                    f"📊 Stats - Processed: {self.stats['processed']} | "
                    f"Fraud: {self.stats['fraud']} | Review: {self.stats['review']} | "
                    f"Clean: {self.stats['clean']} | Throughput: {throughput:.1f} txn/s"
                )
            
            # Return with routing prefix for Kafka sink
            return json.dumps(final_result, ensure_ascii=False)
        
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON decode error: {str(e)}")
            self.stats['errors'] += 1
            error_result = {
                'status': 'ERROR',
                'message': f'JSON decode error: {str(e)}',
                'timestamp': datetime.utcnow().isoformat()
            }
            return json.dumps(error_result)
        
        except Exception as e:
            logger.error(f"❌ Processing error: {str(e)}", exc_info=True)
            self.stats['errors'] += 1
            error_result = {
                'status': 'ERROR',
                'message': f'Processing error: {str(e)}',
                'timestamp': datetime.utcnow().isoformat()
            }
            return json.dumps(error_result)


class RoutingFilter(FilterFunction):
    """
    Filter function to split streams by decision
    """
    
    def __init__(self, target_decision: str):
        """
        target_decision: 'BLOCK' | 'REVIEW' | 'ALLOW' | 'ERROR'
        """
        self.target_decision = target_decision
    
    def filter(self, result_json: str) -> bool:
        """Return True if result matches target decision"""
        try:
            result = json.loads(result_json)
            
            if self.target_decision == 'ERROR':
                return result.get('status') != 'SUCCESS'
            else:
                return result.get('final_decision') == self.target_decision
        except:
            return self.target_decision == 'ERROR'


def create_fraud_detection_pipeline() -> StreamExecutionEnvironment:
    """
    🔥 Create 4-Layer Flink Fraud Detection Pipeline
    
    Features:
    - Exactly-once semantics via checkpointing
    - 3-topic routing (fraud/review/clean/error)
    - Distributed processing (Kubernetes parallelism)
    - Real-time stream enrichment with Redis
    - Business rules evaluation
    - ML inference with LLM alerts
    - Audit trail in MinIO
    """
    
    # ==================== ENVIRONMENT SETUP ====================
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Enable checkpointing for exactly-once semantics
    env.enable_changelog_checkpointing()
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_checkpoint_interval(10000)  # 10 seconds
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)  # 5 seconds
    env.get_checkpoint_config().set_checkpoint_timeout(60000)  # 60 seconds
    env.get_checkpoint_config().enable_external_file_cleanup()
    
    logger.info("✅ Checkpoint configured: exactly-once semantics")
    
    # ==================== KAFKA CONSUMER ====================
    kafka_bootstrap_servers = os.getenv(
        'KAFKA_BOOTSTRAP_SERVERS',
        'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'
    )
    
    kafka_consumer = FlinkKafkaConsumer(
        topics='transactions.raw',
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': 'flink-fraud-detector-group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': 'true',
            'auto.commit.interval.ms': '1000',
        }
    )
    
    # Watermark strategy for out-of-order events
    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()
    kafka_stream = env.add_source(kafka_consumer).assign_timestamps_and_watermarks(watermark_strategy)
    
    logger.info("✅ Kafka Consumer configured")
    
    # ==================== 4-LAYER PROCESSING ====================
    fraud_results = kafka_stream.map(FourLayerFraudDetectionMapFunction()) \
        .name("4-Layer-Fraud-Detection") \
        .uid("4-layer-fraud-detection")
    
    logger.info("✅ 4-Layer processing map function added")
    
    # ==================== STREAM ROUTING ====================
    # Split into 4 output streams based on decision
    
    fraud_stream = fraud_results \
        .filter(RoutingFilter('BLOCK')) \
        .name("Filter-Fraud") \
        .uid("filter-fraud")
    
    review_stream = fraud_results \
        .filter(RoutingFilter('REVIEW')) \
        .name("Filter-Review") \
        .uid("filter-review")
    
    clean_stream = fraud_results \
        .filter(RoutingFilter('ALLOW')) \
        .name("Filter-Clean") \
        .uid("filter-clean")
    
    error_stream = fraud_results \
        .filter(RoutingFilter('ERROR')) \
        .name("Filter-Error") \
        .uid("filter-error")
    
    logger.info("✅ Stream filters configured for 4 output topics")
    
    # ==================== KAFKA PRODUCERS ====================
    def create_kafka_producer(topic_name: str) -> FlinkKafkaProducer:
        """Create Kafka producer for specific topic"""
        return FlinkKafkaProducer(
            topic=topic_name,
            serialization_schema=SimpleStringSchema(),
            producer_config={
                'bootstrap.servers': kafka_bootstrap_servers,
                'acks': 'all',  # Wait for all brokers
                'retries': '3',
                'linger.ms': '100',  # Batch messages
                'client.id': f'flink-fraud-producer-{topic_name}',
            }
        )
    
    # Add sinks for each output topic
    fraud_stream.add_sink(create_kafka_producer('transactions.fraud')) \
        .name("Sink-Fraud") \
        .uid("sink-fraud")
    
    review_stream.add_sink(create_kafka_producer('transactions.review')) \
        .name("Sink-Review") \
        .uid("sink-review")
    
    clean_stream.add_sink(create_kafka_producer('transactions.clean')) \
        .name("Sink-Clean") \
        .uid("sink-clean")
    
    error_stream.add_sink(create_kafka_producer('transactions.error')) \
        .name("Sink-Error") \
        .uid("sink-error")
    
    logger.info("✅ Kafka producers configured for 4 output topics")
    
    logger.info("🔥 Pipeline setup complete! Ready to execute.")
    
    return env


if __name__ == "__main__":
    try:
        logger.info("🚀 Starting Flink 4-Layer Fraud Detection Pipeline...")
        
        env = create_fraud_detection_pipeline()
        
        job_name = "Fraud-Detection-4-Layer-Pipeline"
        logger.info(f"📊 Executing job: {job_name}")
        
        env.execute(job_name)
        
    except Exception as e:
        logger.error(f"❌ FATAL: Pipeline execution failed: {str(e)}", exc_info=True)
        sys.exit(1)
