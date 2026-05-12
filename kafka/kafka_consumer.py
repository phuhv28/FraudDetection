"""
🎯 KAFKA CONSUMER + FRAUD DETECTOR
Standalone consumer - đơn giản hơn Flink, dễ deploy.

Flow:
  1. Lắng nghe topic transactions.raw trên Kafka
  2. Nhận transaction
  3. Gửi qua FraudDetector
  4. Publish result vào 3 topic:
     - transactions.fraud (fraud detected)
     - transactions.clean (legitimate)
     - transactions.error (processing error)
"""

import json
import sys
import os
import logging
from datetime import datetime
from confluent_kafka import Consumer, Producer, KafkaError
import redis

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Add fraud_detector module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'fraud_detector'))

from fraud_detector import FraudDetector
from redis_feature_store import RedisFeatureStore
from rule_engine import RuleEngine
from minio_data_lake import MinIODataLake
from decision_service import DecisionService, MLModelLoader


class FraudDetectionConsumer:
    """
    🎯 Integrated 4-Layer Consumer:
    A. Data Ingestion (Kafka)
    B. Stream Processing (Redis enrichment + Rule Engine + ML)
    C. Serving & Storage (Output topics + MinIO)
    """
    
    def __init__(self, bootstrap_servers=None, group_id='fraud-detector-consumer'):
        """
        Khởi tạo consumer với đầy đủ components.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            group_id: Consumer group ID
        """
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            'KAFKA_BOOTSTRAP_SERVERS',
            'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'
        )
        self.group_id = group_id
        
        # ==================== LAYER A: DATA INGESTION ====================
        # Kafka Consumer
        self.consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
            'session.timeout.ms': 30000
        }
        
        self.consumer = Consumer(self.consumer_config)
        
        # Kafka Producer (for output topics)
        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'fraud-detector-producer',
            'acks': '1',
            'retries': 3,
            'retry.backoff.ms': 100
        }
        self.producer = Producer(self.producer_config)
        
        # ==================== LAYER B: STREAM PROCESSING ====================
        # Initialize AI model
        self._initialize_detector()
        
        # Redis Feature Store (for enrichment)
        try:
            redis_host = os.getenv('REDIS_HOST', 'redis.storage.svc.cluster.local')
            self.feature_store = RedisFeatureStore(host=redis_host)
            logger.info("✅ Redis Feature Store connected")
        except Exception as e:
            logger.warning(f"⚠️  Redis connection failed: {str(e)} - continuing without cache")
            self.feature_store = None
        
        # Rule Engine
        self.rule_engine = RuleEngine()
        logger.info("✅ Rule Engine initialized")
        
        # Decision Service
        self.ml_loader = MLModelLoader()
        self.decision_service = DecisionService(
            self.rule_engine,
            self.ml_loader,
            gemini_api_key=os.getenv('GEMINI_API_KEY')
        )
        logger.info("✅ Decision Service initialized")
        
        # ==================== LAYER C: STORAGE ====================
        # MinIO Data Lake
        try:
            minio_host = os.getenv('MINIO_HOST', 'minio.storage.svc.cluster.local:9000')
            self.data_lake = MinIODataLake(endpoint=minio_host)
            logger.info("✅ MinIO Data Lake connected")
        except Exception as e:
            logger.warning(f"⚠️  MinIO connection failed: {str(e)} - continuing without data lake")
            self.data_lake = None
        
        # Stats
        self.total_processed = 0
        self.total_fraud = 0
        self.total_clean = 0
        self.total_review = 0
        self.total_errors = 0
    
    def _initialize_detector(self):
        """Khởi tạo FraudDetector"""
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
            
            self.detector = FraudDetector(
                model_path=detector_path,
                encoder_path=encoder_path,
                threshold=0.2426
            )
            logger.info("✅ FraudDetector khởi tạo thành công")
        except Exception as e:
            logger.error(f"❌ Lỗi khởi tạo FraudDetector: {str(e)}")
            raise
    
    def _delivery_report(self, err, msg, topic_name):
        """Callback khi gửi message xong"""
        if err is not None:
            logger.error(f"❌ Gửi vào {topic_name} thất bại: {err}")
        else:
            logger.debug(f"✅ Gửi vào {topic_name} thành công")
    
    def _process_transaction(self, transaction_json):
        """
        🎯 Process transaction through 4-layer architecture:
        
        A. Data Ingestion: Parse raw event
        B. Stream Processing:
           1. Enrich with Redis data
           2. Apply Rule Engine
           3. ML inference
           4. Decision logic
        C. Storage: Save to MinIO + route to output topic
        
        Returns:
            (destination_topic, result_json, is_success)
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
                    logger.warning(f"⚠️  Enrichment error: {str(e)}")
            
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
                
                'timestamp_decision': decision_result.get('timestamp_decision', '')
            }
            
            # ==================== C. STORAGE & ROUTING ====================
            
            # Save to MinIO data lake
            if self.data_lake:
                try:
                    self.data_lake.save_raw_transaction(transaction)
                    self.data_lake.save_processed_transaction(final_result)
                except Exception as e:
                    logger.warning(f"⚠️  MinIO save error: {str(e)}")
            
            # Determine destination topic
            decision = final_result['final_decision']
            
            if decision == 'BLOCK':
                destination = 'transactions.fraud'
            elif decision == 'REVIEW':
                destination = 'transactions.review'
            else:  # ALLOW
                destination = 'transactions.clean'
            
            return destination, json.dumps(final_result, ensure_ascii=False), True
        
        except Exception as e:
            logger.error(f"❌ Process error: {str(e)}")
            error_result = {
                'status': 'ERROR',
                'message': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
            return 'transactions.error', json.dumps(error_result), False
    
    def _publish_result(self, topic, message):
        """Publish result vào Kafka"""
        try:
            self.producer.produce(
                topic=topic,
                value=message.encode('utf-8'),
                callback=lambda err, msg: self._delivery_report(err, msg, topic)
            )
            self.producer.poll(0)  # Non-blocking check callbacks
        except Exception as e:
            logger.error(f"❌ Lỗi publish: {str(e)}")
    
    def _print_stats(self):
        """In ra thống kê"""
        logger.info("=" * 70)
        logger.info("📊 FRAUD DETECTION CONSUMER - 4-LAYER PIPELINE STATS")
        logger.info("=" * 70)
        logger.info(f"  ✅ Total Processed:    {self.total_processed}")
        logger.info(f"  🚨 Fraud (BLOCK):      {self.total_fraud}")
        logger.info(f"  ⚠️  Review:             {self.total_review}")
        logger.info(f"  ✔️  Clean (ALLOW):      {self.total_clean}")
        logger.info(f"  ❌ Errors:             {self.total_errors}")
        if self.total_processed > 0:
            fraud_rate = (self.total_fraud / self.total_processed) * 100
            review_rate = (self.total_review / self.total_processed) * 100
            logger.info(f"  📈 Fraud Rate:        {fraud_rate:.2f}%")
            logger.info(f"  📈 Review Rate:       {review_rate:.2f}%")
        logger.info("=" * 70)
    
    def start_consuming(self, topics=None, timeout_ms=30000):
        """
        Bắt đầu lắng nghe và xử lý transactions.
        
        Args:
            topics: List của topics để subscribe. Default: ['transactions.raw']
            timeout_ms: Timeout cho message poll
        """
        if topics is None:
            topics = ['transactions.raw']
        
        logger.info(f"🚀 Bắt đầu lắng nghe topics: {topics}")
        self.consumer.subscribe(topics)
        
        try:
            while True:
                # Poll message
                msg = self.consumer.poll(timeout_ms=timeout_ms)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Hết message - normal case
                        logger.debug("Reached end of partition")
                    else:
                        logger.error(f"❌ Kafka error: {msg.error()}")
                    continue
                
                # Xử lý message
                transaction_json = msg.value().decode('utf-8')
                
                logger.debug(f"📨 Nhận transaction: {transaction_json[:100]}...")
                
                # ==================== PROCESS TRANSACTION ====================
                destination, result_json, is_success = self._process_transaction(transaction_json)
                
                # ==================== UPDATE STATS ====================
                self.total_processed += 1
                if is_success:
                    result = json.loads(result_json)
                    decision = result.get('final_decision', 'ALLOW')
                    
                    if decision == 'BLOCK':
                        self.total_fraud += 1
                        logger.warning(f"🚨 FRAUD BLOCKED! Score: {result.get('combined_score', 0):.1f} | {result.get('cc_num', 'UNKNOWN')[:8]}... → {result.get('merchant', 'UNKNOWN')}")
                    elif decision == 'REVIEW':
                        self.total_review += 1
                        logger.info(f"⚠️  REVIEW NEEDED! Score: {result.get('combined_score', 0):.1f} | {result.get('cc_num', 'UNKNOWN')[:8]}... → {result.get('merchant', 'UNKNOWN')}")
                    else:
                        self.total_clean += 1
                else:
                    self.total_errors += 1
                
                # Publish result
                self._publish_result(destination, result_json)
                
                # In stats mỗi 100 transactions
                if self.total_processed % 100 == 0:
                    self._print_stats()
        
        except KeyboardInterrupt:
            logger.info("⛔ Nhận interrupt signal, đang đóng...")
        finally:
            self._print_stats()
            self.consumer.close()
            self.producer.flush()
            logger.info("🏁 Consumer đóng xong")


def main():
    """Entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Kafka Fraud Detection Consumer')
    parser.add_argument(
        '--bootstrap-servers',
        default='my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092',
        help='Kafka bootstrap servers'
    )
    parser.add_argument(
        '--group-id',
        default='fraud-detector-consumer',
        help='Consumer group ID'
    )
    parser.add_argument(
        '--topics',
        default='transactions.raw',
        help='Comma-separated topics to subscribe'
    )
    
    args = parser.parse_args()
    
    topics = [t.strip() for t in args.topics.split(',')]
    
    consumer = FraudDetectionConsumer(
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id
    )
    
    consumer.start_consuming(topics=topics)


if __name__ == '__main__':
    main()
