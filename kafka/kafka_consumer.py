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

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Add fraud_detector module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'fraud_detector'))

from fraud_detector import FraudDetector


class FraudDetectionConsumer:
    """
    Consumer chính xử lý fraud detection.
    """
    
    def __init__(self, bootstrap_servers=None, group_id='fraud-detector-consumer'):
        """
        Khởi tạo consumer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            group_id: Consumer group ID
        """
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            'KAFKA_BOOTSTRAP_SERVERS',
            'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'
        )
        self.group_id = group_id
        
        # Khởi tạo consumer
        self.consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',  # Lấy từ đầu nếu consumer mới
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
            'session.timeout.ms': 30000
        }
        
        self.consumer = Consumer(self.consumer_config)
        
        # Khởi tạo producer để gửi result
        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': 'fraud-detector-producer',
            'acks': '1',
            'retries': 3,
            'retry.backoff.ms': 100
        }
        self.producer = Producer(self.producer_config)
        
        # Khởi tạo fraud detector
        self._initialize_detector()
        
        # Stats
        self.total_processed = 0
        self.total_fraud = 0
        self.total_clean = 0
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
        Xử lý 1 transaction qua detector.
        
        Returns:
            (destination_topic, result_json, is_success)
        """
        try:
            transaction = json.loads(transaction_json)
            
            # Gọi fraud detector
            result = self.detector.process_and_predict(transaction)
            
            # Thêm metadata
            result['timestamp'] = datetime.utcnow().isoformat()
            result['cc_num'] = transaction.get('cc_num', 'UNKNOWN')
            
            # Xác định destination topic
            if result.get('status') == 'SUCCESS':
                destination = 'transactions.fraud' if result.get('is_fraud') else 'transactions.clean'
            else:
                destination = 'transactions.error'
            
            return destination, json.dumps(result, ensure_ascii=False), True
        
        except Exception as e:
            logger.error(f"❌ Lỗi xử lý: {str(e)}")
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
        logger.info("=" * 60)
        logger.info("📊 THỐNG KÊ FRAUD DETECTION")
        logger.info(f"  Total Processed: {self.total_processed}")
        logger.info(f"  Fraud Detected: {self.total_fraud}")
        logger.info(f"  Clean Transactions: {self.total_clean}")
        logger.info(f"  Errors: {self.total_errors}")
        if self.total_processed > 0:
            fraud_rate = (self.total_fraud / self.total_processed) * 100
            logger.info(f"  Fraud Rate: {fraud_rate:.2f}%")
        logger.info("=" * 60)
    
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
                
                # Chạy fraud detector
                destination, result_json, is_success = self._process_transaction(transaction_json)
                
                # Update stats
                self.total_processed += 1
                if is_success:
                    result = json.loads(result_json)
                    if result.get('is_fraud'):
                        self.total_fraud += 1
                        logger.warning(f"🚨 FRAUD DETECTED! {result_json[:100]}...")
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
