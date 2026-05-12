"""
🔥 FLINK REAL-TIME FRAUD DETECTION PIPELINE 🔥

Xử lý transaction stream từ Kafka theo real-time bằng Apache Flink.
Flow:
  Kafka (transactions.raw) 
    → Flink Deserialize 
    → FraudDetector Process 
    → Flink Serialize 
    → Kafka Output Topics
      - transactions.fraud (chứa fraud transactions)
      - transactions.clean (chứa legitimate transactions)
"""

import json
import sys
import os
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add fraud_detector module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'fraud_detector'))

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction, SinkFunction
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema, JsonSchema
from pyflink.common.typeinfo import TypeInformation

from fraud_detector import FraudDetector


class FraudDetectionMapFunction(MapFunction):
    """
    Hàm xử lý transaction qua FraudDetector.
    - Input: Raw transaction JSON từ Kafka
    - Output: Kết quả fraud detection + shap explanation
    """
    
    def __init__(self):
        self.fraud_detector = None
    
    def open(self, runtime_context):
        """Khởi tạo FraudDetector (Gọi 1 lần lúc bật job)"""
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
            logger.info("✅ FraudDetector khởi tạo thành công")
        except Exception as e:
            logger.error(f"❌ Lỗi khởi tạo FraudDetector: {str(e)}")
            raise
    
    def map(self, transaction_json):
        """
        Xử lý 1 transaction qua fraud detector.
        
        Args:
            transaction_json: JSON string chứa transaction data
        
        Returns:
            JSON string chứa kết quả fraud detection
        """
        try:
            # Deserialize transaction từ JSON string
            transaction = json.loads(transaction_json)
            
            # Gọi fraud detector
            result = self.fraud_detector.process_and_predict(transaction)
            
            # Thêm metadata
            result['timestamp'] = datetime.utcnow().isoformat()
            result['cc_num'] = transaction.get('cc_num', 'UNKNOWN')
            result['amt'] = transaction.get('amt', 0)
            result['merchant'] = transaction.get('merchant', 'UNKNOWN')
            
            return json.dumps(result, ensure_ascii=False)
        
        except json.JSONDecodeError as e:
            logger.error(f"❌ Lỗi deserialize JSON: {str(e)}")
            return json.dumps({
                'status': 'ERROR',
                'message': f'JSON decode error: {str(e)}',
                'timestamp': datetime.utcnow().isoformat()
            })
        except Exception as e:
            logger.error(f"❌ Lỗi xử lý transaction: {str(e)}")
            return json.dumps({
                'status': 'ERROR',
                'message': f'Processing error: {str(e)}',
                'timestamp': datetime.utcnow().isoformat()
            })


class RoutingMapFunction(MapFunction):
    """
    Hàm phân loại kết quả:
    - is_fraud=1 → Gửi vào transactions.fraud
    - is_fraud=0 → Gửi vào transactions.clean
    
    Output có format: (destination_topic, message)
    """
    
    def map(self, result_json):
        """
        Phân loại result sang 2 topic khác nhau.
        """
        try:
            result = json.loads(result_json)
            
            if result.get('status') == 'SUCCESS':
                is_fraud = result.get('is_fraud', 0)
                destination = 'transactions.fraud' if is_fraud else 'transactions.clean'
            else:
                destination = 'transactions.error'
            
            # Return tuple (destination, message) để Flink biết gửi vào topic nào
            return f"{destination}|{result_json}"
        
        except Exception as e:
            logger.error(f"❌ Lỗi routing: {str(e)}")
            return f"transactions.error|{result_json}"


class DualKafkaProducerSink:
    """
    Custom Sink để gửi message vào 2 topic khác nhau dựa vào prefix.
    Format input: "topic_name|message_content"
    """
    
    @staticmethod
    def create_producer(bootstrap_servers='my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'):
        """Tạo Kafka Producer"""
        from confluent_kafka import Producer
        
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'flink-fraud-producer',
            'acks': '1',
            'retries': 3,
            'retry.backoff.ms': 100
        }
        return Producer(conf)


def create_fraud_detection_pipeline():
    """
    Tạo Flink pipeline cho fraud detection.
    
    Architecture:
    ┌─────────────────┐
    │ Kafka Source    │ (transactions.raw)
    └────────┬────────┘
             │
    ┌────────▼────────┐
    │ FraudDetection  │ (MapFunction - Gọi AI)
    │ Map Function    │
    └────────┬────────┘
             │
    ┌────────▼────────┐
    │ Routing Map     │ (Phân loại fraud vs clean)
    │ Function        │
    └────────┬────────┘
             │
    ┌────────▼────────┐
    │ Kafka Sink      │ (Multiple topics)
    │ - fraud         │
    │ - clean         │
    │ - error         │
    └─────────────────┘
    """
    
    # 1. Khởi tạo Flink Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Enable checkpoint để không mất data nếu fail
    env.enable_changelog_checkpointing()
    
    # 2. Cấu hình Kafka Consumer
    kafka_bootstrap_servers = os.getenv(
        'KAFKA_BOOTSTRAP_SERVERS',
        'my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092'
    )
    
    kafka_consumer = FlinkKafkaConsumer(
        topics='transactions.raw',
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': kafka_bootstrap_servers,
            'group.id': 'flink-fraud-detector',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': 'true',
        }
    )
    
    # 3. Tạo Kafka Producer cho 3 output topics
    def create_kafka_producer_for_topic(topic_name):
        """Helper tạo Kafka producer cho specific topic"""
        return FlinkKafkaProducer(
            topic=topic_name,
            serialization_schema=SimpleStringSchema(),
            producer_config={
                'bootstrap.servers': kafka_bootstrap_servers,
                'acks': '1',
                'retries': 3,
                'client.id': f'flink-fraud-producer-{topic_name}'
            }
        )
    
    # 4. Read từ Kafka
    kafka_stream = env.add_source(kafka_consumer)
    
    # 5. Xử lý fraud detection
    fraud_results = kafka_stream.map(FraudDetectionMapFunction())
    
    # 6. Routing để gửi vào topic khác nhau
    # Split stream thành 3 branches
    fraud_stream = fraud_results.filter(
        lambda x: json.loads(x).get('is_fraud', 0) == 1 
        if json.loads(x).get('status') == 'SUCCESS' 
        else False
    )
    clean_stream = fraud_results.filter(
        lambda x: json.loads(x).get('is_fraud', 0) == 0 
        if json.loads(x).get('status') == 'SUCCESS' 
        else False
    )
    error_stream = fraud_results.filter(
        lambda x: json.loads(x).get('status') != 'SUCCESS'
    )
    
    # 7. Send to Kafka topics
    fraud_stream.add_sink(create_kafka_producer_for_topic('transactions.fraud'))
    clean_stream.add_sink(create_kafka_producer_for_topic('transactions.clean'))
    error_stream.add_sink(create_kafka_producer_for_topic('transactions.error'))
    
    logger.info("🔥 Pipeline setup xong, sẵn sàng chạy!")
    
    return env


if __name__ == "__main__":
    try:
        env = create_fraud_detection_pipeline()
        
        logger.info("🚀 Khởi động Flink Fraud Detection Pipeline...")
        env.execute("Fraud Detection Pipeline")
        
    except Exception as e:
        logger.error(f"❌ Lỗi chạy pipeline: {str(e)}", exc_info=True)
        sys.exit(1)
