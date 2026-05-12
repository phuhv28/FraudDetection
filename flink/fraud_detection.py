import json
import os
import logging
from datetime import datetime

from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.datastream.functions import ProcessFunction
from pyflink.common.serialization import SimpleStringSchema

# 1. Định nghĩa Output Tags để tách luồng (Side Outputs)
FRAUD_TAG = OutputTag("fraud", Types.STRING())
CLEAN_TAG = OutputTag("clean", Types.STRING())
ERROR_TAG = OutputTag("error", Types.STRING())

class FraudDetectionProcessFunction(ProcessFunction):
    def __init__(self):
        self.detector = None

    def open(self, runtime_context):
        # Chú ý: Model phải được đóng gói vào Docker image hoặc mount volume vào /app
        from fraud_detector import FraudDetector
        model_path = "/app/fraud_detector/best_fraud_model_096.json"
        encoder_path = "/app/fraud_detector/label_encoders.json"
        
        self.detector = FraudDetector(
            model_path=model_path,
            encoder_path=encoder_path,
            threshold=0.2426
        )

    def process_element(self, value, ctx):
        try:
            transaction = json.loads(value)
            # Gọi AI Inference
            result = self.detector.process_and_predict(transaction)
            result['processed_at'] = datetime.utcnow().isoformat()
            
            output_json = json.dumps(result)
            
            # Tách luồng dựa trên kết quả
            if result.get('is_fraud') == 1:
                yield FRAUD_TAG, output_json
            else:
                yield CLEAN_TAG, output_json
                
        except Exception as e:
            yield ERROR_TAG, json.dumps({"error": str(e), "raw": value})

def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Nạp các file JAR connector (đã tải ở bước trước)
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    jar_files = [f"file://{os.path.join(curr_dir, 'lib', x)}" for x in os.listdir('lib') if x.endswith('.jar')]
    env.add_jars(*jar_files)

    # 2. Cấu hình Kafka Source (API mới)
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092") \
        .set_topics("transactions.raw") \
        .set_group_id("flink-fraud-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # 3. Xử lý chính và Tách luồng
    processed_stream = stream.process(FraudDetectionProcessFunction())

    # 4. Định nghĩa Kafka Sinks cho từng loại
    def create_kafka_sink(topic):
        return KafkaSink.builder() \
            .set_bootstrap_servers("my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092") \
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                    .set_topic(topic)
                    .set_value_serialization_schema(SimpleStringSchema())
                    .build()
            ) \
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
            .build()

    # Gửi dữ liệu ra các Topic tương ứng
    processed_stream.get_side_output(FRAUD_TAG).sink_to(create_kafka_sink("transactions.fraud"))
    processed_stream.get_side_output(CLEAN_TAG).sink_to(create_kafka_sink("transactions.clean"))
    processed_stream.get_side_output(ERROR_TAG).sink_to(create_kafka_sink("transactions.error"))

    # 5. Lưu toàn bộ kết quả sạch vào MinIO Data Lake (S3 Sink)
    # (Bạn có thể dùng FileSink tương tự như Table API đã hướng dẫn ở trên)

    env.execute("Real-time Fraud Detection with AI")

if __name__ == "__main__":
    build_pipeline()