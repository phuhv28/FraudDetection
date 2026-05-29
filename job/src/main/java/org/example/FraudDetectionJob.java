package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.model.DetectionResult;
import org.example.operator.*;
import org.example.sink.KafkaAlertSink;
import org.example.sink.MinIOSink;

import java.util.Map;

import io.prometheus.client.exporter.HTTPServer;


/**
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║            Kappa Architecture — Real-time Fraud Detection        ║
 * ╠══════════════════════════════════════════════════════════════════╣
 * ║                                                                  ║
 * ║  [Kafka: transactions]                                           ║
 * ║        │                                                         ║
 * ║   ParseOperator          ─── branch ──► MinIOSink (Data Lake)    ║
 * ║        │                                                         ║
 * ║   ExtractFeaturesOperator  (hour, dayofweek, age, distance)      ║
 * ║        │                                                         ║
 * ║   EncodeOperator           (category, gender, state → int)       ║
 * ║        │                                                         ║
 * ║   keyBy(cc_num)                                                  ║
 * ║        │                                                         ║
 * ║   WindowAggregationFunction  (24h & 7d stats, write → Redis)     ║
 * ║        │                                                         ║
 * ║   keyBy(cc_num)                                                  ║
 * ║        │                                                         ║
 * ║   RuleEngineFunction       (amt limit, geo-velocity check)       ║
 * ║        │                                                         ║
 * ║   XGBoostInferenceMap      (ML fraud score)                      ║
 * ║        │                                                         ║
 * ║   filter(fraud || rule)                                          ║
 * ║        │                                                         ║
 * ║   [Kafka: fraud-alerts]  ──► Backend khóa giao dịch             ║
 * ║                                                                  ║
 * ╚══════════════════════════════════════════════════════════════════╝
 */
public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {

//        HTTPServer server = new HTTPServer(8000);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000); // checkpoint mỗi 30 giây để đảm bảo exactly-once

        ParameterTool params = ParameterTool.fromArgs(args);
        String bootstrapServers = params.get("bootstrap.servers", "localhost:9092");
        String redisHost         = params.get("redis.host",        "localhost");
        int    redisPort         = params.getInt("redis.port",     6379);
        String minioEndpoint     = params.get("minio.endpoint",    "http://localhost:9000");
        String minioBucket       = params.get("minio.bucket",      "fraud-data-lake");
        String minioAccessKey    = params.get("minio.access-key",  "minioadmin");
        String minioSecretKey    = params.get("minio.secret-key",  "minioadmin");

        // ── A. Data Ingestion ─────────────────────────────────────────────────
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("transactions")
                .setGroupId("fraud-detection-v1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // ── B. Stream Processing ──────────────────────────────────────────────

        // Layer 1: Parse — JSON string → Map<String, Object>
        DataStream<Map<String, Object>> parsed = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(new ParseOperator())
                .name("parse");

        // ── C. Serving & Storage: Data Lake ───────────────────────────────────
        // Lưu toàn bộ raw event vào MinIO ngay sau khi parse,
        // trước bất kỳ transform nào để đảm bảo tính toàn vẹn dữ liệu.
        parsed.addSink(new MinIOSink(minioEndpoint, minioBucket, minioAccessKey, minioSecretKey))
                .name("minio-data-lake");

        // ── B. Stream Processing (tiếp) ───────────────────────────────────────

        // Layer 2: Extract temporal + geographic features
        // Layer 3: Encode categoricals
        DataStream<Map<String, Object>> enriched = parsed
                .map(new ExtractFeaturesOperator())
                .name("extract-features")
                .map(new EncodeOperator())
                .name("encode-categoricals");

        // keyBy cc_num → mọi event của cùng 1 thẻ vào cùng 1 task slot
        // Layer 4: Stateful window aggregation + write-back feature vector vào Redis
        DataStream<Map<String, Object>> windowed = enriched
                .keyBy(event -> event.get("cc_num").toString())
                .process(new WindowAggregationFunction(redisHost, redisPort))
                .name("window-aggregation");

        // keyBy lại để RuleEngineFunction có keyed state riêng
        // Layer 5: Rule-based engine — kiểm tra luật cứng trước ML
        DataStream<Map<String, Object>> ruleApplied = windowed
                .keyBy(event -> event.get("cc_num").toString())
                .process(new RuleEngineFunction())
                .name("rule-engine");

        // Layer 6: ML Inference — tính fraud score bằng XGBoost
//        DataStream<DetectionResult> results = ruleApplied
//                .map(new XGBoostInferenceMap())
//                .name("xgboost-inference");

        DataStream<DetectionResult> results = ruleApplied
                .map(new XGBoostInferenceMap())
                .map(new FraudMetricsMap())
                .name("fraud-metrics");

        // ── D. Action & Dashboard ─────────────────────────────────────────────
        // Chỉ emit khi fraud score vượt ngưỡng HOẶC có rule bị vi phạm
        results
                .filter(r -> r.isFraud || r.ruleTriggered)
                .addSink(new KafkaAlertSink(bootstrapServers))
                .name("kafka-fraud-alerts");

        env.execute("Real-time Fraud Detection — Kappa Architecture");
    }
}
