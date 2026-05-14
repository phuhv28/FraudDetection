package org.example.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.model.DetectionResult;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Sink: Action Layer — Kafka Alert
 *
 * Gửi cảnh báo gian lận vào topic "fraud-alerts" để:
 *   - Backend service lắng nghe và khóa giao dịch ngay lập tức
 *   - Grafana hiển thị alert real-time
 *
 * Message format (JSON):
 * {
 *   "trans_num"       : "abc123",
 *   "cc_num"          : "**** **** **** 1234",
 *   "score"           : 0.87,
 *   "is_fraud_ml"     : true,
 *   "rule_triggered"  : false,
 *   "rule_description": "",
 *   "timestamp_ms"    : 1700000000000
 * }
 *
 * cc_num được MASK trước khi gửi — không bao giờ ghi thẻ gốc vào Kafka log.
 */
public class KafkaAlertSink extends RichSinkFunction<DetectionResult> {

    private static final String ALERT_TOPIC = "fraud-alerts";

    private final String bootstrapServers;
    private transient KafkaProducer<String, String> producer;
    private transient ObjectMapper mapper;

    public KafkaAlertSink(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public void open(Configuration parameters) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");           // at-least-once cho alert
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");      // batch nhỏ để giảm latency
        props.put(ProducerConfig.RETRIES_CONFIG, "3");

        this.producer = new KafkaProducer<>(props);
        this.mapper   = new ObjectMapper();
    }

    @Override
    public void invoke(DetectionResult result, Context context) throws Exception {
        // Mask số thẻ — chỉ giữ 4 chữ số cuối
        String maskedCard = result.ccNum != null && result.ccNum.length() > 4
                ? "**** **** **** " + result.ccNum.substring(result.ccNum.length() - 4)
                : result.ccNum;

        Map<String, Object> alert = new HashMap<>();
        alert.put("trans_num",        result.transNum);
        alert.put("cc_num",           maskedCard);
        alert.put("score",            result.score);
        alert.put("is_fraud_ml",      result.isFraud);
        alert.put("rule_triggered",   result.ruleTriggered);
        alert.put("rule_description", result.ruleDescription);
        alert.put("timestamp_ms",     System.currentTimeMillis());

        String payload = mapper.writeValueAsString(alert);

        // Key = trans_num để backend idempotent dedup nếu cần
        producer.send(new ProducerRecord<>(ALERT_TOPIC, result.transNum, payload));
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }
}
