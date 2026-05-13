package org.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;

public class FraudDetectionFinalJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);
        String bootstrapServers = params.get("bootstrap.servers", "192.168.1.7:9092");
        // 1. Cấu hình Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("transactions")
                .setGroupId("fraud-detection-v1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 2. Xây dựng Pipeline
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(new JsonToMapParser())        // Parser tối ưu, tự nhận diện TypeInfo
                .map(new XGBoostInferenceMap())   // Inference logic
                .filter(res -> res.isFraud)        // Chỉ lọc ra các giao dịch gian lận
                .print();

        env.execute("XGBoost Fraud Detection Job");
    }

    /**
     * Lớp 1: Chuyển đổi JSON String sang Map
     */
    public static class JsonToMapParser extends RichMapFunction<String, Map<String, Object>> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            this.mapper = new ObjectMapper();
        }

        @Override
        public Map<String, Object> map(String value) throws Exception {
            return mapper.readValue(value, new TypeReference<Map<String, Object>>() {
            });
        }
    }

    /**
     * Lớp 2: Thực thi mô hình XGBoost
     */
    public static class XGBoostInferenceMap extends RichMapFunction<Map<String, Object>, DetectionResult> {

        // Đánh dấu transient để Flink không serialize các đối tượng này
        private transient Booster booster;
        private transient Map<String, Map<String, Integer>> encoders;
        private transient DateTimeFormatter formatter;

        private final double threshold = 0.2426;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 1. Khởi tạo Formatter (Class này không Serializable nên phải khởi tạo ở đây)
            this.formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            // 2. Load XGBoost Model từ đường dẫn local của bạn
            String modelPath = "/mnt/models/best_fraud_model_096.json";
            this.booster = XGBoost.loadModel(modelPath);

            // 3. Load Label Encoders
            String encoderPath = "/mnt/models/label_encoders.json";
            ObjectMapper mapper = new ObjectMapper();
            this.encoders = mapper.readValue(new File(encoderPath),
                    new TypeReference<Map<String, Map<String, Integer>>>() {
                    });
        }

        @Override
        public DetectionResult map(Map<String, Object> event) throws Exception {
            try {
                // --- Feature Engineering ---
                // Parse thời gian giao dịch và ngày sinh
                LocalDateTime transTime = LocalDateTime.parse(event.get("trans_date_trans_time").toString(), formatter);
                // Thêm đuôi giờ cho dob để parse chuẩn LocalDateTime
                LocalDateTime dob = LocalDateTime.parse(event.get("dob").toString() + " 00:00:00", formatter);

                double lat = Double.parseDouble(event.get("lat").toString());
                double lon = Double.parseDouble(event.get("long").toString());
                double mLat = Double.parseDouble(event.get("merch_lat").toString());
                double mLon = Double.parseDouble(event.get("merch_long").toString());

                // Khoảng cách Euclide
                float distance = (float) Math.sqrt(Math.pow(lat - mLat, 2) + Math.pow(lon - mLon, 2));

                double amt = Double.parseDouble(event.get("amt").toString());
                double amtSum7d = Double.parseDouble(event.getOrDefault("amt_sum_7d", amt).toString());
                double transCount7d = Double.parseDouble(event.getOrDefault("trans_count_7d", 1.0).toString());
                float amtVsAvg7d = (float) (amt / (amtSum7d / (transCount7d + 1e-5) + 1e-5));

                // --- Vector hóa dữ liệu (Feature Ordering) ---
                float[] features = new float[]{
                        encoders.get("category").getOrDefault(event.get("category").toString(), -1).floatValue(),
                        (float) amt,
                        encoders.get("gender").getOrDefault(event.get("gender").toString(), -1).floatValue(),
                        encoders.get("state").getOrDefault(event.get("state").toString(), -1).floatValue(),
                        (float) lat,
                        (float) lon,
                        Float.parseFloat(event.get("city_pop").toString()),
                        (float) mLat,
                        (float) mLon,
                        (float) transTime.getHour(),
                        (float) (transTime.getDayOfWeek().getValue() - 1), // Monday = 0
                        (float) ChronoUnit.YEARS.between(dob, transTime),
                        distance,
                        Float.parseFloat(event.getOrDefault("trans_count_24h", 1.0).toString()),
                        Float.parseFloat(event.getOrDefault("amt_sum_24h", amt).toString()),
                        Float.parseFloat(event.getOrDefault("trans_count_7d", 1.0).toString()),
                        Float.parseFloat(event.getOrDefault("amt_sum_7d", amt).toString()),
                        amtVsAvg7d
                };

                // --- Inference ---
                DMatrix dMatrix = new DMatrix(features, 1, features.length, Float.NaN);
                try {
                    float[][] predictions = booster.predict(dMatrix);
                    double score = predictions[0][0];
                    return new DetectionResult(score, score >= threshold);
                } finally {
                    // Rất quan trọng: Giải phóng bộ nhớ native của DMatrix ngay lập tức
                    dMatrix.dispose();
                }
            } catch (Exception e) {
                // Log lỗi hoặc trả về một kết quả mặc định để không làm sập luồng stream
                System.err.println("Error processing record: " + e.getMessage());
                return new DetectionResult(0.0, false);
            }
        }

        @Override
        public void close() throws Exception {
            // Giải phóng Booster khi TaskManager đóng job
            if (booster != null) {
                booster.dispose();
            }
        }
    }

    /**
     * Lớp 3: Kết quả dự đoán
     */
    public static class DetectionResult {
        public double score;
        public boolean isFraud;

        public DetectionResult() {}

        public DetectionResult(double score, boolean isFraud) {
            this.score = score;
            this.isFraud = isFraud;
        }

        @Override
        public String toString() {
            return "Fraud Alert! Score: " + score;
        }
    }
}