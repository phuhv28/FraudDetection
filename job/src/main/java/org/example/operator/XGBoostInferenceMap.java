package org.example.operator;

import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.example.model.DetectionResult;

import java.util.Map;

/**
 * Layer 6 — ML Inference  (Stream Processing: XGBoost)
 *
 * Finalize feature vector → score bằng XGBoost model đã train sẵn.
 *
 * Bước finalize duy nhất tại đây: tính amt_vs_avg_7d
 *   = amt / (amt_sum_7d / trans_count_7d)
 * Ratio này không thể tính trước vì cần kết quả từ WindowAggregationFunction.
 *
 * Feature vector (18 features, thứ tự phải khớp lúc train):
 *   [0]  amt
 *   [1]  category_enc
 *   [2]  gender_enc
 *   [3]  state_enc
 *   [4]  lat
 *   [5]  long
 *   [6]  city_pop
 *   [7]  merch_lat
 *   [8]  merch_long
 *   [9]  hour
 *   [10] dayofweek
 *   [11] age
 *   [12] distance
 *   [13] trans_count_24h
 *   [14] amt_sum_24h
 *   [15] trans_count_7d
 *   [16] amt_sum_7d
 *   [17] amt_vs_avg_7d
 *
 * Input : Map enriched (có đủ 18 features + rule_triggered, rule_description)
 * Output: DetectionResult
 */
public class XGBoostInferenceMap extends RichMapFunction<Map<String, Object>, DetectionResult> {

    private static final String MODEL_PATH = "/mnt/models/best_fraud_model_096.json";
    private static final double ML_THRESHOLD = 0.2426;

    private transient Booster booster;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.booster = XGBoost.loadModel(MODEL_PATH);
    }

    @Override
    public DetectionResult map(Map<String, Object> event) throws Exception {
        String transNum        = event.getOrDefault("trans_num", "").toString();
        String ccNum           = event.getOrDefault("cc_num",    "").toString();
        boolean ruleTriggered  = Boolean.TRUE.equals(event.get("rule_triggered"));
        String  ruleDesc       = event.getOrDefault("rule_description", "").toString();

        try {
            double amt      = Double.parseDouble(event.get("amt").toString());
            double amtSum7d = Double.parseDouble(event.get("amt_sum_7d").toString());
            long   count7d  = ((Number) event.get("trans_count_7d")).longValue();

            // Finalize: ratio so với trung bình 7 ngày
            float amtVsAvg7d = (float) (amt / (amtSum7d / (count7d + 1e-5) + 1e-5));

            float[] features = new float[]{
                    (float) amt,                                             // 0  amt
                    ((Number) event.get("category_enc")).floatValue(),       // 1  category_enc
                    ((Number) event.get("gender_enc")).floatValue(),         // 2  gender_enc
                    ((Number) event.get("state_enc")).floatValue(),          // 3  state_enc
                    Float.parseFloat(event.get("lat").toString()),           // 4  lat
                    Float.parseFloat(event.get("long").toString()),          // 5  long
                    Float.parseFloat(event.get("city_pop").toString()),      // 6  city_pop
                    Float.parseFloat(event.get("merch_lat").toString()),     // 7  merch_lat
                    Float.parseFloat(event.get("merch_long").toString()),    // 8  merch_long
                    ((Number) event.get("hour")).floatValue(),               // 9  hour
                    ((Number) event.get("dayofweek")).floatValue(),          // 10 dayofweek
                    ((Number) event.get("age")).floatValue(),                // 11 age
                    ((Number) event.get("distance")).floatValue(),           // 12 distance
                    ((Number) event.get("trans_count_24h")).floatValue(),    // 13 trans_count_24h
                    Float.parseFloat(event.get("amt_sum_24h").toString()),   // 14 amt_sum_24h
                    (float) count7d,                                         // 15 trans_count_7d
                    (float) amtSum7d,                                        // 16 amt_sum_7d
                    amtVsAvg7d                                               // 17 amt_vs_avg_7d
            };

            DMatrix dMatrix = new DMatrix(features, 1, features.length, Float.NaN);
            try {
                float[][] preds = booster.predict(dMatrix);
                double score    = preds[0][0];
                return new DetectionResult(transNum, ccNum, score,
                        score >= ML_THRESHOLD, ruleTriggered, ruleDesc);
            } finally {
                dMatrix.dispose(); // Giải phóng native memory ngay lập tức
            }

        } catch (Exception e) {
            System.err.println("[ERROR] Inference failed for txn=" + transNum + ": " + e.getMessage());
            // Trả về "an toàn" thay vì ném exception làm sập stream
            return new DetectionResult(transNum, ccNum, 0.0, false, ruleTriggered, ruleDesc);
        }
    }

    @Override
    public void close() throws Exception {
        if (booster != null) booster.dispose();
    }
}
