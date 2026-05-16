package org.example.model;

/**
 * Kết quả phát hiện gian lận — được dùng làm output của XGBoostInferenceMap
 * và input của KafkaAlertSink.
 */
public class DetectionResult {

    public String  transNum;        // ID giao dịch gốc
    public String  ccNum;           // Số thẻ (đã mask khi log)
    public double  score;           // Fraud score từ XGBoost [0.0 – 1.0]
    public boolean isFraud;         // score >= ML_THRESHOLD
    public boolean ruleTriggered;   // Có luật cứng nào bị vi phạm không
    public String  ruleDescription; // Mô tả luật bị vi phạm (nếu có)

    public DetectionResult() {}

    public DetectionResult(String transNum, String ccNum,
                           double score, boolean isFraud,
                           boolean ruleTriggered, String ruleDescription) {
        this.transNum        = transNum;
        this.ccNum           = ccNum;
        this.score           = score;
        this.isFraud         = isFraud;
        this.ruleTriggered   = ruleTriggered;
        this.ruleDescription = ruleDescription;
    }

    @Override
    public String toString() {
        String masked = ccNum != null && ccNum.length() > 4
                ? "**** **** **** " + ccNum.substring(ccNum.length() - 4)
                : ccNum;
        return String.format("[FRAUD ALERT] txn=%s card=%s score=%.4f rule=%s (%s)",
                transNum, masked, score, ruleTriggered, ruleDescription);
    }
}
