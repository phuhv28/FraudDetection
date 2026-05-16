package org.example.operator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Layer 3 — Encode Categoricals
 *
 * Label-encode các trường categorical bằng bảng mapping được load từ file JSON.
 * Bảng mapping được load một lần trong open() và tái sử dụng cho mọi record.
 *
 * Thêm 3 field mới:
 *   - category_enc : int
 *   - gender_enc   : int  (M=0 / F=1 thường nằm sẵn trong encoders.json)
 *   - state_enc    : int
 *
 * Input : Map + temporal/geo features từ Layer 2
 * Output: Map + 3 encoded field trên
 */
public class EncodeOperator extends RichMapFunction<Map<String, Object>, Map<String, Object>> {

    private static final String ENCODER_PATH = "/mnt/models/label_encoders.json";

    private transient Map<String, Map<String, Integer>> encoders;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.encoders = new ObjectMapper().readValue(
                new File(ENCODER_PATH),
                new TypeReference<Map<String, Map<String, Integer>>>() {});
    }

    @Override
    public Map<String, Object> map(Map<String, Object> event) throws Exception {
        Map<String, Object> encoded = new HashMap<>(event);

        encoded.put("category_enc",
                encoders.get("category").getOrDefault(event.get("category").toString(), -1));
        encoded.put("gender_enc",
                encoders.get("gender").getOrDefault(event.get("gender").toString(), -1));
        encoded.put("state_enc",
                encoders.get("state").getOrDefault(event.get("state").toString(), -1));

        return encoded;
    }
}
