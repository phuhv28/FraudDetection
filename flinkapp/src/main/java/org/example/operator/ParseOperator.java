package org.example.operator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Map;

/**
 * Layer 1 — Parse
 *
 * Chuyển đổi JSON string nhận từ Kafka thành Map<String, Object>.
 * ObjectMapper được khởi tạo trong open() vì không Serializable.
 *
 * Input : raw JSON string từ topic "transactions"
 * Output: Map<String, Object> với toàn bộ field gốc
 */
public class ParseOperator extends RichMapFunction<String, Map<String, Object>> {

    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
        this.mapper = new ObjectMapper();
    }

    @Override
    public Map<String, Object> map(String value) throws Exception {
        return mapper.readValue(value, new TypeReference<Map<String, Object>>() {});
    }
}
