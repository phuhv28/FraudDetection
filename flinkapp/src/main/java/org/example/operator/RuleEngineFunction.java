package org.example.operator;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * Layer 5 — Rule-based Engine  (Stream Processing: hard rules)
 *
 * Kiểm tra 2 luật cứng TRƯỚC khi đưa vào ML, để có thể block ngay
 * các giao dịch vi phạm rõ ràng mà không cần chờ inference.
 *
 * ┌─ Rule 1: Large Amount ───────────────────────────────────────────┐
 * │  Nếu amt > 4000 USD (~100 triệu VND):                           │
 * │  → Đánh dấu rule_triggered = true, ghi ruleDescription          │
 * └──────────────────────────────────────────────────────────────────┘
 *
 * ┌─ Rule 2: Geo-Velocity ───────────────────────────────────────────┐
 * │  Nếu giao dịch trước và hiện tại:                               │
 * │    - Cách nhau > GEO_VELOCITY_THRESHOLD_KM km                   │
 * │    - Trong vòng < GEO_VELOCITY_TIME_WINDOW_MS ms (5 phút)       │
 * │  → Vật lý không thể di chuyển kịp → fraud                      │
 * └──────────────────────────────────────────────────────────────────┘
 *
 * State: Tuple3<Long, Double, Double> = (last_unix_time_ms, last_lat, last_lon)
 *
 * Input : Map enriched từ WindowAggregationFunction
 * Output: Map gốc + 2 field bổ sung: rule_triggered (boolean), rule_description (String)
 */
public class RuleEngineFunction
        extends KeyedProcessFunction<String, Map<String, Object>, Map<String, Object>> {

    // Rule 1 threshold
    private static final double AMT_THRESHOLD_USD = 4_000.0;

    // Rule 2 thresholds
    private static final double GEO_VELOCITY_THRESHOLD_KM  = 500.0;  // 500 km
    private static final long   GEO_VELOCITY_TIME_WINDOW_MS = 5 * 60 * 1000L; // 5 phút

    // State: (last_unix_time_ms, last_lat, last_lon) của thẻ
    private transient ValueState<Tuple3<Long, Double, Double>> lastTxState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple3<Long, Double, Double>> descriptor =
                new ValueStateDescriptor<>("last-tx-location",
                        TypeInformation.of(new TypeHint<Tuple3<Long, Double, Double>>() {}));
        lastTxState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Map<String, Object> event,
                               Context ctx,
                               Collector<Map<String, Object>> out) throws Exception {
        Map<String, Object> result = new HashMap<>(event);

        boolean ruleTriggered   = false;
        String  ruleDescription = "";

        double amt    = Double.parseDouble(event.get("amt").toString());
        double lat    = Double.parseDouble(event.get("lat").toString());
        double lon    = Double.parseDouble(event.get("long").toString());
        long   nowMs  = ((Number) event.get("unix_time_ms")).longValue();

        // ── Rule 1: Large Amount ──────────────────────────────────────────────
        if (amt > AMT_THRESHOLD_USD) {
            ruleTriggered   = true;
            ruleDescription = String.format("RULE1:LARGE_AMT amt=%.2f > threshold=%.2f",
                    amt, AMT_THRESHOLD_USD);
        }

        // ── Rule 2: Geo-Velocity ──────────────────────────────────────────────
        Tuple3<Long, Double, Double> lastTx = lastTxState.value();
        if (lastTx != null) {
            long   timeDiffMs  = nowMs - lastTx.f0;
            double distKm      = haversineKm(lastTx.f1, lastTx.f2, lat, lon);

            if (timeDiffMs < GEO_VELOCITY_TIME_WINDOW_MS
                    && distKm > GEO_VELOCITY_THRESHOLD_KM) {
                ruleTriggered = true;
                String geoDesc = String.format(
                        "RULE2:GEO_VELOCITY dist=%.1fkm in %ds",
                        distKm, timeDiffMs / 1000);
                // Nối thêm nếu đã có Rule 1
                ruleDescription = ruleDescription.isEmpty()
                        ? geoDesc
                        : ruleDescription + " | " + geoDesc;
            }
        }

        // Cập nhật state vị trí giao dịch mới nhất
        lastTxState.update(Tuple3.of(nowMs, lat, lon));

        result.put("rule_triggered",   ruleTriggered);
        result.put("rule_description", ruleDescription);
        out.collect(result);
    }

    /**
     * Công thức Haversine — tính khoảng cách (km) giữa 2 tọa độ địa lý.
     * Chính xác hơn Euclid khi khoảng cách lớn (> 100 km).
     */
    private static double haversineKm(double lat1, double lon1, double lat2, double lon2) {
        final double R = 6371.0; // bán kính Trái Đất (km)
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}