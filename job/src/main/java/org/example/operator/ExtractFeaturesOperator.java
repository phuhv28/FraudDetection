package org.example.operator;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

/**
 * Layer 2 — Extract Features
 *
 * Tính toán các feature dẫn xuất từ dữ liệu gốc:
 *   - hour       : giờ trong ngày (0–23)
 *   - dayofweek  : thứ trong tuần (0=Mon … 6=Sun)
 *   - age        : tuổi chủ thẻ tính đến thời điểm giao dịch
 *   - distance   : khoảng cách Euclid giữa (lat,lon) và (merch_lat,merch_lon)
 *   - unix_time_ms: epoch milliseconds — dùng để evict state ở WindowAggregationFunction
 *
 * Input : Map gốc có trans_date_trans_time, dob, lat, long, merch_lat, merch_long
 * Output: Map gốc + 5 field mới bên trên
 */
public class ExtractFeaturesOperator extends RichMapFunction<Map<String, Object>, Map<String, Object>> {

    private transient DateTimeFormatter formatter;

    @Override
    public void open(Configuration parameters) {
        this.formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public Map<String, Object> map(Map<String, Object> event) throws Exception {
        Map<String, Object> enriched = new HashMap<>(event);

        // ── Temporal features ─────────────────────────────────────────────────
        LocalDateTime transTime = LocalDateTime.parse(
                event.get("trans_date_trans_time").toString(), formatter);
        LocalDateTime dob = LocalDateTime.parse(
                event.get("dob").toString() + " 00:00:00", formatter);

        enriched.put("hour",         transTime.getHour());
        enriched.put("dayofweek",    transTime.getDayOfWeek().getValue() - 1); // Mon=0
        enriched.put("age",          (int) ChronoUnit.YEARS.between(dob, transTime));
        enriched.put("unix_time_ms", transTime.toEpochSecond(ZoneOffset.UTC) * 1000L);

        // ── Geographic feature ────────────────────────────────────────────────
        double lat  = Double.parseDouble(event.get("lat").toString());
        double lon  = Double.parseDouble(event.get("long").toString());
        double mLat = Double.parseDouble(event.get("merch_lat").toString());
        double mLon = Double.parseDouble(event.get("merch_long").toString());

        // Euclid distance (degree-space) — đủ dùng cho fraud score, không cần Haversine
        enriched.put("distance", Math.sqrt(Math.pow(lat - mLat, 2) + Math.pow(lon - mLon, 2)));

        return enriched;
    }
}
