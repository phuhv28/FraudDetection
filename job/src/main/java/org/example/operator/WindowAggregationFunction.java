package org.example.operator;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Layer 4 — Stateful Window Aggregation  (Serving & Storage: Redis)
 *
 * Chức năng chính:
 *   1. Duy trì ListState<(timestamp_ms, amt)> — lịch sử giao dịch của từng thẻ.
 *   2. Evict entry cũ hơn 7 ngày để giữ state nhỏ gọn.
 *   3. Tính 4 aggregates:
 *        trans_count_24h, amt_sum_24h   (cửa sổ 24 giờ)
 *        trans_count_7d,  amt_sum_7d    (cửa sổ 7 ngày)
 *   4. Ghi feature vector lên Redis để:
 *        - Grafana dashboard đọc và hiển thị real-time
 *        - Warm up lại state nếu Flink job restart
 *
 * Redis key pattern: card:{cc_num}:features (Hash)
 *
 * Input : Map với unix_time_ms, amt, cc_num
 * Output: Map + 4 window aggregate fields
 */
public class WindowAggregationFunction
        extends KeyedProcessFunction<String, Map<String, Object>, Map<String, Object>> {

    private static final long WINDOW_24H_MS = 24L * 3600 * 1000;
    private static final long WINDOW_7D_MS  =  7L * 24 * 3600 * 1000;

    private final String redisHost;
    private final int    redisPort;

    private transient ListState<Tuple2<Long, Double>> txHistory;
    private transient Jedis jedis;

    public WindowAggregationFunction(String redisHost, int redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // Flink state — persisted và replayed khi restart
        ListStateDescriptor<Tuple2<Long, Double>> descriptor = new ListStateDescriptor<>(
                "tx-history",
                TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {}));
        txHistory = getRuntimeContext().getListState(descriptor);

        // Redis — dùng để serving layer (Grafana, external services)
        jedis = new Jedis(redisHost, redisPort);
    }

    @Override
    public void processElement(Map<String, Object> event,
                               Context ctx,
                               Collector<Map<String, Object>> out) throws Exception {
        long   nowMs  = ((Number) event.get("unix_time_ms")).longValue();
        double amt    = Double.parseDouble(event.get("amt").toString());
        String ccNum  = event.get("cc_num").toString();

        // ── Step 1: Evict entries older than 7 days ───────────────────────────
        List<Tuple2<Long, Double>> valid = new ArrayList<>();
        for (Tuple2<Long, Double> entry : txHistory.get()) {
            if (nowMs - entry.f0 <= WINDOW_7D_MS) valid.add(entry);
        }

        // ── Step 2: Compute aggregations over valid history ───────────────────
        long   count24h = 0, count7d = 0;
        double sum24h   = 0.0, sum7d  = 0.0;
        for (Tuple2<Long, Double> entry : valid) {
            long age = nowMs - entry.f0;
            count7d++;
            sum7d += entry.f1;
            if (age <= WINDOW_24H_MS) {
                count24h++;
                sum24h += entry.f1;
            }
        }

        // Include current transaction in counts
        long   finalCount24h = count24h + 1;
        double finalSum24h   = sum24h   + amt;
        long   finalCount7d  = count7d  + 1;
        double finalSum7d    = sum7d    + amt;

        // ── Step 3: Update Flink state ────────────────────────────────────────
        valid.add(Tuple2.of(nowMs, amt));
        txHistory.update(valid);

        // ── Step 4: Write-back feature vector to Redis ────────────────────────
        // Grafana và backend có thể đọc trực tiếp từ Redis với độ trễ < 1ms
        try {
            String redisKey = "card:" + ccNum + ":features";
            Map<String, String> featureMap = new HashMap<>();
            featureMap.put("trans_count_24h", String.valueOf(finalCount24h));
            featureMap.put("amt_sum_24h",     String.valueOf(finalSum24h));
            featureMap.put("trans_count_7d",  String.valueOf(finalCount7d));
            featureMap.put("amt_sum_7d",      String.valueOf(finalSum7d));
            featureMap.put("last_seen_ms",    String.valueOf(nowMs));
            jedis.hset(redisKey, featureMap);
            jedis.expire(redisKey, 7 * 24 * 3600); // TTL 7 ngày — tự dọn key cũ
        } catch (Exception e) {
            // Redis lỗi không được làm sập pipeline — chỉ log warning
            System.err.println("[WARN] Redis write-back failed for card " + ccNum + ": " + e.getMessage());
        }

        // ── Step 5: Emit enriched event ───────────────────────────────────────
        Map<String, Object> enriched = new HashMap<>(event);
        enriched.put("trans_count_24h", finalCount24h);
        enriched.put("amt_sum_24h",     finalSum24h);
        enriched.put("trans_count_7d",  finalCount7d);
        enriched.put("amt_sum_7d",      finalSum7d);
        out.collect(enriched);
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) jedis.close();
    }
}
