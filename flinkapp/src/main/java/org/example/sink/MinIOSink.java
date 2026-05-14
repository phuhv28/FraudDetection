package org.example.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Sink: Serving & Storage — MinIO Data Lake
 *
 * Ghi toàn bộ raw event vào MinIO theo partitioning Hive-style:
 *   fraud-data-lake/transactions/year=YYYY/month=MM/day=DD/HH-{uuid}.ndjson
 *
 * Format: Newline-delimited JSON (NDJSON) — tương thích Spark, Trino, DuckDB.
 *
 * Batching: tích lũy BATCH_SIZE record hoặc mỗi FLUSH_INTERVAL_MS ms,
 * sau đó PUT một object lên MinIO — giảm số lượng API call.
 *
 * Mục đích:
 *   - Lưu trữ toàn bộ lịch sử giao dịch để replay (Kappa architecture).
 *   - Dữ liệu nguồn để retrain model định kỳ.
 *   - Audit trail cho giao dịch bị khóa.
 */
public class MinIOSink extends RichSinkFunction<Map<String, Object>> {

    private static final int  BATCH_SIZE        = 500;
    private static final long FLUSH_INTERVAL_MS = 10_000L; // 10 giây

    private final String endpoint;
    private final String bucket;
    private final String accessKey;
    private final String secretKey;

    private transient MinioClient       minioClient;
    private transient ObjectMapper      mapper;
    private transient List<String>      buffer;
    private transient long              lastFlushMs;
    private transient DateTimeFormatter pathFormatter;

    public MinIOSink(String endpoint, String bucket, String accessKey, String secretKey) {
        this.endpoint  = endpoint;
        this.bucket    = bucket;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();

        mapper        = new ObjectMapper();
        buffer        = new ArrayList<>();
        lastFlushMs   = System.currentTimeMillis();
        pathFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH").withZone(ZoneOffset.UTC);
    }

    @Override
    public void invoke(Map<String, Object> event, Context context) throws Exception {
        buffer.add(mapper.writeValueAsString(event));

        boolean batchFull    = buffer.size() >= BATCH_SIZE;
        boolean timeoutReached = (System.currentTimeMillis() - lastFlushMs) >= FLUSH_INTERVAL_MS;

        if (batchFull || timeoutReached) {
            flush();
        }
    }

    private void flush() throws Exception {
        if (buffer.isEmpty()) return;

        // Partition path: year=YYYY/month=MM/day=DD/HH-<timestamp>.ndjson
        String timePart  = pathFormatter.format(Instant.now());
        String objectKey = "transactions/" + timePart + "-" + System.currentTimeMillis() + ".ndjson";

        byte[] data = String.join("\n", buffer).getBytes(StandardCharsets.UTF_8);

        minioClient.putObject(PutObjectArgs.builder()
                .bucket(bucket)
                .object(objectKey)
                .stream(new ByteArrayInputStream(data), data.length, -1)
                .contentType("application/x-ndjson")
                .build());

        buffer.clear();
        lastFlushMs = System.currentTimeMillis();
    }

    @Override
    public void close() throws Exception {
        // Flush phần còn lại khi job shutdown
        flush();
    }
}
