package org.example.operator;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;

import org.example.model.DetectionResult;

public class FraudMetricsMap
        extends RichMapFunction<DetectionResult, DetectionResult> {

    private transient Counter fraudTransactions;
    private transient Counter normalTransactions;

    private transient double latestScore;
    private transient long lastLatencyMs;

    @Override
    public void open(Configuration parameters) {

        OperatorMetricGroup metricGroup =
                (OperatorMetricGroup) getRuntimeContext().getMetricGroup();

        fraudTransactions =
                metricGroup.counter("fraud_transactions_total");

        normalTransactions =
                metricGroup.counter("normal_transactions_total");

        // Gauge: fraud score
        metricGroup.gauge("fraud_score", () -> latestScore);

        // Gauge: latency (ms)
        metricGroup.gauge("prediction_latency_ms", () -> lastLatencyMs);
    }

    @Override
    public DetectionResult map(DetectionResult result) {

        long start = System.currentTimeMillis();

        // update score
        latestScore = result.score;

        // counters
        if (result.isFraud || result.ruleTriggered) {
            fraudTransactions.inc();
        } else {
            normalTransactions.inc();
        }

        // latency
        lastLatencyMs = System.currentTimeMillis() - start;

        return result;
    }
}