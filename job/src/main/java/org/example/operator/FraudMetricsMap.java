package org.example.operator;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.example.model.DetectionResult;

public class FraudMetricsMap
        extends RichMapFunction<DetectionResult, DetectionResult> {

    private static boolean serverStarted = false;

    private transient Counter fraudTransactions;
    private transient Counter normalTransactions;
    private transient Gauge fraudScore;
    private transient Histogram predictionLatency;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters)
            throws Exception {

        if (!serverStarted) {
            new HTTPServer(8000);
            serverStarted = true;
        }

        fraudTransactions =
                Counter.build()
                        .name("fraud_transactions_total")
                        .help("Total fraud transactions")
                        .register();

        normalTransactions =
                Counter.build()
                        .name("normal_transactions_total")
                        .help("Total normal transactions")
                        .register();

        fraudScore =
                Gauge.build()
                        .name("fraud_score")
                        .help("Current fraud score")
                        .register();

        predictionLatency =
                Histogram.build()
                        .name("prediction_latency_seconds")
                        .help("Prediction latency")
                        .register();
    }

    @Override
    public DetectionResult map(DetectionResult result)
            throws Exception {

        Histogram.Timer timer =
                predictionLatency.startTimer();

        try {

            fraudScore.set(result.score);

            if (result.isFraud || result.ruleTriggered) {
                fraudTransactions.inc();
            } else {
                normalTransactions.inc();
            }

            return result;

        } finally {
            timer.observeDuration();
        }
    }
}