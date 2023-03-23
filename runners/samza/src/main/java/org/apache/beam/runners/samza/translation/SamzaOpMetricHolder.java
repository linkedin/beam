/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.samza.translation;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;

// Todo check per container vs per task wiring of this
// Can this be static
public class SamzaOpMetricHolder implements Serializable {
  private static final String GROUP = SamzaOpMetricHolder.class.getSimpleName();

  private static final String METRIC_NAME_PATTERN = "%s-%s";
  private static final String TRANSFORM_LATENCY_METRIC = "handle-message-ns";
  private static final String TRANSFORM_WATERMARK_PROGRESS = "watermark-progress";
  private static final String TRANSFORM_IP_THROUGHPUT = "num-input-messages";
  private static final String TRANSFORM_OP_THROUGHPUT = "num-output-messages";

  @SuppressFBWarnings("SE_BAD_FIELD")
  private final Map<String, Timer> transformLatency;

  @SuppressFBWarnings("SE_BAD_FIELD")
  private final Map<String, Gauge<Long>> transformWatermarkProgress;

  @SuppressFBWarnings("SE_BAD_FIELD")
  private final Map<String, Counter> transformInputThroughput;

  @SuppressFBWarnings("SE_BAD_FIELD")
  private final Map<String, Counter> transformOutputThroughPut;

  public SamzaOpMetricHolder() {
    this.transformLatency = new ConcurrentHashMap<>();
    this.transformOutputThroughPut = new ConcurrentHashMap<>();
    this.transformWatermarkProgress = new ConcurrentHashMap<>();
    this.transformInputThroughput = new ConcurrentHashMap<>();
  }

  public void register(String transformName, MetricsRegistry metricsRegistry) {
    // TODO: Check the length of metric name is not exceeding the ingraphs limit defined
    transformLatency.putIfAbsent(
        transformName,
        metricsRegistry.newTimer(
            GROUP, getMetricNameWithPrefix(TRANSFORM_LATENCY_METRIC, transformName)));
    transformOutputThroughPut.putIfAbsent(
        transformName,
        metricsRegistry.newCounter(
            GROUP, getMetricNameWithPrefix(TRANSFORM_OP_THROUGHPUT, transformName)));
    transformInputThroughput.putIfAbsent(
        transformName,
        metricsRegistry.newCounter(
            GROUP, getMetricNameWithPrefix(TRANSFORM_IP_THROUGHPUT, transformName)));
    transformWatermarkProgress.putIfAbsent(
        transformName,
        metricsRegistry.newGauge(
            GROUP, getMetricNameWithPrefix(TRANSFORM_WATERMARK_PROGRESS, transformName), 0L));
  }

  private static String getMetricNameWithPrefix(String metricName, String transformName) {
    return String.format(METRIC_NAME_PATTERN, transformName, metricName);
  }

  public Timer getTransformLatencyMetric(String transformName) {
    return transformLatency.get(transformName);
  }

  public Counter getTransformInputThroughput(String transformName) {
    return transformInputThroughput.get(transformName);
  }

  public Counter getTransformOutputThroughput(String transformName) {
    return transformOutputThroughPut.get(transformName);
  }

  public Gauge<Long> getTransformWatermarkProgress(String transformName) {
    return transformWatermarkProgress.get(transformName);
  }
}
