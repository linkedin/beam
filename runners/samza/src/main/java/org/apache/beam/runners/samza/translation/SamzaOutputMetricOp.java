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

import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamzaOutputMetricOp<T> extends SamzaMetricOp<T> {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaOutputMetricOp.class);

  private int count;
  private long minTimestamp;
  private long maxTimestamp;
  private long sumOfTimestamps;
  private boolean overflowNotifier;

  public SamzaOutputMetricOp(
      String pValue, String transformFullName, SamzaOpMetricRegistry samzaOpMetricRegistry) {
    super(pValue, transformFullName, samzaOpMetricRegistry);
    this.count = 0;
    this.sumOfTimestamps = 0L;
    this.maxTimestamp = Long.MIN_VALUE;
    this.minTimestamp = Long.MAX_VALUE;
  }

  @Override
  public void processElement(WindowedValue<T> inputElement, OpEmitter<T> emitter) {
    // update counters for timestamps
    long currTime =
        System
            .nanoTime(); // TODO: check if nano time overflows, switch to milliseconds or use BigInt
    count++;
    minTimestamp = Math.min(minTimestamp, currTime);
    maxTimestamp = Math.max(maxTimestamp, currTime);
    // sum of arrival time - overflow exception sensitive
    try {
      sumOfTimestamps = Math.addExact(sumOfTimestamps, currTime);
    } catch (ArithmeticException e) {
      overflowNotifier = true;
      LOG.warn("Number overflow exception for {}", transformFullName);
    }
    samzaOpMetricRegistry.getSamzaOpMetrics().getTransformOutputThroughput(transformFullName).inc();
    emitter.emitElement(inputElement);
  }

  @Override
  public void processWatermark(Instant watermark, OpEmitter<T> emitter) {
    long avg =
        overflowNotifier
            ? Math.floorDiv(minTimestamp + maxTimestamp, 2)
            : Math.floorDiv(sumOfTimestamps, count);
    // Update MetricOp Registry with counters
    samzaOpMetricRegistry.updateAvgStartTimeMap(
        transformFullName, pValue, watermark.getMillis(), avg);
    // reset all counters
    count = 0;
    sumOfTimestamps = 0L;
    this.maxTimestamp = Long.MIN_VALUE;
    this.minTimestamp = Long.MAX_VALUE;
    overflowNotifier = false;
    // emit the metrics
    samzaOpMetricRegistry
        .getSamzaOpMetrics()
        .getTransformWatermarkProgress(transformFullName)
        .set(watermark.getMillis());
    samzaOpMetricRegistry.emitLatencyMetric(
        transformFullName, transformInputs, transformOutputs, watermark.getMillis());
    // samzaOpMetricRegistry.emitLatencyMetric(transformFullName, watermark.getMillis());
    super.processWatermark(watermark, emitter);
  }
}
