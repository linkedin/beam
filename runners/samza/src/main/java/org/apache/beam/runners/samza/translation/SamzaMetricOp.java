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

import org.apache.beam.runners.samza.runtime.Op;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

public class SamzaMetricOp<T> implements Op<T, T, Void> {
  private int count;
  private long sumOfTimestamps;
  private final String pValue;
  private final String transformFullName;
  private final SamzaOpMetricRegistry samzaOpMetricRegistry;

  public SamzaMetricOp(
      String pValue, String transformFullName, SamzaOpMetricRegistry samzaOpMetricRegistry) {
    this.count = 0;
    this.sumOfTimestamps = 0L;
    this.pValue = pValue;
    this.transformFullName = transformFullName;
    this.samzaOpMetricRegistry = samzaOpMetricRegistry;
  }

  @Override
  public void processElement(WindowedValue<T> inputElement, OpEmitter<T> emitter) {
    // sum of count of elements
    count++;
    // sum of arrival time - overflow exception sensitive
    sumOfTimestamps = Math.addExact(sumOfTimestamps, System.currentTimeMillis());
    emitter.emitElement(inputElement);
  }

  @Override
  public void processWatermark(Instant watermark, OpEmitter<T> emitter) {
    long avg = Math.floorDiv(sumOfTimestamps, count);
    // Update MetricOp Registry with counters
    samzaOpMetricRegistry.updateAvgStartTimeMap(pValue, watermark.getMillis(), avg);
    // reset all counters
    count = 0;
    sumOfTimestamps = 0L;
    // emit the metric
    samzaOpMetricRegistry.emitLatencyMetric(transformFullName, watermark.getMillis());
    Op.super.processWatermark(watermark, emitter);
  }
}
