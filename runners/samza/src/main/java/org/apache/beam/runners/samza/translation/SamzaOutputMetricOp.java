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

import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

public class SamzaOutputMetricOp<T> extends SamzaMetricOp<T> {
  private AtomicLong count;
  private AtomicReference<BigInteger> sumOfTimestamps;

  public SamzaOutputMetricOp(
      String pValue, String transformFullName, SamzaOpMetricRegistry samzaOpMetricRegistry) {
    super(pValue, transformFullName, samzaOpMetricRegistry);
    this.count = new AtomicLong(0L);
    this.sumOfTimestamps = new AtomicReference<>(BigInteger.ZERO);
  }

  @Override
  public void processElement(WindowedValue<T> inputElement, OpEmitter<T> emitter) {
    // update counters for timestamps
    count.incrementAndGet();
    sumOfTimestamps.updateAndGet(sum -> sum.add(BigInteger.valueOf(System.nanoTime())));
    samzaOpMetricRegistry.getSamzaOpMetrics().getTransformOutputThroughput(transformFullName).inc();
    emitter.emitElement(inputElement);
  }

  @Override
  @SuppressWarnings({"CompareToZero"})
  public void processWatermark(Instant watermark, OpEmitter<T> emitter) {
    System.out.println(
        String.format(
            "Output [%s] Processing watermark: %s for task: %s",
            transformFullName,
            watermark.getMillis(),
            taskContext.getTaskModel().getTaskName().getTaskName()));
    if (sumOfTimestamps.get().compareTo(BigInteger.ZERO) == 1) {
      // if BigInt.longValue is out of range for long then only the low-order 64 bits are retained
      long avg = Math.floorDiv(sumOfTimestamps.get().longValue(), count.get());
      // Update MetricOp Registry with counters
      samzaOpMetricRegistry.updateArrivalTimeMap(
          transformFullName, pValue, watermark.getMillis(), avg);
      // emit the metrics
      samzaOpMetricRegistry.emitLatencyMetric(
          transformFullName,
          transformInputs,
          transformOutputs,
          watermark.getMillis(),
          taskContext.getTaskModel().getTaskName().getTaskName());
    } else {
      // Empty data case - you don't need to handle
      System.out.println(
          String.format(
              "Output [%s] SumOfTimestamps: %s zero for watermark: %s for task: %s",
              transformFullName,
              sumOfTimestamps.get().longValue(),
              watermark.getMillis(),
              taskContext.getTaskModel().getTaskName().getTaskName()));
    }
    samzaOpMetricRegistry
        .getSamzaOpMetrics()
        .getTransformWatermarkProgress(transformFullName)
        .set(watermark.getMillis());
    // reset all counters
    this.count = new AtomicLong(0L);
    this.sumOfTimestamps = new AtomicReference<>(BigInteger.ZERO);
    super.processWatermark(watermark, emitter);
  }
}
