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

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamzaInputGBKMetricOp<T> extends SamzaMetricOp<T> {
//  private static final Logger LOG = LoggerFactory.getLogger(SamzaInputGBKMetricOp.class);

  private Map<BoundedWindow, Long> sumOfTimestampsPerWindowId;
  private Map<BoundedWindow, Long> sumOfCountPerWindowId;

  public SamzaInputGBKMetricOp(
      String pValue, String transformFullName, SamzaOpMetricRegistry samzaOpMetricRegistry) {
    super(pValue, transformFullName, samzaOpMetricRegistry);
    this.sumOfTimestampsPerWindowId = new HashMap<>();
    this.sumOfCountPerWindowId = new HashMap<>();
  }

  @Override
  public void processElement(WindowedValue<T> inputElement, OpEmitter<T> emitter) {
    long currTime =
        System
            .nanoTime(); // TODO: check if nano time overflows, switch to milliseconds or use BigInt
    // sum of arrival time - overflow exception sensitive
    try {
      for (BoundedWindow x : inputElement.getWindows()) {
        Long sumTimestamps =
            Math.addExact(sumOfTimestampsPerWindowId.getOrDefault(x, 0L), currTime);
        sumOfTimestampsPerWindowId.put(x, sumTimestamps);
        Long count = sumOfCountPerWindowId.getOrDefault(x, 0L) + 1;
        sumOfCountPerWindowId.put(x, count);
      }
    } catch (ArithmeticException e) {
      System.out.println("HERE Number overflow exception for");
    }
    samzaOpMetricRegistry.getSamzaOpMetrics().getTransformInputThroughput(transformFullName).inc();
    emitter.emitElement(inputElement);
  }

  @Override
  public void processWatermark(Instant watermark, OpEmitter<T> emitter) {
    sumOfTimestampsPerWindowId.forEach(
        (wv, sum) -> {
          System.out.println(String.format("HERE [Input GBK] Window-id=%s for maxTimestamp=%s watermark= %s for task=%s", wv.hashCode(), wv.maxTimestamp().getMillis(),
                  watermark.getMillis(), taskContext.getTaskModel().getTaskName()));
          if (wv.maxTimestamp().getMillis() < watermark.getMillis()) {
            System.out.println(
                String.format("HERE [Input GBK] Window-id=%s for sum=%s count=%s watermark=%s for task=%s", wv.hashCode(), sum, sumOfCountPerWindowId.get(wv),
                    watermark.getMillis(), taskContext.getTaskModel().getTaskName()));
            samzaOpMetricRegistry.updateAvgStartTimeMapGBK(
                transformFullName, wv, Math.floorDiv(sum, sumOfCountPerWindowId.get(wv)));
          }
        });
    super.processWatermark(watermark, emitter);
  }
}
