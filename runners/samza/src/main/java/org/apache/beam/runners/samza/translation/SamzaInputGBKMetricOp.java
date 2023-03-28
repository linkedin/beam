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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamzaInputGBKMetricOp<T> extends SamzaMetricOp<T> {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaInputGBKMetricOp.class);

  private Map<BoundedWindow, BigInteger> sumOfTimestampsPerWindowId;
  private Map<BoundedWindow, Long> sumOfCountPerWindowId;

  public SamzaInputGBKMetricOp(
      String pValue, String transformFullName, SamzaOpMetricRegistry samzaOpMetricRegistry) {
    super(pValue, transformFullName, samzaOpMetricRegistry);
    this.sumOfTimestampsPerWindowId = new HashMap<>();
    this.sumOfCountPerWindowId = new HashMap<>();
  }

  @Override
  public void processElement(WindowedValue<T> inputElement, OpEmitter<T> emitter) {
    for (BoundedWindow windowId : inputElement.getWindows()) {
      updateCounters(windowId);
      samzaOpMetricRegistry
          .getSamzaOpMetrics()
          .getTransformInputThroughput(transformFullName)
          .inc();
      emitter.emitElement(inputElement);
    }
  }

  private synchronized void updateCounters(BoundedWindow windowId) {
    BigInteger sumTimestampsForId =
        sumOfTimestampsPerWindowId.getOrDefault(windowId, BigInteger.ZERO);
    sumOfTimestampsPerWindowId.put(
        windowId, sumTimestampsForId.add(BigInteger.valueOf(System.nanoTime())));
    Long count = sumOfCountPerWindowId.getOrDefault(windowId, 0L);
    sumOfCountPerWindowId.put(windowId, count + 1);
  }

  @Override
  public void processWatermark(Instant watermark, OpEmitter<T> emitter) {
    List<BoundedWindow> toBeRemoved = new ArrayList<>();
    sumOfTimestampsPerWindowId.forEach(
        (windowId, sumOfTimestamps) -> {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                String.format(
                    "Input [%s] Processing watermark: %s for task: %s",
                    transformFullName,
                    watermark.getMillis(),
                    taskContext.getTaskModel().getTaskName().getTaskName()));
          }
          // if the window is closed and there is some data
          if (watermark.isAfter(windowId.maxTimestamp())
              && sumOfTimestamps.compareTo(BigInteger.ZERO) > 0) {
            toBeRemoved.add(windowId);
            samzaOpMetricRegistry.updateArrivalTimeMap(
                transformFullName,
                windowId,
                Math.floorDiv(sumOfTimestamps.longValue(), sumOfCountPerWindowId.get(windowId)));

          }
          // todo: cleanup
          else {
            // Empty data case - you don't need to handle
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                  String.format(
                      "Input [%s] SumOfTimestamps: %s zero for watermark: %s for task: %s",
                      transformFullName,
                      sumOfTimestamps.longValue(),
                      watermark.getMillis(),
                      taskContext.getTaskModel().getTaskName().getTaskName()));
            }
          }
        });
    // todo: use in place removal
    toBeRemoved.forEach(
        window -> {
          sumOfTimestampsPerWindowId.remove(window);
          sumOfCountPerWindowId.remove(window);
        });

    super.processWatermark(watermark, emitter);
  }
}
