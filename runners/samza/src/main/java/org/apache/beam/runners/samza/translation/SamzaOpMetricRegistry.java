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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

public class SamzaOpMetricRegistry implements Serializable {

  // TODO: we dont need per pvalue, if every element has input we need avg arrival time of all
  // elements between two watermark
  // transformName -> pValue/pCollection -> Map<watermarkId, avgArrivalTime>
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>>>
      avgStartTimeMap;

  // tranformName -> <windowId, avgArrivalTime>
  private ConcurrentHashMap<String, ConcurrentHashMap<BoundedWindow, Long>> avgStartTimeMapForGbk;

  private final SamzaOpMetrics samzaOpMetrics;

  public SamzaOpMetricRegistry(Map<String, String> config) {
    this.avgStartTimeMap = new ConcurrentHashMap<>();
    this.samzaOpMetrics = new SamzaOpMetrics();
    this.avgStartTimeMapForGbk = new ConcurrentHashMap<>();
  }

  public SamzaOpMetrics getSamzaOpMetrics() {
    return samzaOpMetrics;
  }

  protected void updateAvgStartTimeMap(
      String transformName, String pValue, long watermark, long avg) {

    if (!avgStartTimeMap.containsKey(transformName)) {
      avgStartTimeMap.putIfAbsent(transformName, new ConcurrentHashMap<>());
    }
    ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>> avgStartMap =
        avgStartTimeMap.get(transformName);
    if (!avgStartMap.containsKey(pValue)) {
      avgStartMap.put(pValue, new ConcurrentHashMap<>());
    }
    avgStartMap.get(pValue).put(watermark, avg);
  }

  protected void updateAvgStartTimeMapGBK(String transformName, BoundedWindow windowId, long avg) {

    if (!avgStartTimeMapForGbk.containsKey(transformName)) {
      avgStartTimeMapForGbk.putIfAbsent(transformName, new ConcurrentHashMap<>());
    }
    avgStartTimeMapForGbk.get(transformName).put(windowId, avg);
  }

  void emitLatencyMetric(String transformName, BoundedWindow w, Long avgArrivalTime) {
    Long ans = avgArrivalTime - avgStartTimeMapForGbk.get(transformName).get(w);
    System.out.println("Latency arrival time: " + ans);
    samzaOpMetrics.getTransformLatencyMetric(transformName).update(ans);
  }

  void emitLatencyMetric(
      String transformName,
      List<String> transformInputs,
      List<String> transformOutputs,
      Long watermark,
      String taskName) {
    ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>> avgStartMap =
        avgStartTimeMap.get(transformName);

    System.out.println(
        String.format(
            "Emit Metric TransformName %s for: %s and watermark: %s for task: %s", transformName, transformName, watermark, taskName));

    if (!transformInputs.isEmpty() && !transformOutputs.isEmpty()) { // skip the io operators
      List<Long> inputPValueStartTimes =
          transformInputs.stream()
              .map(avgStartMap::get)
              .map(startTimeMap -> startTimeMap.get(watermark)) // replace get with remove
              .collect(Collectors.toList());

      List<Long> outputPValueStartTimes =
          transformOutputs.stream()
              .map(avgStartMap::get)
              .map(startTimeMap -> startTimeMap.get(watermark)) // replace get with remove
              .collect(Collectors.toList());

      Long startTime = Collections.min(inputPValueStartTimes);
      Long endTime = Collections.max(outputPValueStartTimes);

      if (startTime != null && endTime != null) {
        Long avgLatency = endTime - startTime;
        // TODO: remove the entries for the watermark from in-memory map
        samzaOpMetrics.getTransformLatencyMetric(transformName).update(avgLatency);
      } else {
        System.out.println(
            String.format(
                "Start Time: [%s] or End Time: [%s] found is null for: %s and watermark: %s for task: %s",
                startTime, endTime, transformName, watermark, taskName));
      }
    }
  }
}
