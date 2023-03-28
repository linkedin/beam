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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.samza.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamzaOpMetricRegistry implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaOpMetricRegistry.class);

  // transformName -> pValue/pCollection -> Map<watermarkId, avgArrivalTime>
  private ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>>>
      avgArrivalTimeMap;
  // transformName -> <windowId, avgArrivalTime>
  @SuppressFBWarnings("SE_BAD_FIELD")
  private ConcurrentHashMap<String, ConcurrentHashMap<BoundedWindow, Long>> avgArrivalTimeMapForGbk;

  private final SamzaOpMetricHolder samzaOpMetricHolder;

  public SamzaOpMetricRegistry() {
    this.avgArrivalTimeMap = new ConcurrentHashMap<>();
    this.avgArrivalTimeMapForGbk = new ConcurrentHashMap<>();
    this.samzaOpMetricHolder = new SamzaOpMetricHolder();
  }

  public void register(String transformFullName, String pValue, MetricsRegistry metricsRegistry) {
    samzaOpMetricHolder.register(transformFullName, metricsRegistry);
    avgArrivalTimeMap.putIfAbsent(transformFullName, new ConcurrentHashMap<>());
    ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>> avgStartMap =
        avgArrivalTimeMap.get(transformFullName);
    avgStartMap.putIfAbsent(pValue, new ConcurrentHashMap<>());
    // todo: check if somehow we can only update this for GroupByKey/Combine
    avgArrivalTimeMapForGbk.putIfAbsent(transformFullName, new ConcurrentHashMap<>());
  }

  public SamzaOpMetricHolder getSamzaOpMetrics() {
    return samzaOpMetricHolder;
  }

  protected void updateArrivalTimeMap(String transformName, BoundedWindow windowId, long avg) {
    avgArrivalTimeMapForGbk.get(transformName).put(windowId, avg);
  }

  protected void updateArrivalTimeMap(
      String transformName, String pValue, long watermark, long avg) {
    avgArrivalTimeMap.get(transformName).get(pValue).put(watermark, avg);
    // remove any stale entries which are lesser than the watermark
    avgArrivalTimeMap
        .get(transformName)
        .get(pValue)
        .entrySet()
        .removeIf(entry -> entry.getKey() < watermark);
  }

  void emitLatencyMetric(
      String transformName, BoundedWindow w, Long avgArrivalEndTime, String taskName) {
    Long avgArrivalStartTime = avgArrivalTimeMapForGbk.get(transformName).getOrDefault(w, 0L);
    if (avgArrivalStartTime.compareTo(0L) > 0 && avgArrivalEndTime.compareTo(0L) > 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            String.format(
                "Success Emit Metric TransformName %s for: %s and window: %s for task: %s",
                transformName, transformName, w, taskName));
      }
      avgArrivalTimeMapForGbk.get(transformName).remove(w);
      samzaOpMetricHolder
          .getTransformLatencyMetric(transformName)
          .update(avgArrivalEndTime - avgArrivalStartTime);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            String.format(
                "Start Time: [%s] or End Time: [%s] found is 0/null for: %s and windowId: %s for task: %s",
                avgArrivalStartTime, avgArrivalEndTime, transformName, w, taskName));
      }
    }
  }

  // TODO: check the case where input has no elements, only watermarks are propogated
  void emitLatencyMetric(
      String transformName,
      List<String> transformInputs,
      List<String> transformOutputs,
      Long watermark,
      String taskName) {
    ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>> avgStartMap =
        avgArrivalTimeMap.get(transformName);

    if (!transformInputs.isEmpty() && !transformOutputs.isEmpty()) { // skip the io operators
      List<ConcurrentHashMap<Long, Long>> tmp =
          transformInputs.stream()
              .map(avgStartMap::get)
              .filter(x -> x != null)
              .collect(Collectors.toList());

      List<Long> inputPValueStartTimes =
          tmp.stream()
              .map(startTimeMap -> startTimeMap.remove(watermark)) // replace get with remove
              .filter(x -> x != null)
              .collect(Collectors.toList());

      List<Long> outputPValueStartTimes =
          transformOutputs.stream()
              .map(avgStartMap::get)
              .map(startTimeMap -> startTimeMap.remove(watermark)) // replace get with remove
              .filter(x -> x != null)
              .collect(Collectors.toList());

      if (!inputPValueStartTimes.isEmpty() && !outputPValueStartTimes.isEmpty()) {
        Long startTime = Collections.min(inputPValueStartTimes);
        Long endTime = Collections.max(outputPValueStartTimes);
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              String.format(
                  "Success Emit Metric TransformName %s for: %s and watermark: %s for task: %s",
                  transformName, transformName, watermark, taskName));
        }
        Long avgLatency = endTime - startTime;
        samzaOpMetricHolder.getTransformLatencyMetric(transformName).update(avgLatency);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              String.format(
                  "Start Time: [%s] or End Time: [%s] found is null for: %s and watermark: %s for task: %s",
                  inputPValueStartTimes,
                  outputPValueStartTimes,
                  transformName,
                  watermark,
                  taskName));
        }
      }
    }
  }
}
