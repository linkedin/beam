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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamzaOpMetricRegistry implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaOpMetricRegistry.class);

  // transformName -> pValue/pCollection -> Map<watermarkId, avgArrivalTime>
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>>>
      avgStartTimeMap;

  private final SamzaOpMetrics samzaOpMetrics;

  // transformName -> List<inputPCollections>,List<outputPCollections>
  // private final Map<String, Map.Entry<String, String>> transformNameToInputOutput;

  // private final ConcurrentHashMap<String, Gauge> transformNameToLatency;

  public SamzaOpMetricRegistry(Map<String, String> config) {
    this.avgStartTimeMap = new ConcurrentHashMap<>();
    this.samzaOpMetrics = new SamzaOpMetrics();
    //    TypeReference<Map<String, Map.Entry<String, String>>> typeRef =
    //        new TypeReference<Map<String, Map.Entry<String, String>>>() {};
    //    try {
    //      ObjectMapper objectMapper = new ObjectMapper();
    //      objectMapper.registerModule(
    //          new SimpleModule()
    //              .addDeserializer(
    //                  Map.Entry.class, new SamzaPipelineTranslator.MapEntryDeserializer()));
    //      this.transformNameToInputOutput =
    //          objectMapper.readValue(
    //              config.get(SamzaRunner.BEAM_TRANSFORMS_WITH_IO),
    //              typeRef); // read from config deserialize
    //    } catch (JsonProcessingException e) {
    //      throw new RuntimeException(e);
    //    }
    //    this.transformNameToLatency = new ConcurrentHashMap<>();
    //    // init this metric map
    //    transformNameToInputOutput
    //        .keySet()
    //        .forEach(
    //            transform -> {
    //              transformNameToLatency.put(
    //                  transform, Metrics.gauge(SamzaMetricOp.class, transform +
    // "-handle-message-ms"));
    //            });
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

  void emitLatencyMetric(
      String transformName,
      List<String> transformInputs,
      List<String> transformOutputs,
      Long watermark) {
    ConcurrentHashMap<String, ConcurrentHashMap<Long, Long>> avgStartMap =
        avgStartTimeMap.get(transformName);
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
      Long avgLatency = startTime - endTime;
      // TODO: remove the entries for the watermark from in-memory map
      samzaOpMetrics.getTransformLatencyMetric(transformName).update(avgLatency);
    } else {
      LOG.info("Water in IO Transform {} for watermark {}", transformName, watermark);
    }
  }
}
