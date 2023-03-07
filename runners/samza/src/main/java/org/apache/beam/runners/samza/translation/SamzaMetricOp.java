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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.runners.samza.runtime.KeyedTimerData;
import org.apache.beam.runners.samza.runtime.Op;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.runners.samza.util.SamzaOpUtils;
import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.operators.Scheduler;

public abstract class SamzaMetricOp<T> implements Op<T, T, Void> {
  protected final String transformFullName;
  protected final SamzaOpMetricRegistry samzaOpMetricRegistry;
  private MetricsRegistry metricsRegistry;
  protected List<String> transformInputs;
  protected List<String> transformOutputs;
  protected final String pValue;

  public SamzaMetricOp(
      String pValue, String transformFullName, SamzaOpMetricRegistry samzaOpMetricRegistry) {
    this.transformFullName = transformFullName;
    this.samzaOpMetricRegistry = samzaOpMetricRegistry;
    this.pValue = pValue;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void open(
      Config config,
      Context context,
      Scheduler<KeyedTimerData<Void>> timerRegistry,
      OpEmitter<T> emitter) {
    Map.Entry<String, String> transformInputOutput =
        SamzaOpUtils.getTransformToIOMap(config).get(transformFullName);
    transformInputs =
        Arrays.stream(transformInputOutput.getKey().split(","))
            .filter(item -> !item.isEmpty())
            .collect(Collectors.toList());
    transformOutputs =
        Arrays.stream(transformInputOutput.getValue().split(","))
            .filter(item -> !item.isEmpty())
            .collect(Collectors.toList());
    // TODO: read config to switch to per task metrics on demand
    this.metricsRegistry = context.getContainerContext().getContainerMetricsRegistry();
    // init the metric
    samzaOpMetricRegistry.getSamzaOpMetrics().register(transformFullName, metricsRegistry);
  }
}
