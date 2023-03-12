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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.runners.samza.runtime.KeyedTimerData;
import org.apache.beam.runners.samza.runtime.Op;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.runners.samza.util.SamzaOpUtils;
import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.context.TaskContext;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.operators.Scheduler;

/**
 * MetricOp for default throughput, latency & watermark progress metric per transform for Beam Samza Runner
 * @param <T> type of the message
 */
public abstract class SamzaMetricOp<T> implements Op<T, T, Void> {
  public static final String ENABLE_TASK_METRICS = "runner.samza.transform.enable.task.metrics";

  protected final String transformFullName;
  protected final SamzaOpMetricRegistry samzaOpMetricRegistry;
  private MetricsRegistry metricsRegistry;
  protected List<String> transformInputs;
  protected List<String> transformOutputs;
  protected final String pValue;
  protected TaskContext taskContext; // only for testing, remove this

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
        SamzaOpUtils.deserializeTransformIOMap(config).get(transformFullName);
    transformInputs = ioFunc(transformInputOutput.getKey()).get();
    transformOutputs = ioFunc(transformInputOutput.getValue()).get();
    // init the metric
    if (config.getBoolean(ENABLE_TASK_METRICS, false)) {
      this.metricsRegistry = context.getTaskContext().getTaskMetricsRegistry();
    } else {
      this.metricsRegistry = context.getContainerContext().getContainerMetricsRegistry();
    }
    samzaOpMetricRegistry.getSamzaOpMetrics().register(transformFullName, metricsRegistry);
    this.taskContext = context.getTaskContext();
  }

  private static Supplier<List<String>> ioFunc(String ioList) {
    return () -> Arrays.stream(ioList.split(SamzaOpUtils.TRANSFORM_IO_MAP_DELIMITER))
        .filter(item -> !item.isEmpty())
        .collect(Collectors.toList());
  }
}
