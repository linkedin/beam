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
package org.apache.beam.runners.flink.metrics;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for managing global metrics in a Flink environment. */
public class GlobalMetricsUtils {
  private static final Logger LOG = LoggerFactory.getLogger(GlobalMetricsUtils.class);
  private static final String GLOBAL_CONTAINER_STEP_NAME = "GLOBAL_METRICS";

  // Maintain a reference to the FlinkMetricContainer for updating global metrics
  private static final AtomicReference<FlinkMetricContainer> GLOBAL_FLINK_METRIC_CONTAINER =
      new AtomicReference<>(null);

  /**
   * Sets the global metrics container if it is not already set.
   *
   * @param flinkMetricContainer The Flink metric container to set as the global container.
   */
  public static synchronized void setGlobalMetrics(FlinkMetricContainer flinkMetricContainer) {
    if (MetricsEnvironment.getGlobalContainer().get() == null) {
      MetricsEnvironment.setGlobalContainer(
          flinkMetricContainer.getMetricsContainer(GLOBAL_CONTAINER_STEP_NAME));
      // Store the FlinkMetricContainer reference for later use
      GLOBAL_FLINK_METRIC_CONTAINER.set(flinkMetricContainer);
      LOG.debug("Set global FlinkMetricContainer reference");
    }
  }

  /**
   * Updates and publishes global metrics to Flink's metrics system.
   *
   * <p>This method retrieves the stored FlinkMetricContainer reference and publishes accumulated
   * metrics from async callback threads or other non-main-thread operations to Flink's metrics
   * framework.
   *
   * <p>This method is synchronized to prevent race conditions when multiple threads attempt to
   * update metrics concurrently, which could lead to incorrect metric values or duplicate metric
   * registrations in Flink's metrics system.
   */
  public static synchronized void updateGlobalMetrics() {
    FlinkMetricContainer container = GLOBAL_FLINK_METRIC_CONTAINER.get();
    if (container == null) {
      LOG.warn("Cannot update global metrics: FlinkMetricContainer reference not set");
      return;
    }

    try {
      container.updateMetrics(GLOBAL_CONTAINER_STEP_NAME);
      LOG.debug("Successfully updated global metrics");
    } catch (Exception e) {
      LOG.warn("Failed to update global metrics", e);
    }
  }
}
