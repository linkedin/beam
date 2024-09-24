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
package org.apache.beam.runners.samza.metrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.beam.runners.core.metrics.DefaultMetricResults;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.GaugeData;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.Metric;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class holds the {@link MetricsContainer}s for BEAM metrics, and update the results to Samza
 * metrics.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SamzaMetricsContainer {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaMetricsContainer.class);
  private static final String BEAM_METRICS_GROUP = "BeamMetrics";
  public static final String GLOBAL_CONTAINER_STEP_NAME = "GLOBAL_METRICS";
  public static final String USE_SHORT_METRIC_NAMES_CONFIG =
      "beam.samza.metrics.useShortMetricNames";
  public static final String COMMIT_ALL_METRIC_UPDATES =
      "beam.samza.metrics.commitAllMetricUpdates";
  public static final String DEFER_TO_EXECUTOR_CONFIG = "beam.samza.metrics.deferToExecutor";
  public static final String DEFER_TO_EXECUTOR_UPDATE_INTERVAL_SEC_CONFIG =
      "beam.samza.metrics.deferToExecutor.updateIntervalSec";
  private final MetricsContainerStepMap metricsContainers = new MetricsContainerStepMap();
  private final MetricsRegistryMap metricsRegistry;
  private final boolean useShortMetricNames;
  private final boolean commitAllMetricUpdates;
  private final boolean deferToExecutor;
  private final Set<String> activeStepNames = ConcurrentHashMap.newKeySet();
  private ScheduledExecutorService scheduler;
  private ScheduledFuture<?> scheduledFuture;
  // Static AtomicInteger to ensure thread-safe incrementing of instance IDs
  private static final AtomicInteger instanceCounter = new AtomicInteger(0);

  public SamzaMetricsContainer(MetricsRegistryMap metricsRegistry, Config config) {
    // Assign a unique ID to this instance by incrementing the static counter
    // Instance-specific ID
    int instanceId = getInstanceId();
    this.metricsRegistry = metricsRegistry;
    this.useShortMetricNames = config.getBoolean(USE_SHORT_METRIC_NAMES_CONFIG, false);
    this.commitAllMetricUpdates = config.getBoolean(COMMIT_ALL_METRIC_UPDATES, false);
    this.deferToExecutor = config.getBoolean(DEFER_TO_EXECUTOR_CONFIG, false);
    long metricUpdateIntervalSec = config.getLong(DEFER_TO_EXECUTOR_UPDATE_INTERVAL_SEC_CONFIG, 1L);
    this.metricsRegistry.metrics().put(BEAM_METRICS_GROUP, new ConcurrentHashMap<>());
    LOG.info(
        "Creating Samza metrics container (instanceId={}) with deferToExecutor={}, metricUpdateIntervalSec={}, useShortMetricNames={}, commitAllMetricUpdates={}",
        instanceId,
        deferToExecutor,
        metricUpdateIntervalSec,
        useShortMetricNames,
        commitAllMetricUpdates);
    // Initialize the executor service if needed based on configuration
    if (deferToExecutor) {
      scheduler =
          Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat("MetricsUpdater-instance-" + instanceId + "-thread-%d")
                  .build());
      scheduledFuture =
          scheduler.scheduleAtFixedRate(
              this::commitPeriodicMetricsForAllSteps, 0, metricUpdateIntervalSec, TimeUnit.SECONDS);
      LOG.info("Executor service for instance {} has been started.", instanceId);
    }
    // Register a shutdown hook to gracefully shut down the executor service
    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownExecutorService));
  }

  // Static method to return a unique ID for each instance
  private static int getInstanceId() {
    return instanceCounter.incrementAndGet();
  }

  public MetricsContainer getContainer(String stepName) {
    return this.metricsContainers.getContainer(stepName);
  }

  public MetricsContainerStepMap getContainers() {
    return this.metricsContainers;
  }

  /**
   * This is the public method for updating metrics. It either defers the update to the executor
   * service or updates the metrics immediately based on the deferToExecutor configuration flag.
   *
   * @param stepName the step name for which metrics are being updated
   */
  public void updateMetrics(String stepName) {
    if (deferToExecutor) {
      // Add the step name to the active steps set
      if (activeStepNames.add(stepName)) {
        LOG.info("Added step '{}' for deferred metrics update.", stepName);
      }
    } else {
      // Update metrics immediately if not deferring to the executor
      updateMetricsInternal(stepName);
    }
  }

  /**
   * Shutdown the executor service and cancel the scheduled task. Before shutting down, update
   * metrics one last time to ensure completeness.
   */
  private void shutdownExecutorService() {
    if (scheduler != null && !scheduler.isShutdown()) {
      LOG.info("Shutting down executor service...");

      // Cancel the scheduled task if it's still running.
      // This ensures that any periodic metrics updates are stopped and no further updates
      // are scheduled once we begin the shutdown process.
      if (scheduledFuture != null && !scheduledFuture.isCancelled()) {
        LOG.info("Cancelling scheduled metrics updates...");
        scheduledFuture.cancel(true);
      }

      // Update metrics one last time, ensuring that we're still committing from a single thread.
      // This guarantees that all remaining metrics are committed before shutting down the executor
      // service.
      commitPeriodicMetricsForAllSteps();

      // Shutdown the executor service gracefully.
      // Allow any currently executing tasks to finish, then terminate the service. If the shutdown
      // process takes longer than 5 seconds, force a shutdown to ensure the service is stopped.
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          LOG.warn("Forcing shutdown of executor service...");
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOG.error("Interrupted during shutdown, forcing shutdown now", e);
        scheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Commits metrics updates for all scheduled steps, invoked periodically.
   *
   * <p>This method is called periodically by the scheduled executor service when `deferToExecutor`
   * is set to true, ensuring that metrics for all active steps are updated at the defined interval.
   * It is also invoked during shutdown to ensure that any remaining metrics are committed before
   * terminating the executor.
   *
   * <p>Synchronization is applied to ensure thread-safe updates when multiple instances might share
   * the same resources.
   */
  private void commitPeriodicMetricsForAllSteps() {
    synchronized (SamzaMetricsContainer.class) {
      activeStepNames.forEach(this::updateMetricsInternal);
    }
  }

  /**
   * Updates metrics for a given step.
   *
   * <p>This method contains the core logic for updating metrics, including counters, gauges, and
   * distributions for the specified step. It can be called either immediately or by the scheduled
   * executor service, based on the `deferToExecutor` configuration.
   *
   * <p>This method assumes that it is called in a thread-safe manner, as it does not implement
   * synchronization internally.
   *
   * @param stepName the step name for which metrics are being updated
   */
  private void updateMetricsInternal(String stepName) {
    List<String> stepNameList = Arrays.asList(stepName, GLOBAL_CONTAINER_STEP_NAME);
    MetricResults metricResults =
        asAttemptedOnlyMetricResultsForSteps(metricsContainers, stepNameList);
    MetricQueryResults results = metricResults.allMetrics();

    final CounterUpdater updateCounter = new CounterUpdater();
    results.getCounters().forEach(updateCounter);

    final GaugeUpdater updateGauge = new GaugeUpdater();
    results.getGauges().forEach(updateGauge);

    final DistributionUpdater updateDistribution = new DistributionUpdater();
    results.getDistributions().forEach(updateDistribution);

    if (commitAllMetricUpdates) {
      stepNameList.stream()
          .map(metricsContainers::getContainer)
          .forEach(MetricsContainerImpl::commitUpdates);
    }
  }

  public void updateExecutableStageBundleMetric(String metricName, long time) {
    @SuppressWarnings("unchecked")
    Gauge<Long> gauge = (Gauge<Long>) getSamzaMetricFor(metricName);
    if (gauge == null) {
      gauge = metricsRegistry.newGauge(BEAM_METRICS_GROUP, metricName, 0L);
    }
    gauge.set(time);
  }

  private class CounterUpdater implements Consumer<MetricResult<Long>> {
    @Override
    public void accept(MetricResult<Long> metricResult) {
      final String metricName = getMetricName(metricResult);
      Counter counter = (Counter) getSamzaMetricFor(metricName);
      if (counter == null) {
        counter = metricsRegistry.newCounter(BEAM_METRICS_GROUP, metricName);
      }
      counter.dec(counter.getCount());
      counter.inc(metricResult.getAttempted());
    }
  }

  private class GaugeUpdater implements Consumer<MetricResult<GaugeResult>> {
    @Override
    public void accept(MetricResult<GaugeResult> metricResult) {
      final String metricName = getMetricName(metricResult);
      @SuppressWarnings("unchecked")
      Gauge<Long> gauge = (Gauge<Long>) getSamzaMetricFor(metricName);
      if (gauge == null) {
        gauge = metricsRegistry.newGauge(BEAM_METRICS_GROUP, metricName, 0L);
      }
      gauge.set(metricResult.getAttempted().getValue());
    }
  }

  private class DistributionUpdater implements Consumer<MetricResult<DistributionResult>> {
    @Override
    public void accept(MetricResult<DistributionResult> metricResult) {
      final String metricName = getMetricName(metricResult);
      final DistributionResult distributionResult = metricResult.getAttempted();
      setLongGauge(metricName + "Sum", distributionResult.getSum());
      setLongGauge(metricName + "Count", distributionResult.getCount());
      setLongGauge(metricName + "Max", distributionResult.getMax());
      setLongGauge(metricName + "Min", distributionResult.getMin());
      distributionResult
          .getPercentiles()
          .forEach(
              (percentile, percentileValue) -> {
                final String percentileMetricName = metricName + getPercentileSuffix(percentile);
                @SuppressWarnings("unchecked")
                Gauge<Double> gauge = (Gauge<Double>) getSamzaMetricFor(percentileMetricName);
                if (gauge == null) {
                  gauge = metricsRegistry.newGauge(BEAM_METRICS_GROUP, percentileMetricName, 0.0D);
                }
                gauge.set(percentileValue);
              });
    }

    private void setLongGauge(String metricName, Long value) {
      @SuppressWarnings("unchecked")
      Gauge<Long> gauge = (Gauge<Long>) getSamzaMetricFor(metricName);
      if (gauge == null) {
        gauge = metricsRegistry.newGauge(BEAM_METRICS_GROUP, metricName, 0L);
      }
      gauge.set(value);
    }

    private String getPercentileSuffix(Double value) {
      String strValue;
      if (value == value.intValue()) {
        strValue = String.valueOf(value.intValue());
      } else {
        strValue = String.valueOf(value).replace(".", "_");
      }
      return "P" + strValue;
    }
  }

  private Metric getSamzaMetricFor(String metricName) {
    return metricsRegistry.getGroup(BEAM_METRICS_GROUP).get(metricName);
  }

  private String getMetricName(MetricResult<?> metricResult) {
    return useShortMetricNames
        ? metricResult.getName().toString()
        : metricResult.getKey().toString();
  }

  /**
   * Similar to {@link MetricsContainerStepMap#asAttemptedOnlyMetricResults}, it gets the metrics
   * results from the MetricsContainerStepMap. Instead of getting from all steps, it gets result
   * from only interested steps. Thus, it's more efficient.
   */
  private static MetricResults asAttemptedOnlyMetricResultsForSteps(
      MetricsContainerStepMap metricsContainers, List<String> steps) {
    List<MetricResult<Long>> counters = new ArrayList<>();
    List<MetricResult<GaugeResult>> gauges = new ArrayList<>();
    List<MetricResult<DistributionResult>> distributions = new ArrayList<>();

    for (String step : steps) {
      MetricsContainerImpl container = metricsContainers.getContainer(step);
      MetricUpdates cumulative = container.getUpdates();

      // Merging counters
      for (MetricUpdates.MetricUpdate<Long> counterUpdate : cumulative.counterUpdates()) {
        counters.add(MetricResult.attempted(counterUpdate.getKey(), counterUpdate.getUpdate()));
      }

      // Merging distributions
      for (MetricUpdates.MetricUpdate<DistributionData> distributionUpdate :
          cumulative.distributionUpdates()) {
        distributions.add(
            MetricResult.attempted(
                distributionUpdate.getKey(), distributionUpdate.getUpdate().extractResult()));
      }

      // Merging gauges
      for (MetricUpdates.MetricUpdate<GaugeData> gaugeUpdate : cumulative.gaugeUpdates()) {
        gauges.add(
            MetricResult.attempted(gaugeUpdate.getKey(), gaugeUpdate.getUpdate().extractResult()));
      }
    }

    return new DefaultMetricResults(counters, distributions, gauges);
  }
}
