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
package org.apache.beam.sdk.metrics;

import java.io.Serializable;
import java.util.Set;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

/**
 * The <code>Metrics</code> is a utility class for producing various kinds of metrics for reporting
 * properties of an executing pipeline.
 *
 * <p>Metrics are created by calling one of the static methods in this class. Each metric is
 * associated with a namespace and a name. The namespace allows grouping related metrics together
 * based on the definition while also disambiguating common names based on where they are defined.
 *
 * <p>Reported metrics are implicitly scoped to the transform within the pipeline that reported
 * them. This allows reporting the same metric name in multiple places and identifying the value
 * each transform reported, as well as aggregating the metric across
 *
 * <p>It is runner-dependent whether Metrics are accessible during pipeline execution or only after
 * jobs have completed.
 *
 * <p>Example:
 *
 * <pre><code> class SomeDoFn extends{@literal DoFn<String, String>} {
 *   private Counter counter = Metrics.counter(SomeDoFn.class, "my-counter");
 *
 *  {@literal @}ProcessElement
 *   public void processElement(ProcessContext c) {
 *     counter.inc();
 *     Metrics.counter(SomeDoFn.class, "my-counter2").inc();
 *   }
 * }</code></pre>
 *
 * <p>See {@link MetricResults} (available from the {@code PipelineResults} interface) for an
 * example off how to query metrics.
 */
@Experimental(Kind.METRICS)
public class Metrics {

  private Metrics() {}

  /**
   * Create a metric that can be incremented and decremented, and is aggregated by taking the sum.
   */
  public static Counter counter(String namespace, String name) {
    return new DelegatingCounter(MetricName.named(namespace, name));
  }

  /**
   * Create a metric that can be incremented and decremented, and is aggregated by taking the sum.
   */
  public static Counter counter(Class<?> namespace, String name) {
    return new DelegatingCounter(MetricName.named(namespace, name));
  }

  /** Create a metric that records various statistics about the distribution of reported values. */
  public static Distribution distribution(String namespace, String name) {
    return new DelegatingDistribution(MetricName.named(namespace, name));
  }

  /** Create a metric that records various statistics about the distribution of reported values. */
  public static Distribution distribution(Class<?> namespace, String name) {
    return new DelegatingDistribution(MetricName.named(namespace, name));
  }

  /**
   * Create a metric that records various statistics about the distribution of reported values.
   *
   * @param namespace namespace for the distribution
   * @param name name of the distribution
   * @param percentiles Set of percentiles to be computed in the distribution. If the user wishes to
   *     compute the 90th and 99th percentile, the set of percentiles would be [90.0D, 99.0D].
   */
  public static Distribution distribution(
      Class<?> namespace, String name, Set<Double> percentiles) {
    validatePercentiles(percentiles);
    return new DelegatingDistribution(MetricName.named(namespace, name), percentiles);
  }

  /**
   * Create a metric that records various statistics about the distribution of reported values.
   *
   * @param namespace namespace for the distribution
   * @param name name of the distribution
   * @param percentiles Set of percentiles to be computed in the distribution. If the user wishes to
   *     compute the 90th and 99th percentile, the set of percentiles would be [90.0D, 99.0D].
   */
  public static Distribution distribution(String namespace, String name, Set<Double> percentiles) {
    validatePercentiles(percentiles);
    return new DelegatingDistribution(MetricName.named(namespace, name), percentiles);
  }

  private static void validatePercentiles(Set<Double> percentiles) {
    Preconditions.checkArgument(
        percentiles != null && !percentiles.isEmpty(),
        "Percentiles cannot be null or an empty set.");
    final ImmutableSet<Double> invalidPercentiles =
        percentiles.stream()
            .filter(perc -> perc < 0.0 || perc > 100.0)
            .collect(ImmutableSet.toImmutableSet());
    Preconditions.checkArgument(
        invalidPercentiles.isEmpty(),
        "User supplied percentiles should be between "
            + "0.0 and 100.0. Following invalid percentiles were supplied: "
            + invalidPercentiles);
  }

  /**
   * Create a metric that can have its new value set, and is aggregated by taking the last reported
   * value.
   */
  public static Gauge gauge(String namespace, String name) {
    return new DelegatingGauge(MetricName.named(namespace, name));
  }

  /**
   * Create a metric that can have its new value set, and is aggregated by taking the last reported
   * value.
   */
  public static Gauge gauge(Class<?> namespace, String name) {
    return new DelegatingGauge(MetricName.named(namespace, name));
  }

  /**
   * Implementation of {@link Distribution} that delegates to the instance for the current context.
   */
  private static class DelegatingDistribution implements Metric, Distribution, Serializable {
    private final MetricName name;
    private final Set<Double> percentiles;

    private DelegatingDistribution(MetricName name) {
      this.name = name;
      this.percentiles = ImmutableSet.of();
    }

    private DelegatingDistribution(MetricName name, Set<Double> percentiles) {
      this.name = name;
      this.percentiles = percentiles;
    }

    @Override
    public void update(long value) {
      MetricsContainer container = MetricsEnvironment.getCurrentContainer();
      if (container != null) {
        container.getDistribution(name, percentiles).update(value);
      }
    }

    @Override
    public void update(long sum, long count, long min, long max) {
      MetricsContainer container = MetricsEnvironment.getCurrentContainer();
      if (container != null) {
        container.getDistribution(name, percentiles).update(sum, count, min, max);
      }
    }

    @Override
    public MetricName getName() {
      return name;
    }
  }

  /** Implementation of {@link Gauge} that delegates to the instance for the current context. */
  private static class DelegatingGauge implements Metric, Gauge, Serializable {
    private final MetricName name;

    private DelegatingGauge(MetricName name) {
      this.name = name;
    }

    @Override
    public void set(long value) {
      MetricsContainer container = MetricsEnvironment.getCurrentContainer();
      if (container != null) {
        container.getGauge(name).set(value);
      }
    }

    @Override
    public MetricName getName() {
      return name;
    }
  }
}
