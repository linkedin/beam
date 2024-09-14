package org.apache.beam.sdk.metrics;

import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * This class wraps around a {@link MetricName} and caches its {@code toString()} value.
 *
 * The purpose of this class is to optimize repeated calls to {@code toString()} by
 * caching the result upon construction. This reduces overhead in scenarios where
 * the {@code toString()} method is frequently invoked.
 *
 * It delegates all other method calls (e.g., {@code getNamespace()}, {@code getName()})
 * to the underlying {@link MetricName} instance.
 */
public class CachedToStringMetricName extends MetricName {
  private final MetricName metricName;
  private final String cachedToString;

  public CachedToStringMetricName(MetricName metricName) {
    this.metricName = metricName;
    this.cachedToString = metricName.toString();
  }

  @Override
  public String getNamespace() {
    return metricName.getNamespace();
  }

  @Override
  public String getName() {
    return metricName.getName();
  }

  @Override
  public boolean equals(@Nullable Object o) {
    return metricName.equals(o);
  }

  @Override
  public int hashCode() {
    return metricName.hashCode();
  }

  @Override
  public String toString() {
    return cachedToString;
  }
}
