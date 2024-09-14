package org.apache.beam.sdk.metrics;

import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * This class is a wrapper around a {@link MetricKey} that caches its {@code toString()} value.
 *
 * The primary goal is to improve performance by caching the result of {@code toString()}
 * at construction, thereby reducing the overhead of repeated calls in performance-sensitive
 * contexts.
 *
 * The {@code stepName()} method delegates to the underlying {@link MetricKey}, while
 * the {@code metricName()} method now returns a {@link CachedToStringMetricName}, which
 * similarly caches the result of its {@code toString()} method for improved efficiency.
 */
public class CachedToStringMetricKey extends MetricKey {
  private final MetricKey metricKey;
  private final CachedToStringMetricName cachedToStringMetricName;
  private final String cachedToString;

  public CachedToStringMetricKey(MetricKey metricKey) {
    this.metricKey = metricKey;
    this.cachedToStringMetricName = new CachedToStringMetricName(metricKey.metricName());
    this.cachedToString = metricKey.toString();
  }

  @Override
  public @Nullable String stepName() {
    return metricKey.stepName();
  }

  @Override
  public MetricName metricName() {
    return cachedToStringMetricName;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    return metricKey.equals(o);
  }

  @Override
  public int hashCode() {
    return metricKey.hashCode();
  }

  @Override
  public String toString() {
    return cachedToString;
  }
}
