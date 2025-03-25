package org.apache.beam.runners.flink.metrics;

import avro.shaded.com.google.common.collect.Iterators;
import java.util.Iterator;
import java.util.ServiceLoader;
import javax.annotation.Nullable;

/** Interface to support flexible metrics supports wire-in for Li. */
public interface CustomizeMetricsRegistrar {
  void setupMetrics(FlinkMetricContainer flinkMetricContainer);

  /**
   * Inject the implementation for the interface.
   * <p>Usage:
   *
   * <pre>{@code
   *  @AutoService(CustomizeMetricsRegistrar.Registrar.class)
   *   public static class Registrar implements CustomizeMetricsRegistrar.Registrar {
   *     @Override
   *     public CustomizeMetricsRegistrar create() {
   *       return new CustomizeMetricsRegistrarImpl();
   *     }
   *   }</pre>
   */
  interface Registrar {
    CustomizeMetricsRegistrar create();
  }

  static @Nullable CustomizeMetricsRegistrar get() {
    final Iterator<CustomizeMetricsRegistrar.Registrar> registrarIterator =
        ServiceLoader.load(CustomizeMetricsRegistrar.Registrar.class).iterator();
    return registrarIterator.hasNext()
        ? Iterators.getOnlyElement(registrarIterator).create()
        : null;
  }
}