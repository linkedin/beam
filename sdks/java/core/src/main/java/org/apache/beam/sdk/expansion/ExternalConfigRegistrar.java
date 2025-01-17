package org.apache.beam.sdk.expansion;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;

/**
 * A LinkedIn factory interface for runner-specific runtime to load external configs.
 */
@Experimental
public interface ExternalConfigRegistrar {
  <K, V> Map<K, V> getExternalConfig(PipelineOptions options);

  /**
   * Find the {@link ExternalConfigRegistrar} to load external configs for different runners.
   * @param options the pipeline options
   * @return  a map contains external configs
   * @param <K> the type of the Key
   * @param <V> the type of the Value
   */
  static <K, V> Map<K, V> getConfig(PipelineOptions options) {
    final Iterator<ExternalConfigRegistrar> factories =
        ServiceLoader.load(ExternalConfigRegistrar.class).iterator();

    return factories.hasNext() ? Iterators.getOnlyElement(factories).getExternalConfig(options) : new HashMap<>();
  }
}