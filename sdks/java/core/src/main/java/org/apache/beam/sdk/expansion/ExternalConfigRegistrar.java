package org.apache.beam.sdk.expansion;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;

@Experimental
/**
 * Inject external configs to pipelineOptions
 */
public interface ExternalConfigRegistrar {
  <K, V> Map<K, V> getExternalConfig(PipelineOptions options);

  static <K, V> Map<K, V> getFactory(PipelineOptions options) {
    final Iterator<ExternalConfigRegistrar> factories =
        ServiceLoader.load(ExternalConfigRegistrar.class).iterator();

    return Iterators.getOnlyElement(factories).getExternalConfig(options);
  }
}
