package org.apache.beam.sdk.options;

import java.util.Iterator;
import java.util.ServiceLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * Interface to support offspring wire-in for Li: if input class is a
 * customized pipelineOptions created by an offspring factory,
 * the pipelineOptions will be initialized.
 *
 */
@SuppressWarnings("rawtypes")
public interface CustomPipelineOptionsInitializer<T> {
  void init(T pipelineOptions, Class<T> clazz);

  interface Registrar {
    CustomPipelineOptionsInitializer create();
  }


  static @Initialized @Nullable CustomPipelineOptionsInitializer get() {
    final Iterator<CustomPipelineOptionsInitializer.Registrar>
        factories = ServiceLoader.load(CustomPipelineOptionsInitializer.Registrar.class).iterator();
    return factories.hasNext() ? Iterators.getOnlyElement(factories).create() : null;
  }
}
