package org.apache.beam.sdk.options;

import java.util.Iterator;
import java.util.ServiceLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * Interface to support offspring wire-in for Li: if input class meets some requirements,
 * the customized pipelineOptions will be initialized.
 *
 */
@SuppressWarnings("rawtypes")
public interface CustomPipelineOptionsFactoryFactory<T> {
  T initializePipelineOptions(T pipelineOptions, Class<T> clazz);

  interface Registrar {
    CustomPipelineOptionsFactoryFactory create();
  }


  static @Initialized @Nullable CustomPipelineOptionsFactoryFactory getFactory() {
    final Iterator<CustomPipelineOptionsFactoryFactory.Registrar>
        factories = ServiceLoader.load(CustomPipelineOptionsFactoryFactory.Registrar.class).iterator();
    return factories.hasNext() ? Iterators.getOnlyElement(factories).create() : null;
  }
}
