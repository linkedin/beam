package org.apache.beam.sdk.options;

import java.util.Iterator;
import java.util.ServiceLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.Nullable;


@SuppressWarnings("rawtypes")
/**
 * Interface to support offspring wire-in for Li: if input class meets some requirements,
 * the customized pipelineOptions will be initialized.
 *
 */
public interface InitializePipelineOptionsFactory<T> {
  T initializePipelineOptions(T pipelineOptions, Class<T> clazz);

  interface Registrar {
    InitializePipelineOptionsFactory create();
  }


  static @Initialized @Nullable InitializePipelineOptionsFactory getFactory() {
    final Iterator<InitializePipelineOptionsFactory.Registrar>
        factories = ServiceLoader.load(InitializePipelineOptionsFactory.Registrar.class).iterator();
    return factories.hasNext() ? Iterators.getOnlyElement(factories).create() : null;
  }
}
