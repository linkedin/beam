package org.apache.beam.sdk.options;

import java.util.Iterator;
import java.util.ServiceLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.Nullable;


@SuppressWarnings("rawtypes")
/**
 * Interface to support offspring wire-in for Li
 */
public interface InvokePipelineOptionsFactory<T> {
  T getPipelineOptions(T pipelineOptions, Class<T> clazz);

  interface Registrar {
    InvokePipelineOptionsFactory create();
  }


  static @Initialized @Nullable InvokePipelineOptionsFactory getFactory() {
    final Iterator<InvokePipelineOptionsFactory.Registrar>
        factories = ServiceLoader.load(InvokePipelineOptionsFactory.Registrar.class).iterator();
    return factories.hasNext() ? Iterators.getOnlyElement(factories).create() : null;
  }
}
