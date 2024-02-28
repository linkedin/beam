package org.apache.beam.sdk.options;

import java.lang.annotation.Annotation;
import java.util.Iterator;
import java.util.ServiceLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.Nullable;


@SuppressWarnings("rawtypes")
public interface DefaultOffspringFactory<T> {
  Object returnDefaultValue(PipelineOptions pipelineOptions, Annotation annotation);

  interface Registrar {
    DefaultOffspringFactory create();
  }

  static @Initialized @Nullable DefaultOffspringFactory getFactory() {
    final Iterator<DefaultOffspringFactory.Registrar>
        factories = ServiceLoader.load(DefaultOffspringFactory.Registrar.class).iterator();
    return factories.hasNext() ? Iterators.getOnlyElement(factories).create() : null;
  }

}
