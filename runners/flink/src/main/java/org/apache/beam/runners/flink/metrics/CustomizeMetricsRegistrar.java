/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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