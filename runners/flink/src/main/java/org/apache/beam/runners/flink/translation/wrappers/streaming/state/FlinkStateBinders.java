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
package org.apache.beam.runners.flink.translation.wrappers.streaming.state;

import java.util.Iterator;
import java.util.ServiceLoader;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.flink.runtime.state.KeyedStateBackend;

/** Allows runners to customize Beam state binding to Flink keyed state. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class FlinkStateBinders {
  /** A registrar for custom {@link FlinkStateInternals.EarlyBinder} implementations. */
  public interface Registrar {
    FlinkStateInternals.EarlyBinder getEarlyBinder(
        KeyedStateBackend keyedStateBackend,
        SerializablePipelineOptions pipelineOptions,
        String stepName,
        Coder<? extends BoundedWindow> windowCoder);
  }

  private static final Registrar REGISTRAR = loadRegistrar();

  private FlinkStateBinders() {}

  public static FlinkStateInternals.EarlyBinder getEarlyBinder(
      KeyedStateBackend keyedStateBackend,
      SerializablePipelineOptions pipelineOptions,
      String stepName,
      Coder<? extends BoundedWindow> windowCoder) {
    if (REGISTRAR != null) {
      return REGISTRAR.getEarlyBinder(keyedStateBackend, pipelineOptions, stepName, windowCoder);
    }
    return new FlinkStateInternals.EarlyBinder(keyedStateBackend, pipelineOptions, windowCoder);
  }

  private static Registrar loadRegistrar() {
    Iterator<Registrar> registrars = ServiceLoader.load(Registrar.class).iterator();
    if (!registrars.hasNext()) {
      return null;
    }
    Registrar registrar = registrars.next();
    if (registrars.hasNext()) {
      throw new IllegalStateException("Expected at most one FlinkStateBinders.Registrar");
    }
    return registrar;
  }
}
