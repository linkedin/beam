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

import java.util.ServiceLoader;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.flink.runtime.state.KeyedStateBackend;

/** LinkedIn-only: allow customization in Beam state binding. */
@SuppressWarnings({"rawtypes", "nullness"})
public class FlinkStateBinders {
  /** An interface that allows custom {@link org.apache.beam.sdk.state.StateBinder}. */
  public interface Registrar {
    FlinkStateInternals.EarlyBinder getEarlyBinder(
        KeyedStateBackend keyedStateBackend,
        SerializablePipelineOptions pipelineOptions,
        String stepName);
  }

  private static final Registrar REGISTRAR =
      Iterables.getOnlyElement(ServiceLoader.load(Registrar.class), null);

  /**
   * Returns {@link
   * org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkStateInternals.EarlyBinder}
   * that creates the user states from the Flink state backend.
   */
  public static FlinkStateInternals.EarlyBinder getEarlyBinder(
      KeyedStateBackend keyedStateBackend,
      SerializablePipelineOptions pipelineOptions,
      String stepName) {
    if (REGISTRAR != null) {
      return REGISTRAR.getEarlyBinder(keyedStateBackend, pipelineOptions, stepName);
    } else {
      return new FlinkStateInternals.EarlyBinder(keyedStateBackend, pipelineOptions);
    }
  }
}
