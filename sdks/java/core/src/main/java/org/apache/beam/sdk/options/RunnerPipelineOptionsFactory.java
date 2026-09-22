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
package org.apache.beam.sdk.options;

import java.util.Iterator;
import java.util.ServiceLoader;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.checkerframework.checker.nullness.qual.Nullable;

/** LinkedIn extension point for runner-specific creation of {@link PipelineOptions}. */
public interface RunnerPipelineOptionsFactory {
  <T extends PipelineOptions> T getPipelineOptions(String[] args, Class<T> clazz);

  /** Registrar for the runner-specific pipeline options factory. */
  interface Registrar {
    RunnerPipelineOptionsFactory create();
  }

  /** Loads the runner-specific pipeline options factory, if present. */
  static @Nullable RunnerPipelineOptionsFactory getFactory() {
    Iterator<Registrar> registrars =
        ServiceLoader.load(Registrar.class, ReflectHelpers.findClassLoader()).iterator();
    if (!registrars.hasNext()) {
      return null;
    }
    Registrar registrar = registrars.next();
    if (registrars.hasNext()) {
      throw new IllegalStateException("Expected exactly one RunnerPipelineOptionsFactory");
    }
    return registrar.create();
  }

  /** Returns the runner factory class currently on the stack, or null outside factory creation. */
  static @Nullable Class<? extends RunnerPipelineOptionsFactory> findFactoryCaller() {
    for (StackTraceElement element : new Throwable().getStackTrace()) {
      try {
        Class<?> clazz =
            Class.forName(element.getClassName(), false, ReflectHelpers.findClassLoader());
        if (RunnerPipelineOptionsFactory.class != clazz
            && RunnerPipelineOptionsFactory.class.isAssignableFrom(clazz)) {
          @SuppressWarnings("unchecked")
          Class<? extends RunnerPipelineOptionsFactory> factoryClass =
              (Class<? extends RunnerPipelineOptionsFactory>) clazz;
          return factoryClass;
        }
      } catch (ClassNotFoundException e) {
        // Stack frames may include generated or VM-private classes that are not loadable here.
      }
    }
    return null;
  }
}
