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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface RunnerPipelineOptionsFactory<T extends PipelineOptions> {
  T getPipelineOptions(String[] args, Class<T> clazz);

  interface Registar {
    <T extends PipelineOptions> RunnerPipelineOptionsFactory<T> create();
  }

  static @Initialized @Nullable <T extends PipelineOptions>
      RunnerPipelineOptionsFactory<T> getFactory() {
    final Iterator<Registar> factories = ServiceLoader.load(Registar.class).iterator();
    return factories.hasNext() ? Iterators.getOnlyElement(factories).create() : null;
  }
}
