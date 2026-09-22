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
package org.apache.beam.sdk.expansion;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/** LinkedIn extension point for runner-specific runtime to expose external configuration. */
public interface ExternalConfigRegistrar {
  <K, V> Map<K, V> getExternalConfig(PipelineOptions options);

  /** Loads runner-specific external configuration for the supplied pipeline options. */
  static <K, V> Map<K, V> getConfig(PipelineOptions options) {
    Iterator<ExternalConfigRegistrar> registrars =
        ServiceLoader.load(ExternalConfigRegistrar.class, ReflectHelpers.findClassLoader())
            .iterator();
    if (!registrars.hasNext()) {
      return Collections.emptyMap();
    }
    ExternalConfigRegistrar registrar = registrars.next();
    if (registrars.hasNext()) {
      throw new IllegalStateException("Expected exactly one ExternalConfigRegistrar");
    }
    return registrar.getExternalConfig(options);
  }
}
