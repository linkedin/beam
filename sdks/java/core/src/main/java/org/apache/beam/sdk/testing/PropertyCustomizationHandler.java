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
package org.apache.beam.sdk.testing;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.ServiceLoader;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.checkerframework.checker.nullness.qual.Nullable;

/** LinkedIn extension point for overriding pipeline option properties in tests. */
public interface PropertyCustomizationHandler {
  Boolean isCustomizationEnabled();

  Boolean containsProperty(Method method, String propertyName);

  Object getProperty(Method method, String propertyName);

  void setProperty(Method method, String propertyName, @Nullable Object value);

  void clear();

  /** Registrar for the property customization handler. */
  interface Registrar {
    PropertyCustomizationHandler create();
  }

  /** Loads the property customization handler, if present. */
  static @Nullable PropertyCustomizationHandler get() {
    Iterator<Registrar> registrars =
        ServiceLoader.load(Registrar.class, ReflectHelpers.findClassLoader()).iterator();
    if (!registrars.hasNext()) {
      return null;
    }
    Registrar registrar = registrars.next();
    if (registrars.hasNext()) {
      throw new IllegalStateException("Expected exactly one PropertyCustomizationHandler");
    }
    return registrar.create();
  }
}
