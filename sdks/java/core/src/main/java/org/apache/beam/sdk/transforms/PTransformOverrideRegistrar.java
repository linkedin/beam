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
package org.apache.beam.sdk.transforms;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This is a singleton class that holds a map that mapping origin PTransform tag to replacement
 * PTransform. LI-specific class.
 */
@SuppressWarnings({"rawtypes"})
@Experimental
public class PTransformOverrideRegistrar {
  @SuppressWarnings("type.argument") // object guaranteed to be non-null
  private static final ThreadLocal<@NonNull Map<String, PTransform>>
      PTRANSFORM_OVERRIDE_REGISTRAR_MAP = ThreadLocal.withInitial(HashMap::new);

  public static <InputT extends PInput, OutputT extends POutput>
      PTransform<? super InputT, OutputT> applyTransformOverride(
          InputT input, PTransform<? super InputT, OutputT> transform) {
    if (transform instanceof OverridablePTransform) {
      PTransform<? super InputT, OutputT> overriddenTransform =
          getOverriddenPTransform(((OverridablePTransform) transform).getTag());
      if (overriddenTransform != null) {
        return overriddenTransform;
      }
    }
    return transform;
  }

  public static void register(String originPTransformTag, PTransform replacementPTransform) {
    checkArgument(!StringUtils.isBlank(originPTransformTag), "PTramsform tag cannot be null.");
    checkArgument(
        !PTRANSFORM_OVERRIDE_REGISTRAR_MAP.get().containsKey(originPTransformTag),
        "PTransform tag: "
            + originPTransformTag
            + " is already registered with PTransform: "
            + PTRANSFORM_OVERRIDE_REGISTRAR_MAP.get().get(originPTransformTag));
    PTRANSFORM_OVERRIDE_REGISTRAR_MAP.get().put(originPTransformTag, replacementPTransform);
  }

  public static @Nullable PTransform getOverriddenPTransform(String tag) {
    return PTRANSFORM_OVERRIDE_REGISTRAR_MAP.get().get(tag);
  }

  public static void clear() {
    PTRANSFORM_OVERRIDE_REGISTRAR_MAP.get().clear();
  }
}
