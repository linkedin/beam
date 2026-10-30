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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LI-SPECIFIC: loads the {@link DoFnRunnerWrapper} implementations and applies them to a runner.
 *
 * <p>The implementations are loaded once per class loader of this class, on first use, with the
 * class loader that loaded {@link DoFnRunnerWrapper}. The thread context class loader is not used:
 * the result is cached in a static field of this class, so it must depend only on the class loader
 * that owns that field, not on whichever task thread happens to call first.
 */
final class DoFnRunnerWrappers {

  private static final Logger LOG = LoggerFactory.getLogger(DoFnRunnerWrappers.class);

  private static final Comparator<DoFnRunnerWrapper> ORDER =
      Comparator.comparingInt(DoFnRunnerWrapper::order)
          .thenComparing(wrapper -> wrapper.getClass().getName());

  private static final Object LOCK = new Object();

  private static volatile @Nullable List<DoFnRunnerWrapper> loaded;

  private static volatile @Nullable List<DoFnRunnerWrapper> wrappersForTesting;

  private DoFnRunnerWrappers() {}

  /**
   * Applies every loaded wrapper to {@code runner}, lowest order first, and returns the outermost
   * runner. Returns {@code runner} itself when no wrapper is loaded or every wrapper returns its
   * input unchanged.
   */
  static <InputT, OutputT> DoFnRunner<InputT, OutputT> apply(
      DoFnRunner<InputT, OutputT> runner, DoFnRunnerWrapper.Context context) {
    DoFnRunner<InputT, OutputT> result = runner;
    for (DoFnRunnerWrapper wrapper : getWrappers()) {
      result =
          Preconditions.checkNotNull(
              wrapper.wrap(result, context),
              "%s returned null for step %s",
              wrapper.getClass().getName(),
              context.getStepName());
    }
    return result;
  }

  static DoFnRunnerWrapper.Context context(
      String stepName, PipelineOptions pipelineOptions, DoFnRunnerWrapper.Kind kind) {
    return new ContextImpl(stepName, pipelineOptions, kind);
  }

  /** The wrappers in the order they apply. */
  static List<DoFnRunnerWrapper> getWrappers() {
    List<DoFnRunnerWrapper> forTesting = wrappersForTesting;
    if (forTesting != null) {
      return forTesting;
    }
    List<DoFnRunnerWrapper> result = loaded;
    if (result == null) {
      synchronized (LOCK) {
        result = loaded;
        if (result == null) {
          result =
              sorted(
                  ServiceLoader.load(
                      DoFnRunnerWrapper.class, DoFnRunnerWrapper.class.getClassLoader()));
          if (!result.isEmpty()) {
            LOG.info(
                "Loaded DoFnRunnerWrappers, innermost first: {}",
                result.stream()
                    .map(wrapper -> wrapper.getClass().getName())
                    .collect(Collectors.toList()));
          }
          loaded = result;
        }
      }
    }
    return result;
  }

  @VisibleForTesting
  static List<DoFnRunnerWrapper> sorted(Iterable<DoFnRunnerWrapper> wrappers) {
    List<DoFnRunnerWrapper> result = new ArrayList<>();
    wrappers.forEach(result::add);
    result.sort(ORDER);
    return ImmutableList.copyOf(result);
  }

  /**
   * Makes {@link #getWrappers()} return {@code wrappers}, sorted, instead of the loaded ones, until
   * {@link #resetForTesting()}. Lets tests install wrappers without a {@code META-INF/services}
   * file that would wrap every operator of every test in the module.
   */
  @VisibleForTesting
  static void setForTesting(List<DoFnRunnerWrapper> wrappers) {
    wrappersForTesting = sorted(wrappers);
  }

  @VisibleForTesting
  static void resetForTesting() {
    wrappersForTesting = null;
  }

  private static final class ContextImpl implements DoFnRunnerWrapper.Context {
    private final String stepName;
    private final PipelineOptions pipelineOptions;
    private final DoFnRunnerWrapper.Kind kind;

    private ContextImpl(
        String stepName, PipelineOptions pipelineOptions, DoFnRunnerWrapper.Kind kind) {
      this.stepName = stepName;
      this.pipelineOptions = pipelineOptions;
      this.kind = kind;
    }

    @Override
    public String getStepName() {
      return stepName;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return pipelineOptions;
    }

    @Override
    public DoFnRunnerWrapper.Kind getKind() {
      return kind;
    }

    @Override
    public String toString() {
      return "DoFnRunnerWrapper.Context{stepName=" + stepName + ", kind=" + kind + "}";
    }
  }
}
