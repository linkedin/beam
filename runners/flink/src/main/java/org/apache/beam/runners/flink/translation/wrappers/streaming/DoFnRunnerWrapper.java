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

import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * LI-SPECIFIC: a plug-in point that lets a runner extension wrap the {@link DoFnRunner} of the
 * classic (non-portable) Flink streaming DoFn operators: {@link DoFnOperator}, {@link
 * SplittableDoFnOperator} and {@link WindowDoFnOperator}. {@link ExecutableStageDoFnOperator} is
 * not wrapped.
 *
 * <p>Implementations are discovered with {@link java.util.ServiceLoader}, only through the class
 * loader that loaded Beam (this interface), never the thread context class loader. They must have a
 * public no-argument constructor and be listed in {@code
 * META-INF/services/org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnRunnerWrapper},
 * and the jar that contains them must be visible to that class loader: in practice, shipped in the
 * same job jar as Beam. A setup where Flink's parent class loader loads Beam and only the user
 * class loader sees the wrapper is not supported: there the wrapper is silently not loaded, and the
 * only sign is that the INFO log line "Loaded DoFnRunnerWrappers" is missing. A listed wrapper that
 * fails to load or to construct fails the operator's {@code open()} rather than being skipped. The
 * wrappers are loaded once per Beam class loader, and one instance of each is shared by every
 * operator that class loader serves, so implementations must be thread-safe and should keep
 * per-operator state in the runner they return.
 *
 * <p>{@link #wrap} is called once per operator instance, from the operator's {@code open()} on the
 * task thread. The returned runner sits outside the runner that the operator builds for the user
 * DoFn (including the stateful and late-data-dropping runners and any wrapper added by an operator
 * subclass), and inside the {@code @RequiresStableInput} buffering runner, the side-input pushback
 * runner and the metrics runner. So it sees every {@code startBundle}, {@code processElement},
 * {@code onTimer} and {@code finishBundle} call the operator makes to run the DoFn, when the call
 * is made: an element held for stable input or pushed back until side inputs are ready reaches the
 * wrapper when it is processed, not when it arrives. State cleanup and {@code @OnWindowExpiration}
 * run inside the {@code onTimer} call of the cleanup timer. {@link WindowDoFnOperator} and {@link
 * SplittableDoFnOperator} deliver fired timers through {@code processElement}.
 *
 * <p>The returned runner must delegate every {@link DoFnRunner} method to {@code runner}, including
 * {@link DoFnRunner#getFn()}.
 */
public interface DoFnRunnerWrapper {

  /** The kind of operator whose runner is wrapped. */
  enum Kind {
    /** A {@link DoFnOperator} running a user {@code ParDo}, stateful or not. */
    PAR_DO,
    /**
     * A {@link SplittableDoFnOperator}. Its input is a {@code KeyedWorkItem} of element and
     * restriction pairs, or of timers.
     */
    SPLITTABLE,
    /**
     * A {@link WindowDoFnOperator} running {@code GroupByKey} or {@code Combine.perKey}. Its input
     * is a {@code KeyedWorkItem} of elements, or of timers.
     */
    WINDOW
  }

  /** What the operator whose runner is wrapped tells the wrapper about itself. */
  interface Context {
    /** The full name of the transform the operator executes. */
    String getStepName();

    /** The operator's pipeline options. */
    PipelineOptions getPipelineOptions();

    /** The kind of operator. */
    Kind getKind();
  }

  /**
   * Wrappers apply in ascending order; a higher order wraps further outside. Wrappers with the same
   * order apply in the order of their class names.
   */
  default int order() {
    return 0;
  }

  /**
   * Returns the runner the operator should use in place of {@code runner}. Must return {@code
   * runner} unchanged when the extension is not enabled for this operator, and must not return
   * {@code null}.
   */
  <InputT, OutputT> DoFnRunner<InputT, OutputT> wrap(
      DoFnRunner<InputT, OutputT> runner, Context context);
}
