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
package org.apache.beam.runners.flink;

import org.apache.beam.sdk.transforms.DoFn;

/**
 * Opt-in interface for a {@link DoFn} that needs to know which physical Flink subtask it is running
 * on, and at what parallelism/max-parallelism.
 *
 * <p>Beam's SDK deliberately keeps user {@link DoFn}s runner-agnostic, so there is normally no way
 * for a {@code DoFn} to observe {@code RuntimeContext#getIndexOfThisSubtask()} or the parallelism
 * it is running with. Some {@code DoFn}s, however, legitimately need this -- for example, a
 * stateless pre-check {@code DoFn} that verifies (using {@code
 * org.apache.flink.runtime.state.KeyGroupRangeAssignment#assignKeyToParallelOperator}) that a
 * record actually belongs on the current subtask before it reaches a downstream keyed operator that
 * skips the normal shuffle (see {@link
 * org.apache.beam.runners.flink.translation.utils.SkipShufflePartitionGuard}), so it can reroute
 * misaligned records (e.g. to a dead-letter output) instead of letting them reach -- and
 * potentially corrupt -- keyed state.
 *
 * <p>The Flink runner's {@code DoFnOperator} calls {@link #setFlinkSubtaskContext} once per
 * operator instance, during {@code open()}, before any elements are processed. This is
 * intentionally a minimal, generic hook: it exposes only the three values needed for this kind of
 * alignment check, and does not expose the full Flink {@code RuntimeContext}, keeping the
 * runner-specific surface area small.
 */
public interface FlinkSubtaskContextAware {

  /**
   * Called once, before {@code @Setup}/element processing begins, with this operator instance's
   * physical placement in the current job.
   *
   * @param subtaskIndex the 0-based index of the subtask this operator instance is running as
   * @param parallelism the operator's current parallelism (number of parallel subtasks)
   * @param maxParallelism the operator's configured max parallelism (number of key groups)
   */
  void setFlinkSubtaskContext(int subtaskIndex, int parallelism, int maxParallelism);
}
