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
package org.apache.beam.runners.flink.translation.utils;

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runtime safety net for this runner's "skip shuffle" optimization: the stateful {@code ParDo}
 * translator's {@code skipReshuffleForParDo}, which for performance forwards an input locally via
 * {@code reinterpretAsKeyedStream} instead of re-partitioning it with a HASH exchange/shuffle,
 * relying on the caller's assertion that the input already arrives partitioned exactly as such an
 * exchange would have partitioned it (e.g. because an upstream keyed GroupByKey/CombinePerKey on
 * the same key already did so, or the caller has otherwise ensured upstream source partitioning
 * matches Flink's own key hash).
 *
 * <p>This class exists as a single, tested implementation of the validation logic, separate from
 * the {@code ParDo} translator itself: the translator is only responsible for deciding
 * <em>when</em> the optimization is safe to attempt (via its own pipeline option) and for wiring
 * the {@link #checkParallelismAligned} check and {@link #guard} flatMap into its transformation
 * graph. This mirrors an analogous guard/pattern used by the Table API/SQL runner for its interval
 * join, added after a production incident there caused by this exact precondition silently not
 * holding -- but lives here as a Beam-internal utility since this runner's build compiles against
 * the public Apache Flink release artifacts, not LinkedIn's internal Flink fork.
 *
 * <p>Two checks are provided, meant to be used together:
 *
 * <ul>
 *   <li>{@link #checkParallelismAligned}: a graph-construction-time check that {@code parallelism
 *       <= maxParallelism}. Forwarding data locally (via {@code reinterpretAsKeyedStream}) to
 *       reinterpret it as correctly key-partitioned relies on the upstream input already being
 *       split into the exact contiguous key-group ranges a HASH exchange would assign per subtask.
 *       When {@code parallelism == maxParallelism} this holds trivially for any 1:1
 *       forward/pointwise edge. When {@code parallelism < maxParallelism} it additionally requires
 *       the upstream source's partition-to-subtask assignment to itself assign contiguous key-group
 *       ranges per subtask instead of a round-robin/striped default -- see {@code
 *       checkParallelismAligned}'s javadoc for details.
 *   <li>{@link #guard}: a runtime {@link RichFlatMapFunction} that validates, for every record,
 *       that the subtask it actually arrived at is the one a HASH exchange on the given key would
 *       have routed it to (via {@link KeyGroupRangeAssignment#assignKeyToParallelOperator}). If the
 *       caller's assertion turns out to be wrong, this fails fast with a clear, actionable error --
 *       instead of the cryptic internal state-backend "Key group X is not in KeyGroupRange{...}"
 *       error (or, worse, silent keyed-state corruption) that would otherwise surface downstream,
 *       on a record whose key may look completely unrelated to the one that actually violated the
 *       precondition.
 * </ul>
 */
public final class SkipShufflePartitionGuard {

  private SkipShufflePartitionGuard() {}

  /**
   * Validates that {@code parallelism <= maxParallelism}, throwing {@link IllegalStateException}
   * otherwise. Must be checked at graph construction time before relying on {@code
   * reinterpretAsKeyedStream} to preserve key-partitioning.
   *
   * <p>Note this only guarantees {@code parallelism <= maxParallelism} holds; it does <em>not</em>
   * by itself guarantee the upstream input is actually partitioned into the exact contiguous
   * key-group ranges {@link KeyGroupRangeAssignment} would assign for this {@code (parallelism,
   * maxParallelism)} pair. When {@code parallelism == maxParallelism}, any 1:1 forward/pointwise
   * edge trivially satisfies this. When {@code parallelism < maxParallelism}, the upstream source's
   * partition assigner must itself assign contiguous key-group ranges per subtask -- matching
   * {@link KeyGroupRangeAssignment#computeKeyGroupRangeForOperatorIndex} -- for this precondition
   * to actually hold; {@link #guard} remains the runtime safety net that catches any such
   * misconfiguration.
   */
  public static void checkParallelismAligned(
      int parallelism, int maxParallelism, String errorMessage) {
    Preconditions.checkState(parallelism <= maxParallelism, errorMessage);
  }

  /**
   * Returns a {@link RichFlatMapFunction} that validates, for every record of type {@code T}, that
   * this subtask actually owns the key group that {@code keySelector}'s extracted key hashes into
   * -- i.e. that the input is truly partitioned exactly as a HASH exchange on {@code keySelector}
   * would partition it. A no-op pass-through (besides the extra hash computation) whenever the
   * caller's precondition actually holds; throws {@link IllegalStateException} with a detailed,
   * actionable message on the first record that violates it.
   *
   * <p>Equivalent to {@code guard(keySelector, optionName, callerDescription, false)} -- i.e.
   * fail-fast on the first misaligned record. See the 4-argument overload to instead drop
   * individual misaligned records and keep the job running.
   *
   * @param keySelector extracts the partitioning key from each record.
   * @param optionName the name of the pipeline option this guard is protecting (included in the
   *     exception message so the error points the user at the right knob).
   * @param callerDescription a short, human-readable description of the caller/translator this
   *     guard protects (e.g. {@code "this stateful ParDo"}), included in the exception message for
   *     context.
   */
  public static <T, K> RichFlatMapFunction<T, T> guard(
      KeySelector<T, K> keySelector, String optionName, String callerDescription) {
    return guard(keySelector, optionName, callerDescription, false);
  }

  /**
   * Same as {@link #guard(KeySelector, String, String)}, with an additional {@code
   * dropMisalignedRecords} switch controlling how a detected misalignment is handled.
   *
   * <p>When {@code dropMisalignedRecords} is {@code false} (the default/fail-fast behavior), the
   * first misaligned record throws {@link IllegalStateException}, crashing the job -- appropriate
   * when a misalignment indicates a systemic upstream partitioning bug that should be fixed rather
   * than silently tolerated.
   *
   * <p>When {@code dropMisalignedRecords} is {@code true}, a misaligned record is instead logged at
   * ERROR level, counted via the {@code skipShufflePartitionGuardDroppedRecords} metric, and
   * dropped (not emitted downstream) -- the job keeps running. This is intended as a resilience
   * valve for isolated/transient bad messages (e.g. a producer bug affecting a small subset of
   * keys) where crashing the whole job on every such record is worse than losing that one record;
   * it is NOT a substitute for fixing a systemic partitioning misconfiguration, since silently
   * dropping a large fraction of traffic would go unnoticed without alerting on the metric.
   *
   * @param keySelector extracts the partitioning key from each record.
   * @param optionName the name of the pipeline option this guard is protecting (included in the
   *     exception/log message so it points the user at the right knob).
   * @param callerDescription a short, human-readable description of the caller/translator this
   *     guard protects (e.g. {@code "this stateful ParDo"}), included in the exception/log message
   *     for context.
   * @param dropMisalignedRecords when true, drop+log+count misaligned records instead of throwing.
   */
  public static <T, K> RichFlatMapFunction<T, T> guard(
      KeySelector<T, K> keySelector,
      String optionName,
      String callerDescription,
      boolean dropMisalignedRecords) {
    return new Guard<>(keySelector, optionName, callerDescription, dropMisalignedRecords);
  }

  private static final class Guard<T, K> extends RichFlatMapFunction<T, T> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Guard.class);

    @VisibleForTesting
    static final String DROPPED_RECORDS_METRIC_NAME = "skipShufflePartitionGuardDroppedRecords";

    private final KeySelector<T, K> keySelector;
    private final String optionName;
    private final String callerDescription;
    private final boolean dropMisalignedRecords;

    private transient int parallelism;
    private transient int maxParallelism;
    private transient int subtaskIndex;
    private transient Counter droppedRecordsCounter = new SimpleCounter();

    private Guard(
        KeySelector<T, K> keySelector,
        String optionName,
        String callerDescription,
        boolean dropMisalignedRecords) {
      this.keySelector = keySelector;
      this.optionName = optionName;
      this.callerDescription = callerDescription;
      this.dropMisalignedRecords = dropMisalignedRecords;
    }

    @Override
    public void open(Configuration parameters) {
      this.parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
      this.maxParallelism = getRuntimeContext().getMaxNumberOfParallelSubtasks();
      this.subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
      this.droppedRecordsCounter =
          getRuntimeContext().getMetricGroup().counter(DROPPED_RECORDS_METRIC_NAME);
    }

    @Override
    public void flatMap(T value, Collector<T> out) throws Exception {
      K key = Preconditions.checkNotNull(keySelector.getKey(value));
      int expectedSubtaskIndex =
          KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, parallelism);
      if (expectedSubtaskIndex != subtaskIndex) {
        String message =
            String.format(
                "%s is enabled, but the input to %s is not actually partitioned the way a HASH "
                    + "exchange on the key would partition it: a record with key %s arrived at "
                    + "subtask %d (parallelism=%d, maxParallelism=%d), but a HASH exchange on the "
                    + "key would have routed it to subtask %d. Either disable this option, or "
                    + "ensure the input is explicitly re-partitioned (e.g. via an upstream keyed "
                    + "GroupByKey/CombinePerKey) on exactly this key, with matching parallelism "
                    + "and max parallelism, before it reaches %s. Also double check the upstream "
                    + "producer partitioning is aligned to Beam's key hash (MurmurHash over a "
                    + "ByteBuffer-wrapped, Coder.Context.NESTED-encoded key -- not a raw object "
                    + "hashCode), and to however the source assigns partitions/splits to "
                    + "subtasks.",
                optionName,
                callerDescription,
                key,
                subtaskIndex,
                parallelism,
                maxParallelism,
                expectedSubtaskIndex,
                callerDescription);
        if (!dropMisalignedRecords) {
          throw new IllegalStateException(message);
        }
        // Resilience mode: log + count + drop the single offending record instead of crashing the
        // job. The counter is the load-bearing signal here -- it must be alerted on, since a
        // systemic partitioning bug would otherwise silently drop a large fraction of traffic
        // without failing the job.
        LOG.error("Dropping misaligned record instead of failing the job. {}", message);
        droppedRecordsCounter.inc();
        return;
      }
      out.collect(value);
    }
  }
}
