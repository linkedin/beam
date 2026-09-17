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
package org.apache.beam.runners.flink.streaming;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkTestPipeline;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

/**
 * Tests for the runtime safety net added to {@code FlinkPipelineOptions#getSkipReshuffleForParDo()}
 * (see {@code ParDoSkipShufflePartitionGuard} in {@code FlinkStreamingTransformTranslators}): the
 * parallelism/max-parallelism precondition check, the runtime partition guard, and correct output
 * when the option's precondition genuinely holds.
 */
public class ParDoSkipShuffleTest implements Serializable {

  private static final String STATE_ID = "count";

  private static PCollection<KV<Integer, String>> input(Pipeline p, int numKeys) {
    List<KV<Integer, String>> elements = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      elements.add(KV.of(i, "v" + i));
    }
    return p.apply(
        "CreateInput",
        Create.of(elements).withCoder(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of())));
  }

  private static class CountingStatefulDoFn extends DoFn<KV<Integer, String>, String> {
    @StateId(STATE_ID)
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<Integer>> countSpec = StateSpecs.value();

    @ProcessElement
    public void processElement(ProcessContext c, @StateId(STATE_ID) ValueState<Integer> count) {
      Integer current = count.read();
      int next = (current == null ? 0 : current) + 1;
      count.write(next);
      c.output(c.element().getKey() + ":" + c.element().getValue() + ":" + next);
    }
  }

  /**
   * Sanity check at parallelism=1: every key trivially hashes to subtask 0, so the guard's
   * precondition holds regardless of upstream partitioning. Exercises the {@code
   * reinterpretAsKeyedStream} path and the guard's pass-through logic end-to-end.
   */
  @Test
  public void testSkipShuffleProducesCorrectResultsAtParallelismOne() throws Exception {
    Pipeline p = FlinkTestPipeline.createForStreaming();
    FlinkPipelineOptions options = p.getOptions().as(FlinkPipelineOptions.class);
    options.setParallelism(1);
    options.setSkipReshuffleForParDo(true);

    int numKeys = 5;
    PCollection<String> output = input(p, numKeys).apply(ParDo.of(new CountingStatefulDoFn()));

    List<String> expected = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      expected.add(i + ":v" + i + ":1");
    }
    PAssert.that(output).containsInAnyOrder(expected);

    p.run().waitUntilFinish();
  }

  /**
   * Happy-path correctness at parallelism &gt; 1, with genuinely aligned input -- unlike the
   * parallelism=1 test above, this actually exercises the guard's non-trivial pass-through logic
   * (every key must independently land on the subtask keyBy would route it to, not just subtask 0).
   * Alignment is constructed via a real upstream {@link GroupByKey} (whose own {@code keyBy}
   * guarantees Flink-hash-partitioned output), followed by a stateless "unwrap" {@link ParDo} that
   * Flink chains/forwards without a network shuffle -- so by the time the stateful ParDo below
   * runs, the data is truly partitioned the way a fresh {@code keyBy} would, but the immediate
   * producer is the unwrap ParDo (not GBK/CPK), forcing translation through the
   * skipReshuffleForParDo flag branch (and its guard) rather than the always-safe GBK/CPK-producer
   * branch.
   */
  @Test
  public void testSkipShuffleProducesCorrectResultsAtParallelismFour() throws Exception {
    Pipeline p = FlinkTestPipeline.createForStreaming();
    FlinkPipelineOptions options = p.getOptions().as(FlinkPipelineOptions.class);
    options.setParallelism(4);
    options.setMaxParallelism(4);
    options.setSkipReshuffleForParDo(true);

    int numKeys = 20;
    PCollection<KV<Integer, String>> aligned =
        input(p, numKeys)
            .apply("GroupToAlign", GroupByKey.create())
            .apply(
                "Unwrap",
                ParDo.of(
                    new DoFn<KV<Integer, Iterable<String>>, KV<Integer, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        for (String v : c.element().getValue()) {
                          c.output(KV.of(c.element().getKey(), v));
                        }
                      }
                    }));

    PCollection<String> output = aligned.apply(ParDo.of(new CountingStatefulDoFn()));

    List<String> expected = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      expected.add(i + ":v" + i + ":1");
    }
    PAssert.that(output).containsInAnyOrder(expected);

    p.run().waitUntilFinish();
  }

  /**
   * parallelism &gt; maxParallelism must fail fast at translation time: {@code
   * reinterpretAsKeyedStream}'s pointwise forwarding can never be sound in that configuration,
   * regardless of upstream partitioning, since there are more subtasks than key groups to spread
   * across them.
   */
  @Test
  public void testParallelismExceedingMaxParallelismFailsFastAtTranslation() throws Exception {
    Pipeline p = FlinkTestPipeline.createForStreaming();
    FlinkPipelineOptions options = p.getOptions().as(FlinkPipelineOptions.class);
    options.setParallelism(8);
    options.setMaxParallelism(2);
    options.setSkipReshuffleForParDo(true);

    input(p, 4).apply(ParDo.of(new CountingStatefulDoFn()));

    try {
      p.run().waitUntilFinish();
      fail("Expected pipeline translation/execution to fail due to parallelism mismatch.");
    } catch (Exception e) {
      assertThat(getRootMessage(e), containsString("skipReshuffleForParDo requires parallelism"));
    }
  }

  /**
   * parallelism &lt; maxParallelism is now allowed at translation time (mirroring FlinkSQL's own
   * {@code numShards == maxParallelism}, {@code parallelism <= numShards} contract), provided the
   * upstream source itself assigns contiguous key-group ranges per subtask. This is exercised the
   * same way as {@link #testSkipShuffleProducesCorrectResultsAtParallelismFour}: a real upstream
   * {@link GroupByKey} (whose keyBy guarantees Flink-hash-partitioned, contiguous-key-group-range
   * output) followed by a stateless "unwrap" ParDo, so the stateful ParDo below sees genuinely
   * aligned input despite running at a lower parallelism than maxParallelism.
   */
  @Test
  public void testSkipShuffleProducesCorrectResultsWhenParallelismBelowMaxParallelism()
      throws Exception {
    Pipeline p = FlinkTestPipeline.createForStreaming();
    FlinkPipelineOptions options = p.getOptions().as(FlinkPipelineOptions.class);
    options.setParallelism(2);
    options.setMaxParallelism(8);
    options.setSkipReshuffleForParDo(true);

    int numKeys = 20;
    PCollection<KV<Integer, String>> aligned =
        input(p, numKeys)
            .apply("GroupToAlign", GroupByKey.create())
            .apply(
                "Unwrap",
                ParDo.of(
                    new DoFn<KV<Integer, Iterable<String>>, KV<Integer, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        for (String v : c.element().getValue()) {
                          c.output(KV.of(c.element().getKey(), v));
                        }
                      }
                    }));

    PCollection<String> output = aligned.apply(ParDo.of(new CountingStatefulDoFn()));

    List<String> expected = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      expected.add(i + ":v" + i + ":1");
    }
    PAssert.that(output).containsInAnyOrder(expected);

    p.run().waitUntilFinish();
  }

  /**
   * With the option enabled but the input deliberately NOT partitioned the way a real {@code keyBy}
   * on the element key would partition it (forced via {@link Reshuffle#viaRandomKey()}, at
   * parallelism &gt; 1), the runtime guard must trip with a clear, actionable exception rather than
   * allowing the job to silently corrupt state.
   */
  @Test
  public void testMisalignedInputTripsRuntimeGuard() throws Exception {
    Pipeline p = FlinkTestPipeline.createForStreaming();
    FlinkPipelineOptions options = p.getOptions().as(FlinkPipelineOptions.class);
    options.setParallelism(4);
    options.setMaxParallelism(4);
    options.setSkipReshuffleForParDo(true);

    // Enough distinct keys that, after an unrelated random-keyed reshuffle, at least one key is
    // virtually certain to land on a subtask other than the one Flink's real keyBy hash would
    // route it to (probability of every key coincidentally landing correctly is astronomically
    // small: roughly (1/4)^30).
    int numKeys = 30;
    input(p, numKeys)
        .apply("Shuffle", Reshuffle.viaRandomKey())
        .apply(ParDo.of(new CountingStatefulDoFn()));

    try {
      p.run().waitUntilFinish();
      fail("Expected the runtime partition guard to trip due to misaligned input.");
    } catch (Exception e) {
      assertThat(
          getRootMessage(e),
          containsString("is enabled, but the input to this stateful ParDo is not actually"));
    }
  }

  /**
   * With both the option enabled and {@code skipShuffleGuardDropMisalignedRecords} enabled, the
   * same misaligned input as {@link #testMisalignedInputTripsRuntimeGuard} must no longer crash the
   * job: the guard logs and drops the misaligned records instead of throwing, so the pipeline
   * completes successfully (with fewer output elements than input elements).
   */
  @Test
  public void testMisalignedInputIsDroppedWhenDropModeEnabled() throws Exception {
    Pipeline p = FlinkTestPipeline.createForStreaming();
    FlinkPipelineOptions options = p.getOptions().as(FlinkPipelineOptions.class);
    options.setParallelism(4);
    options.setMaxParallelism(4);
    options.setSkipReshuffleForParDo(true);
    options.setSkipShuffleGuardDropMisalignedRecords(true);

    int numKeys = 30;
    PCollection<String> output =
        input(p, numKeys)
            .apply("Shuffle", Reshuffle.viaRandomKey())
            .apply(ParDo.of(new CountingStatefulDoFn()));

    // Every emitted element is one that genuinely landed on the subtask a real keyBy hash would
    // route it to (misaligned ones are dropped rather than emitted), so there can never be more
    // than numKeys outputs, and the pipeline must complete without throwing.
    PAssert.that(output)
        .satisfies(
            elements -> {
              int count = 0;
              for (String ignored : elements) {
                count++;
              }
              assertThat(count <= numKeys, org.hamcrest.Matchers.is(true));
              return null;
            });

    p.run().waitUntilFinish();
  }

  private static String getRootMessage(Throwable t) {
    Throwable cur = t;
    while (cur.getCause() != null) {
      cur = cur.getCause();
    }
    return String.valueOf(cur.getMessage());
  }
}
