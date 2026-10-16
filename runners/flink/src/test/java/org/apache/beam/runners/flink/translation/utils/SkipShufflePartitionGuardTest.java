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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Test;

/** Tests for {@link SkipShufflePartitionGuard} with deterministic subtask placement. */
public class SkipShufflePartitionGuardTest {

  private static final KeySelector<Integer, Integer> IDENTITY = value -> value;

  private static List<Integer> keys(
      boolean owned, int maxParallelism, int parallelism, int subtaskIndex) {
    List<Integer> keys = new ArrayList<>();
    for (int k = 0; k < 200; k++) {
      boolean isOwner =
          KeyGroupRangeAssignment.assignKeyToParallelOperator(k, maxParallelism, parallelism)
              == subtaskIndex;
      if (isOwner == owned) {
        keys.add(k);
      }
    }
    return keys;
  }

  private static void assertAlignedPassesAndMisalignedThrows(
      int maxParallelism, int parallelism, int subtaskIndex) throws Exception {
    List<Integer> aligned = keys(true, maxParallelism, parallelism, subtaskIndex);
    List<Integer> misaligned = keys(false, maxParallelism, parallelism, subtaskIndex);
    assertFalse(aligned.isEmpty());
    assertFalse(misaligned.isEmpty());

    try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
        new OneInputStreamOperatorTestHarness<>(
            new StreamFlatMap<>(SkipShufflePartitionGuard.guard(IDENTITY, "opt", "test op")),
            maxParallelism,
            parallelism,
            subtaskIndex)) {
      harness.open();
      for (int key : aligned) {
        harness.processElement(new StreamRecord<>(key));
      }
      assertEquals(aligned, harness.extractOutputValues());

      IllegalStateException e =
          assertThrows(
              IllegalStateException.class,
              () -> harness.processElement(new StreamRecord<>(misaligned.get(0))));
      assertThat(e.getMessage(), containsString("opt is enabled, but the input to test op"));
    }
  }

  @Test
  public void testParallelismEqualsMaxParallelism() throws Exception {
    assertAlignedPassesAndMisalignedThrows(8, 8, 3);
  }

  @Test
  public void testParallelismBelowMaxParallelism() throws Exception {
    assertAlignedPassesAndMisalignedThrows(8, 2, 1);
  }
}
