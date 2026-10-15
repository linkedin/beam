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
package org.apache.beam.runners.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.PTransformOverrideRegistrar;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests the caller-thread override lifecycle independently of Spark execution. */
public class SparkPipelineResultTest {
  private static final String TAG = "spark-result-test";

  @Before
  @After
  public void clearOverrides() {
    PTransformOverrideRegistrar.clear();
  }

  @Test
  public void untimedTerminalWaitClearsOverridesAndAllowsTagReuse() {
    for (State state : new State[] {State.DONE, State.FAILED, State.CANCELLED}) {
      PTransformOverrideRegistrar.register(TAG, Create.of("first"));
      assertEquals(state, new TestResult(state).waitUntilFinish());
      assertNull(PTransformOverrideRegistrar.getOverriddenPTransform(TAG));
      PTransformOverrideRegistrar.register(TAG, Create.of("second"));
      PTransformOverrideRegistrar.clear();
    }
  }

  @Test
  public void untimedNonterminalWaitRetainsOverrides() {
    PTransform<?, ?> replacement = Create.of("value");
    PTransformOverrideRegistrar.register(TAG, replacement);
    assertEquals(State.RUNNING, new TestResult(State.RUNNING).waitUntilFinish());
    assertSame(replacement, PTransformOverrideRegistrar.getOverriddenPTransform(TAG));
  }

  @Test
  public void timedWaitRetainsLegacyBehavior() {
    PTransform<?, ?> replacement = Create.of("value");
    PTransformOverrideRegistrar.register(TAG, replacement);
    assertEquals(
        State.DONE, new TestResult(State.DONE).waitUntilFinish(Duration.standardSeconds(1)));
    assertSame(replacement, PTransformOverrideRegistrar.getOverriddenPTransform(TAG));
  }

  @Test
  public void timeoutRetainsOverrides() {
    PTransform<?, ?> replacement = Create.of("value");
    PTransformOverrideRegistrar.register(TAG, replacement);
    TestResult result =
        new TestResult(State.RUNNING) {
          @Override
          protected State awaitTermination(Duration duration) throws TimeoutException {
            throw new TimeoutException("still running");
          }
        };
    assertEquals(State.RUNNING, result.waitUntilFinish(Duration.millis(1)));
    assertSame(replacement, PTransformOverrideRegistrar.getOverriddenPTransform(TAG));
  }

  @Test
  public void failedWaitPropagatesAndRetainsLegacyBehavior() {
    PTransform<?, ?> replacement = Create.of("value");
    PTransformOverrideRegistrar.register(TAG, replacement);
    IllegalStateException failure = new IllegalStateException("execution failed");
    TestResult result =
        new TestResult(State.FAILED) {
          @Override
          protected State awaitTermination(Duration duration) {
            throw failure;
          }
        };
    assertSame(failure, assertThrows(IllegalStateException.class, result::waitUntilFinish));
    assertEquals(State.FAILED, result.getState());
    assertSame(replacement, PTransformOverrideRegistrar.getOverriddenPTransform(TAG));
  }

  @Test
  public void stateQueryAndCancelDoNotClearOverrides() throws Exception {
    PTransform<?, ?> replacement = Create.of("value");
    PTransformOverrideRegistrar.register(TAG, replacement);
    TestResult result = new TestResult(State.DONE);
    assertEquals(State.RUNNING, result.getState());
    assertEquals(State.CANCELLED, result.cancel());
    assertSame(replacement, PTransformOverrideRegistrar.getOverriddenPTransform(TAG));
  }

  @Test
  public void cleanupDoesNotClearAnotherThreadsOverrides() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    PTransform<?, ?> replacement = Create.of("other thread");
    try {
      executor
          .submit(() -> PTransformOverrideRegistrar.register(TAG, replacement))
          .get(10, TimeUnit.SECONDS);
      PTransformOverrideRegistrar.register(TAG, Create.of("caller"));
      assertEquals(State.DONE, new TestResult(State.DONE).waitUntilFinish());
      assertNull(PTransformOverrideRegistrar.getOverriddenPTransform(TAG));
      assertSame(
          replacement,
          executor
              .submit(() -> PTransformOverrideRegistrar.getOverriddenPTransform(TAG))
              .get(10, TimeUnit.SECONDS));
    } finally {
      try {
        executor.submit(PTransformOverrideRegistrar::clear).get(10, TimeUnit.SECONDS);
      } finally {
        executor.shutdownNow();
      }
    }
  }

  private static class TestResult extends SparkPipelineResult {
    private final State finishState;

    TestResult(State finishState) {
      super(CompletableFuture.completedFuture(null), mock(JavaSparkContext.class));
      this.finishState = finishState;
    }

    @Override
    protected void stop() {}

    @Override
    protected State awaitTermination(Duration duration) throws TimeoutException {
      return finishState;
    }
  }
}
