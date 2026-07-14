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
package org.apache.beam.runners.core.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DistributionData}. */
@RunWith(JUnit4.class)
public class DistributionDataTest {
  private static final Set<Double> PERCENTILES = ImmutableSet.of(50.0, 90.0, 99.0);

  @Test
  public void testSingleton() {
    DistributionData data = DistributionData.singleton(5);
    assertEquals(5, data.sum());
    assertEquals(1, data.count());
    assertEquals(5, data.min());
    assertEquals(5, data.max());
  }

  @Test
  public void testCreate() {
    DistributionData data = DistributionData.create(5, 2, 1, 4);
    assertEquals(5, data.sum());
    assertEquals(2, data.count());
    assertEquals(1, data.min());
    assertEquals(4, data.max());
  }

  @Test
  public void testCombine() {
    DistributionData data = DistributionData.create(5, 2, 1, 4).combine(7);
    assertEquals(12, data.sum());
    assertEquals(3, data.count());
    assertEquals(1, data.min());
    assertEquals(7, data.max());

    data = DistributionData.create(5, 2, 1, 4).combine(1, 2, 0, 1);
    assertEquals(6, data.sum());
    assertEquals(4, data.count());
    assertEquals(0, data.min());
    assertEquals(4, data.max());

    data = DistributionData.create(5, 2, 1, 4).combine(DistributionData.EMPTY);
    assertEquals(5, data.sum());
    assertEquals(2, data.count());
    assertEquals(1, data.min());
    assertEquals(4, data.max());
  }

  @Test
  public void testPercentilesEmptySketch() {
    // No data added -> percentiles must be empty, and combine skips the union (unchanged behavior).
    DistributionData data = DistributionData.withPercentiles(PERCENTILES);
    assertTrue(data.percentiles().isEmpty());

    DistributionData other = DistributionData.withPercentiles(PERCENTILES);
    assertTrue(data.combine(other).percentiles().isEmpty());
  }

  @Test
  public void testPercentilesPopulated() {
    DistributionData data = DistributionData.withPercentiles(PERCENTILES);
    for (long i = 1; i <= 1000; i++) {
      data.update(i);
    }
    assertEquals(1000, data.count());
    assertEquals(1, data.min());
    assertEquals(1000, data.max());

    Map<Double, Double> percentiles = data.percentiles();
    assertEquals(PERCENTILES.size(), percentiles.size());
    // Percentiles must be monotonically non-decreasing and within the observed value range.
    assertTrue(percentiles.get(50.0) <= percentiles.get(90.0));
    assertTrue(percentiles.get(90.0) <= percentiles.get(99.0));
    assertTrue(percentiles.get(50.0) >= 1.0 && percentiles.get(99.0) <= 1000.0);
  }

  @Test
  public void testSerializationRoundTripPreservesState() {
    DistributionData data = DistributionData.withPercentiles(PERCENTILES);
    for (long i = 1; i <= 500; i++) {
      data.update(i);
    }

    DistributionData restored =
        (DistributionData)
            SerializableUtils.deserializeFromByteArray(
                SerializableUtils.serializeToByteArray(data), "DistributionData");

    assertEquals(data.sum(), restored.sum());
    assertEquals(data.count(), restored.count());
    assertEquals(data.min(), restored.min());
    assertEquals(data.max(), restored.max());
    assertEquals(data.percentiles(), restored.percentiles());
  }

  @Test
  public void testCombineWithPercentilesMerges() {
    DistributionData left = DistributionData.withPercentiles(PERCENTILES);
    DistributionData right = DistributionData.withPercentiles(PERCENTILES);
    for (long i = 1; i <= 500; i++) {
      left.update(i);
    }
    for (long i = 501; i <= 1000; i++) {
      right.update(i);
    }

    left.combine(right);
    assertEquals(1000, left.count());
    assertEquals(1, left.min());
    assertEquals(1000, left.max());
    // The merged sketch spans both halves.
    Map<Double, Double> percentiles = left.percentiles();
    assertTrue(percentiles.get(50.0) <= percentiles.get(99.0));
    assertTrue(percentiles.get(99.0) <= 1000.0);
    // Combining does not mutate the other instance.
    assertEquals(500, right.count());
  }

  /**
   * Regression guard for the {@code DistributionData} sketch race: concurrent writers ({@link
   * DistributionData#update}) racing readers ({@link DistributionData#percentiles} / {@link
   * DistributionData#extractResult}) and accumulator serialization ({@code writeObject}) previously
   * threw {@code ArrayIndexOutOfBoundsException} from {@code getQuantiles}. With the
   * snapshot-under-lock fix, no exception should be thrown.
   */
  @Test
  public void testConcurrentAccessDoesNotThrow() throws InterruptedException, ExecutionException {
    final int writerCount = 4;
    final int readerCount = 2;
    final int serializerCount = 2;
    final long durationMillis = 3000;

    final DistributionData data = DistributionData.withPercentiles(PERCENTILES);
    // Seed so the sketch is non-empty before readers start.
    for (long i = 1; i <= 256; i++) {
      data.update(i);
    }

    final ExecutorService executor =
        Executors.newFixedThreadPool(writerCount + readerCount + serializerCount);
    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final CountDownLatch started = new CountDownLatch(writerCount + readerCount + serializerCount);

    final ImmutableList.Builder<Runnable> tasksBuilder = ImmutableList.builder();
    for (int w = 0; w < writerCount; w++) {
      tasksBuilder.add(
          () -> {
            started.countDown();
            long v = 0;
            while (!stop.get() && failure.get() == null) {
              data.update(v++);
            }
          });
    }
    for (int r = 0; r < readerCount; r++) {
      tasksBuilder.add(
          () -> {
            started.countDown();
            while (!stop.get() && failure.get() == null) {
              data.percentiles();
              data.extractResult();
            }
          });
    }
    for (int s = 0; s < serializerCount; s++) {
      tasksBuilder.add(
          () -> {
            started.countDown();
            while (!stop.get() && failure.get() == null) {
              SerializableUtils.deserializeFromByteArray(
                  SerializableUtils.serializeToByteArray(data), "DistributionData");
            }
          });
    }

    final List<Runnable> tasks = tasksBuilder.build();
    final List<Future<?>> futures = new ArrayList<>();
    for (Runnable task : tasks) {
      futures.add(
          executor.submit(
              () -> {
                try {
                  task.run();
                } catch (Throwable t) {
                  failure.compareAndSet(null, t);
                }
              }));
    }

    started.await(10, TimeUnit.SECONDS);
    Thread.sleep(durationMillis);
    stop.set(true);
    executor.shutdown();
    assertTrue("Threads did not finish", executor.awaitTermination(30, TimeUnit.SECONDS));

    // Drain futures so no submitted task's completion is silently ignored.
    for (Future<?> future : futures) {
      future.get();
    }

    if (failure.get() != null) {
      throw new AssertionError("Concurrent access threw an exception", failure.get());
    }
  }
}
