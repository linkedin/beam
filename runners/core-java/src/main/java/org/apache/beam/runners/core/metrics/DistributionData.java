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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.DoublesSketchBuilder;
import org.apache.datasketches.quantiles.DoublesUnion;
import org.apache.datasketches.quantiles.DoublesUnionBuilder;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Data describing the the distribution. This should retain enough detail that it can be combined
 * with other {@link DistributionData}.
 *
 * <p>Datasketch library is used to compute percentiles. See {@linktourl
 * https://datasketches.apache.org/}.
 *
 * <p>This is kept distinct from {@link DistributionResult} since this may be extended to include
 * data necessary to approximate quantiles, etc. while {@link DistributionResult} would just include
 * the approximate value of those quantiles.
 */
public class DistributionData implements Serializable {
  // k = 256 should yield an approximate error ε of less than 1%
  private static final int SKETCH_SUMMARY_SIZE = 256;

  private final Set<Double> percentiles;
  private long sum;
  private long count;
  private long min;
  private long max;
  private transient Optional<UpdateDoublesSketch> sketch;

  /**
   * Guards all access to the mutable primitive aggregates and the non-thread-safe percentile {@code
   * sketch}. DataSketches {@code UpdateDoublesSketch} is documented single-threaded, so the writer
   * (task/mailbox thread, per-record {@link #update}) and the readers (metric extraction via {@link
   * #percentiles}, accumulator serialization via {@link #writeObject}, and the JobManager merge via
   * {@link #combine(DistributionData)}) must be mutually exclusive. A {@link ReentrantLock} is used
   * (rather than {@code synchronized} on a field) because it is {@code final} and {@link
   * Serializable}: it survives the TM&rarr;JM accumulator round-trip and always deserializes in the
   * unlocked state, so no {@code transient} re-initialization is required.
   */
  private final ReentrantLock lock = new ReentrantLock();

  public static final DistributionData EMPTY = create(0, 0, Long.MAX_VALUE, Long.MIN_VALUE);

  /** Creates an instance of DistributionData with custom percentiles. */
  public static DistributionData withPercentiles(Set<Double> percentiles) {
    return new DistributionData(0L, 0L, Long.MAX_VALUE, Long.MIN_VALUE, percentiles);
  }

  /** Backward compatible static factory method. */
  public static DistributionData empty() {
    return new DistributionData(0L, 0L, Long.MAX_VALUE, Long.MIN_VALUE, ImmutableSet.of());
  }

  /** Static factory method primary used for testing. */
  @VisibleForTesting
  public static DistributionData create(long sum, long count, long min, long max) {
    return new DistributionData(sum, count, min, max, ImmutableSet.of());
  }

  private DistributionData(long sum, long count, long min, long max, Set<Double> percentiles) {
    this.sum = sum;
    this.count = count;
    this.min = min;
    this.max = max;
    this.percentiles = percentiles;
    if (!percentiles.isEmpty()) {
      final DoublesSketchBuilder doublesSketchBuilder = new DoublesSketchBuilder();
      this.sketch = Optional.of(doublesSketchBuilder.setK(SKETCH_SUMMARY_SIZE).build());
    } else {
      this.sketch = Optional.empty();
    }
  }

  public static DistributionData singleton(long value) {
    final DistributionData distributionData = empty();
    distributionData.update(value);
    return distributionData;
  }

  public DistributionData combine(long value) {
    lock.lock();
    try {
      return create(sum + value, count + 1, Math.min(value, min), Math.max(value, max));
    } finally {
      lock.unlock();
    }
  }

  public DistributionData combine(long sum, long count, long min, long max) {
    lock.lock();
    try {
      return create(
          this.sum + sum, this.count + count, Math.min(min, this.min), Math.max(max, this.max));
    } finally {
      lock.unlock();
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Getters

  public long sum() {
    lock.lock();
    try {
      return sum;
    } finally {
      lock.unlock();
    }
  }

  public long count() {
    lock.lock();
    try {
      return count;
    } finally {
      lock.unlock();
    }
  }

  public long min() {
    lock.lock();
    try {
      return min;
    } finally {
      lock.unlock();
    }
  }

  public long max() {
    lock.lock();
    try {
      return max;
    } finally {
      lock.unlock();
    }
  }

  /** Gets the percentiles and the percentiles values as a map. */
  public Map<Double, Double> percentiles() {
    final UpdateDoublesSketch snapshot;
    final double[] quantiles;
    lock.lock();
    try {
      if (!sketch.isPresent() || sketch.get().getN() == 0) {
        // if the sketch is not present or is empty, do not compute the percentile
        return ImmutableMap.of();
      }
      quantiles = percentiles.stream().mapToDouble(i -> i / 100).toArray();
      // Cheap copy of the bounded (k=256 -> a few KB) sketch under the lock; the expensive O(n)
      // getQuantiles then runs on the immutable snapshot with the lock released, so concurrent
      // update() writers never race the read that sizes the destination array.
      snapshot = UpdateDoublesSketch.heapify(Memory.wrap(sketch.get().toByteArray()));
    } finally {
      lock.unlock();
    }

    double[] quantileResults = snapshot.getQuantiles(quantiles);

    final ImmutableMap.Builder<Double, Double> resultBuilder = ImmutableMap.builder();
    for (int k = 0; k < quantiles.length; k++) {
      resultBuilder.put(quantiles[k] * 100, quantileResults[k]);
    }
    return resultBuilder.build();
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Updates the distribution with a value. For percentiles, only add the value to the sketch.
   * Percentile will be computed prior to calling {@link DistributionCell#getCumulative()} or in
   * {@link #extractResult()}.
   *
   * @param value value to update the distribution with.
   */
  public void update(long value) {
    lock.lock();
    try {
      ++count;
      min = Math.min(min, value);
      max = Math.max(max, value);
      sum += value;
      sketch.ifPresent(currSketch -> currSketch.update(value));
    } finally {
      lock.unlock();
    }
  }

  /** Merges two distributions. */
  public DistributionData combine(DistributionData other) {
    // Snapshot the other instance's state under its own lock first (copying the sketch bytes rather
    // than aliasing the live sketch), then mutate this instance under this lock. Snapshotting
    // other-first keeps this deadlock-free even if the merge ever runs off the single-threaded JM
    // path, and avoids sharing a mutable sketch reference between two DistributionData instances.
    final Optional<UpdateDoublesSketch> otherSketch;
    final long otherSum;
    final long otherCount;
    final long otherMin;
    final long otherMax;
    other.lock.lock();
    try {
      otherSum = other.sum;
      otherCount = other.count;
      otherMin = other.min;
      otherMax = other.max;
      otherSketch =
          (other.sketch.isPresent() && other.sketch.get().getN() > 0)
              ? Optional.of(
                  UpdateDoublesSketch.heapify(Memory.wrap(other.sketch.get().toByteArray())))
              : Optional.empty();
    } finally {
      other.lock.unlock();
    }

    lock.lock();
    try {
      if (sketch.isPresent() && otherSketch.isPresent() && sketch.get().getN() > 0) {
        final DoublesUnion union = new DoublesUnionBuilder().build();
        // datasketches 6.x renamed DoublesUnion.update(DoublesSketch) to union(DoublesSketch);
        // update(double) now adds a single value. Semantics of the merge are unchanged.
        union.union(sketch.get());
        union.union(otherSketch.get());
        sketch = Optional.of(union.getResult());
      } else if (otherSketch.isPresent()) {
        sketch = otherSketch;
      }
      sum += otherSum;
      count += otherCount;
      max = Math.max(max, otherMax);
      min = Math.min(min, otherMin);
    } finally {
      lock.unlock();
    }
    return this;
  }

  public DistributionData reset() {
    lock.lock();
    try {
      this.sum = 0L;
      this.count = 0L;
      this.min = Long.MAX_VALUE;
      this.max = Long.MIN_VALUE;
      if (!this.percentiles.isEmpty()) {
        final DoublesSketchBuilder doublesSketchBuilder = new DoublesSketchBuilder();
        this.sketch = Optional.of(doublesSketchBuilder.setK(SKETCH_SUMMARY_SIZE).build());
      } else {
        this.sketch = Optional.empty();
      }
    } finally {
      lock.unlock();
    }
    return this;
  }

  /** Generates DistributionResult from DistributionData. */
  public DistributionResult extractResult() {
    return DistributionResult.create(sum(), count(), min(), max(), percentiles());
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (object instanceof DistributionData) {
      DistributionData other = (DistributionData) object;
      return max == other.max()
          && min == other.min()
          && count == other.count()
          && sum == other.sum()
          && percentiles() == other.percentiles();
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(min, max, sum, count, percentiles());
  }

  @Override
  public String toString() {
    return "DistributionData{"
        + "sum="
        + sum
        + ", "
        + "count="
        + count
        + ", "
        + "min="
        + min
        + ", "
        + "max="
        + max
        + ", "
        + "percentiles="
        + percentiles()
        + "}";
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    // Serializing the accumulator (TM->JM) is a cross-thread reader of the sketch that races the
    // task thread's update(); guard it so it cannot observe the sketch mid-mutation.
    lock.lock();
    try {
      out.defaultWriteObject();
      if (sketch.isPresent()) {
        byte[] bytes = sketch.get().toByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);
      }
    } finally {
      lock.unlock();
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
    in.defaultReadObject();
    if (!this.percentiles.isEmpty()) {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      // readFully (not read) guarantees the whole sketch image is read; a plain read() may return
      // fewer bytes when the array spans the stream buffer, corrupting the deserialized sketch.
      in.readFully(bytes);
      this.sketch = Optional.of(UpdateDoublesSketch.heapify(Memory.wrap(bytes)));
    } else {
      this.sketch = Optional.empty();
    }
  }
}
