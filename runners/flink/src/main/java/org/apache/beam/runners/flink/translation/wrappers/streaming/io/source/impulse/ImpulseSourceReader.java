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

package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.impulse;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;


/**
 * A SourceReader that only emit a single record. The boundedness of the ImpulseSourceReader is the
 * same as the ImpulseSource creating this instance.
 */
public class ImpulseSourceReader implements SourceReader<WindowedValue<byte[]>, ImpulseSplit> {
  private static final ImpulseSplit DUMMY_SPLIT = new ImpulseSplit();
  private final Boundedness boundedness;
  private final long idleTimeoutMs;
  private final ScheduledExecutorService executor;
  private ImpulseSplit split;
  private boolean noMoreSplits;
  private CompletableFuture<Void> isAvailableFuture;
  private CompletableFuture<Void> idleTimeoutFuture;
  private @Nullable ScheduledFuture<?> future;

  ImpulseSourceReader(SourceReaderContext context, Boundedness boundedness, long idleTimeoutMs) {
    this.boundedness = boundedness;
    this.idleTimeoutMs = idleTimeoutMs;
    this.executor = Executors.newSingleThreadScheduledExecutor(
        r -> new Thread(r, "ImpulseSource-Executor-Thread-" + context.getIndexOfSubtask()));
    this.split = DUMMY_SPLIT;
    this.noMoreSplits = false;
    this.isAvailableFuture = CompletableFuture.completedFuture(null);
    this.idleTimeoutFuture = new CompletableFuture<>();
    this.future = null;
  }

  @Override
  public void start() {

  }

  @Override
  public InputStatus pollNext(ReaderOutput<WindowedValue<byte[]>> output) throws Exception {
    if (split == DUMMY_SPLIT) {
      if (noMoreSplits && (isBounded() || hasIdleTimeoutReached())) {
        return InputStatus.END_OF_INPUT;
      } else {
        return InputStatus.NOTHING_AVAILABLE;
      }
    } else {
      long now = System.currentTimeMillis();
      if (!split.hasEmitted()) {
        output.collect(WindowedValue.valueInGlobalWindow(new byte[0]), Watermark.MAX_WATERMARK.getTimestamp());
        split.setEmitted(now);
        long delayMs = Math.min(Long.MAX_VALUE - now, idleTimeoutMs);
        future = executor.schedule(() -> idleTimeoutFuture.complete(null), delayMs, TimeUnit.MILLISECONDS);
      }
      if (isBounded() || now - split.lastEmittingTimeMs() > idleTimeoutMs) {
        // Stop the stream if either the source is bounded or the idle timeout has reached.
        idleTimeoutFuture.complete(null);
        return InputStatus.END_OF_INPUT;
      } else {
        return InputStatus.NOTHING_AVAILABLE;
      }
    }
  }

  @Override
  public List<ImpulseSplit> snapshotState(long checkpointId) {
    if (split != DUMMY_SPLIT) {
      return Collections.singletonList(split);
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    if (split == DUMMY_SPLIT) {
      if (noMoreSplits) {
        // This is an idle source reader. Return a completed future so pollNext() can be
        // invoked to return an END_OF_INPUT.
        return CompletableFuture.completedFuture(null);
      } else {
        // Return an uncompleted future to wait for the split enumerator messages.
        // The future will be completed either a split is added or noMoreSplits signal
        // is received.
        return newUncompletedIsAvailableFuture();
      }
    } else if (!split.hasEmitted() || isBounded() || hasIdleTimeoutReached()) {
      // The impulse has not been emitted, or the impulse has been emitted for the bounded source,
      // or the unbounded source idle timeout has reached.
      // In the above cases, pollNext() should be called to make progress. Therefore, return a
      // completed future here.
      return CompletableFuture.completedFuture(null);
    } else {
      if (!noMoreSplits) {
        return newUncompletedIsAvailableFuture();
      } else {
        // The impulse has been sent for an unbounded source. Return an incomplete future here
        return idleTimeoutFuture;
      }
    }
  }

  @Override
  public void addSplits(List<ImpulseSplit> splits) {
    if (splits != null && !splits.isEmpty()) {
      split = splits.get(0);
      isAvailableFuture.complete(null);
    }
  }

  @Override
  public void notifyNoMoreSplits() {
    noMoreSplits = true;
    isAvailableFuture.complete(null);
  }

  @Override
  public void close() throws Exception {
    if (future != null && !future.isDone()) {
      future.cancel(true);
    }
    executor.shutdown();
  }

  private boolean hasIdleTimeoutReached() {
    return System.currentTimeMillis() > idleTimeoutMs + split.lastEmittingTimeMs();
  }

  private boolean isBounded() {
    return boundedness == Boundedness.BOUNDED;
  }

  private CompletableFuture<Void> newUncompletedIsAvailableFuture() {
    isAvailableFuture.complete(null);
    isAvailableFuture = new CompletableFuture<>();
    return isAvailableFuture;
  }
}
