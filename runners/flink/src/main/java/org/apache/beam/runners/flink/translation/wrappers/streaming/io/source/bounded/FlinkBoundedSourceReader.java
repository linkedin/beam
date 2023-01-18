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
 *
 */

package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.bounded;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkBoundedSourceReader<OutputT>
    implements SourceReader<WindowedValue<OutputT>, FlinkBoundedSourceSplit<OutputT>> {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkBoundedSourceReader.class);
  private static final CompletableFuture<Void> DATA_AVAILABLE = CompletableFuture.completedFuture(null);

  private final PipelineOptions pipelineOptions;
  private final Queue<FlinkBoundedSourceSplit<OutputT>> beamSourceSplits;
  private final ScheduledExecutorService executor;
  private final long idleTimeoutMs;
  private final CompletableFuture<Void> idleTimeoutFuture;
  private @Nullable FlinkBoundedSourceSplit<OutputT> currentSourceSplit;
  private @Nullable BoundedSource.BoundedReader<OutputT> currentBeamReader;
  private boolean currentSplitHasNextRecord;
  private boolean noMoreSplits;
  private long lastRecordEmittedMs;
  private CompletableFuture<Void> isAvailableFuture;
  private boolean idleTimeoutCountingDown;
  private @Nullable ScheduledFuture<?> future;

  FlinkBoundedSourceReader(SourceReaderContext context, PipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
    this.beamSourceSplits = new ArrayDeque<>();
    this.executor = Executors.newSingleThreadScheduledExecutor(
        r -> new Thread(r, "FlinkBoundedSource-Executor-Thread-" + context.getIndexOfSubtask()));
    this.idleTimeoutMs = pipelineOptions.as(FlinkPipelineOptions.class).getShutdownSourcesAfterIdleMs();
    this.currentSourceSplit = null;
    this.currentBeamReader = null;
    this.currentSplitHasNextRecord = false;
    this.noMoreSplits = false;
    this.lastRecordEmittedMs = -1L;
    this.idleTimeoutFuture = new CompletableFuture<>();
    this.isAvailableFuture = DATA_AVAILABLE;
    this.idleTimeoutCountingDown = false;
  }

  @Override
  public void start() {
    this.lastRecordEmittedMs = System.currentTimeMillis();
  }

  @Override
  public InputStatus pollNext(ReaderOutput<WindowedValue<OutputT>> output) throws Exception {
    if (!currentSplitHasNextRecord && !moveToNextSplitWithRecords()) {
      if (allSplitsFinished() && idleTimeoutReached()) {
        // All the source splits have been read.
        LOG.info("All splits have finished reading, and idle time has passed.");
        return InputStatus.END_OF_INPUT;
      } else {
        // This reader hasn't received NoMoreSplitsEvent yet, but there is no source split to reader at the
        // moment either.
        if (allSplitsFinished() && !idleTimeoutCountingDown) {
          idleTimeoutCountingDown = true;
          future = executor.schedule(() -> idleTimeoutFuture.complete(null), idleTimeoutMs, TimeUnit.MILLISECONDS);
        }
        return InputStatus.NOTHING_AVAILABLE;
      }
    }
    BoundedSource.BoundedReader<OutputT> tempCurrentBeamReader = currentBeamReader;
    if (tempCurrentBeamReader != null) {
      OutputT record = tempCurrentBeamReader.getCurrent();
      output.collect(WindowedValue.of(record, tempCurrentBeamReader.getCurrentTimestamp(), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING));
      // If the advance() invocation throws exception here, the job will just fail over and read everything again from
      // the beginning. So the failover granularity is the entire Flink job.
      currentSplitHasNextRecord = tempCurrentBeamReader.advance();
      // Always return MORE_AVAILABLE here regardless of the availability of next record. If there is no more
      // records available in the current split, the next invocation of pollNext() will handle that.
      return InputStatus.MORE_AVAILABLE;
    } else {
      throw new IllegalArgumentException("If we reach here, the current beam reader should not be null");
    }
  }

  @Override
  public List<FlinkBoundedSourceSplit<OutputT>> snapshotState(long checkpointId) {
    // Although the flink jobs in batch execution mode does not have checkpoints, sometimes users may
    // want to use the bounded source in streaming execution mode, therefore we are just returning
    // the unfinished splits here, instead of throwing UnsupportedOperationException.
    List<FlinkBoundedSourceSplit<OutputT>> splitList = new ArrayList<>(beamSourceSplits.size() + 1);
    // Need to add the current source split as well.
    if (currentSourceSplit != null) {
      splitList.add(currentSourceSplit);
    }
    splitList.addAll(beamSourceSplits);
    return splitList;
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    try {
      if (currentSplitHasNextRecord || moveToNextSplitWithRecords()) {
        return DATA_AVAILABLE;
      } else if (allSplitsFinished()) {
        if (idleTimeoutReached()) {
          return DATA_AVAILABLE;
        } else {
          return idleTimeoutFuture;
        }
      } else {
        if (isAvailableFuture.isDone()) {
          isAvailableFuture = new CompletableFuture<>();
        }
        return isAvailableFuture;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void addSplits(List<FlinkBoundedSourceSplit<OutputT>> splits) {
    LOG.info("Adding splits {}.", splits);
    beamSourceSplits.addAll(splits);
    try {
      moveToNextSplitWithRecords();
      isAvailableFuture.complete(null);
    } catch (IOException e) {
      throw new RuntimeException("Caught exception when starting beam reader.", e);
    }
  }

  @Override
  public void notifyNoMoreSplits() {
    LOG.info("Received NoMoreSplits signal from enumerator.");
    noMoreSplits = true;
    isAvailableFuture.complete(null);
  }

  @Override
  public void close() throws Exception {
    if (future != null && !future.isDone()) {
      future.cancel(true);
    }
    executor.shutdown();
    maybeCloseCurrentBeamReader();
  }

  private boolean moveToNextSplitWithRecords() throws IOException {
    while (!currentSplitHasNextRecord && !beamSourceSplits.isEmpty()) {
      maybeCloseCurrentBeamReader();
      FlinkBoundedSourceSplit<OutputT> split = beamSourceSplits.remove();
      currentSourceSplit = split;
      currentBeamReader = split.getBeamSplitSource().createReader(pipelineOptions);
      currentSplitHasNextRecord = currentBeamReader.start();
    }

    if (currentSplitHasNextRecord) {
      // Found next split with records.
      isAvailableFuture.complete(null);
      return true;
    } else {
      // There is no reader available to read.
      currentSourceSplit = null;
      currentBeamReader = null;
      return false;
    }
  }

  private boolean allSplitsFinished() {
    return noMoreSplits && beamSourceSplits.isEmpty() && !currentSplitHasNextRecord;
  }

  private boolean idleTimeoutReached() {
    return System.currentTimeMillis() - lastRecordEmittedMs > idleTimeoutMs;
  }

  private void maybeCloseCurrentBeamReader() throws IOException {
    if (currentBeamReader != null) {
      currentBeamReader.close();
    }
  }

}
