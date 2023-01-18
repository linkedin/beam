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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FlinkBoundedSourceSplitEnumerator<OutputT>
    implements SplitEnumerator<FlinkBoundedSourceSplit<OutputT>, Map<Integer, List<FlinkBoundedSourceSplit<OutputT>>>> {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkBoundedSourceSplitEnumerator.class);
  private final SplitEnumeratorContext<FlinkBoundedSourceSplit<OutputT>> context;
  private final BoundedSource<OutputT> beamSource;
  private final PipelineOptions pipelineOptions;
  private final int numSplits;

  private final Map<Integer, List<FlinkBoundedSourceSplit<OutputT>>> pendingSplits;

  FlinkBoundedSourceSplitEnumerator(
      SplitEnumeratorContext<FlinkBoundedSourceSplit<OutputT>> context,
      BoundedSource<OutputT> beamSource,
      PipelineOptions pipelineOptions,
      int numSplits) {
    this.context = context;
    this.beamSource = beamSource;
    this.pipelineOptions = pipelineOptions;
    this.numSplits = numSplits;
    this.pendingSplits = new HashMap<>(numSplits);
  }

  @Override
  public void start() {
    context.callAsync(() -> {
      try {
        long desiredSizeBytes = beamSource.getEstimatedSizeBytes(pipelineOptions) / numSplits;
        List<? extends BoundedSource<OutputT>> beamSplitSourceList = beamSource.split(desiredSizeBytes, pipelineOptions);
        Map<Integer, List<FlinkBoundedSourceSplit<OutputT>>> flinkSourceSplitsList = new HashMap<>();
        int i = 0;
        for (BoundedSource<OutputT> beamSplitSource : beamSplitSourceList) {
          int targetSubtask = i % context.currentParallelism();
          List<FlinkBoundedSourceSplit<OutputT>> splitsForTask =
              flinkSourceSplitsList.computeIfAbsent(targetSubtask, ignored -> new ArrayList<>());
          splitsForTask.add(new FlinkBoundedSourceSplit<>(i, beamSplitSource));
          i++;
        }
        return flinkSourceSplitsList;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, (sourceSplits, error) -> {
      if (error != null) {
        throw new RuntimeException("Failed to start source enumerator.", error);
      } else {
        pendingSplits.putAll(sourceSplits);
        sendPendingSplitsToSourceReaders();
      }
    });
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    // Not used.
  }

  @Override
  public void addSplitsBack(List<FlinkBoundedSourceSplit<OutputT>> splits, int subtaskId) {
    LOG.info("Adding splits {} back from subtask {}", splits, subtaskId);
    List<FlinkBoundedSourceSplit<OutputT>> splitsForSubtask =
        pendingSplits.computeIfAbsent(subtaskId, ignored -> new ArrayList<>());
    splitsForSubtask.addAll(splits);
  }

  @Override
  public void addReader(int subtaskId) {
    List<FlinkBoundedSourceSplit<OutputT>> splitsForSubtask = pendingSplits.remove(subtaskId);
    if (splitsForSubtask != null) {
      assignSplitsAndLog(splitsForSubtask, subtaskId);
      pendingSplits.remove(subtaskId);
    } else {
      LOG.info("There is no split for subtask {}. Signaling no more splits.", subtaskId);
      context.signalNoMoreSplits(subtaskId);
    }
  }

  @Override
  public Map<Integer, List<FlinkBoundedSourceSplit<OutputT>>> snapshotState() throws Exception {
    return pendingSplits;
  }

  @Override
  public void close() throws IOException {
    // NoOp
  }

  private void sendPendingSplitsToSourceReaders() {
    Iterator<Map.Entry<Integer, List<FlinkBoundedSourceSplit<OutputT>>>> splitIter = pendingSplits.entrySet().iterator();
    while (splitIter.hasNext()) {
      Map.Entry<Integer, List<FlinkBoundedSourceSplit<OutputT>>> entry = splitIter.next();
      int readerIndex = entry.getKey();
      int targetSubtask = readerIndex % context.currentParallelism();
      if (context.registeredReaders().containsKey(targetSubtask)) {
        assignSplitsAndLog(entry.getValue(), targetSubtask);
        splitIter.remove();
      }
    }
  }

  private void assignSplitsAndLog(List<FlinkBoundedSourceSplit<OutputT>> splits, int subtaskId) {
    splits.forEach(split -> context.assignSplit(split, subtaskId));
    context.signalNoMoreSplits(subtaskId);
    LOG.info("Assigned splits {} to subtask {}", splits, subtaskId);
  }
}
