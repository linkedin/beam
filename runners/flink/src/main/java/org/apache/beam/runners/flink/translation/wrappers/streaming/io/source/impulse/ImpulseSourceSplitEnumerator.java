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

package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.impulse;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.compat.SplitEnumeratorCompat;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;


/**
 * SplitEnumerator class for ImpulseSource. This enumerator ensures that there will only
 * be a single global impulse sent regardless of the parallelism of the Source. This is
 * achieved by assigning a global singleton split to one of the SourceReaders.
 */
public class ImpulseSourceSplitEnumerator implements SplitEnumeratorCompat<ImpulseSplit, Boolean> {
  private final SplitEnumeratorContext<ImpulseSplit> context;
  private boolean hasAssignedSplit = false;

  ImpulseSourceSplitEnumerator(SplitEnumeratorContext<ImpulseSplit> context) {
    this.context = context;
  }

  @Override
  public void start() {

  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

  }

  @Override
  public void addSplitsBack(List<ImpulseSplit> splits, int subtaskId) {
    if (!splits.isEmpty()) {
      hasAssignedSplit = false;
    }
  }

  @Override
  public void addReader(int subtaskId) {
    if (!hasAssignedSplit) {
      context.assignSplit(new ImpulseSplit(), subtaskId);
      hasAssignedSplit = true;
    }
    context.signalNoMoreSplits(subtaskId);
  }

  @Override
  public Boolean snapshotState(long checkpointId) throws Exception {
    return snapshotState();
  }

  @Override
  public Boolean snapshotState() throws Exception {
    return hasAssignedSplit;
  }

  @Override
  public void close() throws IOException {

  }

  public void setHasAssignedSplit(boolean hasAssignedSplit) {
    this.hasAssignedSplit = hasAssignedSplit;
  }
}
