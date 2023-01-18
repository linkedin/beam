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

import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.translation.utils.SerdeUtils;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;


public class FlinkBoundedSource<OutputT>
    implements Source<WindowedValue<OutputT>, FlinkBoundedSourceSplit<OutputT>, Map<Integer, List<FlinkBoundedSourceSplit<OutputT>>>> {

  private final BoundedSource<OutputT> beamSource;
  private final SerializablePipelineOptions serializablePipelineOptions;
  private final int numSplits;

  public FlinkBoundedSource(
      BoundedSource<OutputT> beamSource,
      SerializablePipelineOptions serializablePipelineOptions,
      int numSplits) {
    this.beamSource = beamSource;
    this.serializablePipelineOptions = serializablePipelineOptions;
    this.numSplits = numSplits;
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.BOUNDED;
  }

  @Override
  public SourceReader<WindowedValue<OutputT>, FlinkBoundedSourceSplit<OutputT>> createReader(
      SourceReaderContext readerContext) throws Exception {
    return new FlinkBoundedSourceReader<>(readerContext, serializablePipelineOptions.get());
  }

  @Override
  public SplitEnumerator<FlinkBoundedSourceSplit<OutputT>, Map<Integer, List<FlinkBoundedSourceSplit<OutputT>>>> createEnumerator(
      SplitEnumeratorContext<FlinkBoundedSourceSplit<OutputT>> enumContext) throws Exception {
    return new FlinkBoundedSourceSplitEnumerator<>(enumContext, beamSource, serializablePipelineOptions.get(), numSplits);
  }

  @Override
  public SplitEnumerator<FlinkBoundedSourceSplit<OutputT>, Map<Integer, List<FlinkBoundedSourceSplit<OutputT>>>> restoreEnumerator(
      SplitEnumeratorContext<FlinkBoundedSourceSplit<OutputT>> enumContext,
      Map<Integer, List<FlinkBoundedSourceSplit<OutputT>>> checkpoint) throws Exception {
    FlinkBoundedSourceSplitEnumerator<OutputT> enumerator =
        new FlinkBoundedSourceSplitEnumerator<>(enumContext, beamSource, serializablePipelineOptions.get(), numSplits);
    checkpoint.forEach((subtaskId, splitsForSubtask) -> enumerator.addSplitsBack(splitsForSubtask, subtaskId));
    return enumerator;
  }

  @Override
  public SimpleVersionedSerializer<FlinkBoundedSourceSplit<OutputT>> getSplitSerializer() {
    return FlinkBoundedSourceSplit.serializer();
  }

  @Override
  public SimpleVersionedSerializer<Map<Integer, List<FlinkBoundedSourceSplit<OutputT>>>> getEnumeratorCheckpointSerializer() {
    return SerdeUtils.getNaiveObjectSerializer();
  }

  public int getNumSplits() {
    return numSplits;
  }
}
