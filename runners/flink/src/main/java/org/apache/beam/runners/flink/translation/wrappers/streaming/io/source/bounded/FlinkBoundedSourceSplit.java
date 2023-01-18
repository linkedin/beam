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

import java.io.Serializable;
import org.apache.beam.runners.flink.translation.utils.SerdeUtils;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;


public class FlinkBoundedSourceSplit<T> implements SourceSplit, Serializable {
  // The index of the split.
  private final int splitIndex;
  private final BoundedSource<T> beamSplitSource;

  FlinkBoundedSourceSplit(int splitIndex, BoundedSource<T> beamSplitSource) {
    this.splitIndex = splitIndex;
    this.beamSplitSource = beamSplitSource;
  }

  public int getSplitIndex() {
    return splitIndex;
  }

  public BoundedSource<T> getBeamSplitSource() {
    return beamSplitSource;
  }

  @Override
  public String splitId() {
    return Integer.toString(splitIndex);
  }

  @Override
  public String toString() {
    return String.format("[SplitIndex: %d, BeamSource: %s]", splitIndex, beamSplitSource);
  }

  public static <T> SimpleVersionedSerializer<FlinkBoundedSourceSplit<T>> serializer() {
    return SerdeUtils.getNaiveObjectSerializer();
  }
}
