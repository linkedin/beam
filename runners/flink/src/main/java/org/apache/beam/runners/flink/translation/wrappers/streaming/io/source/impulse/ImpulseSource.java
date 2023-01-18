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

import java.io.IOException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class ImpulseSource implements Source<WindowedValue<byte[]>, ImpulseSplit, Boolean> {
  public static final long NO_IDLE_TIMEOUT = -1L;
  private final Boundedness boundedness;
  private final long idleTimeoutMs;

  private ImpulseSource(Boundedness boundedness, long idleTimeoutMs) {
    this.boundedness = boundedness;
    this.idleTimeoutMs = idleTimeoutMs;
  }

  //------------------- public creators ------------------------

  public static ImpulseSource bounded() {
    return new ImpulseSource(Boundedness.BOUNDED, NO_IDLE_TIMEOUT);
  }

  public static ImpulseSource unbounded(long idleTimeoutMs) {
    return new ImpulseSource(Boundedness.CONTINUOUS_UNBOUNDED, idleTimeoutMs);
  }

  // ----------------------------------------------------------

  @Override
  public Boundedness getBoundedness() {
    return boundedness;
  }

  @Override
  public SourceReader<WindowedValue<byte[]>, ImpulseSplit> createReader(SourceReaderContext readerContext)
      throws Exception {
    return new ImpulseSourceReader(readerContext, boundedness, idleTimeoutMs);
  }

  @Override
  public SplitEnumerator<ImpulseSplit, Boolean> createEnumerator(SplitEnumeratorContext<ImpulseSplit> enumContext)
      throws Exception {
    return new ImpulseSourceSplitEnumerator(enumContext);
  }

  @Override
  public SplitEnumerator<ImpulseSplit, Boolean> restoreEnumerator(SplitEnumeratorContext<ImpulseSplit> enumContext,
      Boolean checkpoint) throws Exception {
    ImpulseSourceSplitEnumerator enumerator = new ImpulseSourceSplitEnumerator(enumContext);
    enumerator.setHasAssignedSplit(checkpoint);
    return enumerator;
  }

  @Override
  public SimpleVersionedSerializer<ImpulseSplit> getSplitSerializer() {
    return ImpulseSplit.SERIALIZER;
  }

  @Override
  public SimpleVersionedSerializer<Boolean> getEnumeratorCheckpointSerializer() {
    return new SimpleVersionedSerializer<Boolean>() {
      @Override
      public int getVersion() {
        return 0;
      }

      @Override
      public byte[] serialize(Boolean obj) throws IOException {
        byte[] bytes = new byte[1];
        bytes[0] = (byte) (obj ? 1 : 0);
        return bytes;
      }

      @Override
      public Boolean deserialize(int version, byte[] serialized) throws IOException {
        return serialized[0] != 0;
      }
    };
  }
}
