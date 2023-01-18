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
import java.nio.ByteBuffer;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;


/**
 * A split used by the {@link ImpulseSource} to emit just one element.
 */
public final class ImpulseSplit implements SourceSplit {

  public static final SimpleVersionedSerializer<ImpulseSplit> SERIALIZER =
      new SimpleVersionedSerializer<ImpulseSplit>() {
        private static final int SIZE = Long.BYTES/* Emit Time */;
        @Override
        public int getVersion() {
          return 0;
        }

        @Override
        public byte[] serialize(ImpulseSplit obj) throws IOException {
          ByteBuffer buffer = ByteBuffer.allocate(SIZE);
          buffer.putLong(obj.lastEmittingTimeMs);
          return buffer.array();
        }

        @Override
        public ImpulseSplit deserialize(int version, byte[] serialized) throws IOException {
          if (version > getVersion()) {
            throw new IOException(String.format("Cannot deserialize version %d which is higher "
                    + "than supported version %d.", version, getVersion()));
          }
          ByteBuffer buffer = ByteBuffer.wrap(serialized);
          ImpulseSplit split = new ImpulseSplit();
          split.setEmitted(buffer.getLong());
          return split;
        }
      };

  private long lastEmittingTimeMs = -1L;

  public void setEmitted(long emittingTimeMs) {
    lastEmittingTimeMs = emittingTimeMs;
  }

  public boolean hasEmitted() {
    return lastEmittingTimeMs >= 0L;
  }

  public long lastEmittingTimeMs() {
    return lastEmittingTimeMs;
  }

  @Override
  public String splitId() {
    return "DummySplit";
  }

}
