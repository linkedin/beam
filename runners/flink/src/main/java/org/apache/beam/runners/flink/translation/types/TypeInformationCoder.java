/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.runners.flink.translation.types;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;


/**
 * Create a Beam {@link Coder} from a Flink {@link TypeInformation}.
 */
public class TypeInformationCoder<T> extends Coder<T> {

  private final TypeSerializer<T> serializer;

  public static <T> TypeInformationCoder<T> of(Class<T> clazz) {
    return new TypeInformationCoder<>(TypeInformation.of(clazz));
  }

  public TypeInformationCoder(TypeInformation<T> typeInfo) {
    this.serializer = typeInfo.createSerializer(new ExecutionConfig());
  }

  @Override
  public void encode(T value, OutputStream outStream) throws IOException {
    serializer.serialize(value, new DataOutputViewStreamWrapper(outStream));
  }

  @Override
  public T decode(InputStream inStream) throws IOException {
    return serializer.deserialize(new DataInputViewStreamWrapper(inStream));
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public void verifyDeterministic() {}
}
