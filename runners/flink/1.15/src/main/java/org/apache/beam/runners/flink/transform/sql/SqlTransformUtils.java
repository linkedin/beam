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
package org.apache.beam.runners.flink.transform.sql;

import org.apache.beam.runners.flink.translation.types.TypeInformationCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;

class SqlTransformUtils {

  private SqlTransformUtils() {}

  @SuppressWarnings("unchecked")
  public static <T> void setCoderForOutput(
      SchemaRegistry schemaRegistry,
      CoderRegistry coderRegistry,
      TupleTag<?> tag,
      PCollection<?> output,
      TypeInformation<T> typeInfo) {
    TypeDescriptor<T> outputTypeDescriptor = (TypeDescriptor<T>) tag.getTypeDescriptor();
    PCollection<T> out = (PCollection<T>) output;
    try {
      out.setSchema(
          schemaRegistry.getSchema(outputTypeDescriptor),
          outputTypeDescriptor,
          schemaRegistry.getToRowFunction(outputTypeDescriptor),
          schemaRegistry.getFromRowFunction(outputTypeDescriptor));
    } catch (NoSuchSchemaException e) {
      try {
        out.setCoder(coderRegistry.getCoder(outputTypeDescriptor));
      } catch (CannotProvideCoderException e2) {
        out.setCoder(new TypeInformationCoder<>(typeInfo));
      }
    }
  }
}
