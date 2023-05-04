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

import java.io.Serializable;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AbstractDataType;

/**
 * A package private container class holding the classes with Table information.
 *
 * <ul>
 *   <li>{@link InputTableInfo InputTableInfo} contains information for the main input table of a
 *       Sql Transform..
 *   <li>{@link AdditionalInputTableInfo InputTableInfo} contains information for an additional
 *       input table of a Sql Transform.
 *   <li>{@link OutputTableInfo InputTableInfo} contains information for the main output table of a
 *       Sql transform.
 *   <li>{@link AdditionalOutputTableInfo InputTableInfo} contains information for an additional
 *       output table of a Sql Transform.
 * </ul>
 */
class TableInfo {
  public static class InputTableInfo<T> implements Serializable {
    private final Class<T> clazz;
    private TypeInformation<?> typeInfo;

    public static <T> InputTableInfo<T> of(Class<T> clazz) {
      return new InputTableInfo<>(clazz, TypeInformation.of(clazz));
    }

    InputTableInfo(Class<T> clazz, TypeInformation<?> typeInfo) {
      this.clazz = clazz;
      this.typeInfo = typeInfo;
    }

    public Class<T> getClazz() {
      return clazz;
    }

    public TypeInformation<?> getTypeInfo() {
      return typeInfo;
    }

    public void setTypeInfo(TypeInformation<?> typeInfo) {
      this.typeInfo = typeInfo;
    }
  }

  public static class AdditionalInputTableInfo<T> extends InputTableInfo<T> {
    private final TupleTag<?> tag;

    public AdditionalInputTableInfo(Class<T> clazz, TupleTag<?> tag, TypeInformation<?> typeInfo) {
      super(clazz, typeInfo);
      this.tag = tag;
    }

    static AdditionalInputTableInfo<?> of(TupleTag<?> tag) {
      Class<?> clazz = tag.getTypeDescriptor().getRawType();
      return new AdditionalInputTableInfo<>(clazz, tag, TypeInformation.of(clazz));
    }

    public TupleTag<?> getTag() {
      return tag;
    }
  }

  public static class OutputTableInfo<T> implements Serializable {
    private final Class<T> clazz;
    private TypeInformation<?> typeInfo;
    private AbstractDataType<?> dataType;

    public OutputTableInfo(
        Class<T> clazz, TypeInformation<?> typeInfo, AbstractDataType<?> dataType) {
      this.clazz = clazz;
      this.typeInfo = typeInfo;
      this.dataType = dataType;
    }

    public static <T> OutputTableInfo<T> of(Class<T> clazz) {
      return new OutputTableInfo<>(clazz, TypeInformation.of(clazz), DataTypes.of(clazz));
    }

    public Class<T> getClazz() {
      return clazz;
    }

    @SuppressWarnings("unchecked")
    public TypeInformation<T> getTypeInfo() {
      return (TypeInformation<T>) typeInfo;
    }

    public AbstractDataType<?> getDataType() {
      return dataType;
    }

    public void setTypeInfo(TypeInformation<?> typeInfo) {
      this.typeInfo = typeInfo;
    }

    public void setDataType(AbstractDataType<?> dataType) {
      this.dataType = dataType;
    }
  }

  static class AdditionalOutputTableInfo<T> extends OutputTableInfo<T> {
    private final TupleTag<?> tag;

    public AdditionalOutputTableInfo(
        Class<T> clazz,
        TupleTag<?> tag,
        TypeInformation<?> typeInfo,
        AbstractDataType<?> dataType) {
      super(clazz, typeInfo, dataType);
      this.tag = tag;
    }

    public static AdditionalOutputTableInfo<?> of(TupleTag<?> tag) {
      Class<?> clazz = tag.getTypeDescriptor().getRawType();
      return new AdditionalOutputTableInfo<>(
          clazz, tag, TypeInformation.of(clazz), DataTypes.of(clazz));
    }

    public TupleTag<?> getTag() {
      return tag;
    }
  }
}
