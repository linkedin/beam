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

import static org.apache.beam.runners.flink.transform.sql.SqlTransform.FLINK_SQL_URN;

import com.google.auto.service.AutoService;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.runners.flink.FlinkCustomTransformTranslatorRegistrar;
import org.apache.beam.runners.flink.FlinkStreamingPipelineTranslator;
import org.apache.beam.runners.flink.FlinkStreamingTranslationContext;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.AbstractDataType;

/**
 * Translator class for {@link SqlTransform}. The input Tables for SQL are available in two ways. 1.
 * From the upstream PTransforms. In this case, the table name is
 *
 * @param <InputT> the input data type.
 * @param <OutputT> the output data type.
 */
class FlinkSQLTransformTranslator<InputT, OutputT>
    extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
        PTransform<PCollection<InputT>, PCollection<OutputT>>> {

  @Override
  public void translateNode(
      PTransform<PCollection<InputT>, PCollection<OutputT>> transform,
      FlinkStreamingTranslationContext context) {
    MultiOutputSqlTransformWithInput<InputT, OutputT> sqlTransform =
        (MultiOutputSqlTransformWithInput) transform;
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(context.getExecutionEnvironment());

    // If the main input table is from an upstream PTransform, create a temporary view for the
    // main input DataStream.
    PCollection<InputT> input = context.getInput(transform);
    if (sqlTransform.isMainInputTableUsed() && input != null) {
      DataStream<WindowedValue<InputT>> inputDataStream = context.getInputDataStream(input);
      TypeInformation<InputT> inputTypeInfo = sqlTransform.getMainInputTypeInformation();
      DataStream<InputT> unwrappedDataStream =
          inputDataStream.map(r -> r.getValue(), inputTypeInfo);
      tEnv.createTemporaryView(sqlTransform.getMainInputTableName(), unwrappedDataStream);
    }

    // Prepare the Flink tables for the side inputs.
    sqlTransform
        .getAdditionalInputs()
        .forEach(
            (tag, inputPCollection) -> {
              String inputTableName = tag.getId();
              Class<?> inputClass = sqlTransform.getClassForAdditionalInput(inputTableName);
              @SuppressWarnings("rawtypes")
              DataStream inputDataStream = context.getInputDataStream(inputPCollection);
              DataStream<?> unwrappedDataStream =
                  inputDataStream.map(
                      r -> inputClass.cast(((WindowedValue<?>) r).getValue()),
                      sqlTransform.getTypeInformationForAdditionalInput(inputTableName));
              tEnv.createTemporaryView(inputTableName, unwrappedDataStream);
            });

    // Register the catalogs.
    sqlTransform.getCatalogs().forEach(tEnv::registerCatalog);

    // Register the functions.
    sqlTransform.getFunctionClasses().forEach(tEnv::createTemporarySystemFunction);
    sqlTransform.getFunctionInstances().forEach(tEnv::createTemporarySystemFunction);

    // Create the tables from DDL.
    sqlTransform.getDdlList().forEach(tEnv::executeSql);

    // Execute the queries.
    Map<String, Table> tablesFromQuery = new HashMap<>();
    sqlTransform
        .getQueries()
        .forEach(
            (tableName, query) -> {
              Table table = tEnv.sqlQuery(query);
              tEnv.createTemporaryView(tableName, table);
              tablesFromQuery.put(tableName, table);
            });

    // prepare the main output.
    TupleTag<OutputT> mainOutputTag = sqlTransform.getMainOutputTableTag();
    setOutputStream(
        mainOutputTag, sqlTransform.getMainOutputDataType(), tablesFromQuery, tEnv, context);

    // prepare the additional outputs.
    sqlTransform
        .getAdditionalOutputTables()
        .getAll()
        .forEach(
            tag ->
                setOutputStream(
                    tag,
                    sqlTransform.getAdditionalOutputDataType(tag),
                    tablesFromQuery,
                    tEnv,
                    context));
  }

  @SuppressWarnings("unchecked")
  private <T> void setOutputStream(
      TupleTag<T> tag,
      AbstractDataType<?> outputDataType,
      Map<String, Table> tables,
      StreamTableEnvironment tEnv,
      FlinkStreamingTranslationContext context) {
    Map<TupleTag<?>, PCollection<?>> outputs = context.getCurrentTransform().getOutputs();
    PCollection<T> outputPCollection =
        Preconditions.checkStateNotNull((PCollection<T>) outputs.get(tag));
    DataStream<T> outputStream =
        tEnv.toDataStream(Preconditions.checkStateNotNull(tables.get(tag.getId())), outputDataType);
    DataStream<WindowedValue<T>> outputStreamForBeam =
        outputStream.map(
            record ->
                WindowedValue.of(
                    record,
                    BoundedWindow.TIMESTAMP_MIN_VALUE,
                    GlobalWindow.INSTANCE,
                    PaneInfo.NO_FIRING),
            new CoderTypeInformation<>(
                context.getWindowedInputCoder(outputPCollection), context.getPipelineOptions()));
    context.setOutputDataStream(outputPCollection, outputStreamForBeam);
  }

  /** Registers Flink SQL PTransform URN. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  @SuppressWarnings("rawtypes")
  public static class FlinkTransformsRegistrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<
            ? extends Class<? extends PTransform>,
            ? extends PTransformTranslation.TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap
          .<Class<? extends PTransform>, PTransformTranslation.TransformPayloadTranslator>builder()
          .put(
              MultiOutputSqlTransformWithInput.class,
              PTransformTranslation.TransformPayloadTranslator.NotSerializable.forUrn(
                  FLINK_SQL_URN))
          .build();
    }
  }

  /** Registers Flink SQL PTransform to the Flink runner. */
  @AutoService(FlinkCustomTransformTranslatorRegistrar.class)
  public static class FlinkSqlTransformsRegistrar
      implements FlinkCustomTransformTranslatorRegistrar {
    @Override
    public Map<String, FlinkStreamingPipelineTranslator.StreamTransformTranslator<?>>
        getTransformTranslators() {
      return ImmutableMap
          .<String, FlinkStreamingPipelineTranslator.StreamTransformTranslator<?>>builder()
          .put(FLINK_SQL_URN, new FlinkSQLTransformTranslator<>())
          .build();
    }
  }
}
