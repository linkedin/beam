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

import com.google.auto.service.AutoService;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.AbstractDataType;

/**
 * A {@link PTransform} that wraps Flink SQL statements. This is the entrypoint class to create a
 * Flink SQL PTransform. Users can create a {@link SingleOutputSqlTransform} by calling {@link
 * #of(Class, Class) of(Class<InputT> inputClass, Class<OutputT> outputClass)}.
 *
 * <p>See {@link SingleOutputSqlTransform} for more details regarding how to specify the input
 * table, output table and SQL logic.
 */
public class FlinkSql<InputT, OutputT> implements Serializable {

  public static final String FLINK_SQL_URN = "beam:transform:flink:sql-statements:v1";
  public static final String UNUSED_MAIN_INPUT_TABLE_NAME = "UnusedMainInputTable";
  public static final String DEFAULT_MAIN_OUTPUT_TABLE_NAME = "UnnamedMainOutputTable";
  // A mapping from table name to the corresponding query. A LinkedHashMap is needed
  // so a later query can refer to the result of a previous query.
  private final LinkedHashMap<String, String> queries;
  private final List<String> ddlList;
  private final Map<TupleTag<?>, PValue> additionalInputs;
  private final List<TupleTag<?>> additionalOutputs;
  private final Map<String, TableInfo.InputTableInfo<?>> inputTableInfo;
  private final Map<String, TableInfo.OutputTableInfo<?>> outputTableInfo;
  private String mainInputTableName;
  private String mainOutputTableName;

  FlinkSql(Class<InputT> inputClass, Class<OutputT> outputClass) {
    this.queries = new LinkedHashMap<>();
    this.ddlList = new ArrayList<>();
    this.additionalInputs = new HashMap<>();
    this.additionalOutputs = new ArrayList<>();
    this.inputTableInfo = new HashMap<>();
    this.outputTableInfo = new HashMap<>();
    this.mainInputTableName = UNUSED_MAIN_INPUT_TABLE_NAME;
    this.mainOutputTableName = DEFAULT_MAIN_OUTPUT_TABLE_NAME;
    inputTableInfo.put(mainInputTableName, TableInfo.InputTableInfo.of(inputClass));
    outputTableInfo.put(mainOutputTableName, TableInfo.OutputTableInfo.of(outputClass));
  }

  /**
   * Create a {@link SingleOutputSqlTransform} with the specified input and output class. By
   * default, the created SQL Transform assumes both the input and output classes are of the <a
   * href="https://en.wikipedia.org/wiki/Plain_old_Java_object">POJO type</a>. So, the SQL transform
   * can convert a record to a Flink Table Row and vice versa.
   *
   * <p>Users can also specify the conversion between a record of Java object and a Flink Table Row
   * by specifying a {@link TypeInformation} and {@link AbstractDataType DataType} if needed.
   *
   * @param inputClass the class of the input records for the Sql transform.
   * @param outputClass the class of the output records for the sql transform.
   * @return A {@link SingleOutputSqlTransform}.
   */
  public static <InputT, OutputT> SingleOutputSqlTransform<InputT, OutputT> of(
      Class<InputT> inputClass, Class<OutputT> outputClass) {
    return new SingleOutputSqlTransform<>(new FlinkSql<>(inputClass, outputClass));
  }

  // --------------------- setters ----------------------------
  /**
   * Use DDL to define Tables. The DDL string can contain multiple {@code CREATE TABLE} / {@code
   * CREATE VIEW} statements. The DDL string should not contain any DQL / DML statement.
   *
   * @param ddl the table definition
   * @return this {@link FlinkSql} itself.
   */
  FlinkSql<InputT, OutputT> withDDL(String ddl) {
    ddlList.add(ddl);
    return this;
  }

  /**
   * Use DQL to express the query logic. The query should only contain one DQL, i.e. one top level
   * {@code SELECT} statement. The query result will be registered as a temporary view. The query
   * statements comes after this query can refer to the result of this query with the specified
   * result table name.
   *
   * @param resultTableName the table name of the query result.
   * @param query the SQL DQL statement.
   */
  FlinkSql<InputT, OutputT> withQuery(String resultTableName, String query) {
    queries.put(resultTableName, query);
    return this;
  }

  /**
   * Sets the name of the table created from the input {@link PCollection} to this Sql Transform. By
   * Default the main input is not used because the tables can be defined from the DDLs.
   *
   * @param name the name of the table created from the input {@link PCollection}.
   */
  FlinkSql<InputT, OutputT> withMainInputTableName(String name) {
    if (name.equals(UNUSED_MAIN_INPUT_TABLE_NAME)) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot set main input table name to %s which is "
                  + "reserved for unused main input.",
              name));
    }

    if (!mainInputTableName.equals(name)) {
      TableInfo.InputTableInfo<InputT> mainInputTableInfo = getMainInputTableInfo();
      inputTableInfo.put(name, mainInputTableInfo);
      inputTableInfo.remove(mainInputTableName);
      mainInputTableName = name;
    }
    return this;
  }

  /**
   * Specify the {@link TypeInformation} of the main input {@link PCollection} for this Sql
   * Transform. The {@link TypeInformation} is used when converting the main input {@link
   * PCollection} to the main input {@link Table}.
   *
   * <p>By default, the {@link TypeInformation} is a POJO TypeInformation based on the input class.
   * Users can override the {@link TypeInformation} with this method.
   *
   * @param typeInfo the {@link TypeInformation} for the main input {@link PCollection} for this Sql
   *     Transform.
   * @see StreamTableEnvironment#createTemporaryView(String, DataStream)
   */
  FlinkSql<InputT, OutputT> withMainInputTableTypeInformation(TypeInformation<InputT> typeInfo) {
    getMainInputTableInfo().setTypeInfo(typeInfo);
    return this;
  }

  /**
   * Specify the table to be set as the main output of this Sql Transform.
   *
   * @param name the name of the table to be set as the main output of this Sql transform.
   */
  FlinkSql<InputT, OutputT> withMainOutputTableName(String name) {
    validateOutputTableName(name);
    if (!name.equals(mainOutputTableName)) {
      outputTableInfo.put(name, getMainOutputTableInfo());
      outputTableInfo.remove(mainOutputTableName);
      mainOutputTableName = name;
    }
    return this;
  }

  /**
   * Specify the {@link TypeInformation} used when converting the main output {@link Table} to a
   * {@link DataStream}.
   *
   * @param typeInfo the specified {@link TypeInformation}
   * @see StreamTableEnvironment#toDataStream(Table)
   */
  FlinkSql<InputT, OutputT> withMainOutputTableTypeInformation(TypeInformation<OutputT> typeInfo) {
    getMainOutputTableInfo().setTypeInfo(typeInfo);
    return this;
  }

  /**
   * Specify the {@link AbstractDataType DataType} used when converting the main output {@link
   * Table} to a {@link DataStream}.
   *
   * @param dataType the specified {@link AbstractDataType DataType}
   * @see StreamTableEnvironment#toDataStream(Table, AbstractDataType)
   */
  FlinkSql<InputT, OutputT> withMainOutputTableDataType(AbstractDataType<?> dataType) {
    getMainOutputTableInfo().setDataType(dataType);
    return this;
  }

  /**
   * Add another {@link PCollection} as an additional input table to this Sql transform.
   *
   * @param tag the {@link TupleTag} of the additional input {@link Table}.
   * @param pCollection The {@link PCollection} as the additional input table.
   * @param tableInfo the table information of the additional input table.
   */
  FlinkSql<InputT, OutputT> withAdditionalInputTable(
      TupleTag<?> tag,
      PCollection<?> pCollection,
      TableInfo.AdditionalInputTableInfo<?> tableInfo) {
    additionalInputs.put(tag, pCollection);
    inputTableInfo.put(tag.getId(), tableInfo);
    return this;
  }

  /**
   * Specify the information for an additional output table of this Sql Transform.
   *
   * @param tag the {@link TupleTag} of the additional output. The tag id must be one of the tables
   *     defined by {@link #withQuery(String, String)}.
   * @param tableInfo the table information for the additional table.
   */
  FlinkSql<InputT, OutputT> withAdditionalOutputTable(
      TupleTag<?> tag, TableInfo.AdditionalOutputTableInfo<?> tableInfo) {
    validateOutputTableName(tag.getId());
    additionalOutputs.add(tag);
    outputTableInfo.put(tag.getId(), tableInfo);
    return this;
  }

  // ------------ getters ------------------
  List<String> getDdlList() {
    return ddlList;
  }

  LinkedHashMap<String, String> getQueries() {
    return queries;
  }

  String getMainInputTableName() {
    return mainInputTableName;
  }

  public String getMainOutputTableName() {
    return mainOutputTableName;
  }

  @SuppressWarnings("unchecked")
  TableInfo.InputTableInfo<InputT> getMainInputTableInfo() {
    return Preconditions.checkStateNotNull(
        (TableInfo.InputTableInfo<InputT>) inputTableInfo.get(mainInputTableName));
  }

  @SuppressWarnings("unchecked")
  TableInfo.OutputTableInfo<OutputT> getMainOutputTableInfo() {
    return Preconditions.checkStateNotNull(
        (TableInfo.OutputTableInfo<OutputT>) outputTableInfo.get(mainOutputTableName));
  }

  public Map<TupleTag<?>, PValue> getAdditionalInputs() {
    return additionalInputs;
  }

  public TableInfo.AdditionalInputTableInfo<?> getAdditionalInputTableInfo(String name) {
    validateInputTableName(name);
    return Preconditions.checkStateNotNull(
        (TableInfo.AdditionalInputTableInfo<?>) inputTableInfo.get(name));
  }

  public TupleTagList getAdditionalOutputs() {
    return TupleTagList.of(additionalOutputs);
  }

  public TableInfo.AdditionalOutputTableInfo<?> getAdditionalOutputTableInfo(String name) {
    validateOutputTableName(name);
    return Preconditions.checkStateNotNull(
        (TableInfo.AdditionalOutputTableInfo<?>) outputTableInfo.get(name));
  }

  public boolean hasSpecifiedMainOutputTableName() {
    return !DEFAULT_MAIN_OUTPUT_TABLE_NAME.equals(mainOutputTableName);
  }

  public boolean isMainInputUsed() {
    return !UNUSED_MAIN_INPUT_TABLE_NAME.equals(mainInputTableName);
  }

  // ------------------ private helper methods ---------------------------

  private void validateInputTableName(String name) {
    if (!inputTableInfo.containsKey(name)) {
      throw new IllegalArgumentException(
          String.format(
              "The input table %s is not found. The valid table names for input are %s",
              name, inputTableInfo.keySet()));
    }
  }

  private void validateOutputTableName(String name) {
    if (!queries.containsKey(name)) {
      throw new IllegalArgumentException(
          String.format(
              "The output table %s is not found. The valid table names for output are %s",
              name, queries.keySet()));
    }
  }

  // ------------------ public nested classes -------------------------

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
              MultiOutputSqlTransform.class,
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

  /**
   * Translator class for {@link FlinkSql}. The input Tables for SQL are available in two ways. 1.
   * From the upstream PTransforms. In this case, the table name is
   *
   * @param <InputT> the input data type.
   * @param <OutputT> the output data type.
   */
  private static class FlinkSQLTransformTranslator<InputT, OutputT>
      extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<
          PTransform<PCollection<InputT>, PCollection<OutputT>>> {

    @Override
    public void translateNode(
        PTransform<PCollection<InputT>, PCollection<OutputT>> transform,
        FlinkStreamingTranslationContext context) {
      if (context.isStreaming()) {
        throw new IllegalStateException(
            "The current job is a streaming job. Flink SQL transform only support batch jobs.");
      }
      MultiOutputSqlTransform<InputT, OutputT> sqlTransform = (MultiOutputSqlTransform) transform;
      StreamTableEnvironment tEnv =
          StreamTableEnvironment.create(context.getExecutionEnvironment());

      // If the main input table is from an upstream PTransform, create a temporary view for the
      // main input DataStream.
      PValue input = context.getInput(transform);
      if (sqlTransform.isMainInputTableUsed() && input != null) {
        DataStream<WindowedValue<InputT>> inputDataStream = context.getInputDataStream(input);
        DataStream<InputT> unwrappedDataStream =
            inputDataStream.map(r -> r.getValue(), sqlTransform.getMainInputTypeInformation());
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
          tEnv.toDataStream(
              Preconditions.checkStateNotNull(tables.get(tag.getId())), outputDataType);
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
  }
}
