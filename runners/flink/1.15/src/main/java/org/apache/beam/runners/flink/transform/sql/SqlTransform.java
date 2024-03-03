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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.AbstractDataType;

/**
 * A {@link PTransform} that wraps Flink SQL statements. This is the entrypoint class to create a
 * Flink SQL PTransform. Users can create a {@link SingleOutputSqlTransformWithInput} by calling
 * {@link #of(Class, Class) of(Class<InputT> inputClass, Class<OutputT> outputClass)}.
 *
 * <p>See {@link SingleOutputSqlTransformWithInput} for more details regarding how to specify the
 * input table, output table and SQL logic.
 */
public class SqlTransform<InputT, OutputT> implements Serializable {

  public static final String FLINK_SQL_URN = "beam:transform:flink:sql-statements:v1";
  static final String UNUSED_MAIN_INPUT_TABLE_NAME = "UnusedMainInputTable";
  static final String DEFAULT_MAIN_OUTPUT_TABLE_NAME = "UnnamedMainOutputTable";
  // A mapping from table name to the corresponding query. A LinkedHashMap is needed
  // so a later query can refer to the result of a previous query.
  private final LinkedHashMap<String, String> queries;
  private final List<String> ddlList;
  private final Map<String, Class<? extends UserDefinedFunction>> functionClasses;
  private final Map<String, UserDefinedFunction> functionInstances;
  private final Map<TupleTag<?>, PValue> additionalInputs;
  private final List<TupleTag<?>> additionalOutputs;
  private final Map<String, TableInfo.InputTableInfo<?>> inputTableInfo;
  private final Map<String, TableInfo.OutputTableInfo<?>> outputTableInfo;
  private final Map<String, SerializableCatalog> catalogs;
  private String mainInputTableName;
  private String mainOutputTableName;

  SqlTransform(Class<InputT> inputClass, Class<OutputT> outputClass) {
    this.queries = new LinkedHashMap<>();
    this.ddlList = new ArrayList<>();
    this.additionalInputs = new HashMap<>();
    this.additionalOutputs = new ArrayList<>();
    this.inputTableInfo = new HashMap<>();
    this.outputTableInfo = new HashMap<>();
    this.catalogs = new HashMap<>();
    this.functionClasses = new HashMap<>();
    this.functionInstances = new HashMap<>();
    this.mainInputTableName = UNUSED_MAIN_INPUT_TABLE_NAME;
    this.mainOutputTableName = DEFAULT_MAIN_OUTPUT_TABLE_NAME;
    inputTableInfo.put(mainInputTableName, TableInfo.InputTableInfo.of(inputClass));
    outputTableInfo.put(mainOutputTableName, TableInfo.OutputTableInfo.of(outputClass));
  }

  /**
   * Create a {@link SingleOutputSqlTransformWithInput} with the specified input and output class.
   * By default, the created SQL Transform assumes both the input and output classes are of the <a
   * href="https://en.wikipedia.org/wiki/Plain_old_Java_object">POJO type</a>. So, the SQL transform
   * can convert a record to a Flink Table Row and vice versa.
   *
   * <p>Users can also specify the conversion between a record of Java object and a Flink Table Row
   * by specifying a {@link TypeInformation} and {@link AbstractDataType DataType} if needed.
   *
   * @param inputClass the class of the input records for the Sql transform.
   * @param outputClass the class of the output records for the sql transform.
   * @return A {@link SingleOutputSqlTransformWithInput}.
   */
  public static <InputT, OutputT> SingleOutputSqlTransformWithInput<InputT, OutputT> of(
      Class<InputT> inputClass, Class<OutputT> outputClass) {
    return new SingleOutputSqlTransformWithInput<>(new SqlTransform<>(inputClass, outputClass));
  }

  /**
   * Create a {@link SingleOutputSqlTransform} with the specified output class. By default, the
   * created SQL Transform assumes both the output classes are of the <a
   * href="https://en.wikipedia.org/wiki/Plain_old_Java_object">POJO type</a>. So, the SQL transform
   * can convert a Flink Table Row to a Java object.
   *
   * <p>Users can also specify the Flink Table Row to Java object conversion by specifying a {@link
   * TypeInformation} and {@link AbstractDataType DataType} if needed.
   *
   * @param outputClass the class of the output records for the sql transform.
   * @return A {@link SingleOutputSqlTransform}.
   */
  public static <T> SingleOutputSqlTransform<T> of(Class<T> outputClass) {
    return new SingleOutputSqlTransform<>(of(Integer.class, outputClass));
  }

  /**
   * Create a {@link StatementOnlySqlTransform} which takes a full script of SQL statements and
   * execute them. The statements must have at least one <code>INSERT</code> statement.
   *
   * @return A {@link StatementOnlySqlTransform}.
   */
  public static StatementOnlySqlTransform ofStatements() {
    return new StatementOnlySqlTransform();
  }

  // --------------------- setters ----------------------------
  /**
   * Use DDL to define Tables. The DDL string can contain multiple {@code CREATE TABLE} / {@code
   * CREATE VIEW} statements. The DDL string should not contain any DQL / DML statement.
   *
   * @param ddl the table definition
   * @return this {@link SqlTransform} itself.
   */
  SqlTransform<InputT, OutputT> withDDL(String ddl) {
    ddlList.add(ddl);
    return this;
  }

  /**
   * Add a {@link Catalog} to be used by the SQL transform.
   *
   * @param name the name of the catalog.
   * @param catalog the catalog instance.
   * @return this {@link SqlTransform} itself.
   */
  SqlTransform<InputT, OutputT> withCatalog(String name, SerializableCatalog catalog) {
    catalogs.put(name, catalog);
    return this;
  }

  /**
   * Add a new user defined function for the SQL transform.
   *
   * @param name the function name.
   * @param functionClass the class of the user defined function.
   * @return this {@link SqlTransform} itself.
   */
  SqlTransform<InputT, OutputT> withFunction(
      String name, Class<? extends UserDefinedFunction> functionClass) {
    functionClasses.put(name, functionClass);
    return this;
  }

  /**
   * Add a new user defined function for the SQL transform.
   *
   * @param name the function name.
   * @param functionInstance the instance of the user defined function.
   * @return this {@link SqlTransform} itself.
   */
  SqlTransform<InputT, OutputT> withFunction(String name, UserDefinedFunction functionInstance) {
    functionInstances.put(name, functionInstance);
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
  SqlTransform<InputT, OutputT> withQuery(String resultTableName, String query) {
    if (queries.containsKey(resultTableName)) {
      throw new IllegalArgumentException(
          String.format("The result table %s has already been defined.", resultTableName));
    }
    queries.put(resultTableName, query);
    return this;
  }

  /**
   * Sets the name of the table created from the input {@link PCollection} to this Sql Transform. By
   * Default the main input is not used because the tables can be defined from the DDLs.
   *
   * @param name the name of the table created from the input {@link PCollection}.
   */
  SqlTransform<InputT, OutputT> withMainInputTableName(String name) {
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
  SqlTransform<InputT, OutputT> withMainInputTableTypeInformation(
      TypeInformation<InputT> typeInfo) {
    getMainInputTableInfo().setTypeInfo(typeInfo);
    return this;
  }

  /**
   * Specify the table to be set as the main output of this Sql Transform.
   *
   * @param name the name of the table to be set as the main output of this Sql transform.
   */
  SqlTransform<InputT, OutputT> withMainOutputTableName(String name) {
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
  SqlTransform<InputT, OutputT> withMainOutputTableTypeInformation(
      TypeInformation<OutputT> typeInfo) {
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
  SqlTransform<InputT, OutputT> withMainOutputTableDataType(AbstractDataType<?> dataType) {
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
  SqlTransform<InputT, OutputT> withAdditionalInputTable(
      TupleTag<?> tag, PCollection<?> pCollection, TableInfo.InputTableInfo<?> tableInfo) {
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
  SqlTransform<InputT, OutputT> withAdditionalOutputTable(
      TupleTag<?> tag, TableInfo.OutputTableInfo<?> tableInfo) {
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

  String getMainOutputTableName() {
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

  Map<TupleTag<?>, PValue> getAdditionalInputs() {
    return additionalInputs;
  }

  TableInfo.InputTableInfo<?> getAdditionalInputTableInfo(String name) {
    validateInputTableName(name);
    return Preconditions.checkStateNotNull(inputTableInfo.get(name));
  }

  TupleTagList getAdditionalOutputs() {
    return TupleTagList.of(additionalOutputs);
  }

  TableInfo.OutputTableInfo<?> getAdditionalOutputTableInfo(String name) {
    validateOutputTableName(name);
    return Preconditions.checkStateNotNull(outputTableInfo.get(name));
  }

  Map<String, SerializableCatalog> getCatalogs() {
    return catalogs;
  }

  public Map<String, Class<? extends UserDefinedFunction>> getFunctionClasses() {
    return functionClasses;
  }

  public Map<String, UserDefinedFunction> getFunctionInstances() {
    return functionInstances;
  }

  boolean hasSpecifiedMainOutputTableName() {
    return !DEFAULT_MAIN_OUTPUT_TABLE_NAME.equals(mainOutputTableName);
  }

  boolean isMainInputUsed() {
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
}
