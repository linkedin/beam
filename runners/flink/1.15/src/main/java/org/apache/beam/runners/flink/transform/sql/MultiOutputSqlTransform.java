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

import static org.apache.beam.runners.flink.transform.sql.SqlTransformUtils.setCoderForOutput;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.types.AbstractDataType;

/**
 * A {@link PTransform} that supports Flink SQL as the DSL for transformation on the {@link
 * PCollection PCollections}. The {@link MultiOutputSqlTransform} differs from {@link
 * SingleOutputSqlTransform} that it supports multiple output {@link PCollection}s.
 *
 * <p>NOTE: <b>This {@link PTransform} only works with Flink Runner in batch mode.</b>
 *
 * <p>
 *
 * <p>
 *
 * <h1>Specify the input tables</h1>
 *
 * <p>A {@link MultiOutputSqlTransform} has following three types of input tables.
 *
 * <ul>
 *   <li><b>Main Input Table</b> - The main input table is converted from the {@link PCollection}
 *       this {@link MultiOutputSqlTransform} directly applied to. To refer the main input table,
 *       users need to specify a name of the main input table via {@link
 *       #withMainInputTable(String)}, so subsequent SQL queries can refer to the main input table
 *       by name. If the name of the main input table is not specified, the main input table will be
 *       treated as unused, and thus ignored by the Flink Runner. Hence, there might be zero or one
 *       main input table depending on whether user has specified the name of the main input table.
 *   <li><b>Additional Input Tables</b> - Users may define zero or more input tables in addition to
 *       the main input table via {@link #withAdditionalInputTable(TupleTag, PCollection)}. An
 *       additional input table is converted from the provided {@link PCollection} and the name of
 *       the table is the tag id.
 *   <li><b>Tables defined by Flink DDL</b> - Users may also define zero or more input tables using
 *       Flink SQL DDL statements by calling {@link #withDDL(String)}.
 * </ul>
 *
 * <p>In order to convert a {@link PCollection} into a {@link Table Table}, the Flink Runner needs
 * to know the {@link TypeInformation} of the records in the {@link PCollection}. If the records are
 * of POJO type, Flink Runner can infer the {@link TypeInformation} automatically. Otherwise, Flink
 * will treat each record as a {@link org.apache.flink.table.types.logical.RawType RawType} which
 * means the converted table only has one column. To facilitate with the Table conversion for non
 * POJO record types, users need to specify the {@link TypeInformation} of an input {@link
 * PCollection} via {@link #withMainInputTable(String, TypeInformation)} or {@link
 * #withAdditionalInputTable(TupleTag, PCollection, TypeInformation)}.
 *
 * <p>
 *
 * <p>
 *
 * <h1>Specify processing logic</h1>
 *
 * <p>With the tables defined, users can specify the processing logic using Flink SQL by calling
 * {@link #withQuery(String, String)}. Multiple queries can be added to this Sql Transform. Each
 * query will create a named table that can be used in subsequent queries or be set as an output
 * table. At least one query is required.
 *
 * <p>
 *
 * <p>
 *
 * <h1>Specify output tables</h1>
 *
 * <p>The {@link MultiOutputSqlTransform} has one main output and one or more additional output.
 * Each of the output tables must be a result table of the {@link #withQuery(String, String)} call.
 * Users should specify the main output table and additional output tables of this Sql Transform via
 * {@link #withMainOutputTable(String)} and {@link #withAdditionalOutputTable(TupleTag)}
 * respectively. All the output tables will be converted to Flink {@link DataStream DataStream} by
 * the Flink SQL Runner.
 *
 * <p>
 *
 * <h1>Example</h1>
 *
 * <pre>
 *     MultiOutputSqlTransform<Integer, CountryAndSales> transform =
 *         FlinkSql.of(Integer.class, CountryAndSales.class)
 *             .withDDL(ORDERS_DDL)
 *             .withDDL(PRODUCTS_DDL)
 *             .withQuery("SalesByCountry",
 *                 "SELECT country, SUM(sales) FROM (\n"
 *                     + "    SELECT Products.country, Orders.price * Orders.amount AS sales\n"
 *                     + "    FROM Orders, Products\n"
 *                     + "    WHERE Orders.product = Products.name)\n"
 *                     + "GROUP BY country")
 *             .withQuery("SalesByProduct",
 *                 "SELECT product, SUM(price * amount) AS sales FROM Orders GROUP BY product")
 *             .withMainOutputTable("SalesByCountry")
 *             .withAdditionalOutputTable(new TupleTag<ProductAndSales>("SalesByProduct") {});
 *
 *     PCollectionTuple outputs = pipeline
 *         .apply("DummyInput", Create.empty(TextualIntegerCoder.of()))
 *         .apply("MySqlTransform", transform);
 *
 *     // Print out the main output.
 *     outputs
 *         .get("SalesByCountry")
 *         .apply("PrintToConsole", MapElements.into(TypeDescriptors.nulls()).via(record -> {
 *           System.out.println();
 *           return null;
 *         }));
 *
 *     // Print out the additional output.
 *     outputs
 *         .get("SalesByProduct")
 *         .apply("PrintToConsole", MapElements.into(TypeDescriptors.nulls()).via(record -> {
 *           System.out.println();
 *           return null;
 *         }));
 *
 *     pipeline.run(options);
 * </pre>
 *
 * @param <InputT> the type of the input records to this Sql transform.
 * @param <OutputT> the type of the output records of this Sql transform.
 * @see SingleOutputSqlTransform
 */
public class MultiOutputSqlTransform<InputT, OutputT>
    extends PTransform<PCollection<InputT>, PCollectionTuple> {
  private final FlinkSql<InputT, OutputT> sql;

  MultiOutputSqlTransform(FlinkSql<InputT, OutputT> sql) {
    this.sql = sql;
  }

  @Override
  public PCollectionTuple expand(PCollection<InputT> input) {
    if (!sql.hasSpecifiedMainOutputTableName()) {
      throw new IllegalStateException(
          "The main output of the MultiOutputSqlTransform is not specified. "
              + "The MultiOutputSqlTransform must have a main output.");
    }

    CoderRegistry coderRegistry = input.getPipeline().getCoderRegistry();
    SchemaRegistry schemaRegistry = input.getPipeline().getSchemaRegistry();
    TupleTag<OutputT> mainOutputTableTag = getMainOutputTableTag();

    PCollectionTuple outputs =
        PCollectionTuple.ofPrimitiveOutputsInternal(
            input.getPipeline(),
            TupleTagList.of(mainOutputTableTag).and(getAdditionalOutputTables().getAll()),
            Collections.emptyMap(),
            input.getWindowingStrategy(),
            PCollection.IsBounded.BOUNDED);

    for (Map.Entry<TupleTag<?>, PCollection<?>> entry : outputs.getAll().entrySet()) {
      TupleTag<?> tag = entry.getKey();
      PCollection<?> out = entry.getValue();
      TypeInformation<?> typeInfo =
          tag.equals(mainOutputTableTag)
              ? sql.getMainOutputTableInfo().getTypeInfo()
              : sql.getAdditionalOutputTableInfo(tag.getId()).getTypeInfo();
      setCoderForOutput(schemaRegistry, coderRegistry, tag, out, typeInfo);
    }

    return outputs;
  }

  /**
   * Use DDL to define Tables. The DDL string can contain multiple {@code CREATE TABLE} / {@code
   * CREATE VIEW} statements. The DDL string should not contain any DQL / DML statement.
   *
   * @param ddl the table definition
   * @return this {@link MultiOutputSqlTransform} itself.
   */
  public MultiOutputSqlTransform<InputT, OutputT> withDDL(String ddl) {
    sql.withDDL(ddl);
    return this;
  }

  /**
   * Use DQL to express the query logic. The query should only contain one DQL, i.e. one top level
   * {@code SELECT} statement. The query result will be registered as a temporary view. The query
   * statements comes after this query can refer to the result of this query with the specified
   * result table name.
   *
   * <p>If only one queries is specified for this {@link MultiOutputSqlTransform}, the output table
   * of that query will be used as the main output of this Sql transform by default. If more than
   * one queries are specified, users will need to specify the main output via {@link
   * #withMainOutputTable(String)}.
   *
   * @param resultTableName the table name of the query result.
   * @param query the SQL DQL statement.
   * @return this {@link MultiOutputSqlTransform} itself.
   */
  public MultiOutputSqlTransform<InputT, OutputT> withQuery(String resultTableName, String query) {
    sql.withQuery(resultTableName, query);
    return this;
  }

  /**
   * Specify the main input table name. The main output table will assume that <tt>InputT</tt> is a
   * POJO class.
   *
   * <p>See the {@link MultiOutputSqlTransform} class Java doc for more details about the main input
   * table.
   *
   * @param name the name of the main input table.
   * @return this {@link MultiOutputSqlTransform}.
   * @see #withMainInputTable(String, TypeInformation)
   * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#createTemporaryView(String,
   *     DataStream)
   */
  public MultiOutputSqlTransform<InputT, OutputT> withMainInputTable(String name) {
    Class<InputT> clazz = sql.getMainInputTableInfo().getClazz();
    return withMainInputTable(name, TypeInformation.of(clazz));
  }

  /**
   * Specify the main input table name and {@link TypeInformation}. The given {@link
   * TypeInformation} will be used to convert the main input {@link PCollection} into a Flink {@link
   * Table Table}.
   *
   * <p>See the {@link MultiOutputSqlTransform} class Java doc for more details about the main input
   * table.
   *
   * @param name the name of the main input table.
   * @param typeInfo the {@link TypeInformation} of the main input {@link PCollection}.
   * @return this {@link MultiOutputSqlTransform}.
   * @see #withMainInputTable(String)
   * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#createTemporaryView(String,
   *     DataStream)
   */
  public MultiOutputSqlTransform<InputT, OutputT> withMainInputTable(
      String name, TypeInformation<InputT> typeInfo) {
    sql.withMainInputTableName(name);
    sql.withMainInputTableTypeInformation(typeInfo);
    return this;
  }

  /**
   * Use the table with the given name as the main output table. The Flink runner will assume the
   * <tt>OutputT</tt> is a POJO class and the specified table schema matches it.
   *
   * <p>See the {@link MultiOutputSqlTransform} class Java doc for more details about the main
   * output table.
   *
   * @param name the name of the table to be used as main output table.
   * @return this {@link MultiOutputSqlTransform}.
   * @see #withMainOutputTable(String, AbstractDataType)
   * @see #withMainOutputTable(String, TypeInformation, AbstractDataType)
   * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream(Table)
   */
  public MultiOutputSqlTransform<InputT, OutputT> withMainOutputTable(String name) {
    Class<OutputT> clazz = sql.getMainOutputTableInfo().getClazz();
    return withMainOutputTable(name, TypeInformation.of(clazz), DataTypes.of(clazz));
  }

  /**
   * Use the table with the given name as the main output table. The Flink runner will assume the
   * <tt>OutputT</tt> is a POJO class and use the given {@link AbstractDataType DataType} to convert
   * the main output Table to a {@link PCollection PCollection&lt;OutputT&gt;}.
   *
   * <p>See the {@link MultiOutputSqlTransform} class Java doc for more details about the main
   * output table.
   *
   * @param name the name of the table to be used as main output table.
   * @param dataType the {@link AbstractDataType} used to convert the main output table to a {@link
   *     PCollection PCollection&lt;OutputT&gt;}.
   * @return this {@link MultiOutputSqlTransform}.
   * @see #withMainOutputTable(String)
   * @see #withMainOutputTable(String, TypeInformation, AbstractDataType)
   * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream(Table,
   *     AbstractDataType)
   */
  public MultiOutputSqlTransform<InputT, OutputT> withMainOutputTable(
      String name, AbstractDataType<?> dataType) {
    Class<OutputT> clazz = sql.getMainOutputTableInfo().getClazz();
    return withMainOutputTable(name, TypeInformation.of(clazz), dataType);
  }

  /**
   * Use the table with the given name as the main output table. The Flink runner will use the given
   * {@link TypeInformation} and {@link AbstractDataType DataType} to convert the main output Table
   * to a {@link PCollection PCollection&lt;OutputT&gt;}.
   *
   * <p>See the {@link MultiOutputSqlTransform} class Java doc for more details about the main
   * output table.
   *
   * @param name the name of the table to be used as main output table.
   * @param typeInfo the {@link TypeInformation} of the specified main output table.
   * @param dataType the {@link AbstractDataType} used to convert the main output table to a {@link
   *     PCollection PCollection&lt;OutputT&gt;}.
   * @return this {@link MultiOutputSqlTransform}.
   * @see #withMainOutputTable(String)
   * @see #withMainOutputTable(String, AbstractDataType)
   * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream(Table,
   *     AbstractDataType)
   */
  public MultiOutputSqlTransform<InputT, OutputT> withMainOutputTable(
      String name, TypeInformation<OutputT> typeInfo, AbstractDataType<?> dataType) {
    sql.withMainOutputTableName(name);
    sql.withMainOutputTableTypeInformation(typeInfo);
    sql.withMainOutputTableDataType(dataType);
    return this;
  }

  /**
   * Add another {@link PCollection} as an additional input table to this Sql transform.
   *
   * @param tag the {@link TupleTag} of the additional input {@link Table}.
   * @param pCollection The {@link PCollection} as the additional input table.
   * @return this {@link MultiOutputSqlTransform}
   */
  public MultiOutputSqlTransform<InputT, OutputT> withAdditionalInputTable(
      TupleTag<?> tag, PCollection<?> pCollection) {
    Class<?> clazz = tag.getTypeDescriptor().getRawType();
    return withAdditionalInputTable(tag, pCollection, TypeInformation.of(clazz));
  }

  /**
   * Add another {@link PCollection} as an additional input table to this Sql transform.
   *
   * @param tag the {@link TupleTag} of the additional input {@link Table}.
   * @param pCollection The {@link PCollection} as the additional input table.
   * @param typeInfo the {@link TypeInformation} of the records in the given {@link PCollection}.
   * @return this {@link MultiOutputSqlTransform}
   * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#fromDataStream(DataStream)
   */
  public MultiOutputSqlTransform<InputT, OutputT> withAdditionalInputTable(
      TupleTag<?> tag, PCollection<?> pCollection, TypeInformation<?> typeInfo) {
    Class<?> clazz = tag.getTypeDescriptor().getRawType();
    sql.withAdditionalInputTable(
        tag, pCollection, new TableInfo.AdditionalInputTableInfo<>(clazz, tag, typeInfo));
    return this;
  }

  /**
   * Specify the information for an additional output table of this Sql Transform.
   *
   * @param tag the {@link TupleTag} of the additional output. The tag id must be one of the tables
   *     defined by {@link #withQuery(String, String)}.
   * @return this {@link MultiOutputSqlTransform}
   * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream(Table)
   */
  public MultiOutputSqlTransform<InputT, OutputT> withAdditionalOutputTable(TupleTag<?> tag) {
    Class<?> clazz = tag.getTypeDescriptor().getRawType();
    return withAdditionalOutputTable(tag, DataTypes.of(clazz));
  }

  /**
   * Specify the information for an additional output table of this Sql Transform.
   *
   * @param tag the {@link TupleTag} of the additional output. The tag id must be one of the tables
   *     defined by {@link #withQuery(String, String)}.
   * @param dataType the {@link AbstractDataType DataType} of the additional output table.
   * @return This {@link MultiOutputSqlTransform}
   * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream(Table,
   *     AbstractDataType)
   */
  public MultiOutputSqlTransform<InputT, OutputT> withAdditionalOutputTable(
      TupleTag<?> tag, AbstractDataType<?> dataType) {
    Class<?> clazz = tag.getTypeDescriptor().getRawType();
    return withAdditionalOutputTable(tag, dataType, TypeInformation.of(clazz));
  }

  /**
   * Specify the information for an additional output table of this Sql Transform.
   *
   * @param tag the {@link TupleTag} of the additional output. The tag id must be one of the tables
   *     defined by {@link #withQuery(String, String)}.
   * @param typeInfo the {@link TypeInformation} of the records in the additional output.
   * @param dataType the {@link AbstractDataType DataType} of the additional output table.
   * @return this {@link MultiOutputSqlTransform}
   * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream(Table,
   *     AbstractDataType)
   */
  public MultiOutputSqlTransform<InputT, OutputT> withAdditionalOutputTable(
      TupleTag<?> tag, AbstractDataType<?> dataType, TypeInformation<?> typeInfo) {
    Class<?> clazz = tag.getTypeDescriptor().getRawType();
    sql.withAdditionalOutputTable(
        tag, new TableInfo.AdditionalOutputTableInfo<>(clazz, tag, typeInfo, dataType));
    return this;
  }

  // --------------- package private getter methods ------------------
  // These getter methods are package private because they are only used by the Sql transform
  // translators.

  boolean isMainInputTableUsed() {
    return sql.isMainInputUsed();
  }

  String getMainInputTableName() {
    return sql.getMainInputTableName();
  }

  @SuppressWarnings("unchecked")
  TypeInformation<InputT> getMainInputTypeInformation() {
    return (TypeInformation<InputT>) sql.getMainInputTableInfo().getTypeInfo();
  }

  @Override
  public Map<TupleTag<?>, PValue> getAdditionalInputs() {
    return sql.getAdditionalInputs();
  }

  Class<?> getClassForAdditionalInput(String name) {
    return Preconditions.checkStateNotNull(sql.getAdditionalInputTableInfo(name).getClazz());
  }

  TypeInformation<?> getTypeInformationForAdditionalInput(String name) {
    return Preconditions.checkStateNotNull(sql.getAdditionalInputTableInfo(name).getTypeInfo());
  }

  TupleTag<OutputT> getMainOutputTableTag() {
    return new TupleTag<>(sql.getMainOutputTableName());
  }

  AbstractDataType<?> getMainOutputDataType() {
    return sql.getMainOutputTableInfo().getDataType();
  }

  TupleTagList getAdditionalOutputTables() {
    return sql.getAdditionalOutputs();
  }

  AbstractDataType<?> getAdditionalOutputDataType(TupleTag<?> tag) {
    return sql.getAdditionalOutputTableInfo(tag.getId()).getDataType();
  }

  Map<String, String> getQueries() {
    return Collections.unmodifiableMap(sql.getQueries());
  }

  List<String> getDdlList() {
    return Collections.unmodifiableList(sql.getDdlList());
  }
}
