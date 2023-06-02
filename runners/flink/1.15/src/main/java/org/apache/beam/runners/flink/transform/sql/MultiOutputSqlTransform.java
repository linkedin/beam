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

import org.apache.beam.sdk.coders.TextualIntegerCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.AbstractDataType;

/**
 * A {@link PTransform} that supports Flink SQL as the DSL to create Flink tables, perform
 * transformations and emit the result as {@link PCollection PCollections}. The {@link
 * MultiOutputSqlTransform} differs from {@link SingleOutputSqlTransform} that it supports multiple
 * output {@link PCollection PCollections}. Also, if users want to apply a SQL Transform to existing
 * {@link PCollection PCollections}, either {@link SingleOutputSqlTransformWithInput} or {@link
 * MultiOutputSqlTransformWithInput} is the way to go.
 *
 * <p>NOTE: <b>This {@link PTransform} only works with Flink Runner in batch mode.</b>
 *
 * <p>
 *
 * <p>
 *
 * <h1>Specify the input tables</h1>
 *
 * <p>A {@link MultiOutputSqlTransform} has following two types of input tables.
 *
 * <ul>
 *   <li><b>Tables from Flink {@link Catalog}</b> - Users can provide a Flink {@link Catalog} and
 *       use the Tables defined there.
 *   <li><b>Tables defined by Flink DDL</b> - Users may also define zero or more input tables using
 *       Flink SQL DDL statements by calling {@link #withDDL(String)}.
 * </ul>
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
 *     MultiOutputSqlTransform&lt;CountryAndSales> transform =
 *         FlinkSql.of(CountryAndSales.class)
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
 *     PCollectionTuple outputs = pipeline.apply("MySqlTransform", transform);
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
 * @param <T> the type of the output records of this Sql transform.
 * @see SingleOutputSqlTransformWithInput
 */
public class MultiOutputSqlTransform<T> extends PTransform<PBegin, PCollectionTuple> {

  private final MultiOutputSqlTransformWithInput<Integer, T> transform;

  MultiOutputSqlTransform(MultiOutputSqlTransformWithInput<Integer, T> transform) {
    this.transform = transform;
  }

  @Override
  public PCollectionTuple expand(PBegin input) {
    return input.apply(Create.empty(TextualIntegerCoder.of())).apply(transform);
  }

  /**
   * Use DDL to define Tables. The DDL string can contain multiple {@code CREATE TABLE} / {@code
   * CREATE VIEW} statements. The DDL string should not contain any DQL / DML statement.
   *
   * @param ddl the table definition
   * @return this {@link MultiOutputSqlTransformWithInput} itself.
   */
  public MultiOutputSqlTransform<T> withDDL(String ddl) {
    transform.withDDL(ddl);
    return this;
  }

  /**
   * Define add a new {@link Catalog} to be used by the SQL query.
   *
   * @param name the name of the catalog.
   * @param catalog the catalog to use.
   * @return this {@link MultiOutputSqlTransformWithInput} itself.
   */
  public MultiOutputSqlTransform<T> withCatalog(String name, SerializableCatalog catalog) {
    transform.withCatalog(name, catalog);
    return this;
  }

  /**
   * Register a temporary user defined function for this SQL transform. The function will be
   * registered as a <i>System Function</i> which means it will temporarily override other functions
   * with the same name, if such function exists.
   *
   * @param name the name of the function.
   * @param functionClass the class of the user defined function.
   * @return this {@link MultiOutputSqlTransform} itself.
   */
  public MultiOutputSqlTransform<T> withFunction(
      String name, Class<? extends UserDefinedFunction> functionClass) {
    transform.withFunction(name, functionClass);
    return this;
  }

  /**
   * Register a temporary user defined function for this SQL transform. The function will be
   * registered as a <i>System Function</i> which means it will temporarily override other functions
   * with the same name, if such function exists.
   *
   * @param name the name of the function.
   * @param functionInstance the user defined function instance.
   * @return this {@link MultiOutputSqlTransform} itself.
   */
  public MultiOutputSqlTransform<T> withFunction(
      String name, UserDefinedFunction functionInstance) {
    transform.withFunction(name, functionInstance);
    return this;
  }

  /**
   * Use DQL to express the query logic. The query should only contain one DQL, i.e. one top level
   * {@code SELECT} statement. The query result will be registered as a temporary view. The query
   * statements comes after this query can refer to the result of this query with the specified
   * result table name.
   *
   * <p>If only one queries is specified for this {@link MultiOutputSqlTransformWithInput}, the
   * output table of that query will be used as the main output of this Sql transform by default. If
   * more than one queries are specified, users will need to specify the main output via {@link
   * #withMainOutputTable(String)}.
   *
   * @param resultTableName the table name of the query result.
   * @param query the SQL DQL statement.
   * @return this {@link MultiOutputSqlTransformWithInput} itself.
   */
  public MultiOutputSqlTransform<T> withQuery(String resultTableName, String query) {
    transform.withQuery(resultTableName, query);
    return this;
  }

  /**
   * Use the table with the given name as the main output table. The Flink runner will assume the
   * <tt>OutputT</tt> is a POJO class and the specified table schema matches it.
   *
   * <p>See the {@link MultiOutputSqlTransformWithInput} class Java doc for more details about the
   * main output table.
   *
   * @param name the name of the table to be used as main output table.
   * @return this {@link MultiOutputSqlTransformWithInput}.
   * @see #withMainOutputTable(String, AbstractDataType)
   * @see #withMainOutputTable(String, TypeInformation, AbstractDataType)
   * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream(Table)
   */
  public MultiOutputSqlTransform<T> withMainOutputTable(String name) {
    transform.withMainOutputTable(name);
    return this;
  }

  /**
   * Use the table with the given name as the main output table. The Flink runner will assume the
   * <tt>OutputT</tt> is a POJO class and use the given {@link AbstractDataType DataType} to convert
   * the main output Table to a {@link PCollection PCollection&lt;OutputT&gt;}.
   *
   * <p>See the {@link MultiOutputSqlTransformWithInput} class Java doc for more details about the
   * main output table.
   *
   * @param name the name of the table to be used as main output table.
   * @param dataType the {@link AbstractDataType} used to convert the main output table to a {@link
   *     PCollection PCollection&lt;OutputT&gt;}.
   * @return this {@link MultiOutputSqlTransformWithInput}.
   * @see #withMainOutputTable(String)
   * @see #withMainOutputTable(String, TypeInformation, AbstractDataType)
   * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream(Table,
   *     AbstractDataType)
   */
  public MultiOutputSqlTransform<T> withMainOutputTable(String name, AbstractDataType<?> dataType) {
    transform.withMainOutputTable(name, dataType);
    return this;
  }

  /**
   * Use the table with the given name as the main output table. The Flink runner will use the given
   * {@link TypeInformation} and {@link AbstractDataType DataType} to convert the main output Table
   * to a {@link PCollection PCollection&lt;OutputT&gt;}.
   *
   * <p>See the {@link MultiOutputSqlTransformWithInput} class Java doc for more details about the
   * main output table.
   *
   * @param name the name of the table to be used as main output table.
   * @param typeInfo the {@link TypeInformation} of the specified main output table.
   * @param dataType the {@link AbstractDataType} used to convert the main output table to a {@link
   *     PCollection PCollection&lt;OutputT&gt;}.
   * @return this {@link MultiOutputSqlTransformWithInput}.
   * @see #withMainOutputTable(String)
   * @see #withMainOutputTable(String, AbstractDataType)
   * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream(Table,
   *     AbstractDataType)
   */
  public MultiOutputSqlTransform<T> withMainOutputTable(
      String name, TypeInformation<T> typeInfo, AbstractDataType<?> dataType) {
    transform.withMainOutputTable(name, typeInfo, dataType);
    return this;
  }

  /**
   * Specify the information for an additional output table of this Sql Transform.
   *
   * @param tag the {@link TupleTag} of the additional output. The tag id must be one of the tables
   *     defined by {@link #withQuery(String, String)}.
   * @return this {@link MultiOutputSqlTransformWithInput}
   * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream(Table)
   */
  public MultiOutputSqlTransform<T> withAdditionalOutputTable(TupleTag<?> tag) {
    transform.withAdditionalOutputTable(tag);
    return this;
  }

  /**
   * Specify the information for an additional output table of this Sql Transform.
   *
   * @param tag the {@link TupleTag} of the additional output. The tag id must be one of the tables
   *     defined by {@link #withQuery(String, String)}.
   * @param dataType the {@link AbstractDataType DataType} of the additional output table.
   * @return This {@link MultiOutputSqlTransformWithInput}
   * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream(Table,
   *     AbstractDataType)
   */
  public MultiOutputSqlTransform<T> withAdditionalOutputTable(
      TupleTag<?> tag, AbstractDataType<?> dataType) {
    transform.withAdditionalOutputTable(tag, dataType);
    return this;
  }

  /**
   * Specify the information for an additional output table of this Sql Transform.
   *
   * @param tag the {@link TupleTag} of the additional output. The tag id must be one of the tables
   *     defined by {@link #withQuery(String, String)}.
   * @param typeInfo the {@link TypeInformation} of the records in the additional output.
   * @param dataType the {@link AbstractDataType DataType} of the additional output table.
   * @return this {@link MultiOutputSqlTransformWithInput}
   * @see org.apache.flink.table.api.bridge.java.StreamTableEnvironment#toDataStream(Table,
   *     AbstractDataType)
   */
  public MultiOutputSqlTransform<T> withAdditionalOutputTable(
      TupleTag<?> tag, AbstractDataType<?> dataType, TypeInformation<?> typeInfo) {
    transform.withAdditionalOutputTable(tag, typeInfo, dataType);
    return this;
  }
}
