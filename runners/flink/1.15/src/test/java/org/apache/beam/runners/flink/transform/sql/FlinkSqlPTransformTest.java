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

import static org.apache.beam.runners.flink.transform.sql.FlinkSqlTestUtils.NON_POJO_PRODUCT;
import static org.apache.beam.runners.flink.transform.sql.FlinkSqlTestUtils.ORDER;
import static org.apache.beam.runners.flink.transform.sql.FlinkSqlTestUtils.ORDERS_DDL;
import static org.apache.beam.runners.flink.transform.sql.FlinkSqlTestUtils.PRODUCTS_DDL;
import static org.apache.beam.runners.flink.transform.sql.FlinkSqlTestUtils.getSingletonOrderPCollection;
import static org.apache.beam.runners.flink.transform.sql.FlinkSqlTestUtils.getSingletonPCollection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.TextualIntegerCoder;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MapperFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MappingIterator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.table.api.DataTypes;
import org.junit.Test;

/** The unit tests for the Flink SQL PTransform. */
public class FlinkSqlPTransformTest {

  @Test
  public void testTableDefinedViaDdl() throws IOException {
    Pipeline pipeline = Pipeline.create();
    SingleOutputSqlTransform<Integer, FlinkSqlTestUtils.Order> transform =
        FlinkSql.of(Integer.class, FlinkSqlTestUtils.Order.class)
            .withDDL(ORDERS_DDL)
            .withQuery(
                "OrdersForOutput",
                "SELECT orderNumber, product, amount, price, buyer, orderTime FROM Orders");
    PCollection<FlinkSqlTestUtils.Order> output =
        pipeline.apply(Create.empty(TextualIntegerCoder.of())).apply(transform);

    verifyRecords(output, "Orders", FlinkSqlTestUtils.Order.class);

    pipeline.run(getPipelineOptions());
  }

  @Test
  public void testTableFromPCollectionInput() {
    Pipeline pipeline = Pipeline.create();
    SingleOutputSqlTransform<FlinkSqlTestUtils.Order, FlinkSqlTestUtils.Order> transform =
        FlinkSql.of(FlinkSqlTestUtils.Order.class, FlinkSqlTestUtils.Order.class)
            .withMainInputTable("OrdersFromInput")
            .withQuery(
                "PrintOrders",
                "SELECT orderNumber, product, amount, price, buyer, orderTime FROM OrdersFromInput");

    PCollection<FlinkSqlTestUtils.Order> output =
        getSingletonOrderPCollection("OrdersFromInput", pipeline).apply(transform);

    output.apply(
        "PrintToConsole",
        MapElements.into(TypeDescriptors.nulls())
            .via(
                order -> {
                  assertEquals(order, ORDER);
                  return null;
                }));

    pipeline.run(getPipelineOptions());
  }

  @Test
  public void testAdditionalInput() {
    Pipeline pipeline = Pipeline.create();
    PCollection<FlinkSqlTestUtils.Order> sideInput =
        getSingletonOrderPCollection("SideInput", pipeline);
    SingleOutputSqlTransform<Integer, FlinkSqlTestUtils.Order> transform =
        FlinkSql.of(Integer.class, FlinkSqlTestUtils.Order.class)
            .withAdditionalInputTable(
                new TupleTag<FlinkSqlTestUtils.Order>("OrdersFromSideInput") {}, sideInput)
            .withQuery(
                "OrdersFromSidInput",
                "SELECT orderNumber, product, amount, price, buyer, orderTime FROM OrdersFromSideInput");

    PCollection<FlinkSqlTestUtils.Order> output =
        pipeline.apply(Create.empty(TextualIntegerCoder.of())).apply(transform);

    output.apply(
        "PrintToConsole",
        MapElements.into(TypeDescriptors.nulls())
            .via(
                order -> {
                  assertEquals(order, ORDER);
                  return null;
                }));

    pipeline.run(getPipelineOptions());
  }

  @Test
  public void testSingleOutputSqlPTransform() throws IOException {
    Pipeline pipeline = Pipeline.create();
    SingleOutputSqlTransform<Integer, FlinkSqlTestUtils.CountryAndSales> transform =
        FlinkSql.of(Integer.class, FlinkSqlTestUtils.CountryAndSales.class)
            .withDDL(ORDERS_DDL)
            .withDDL(PRODUCTS_DDL)
            .withQuery(
                "SalesByCountry",
                "SELECT country, SUM(sales) FROM (\n"
                    + "    SELECT Products.country, Orders.price * Orders.amount AS sales\n"
                    + "    FROM Orders, Products\n"
                    + "    WHERE Orders.product = Products.name)\n"
                    + "GROUP BY country");

    PCollection<FlinkSqlTestUtils.CountryAndSales> output =
        pipeline.apply(Create.empty(TextualIntegerCoder.of())).apply(transform);

    verifyRecords(output, "SalesByCountry", FlinkSqlTestUtils.CountryAndSales.class);

    pipeline.run(getPipelineOptions());
  }

  @Test
  public void testMultiOutputSqlPTransform() throws IOException {
    Pipeline pipeline = Pipeline.create();
    MultiOutputSqlTransform<Integer, FlinkSqlTestUtils.CountryAndSales> transform =
        FlinkSql.of(Integer.class, FlinkSqlTestUtils.CountryAndSales.class)
            .withDDL(ORDERS_DDL)
            .withDDL(PRODUCTS_DDL)
            .withQuery(
                "SalesByCountry",
                "SELECT country, SUM(sales) FROM (\n"
                    + "    SELECT Products.country, Orders.price * Orders.amount AS sales\n"
                    + "    FROM Orders, Products\n"
                    + "    WHERE Orders.product = Products.name)\n"
                    + "GROUP BY country")
            .withQuery(
                "SalesByProduct",
                "SELECT product, SUM(price * amount) AS sales FROM Orders GROUP BY product")
            .withMainOutputTable("SalesByCountry")
            .withAdditionalOutputTable(
                new TupleTag<FlinkSqlTestUtils.ProductAndSales>("SalesByProduct") {});

    PCollectionTuple outputs =
        pipeline.apply(Create.empty(TextualIntegerCoder.of())).apply(transform);

    verifyRecords(
        outputs.get("SalesByCountry"), "SalesByCountry", FlinkSqlTestUtils.CountryAndSales.class);

    verifyRecords(
        outputs.get("SalesByProduct"), "SalesByProduct", FlinkSqlTestUtils.ProductAndSales.class);

    pipeline.run(getPipelineOptions());
  }

  @Test
  public void testTableFromQueryUsedBySubsequentQueries() throws IOException {
    Pipeline pipeline = Pipeline.create();
    SingleOutputSqlTransform<Integer, FlinkSqlTestUtils.CountryAndSales> transform =
        FlinkSql.of(Integer.class, FlinkSqlTestUtils.CountryAndSales.class)
            .withDDL(ORDERS_DDL)
            .withDDL(PRODUCTS_DDL)
            .withQuery(
                "SalesByCountry",
                "SELECT country, SUM(sales) as sales FROM (\n"
                    + "    SELECT Products.country, Orders.price * Orders.amount AS sales\n"
                    + "    FROM Orders, Products\n"
                    + "    WHERE Orders.product = Products.name)\n"
                    + "GROUP BY country")
            .withQuery(
                "TopSalesCountries",
                "SELECT country, sales "
                    + "FROM SalesByCountry "
                    + "WHERE sales > "
                    + "  (SELECT AVG(sales) FROM SalesByCountry)"
                    + "ORDER BY sales")
            .withMainOutputTable("TopSalesCountries");

    PCollection<FlinkSqlTestUtils.CountryAndSales> output =
        pipeline.apply(Create.empty(TextualIntegerCoder.of())).apply(transform);

    verifyRecords(output, "TopSalesCountries", FlinkSqlTestUtils.CountryAndSales.class);

    pipeline.run(getPipelineOptions());
  }

  @Test
  public void testSqlWithNonPojoClassType() {
    Pipeline pipeline = Pipeline.create();
    TypeInformation<FlinkSqlTestUtils.NonPojoProduct> typeInfo =
        FlinkSqlTestUtils.NonPojoProduct.getTypeInfo();

    SingleOutputSqlTransform<FlinkSqlTestUtils.NonPojoProduct, FlinkSqlTestUtils.NonPojoProduct>
        transform =
            FlinkSql.of(
                    FlinkSqlTestUtils.NonPojoProduct.class, FlinkSqlTestUtils.NonPojoProduct.class)
                .withMainInputTable("NonPojoProductTable", typeInfo)
                .withQuery("NonPojoProduct", "SELECT * FROM NonPojoProductTable")
                .withMainOutputTable("NonPojoProduct", DataTypes.RAW(typeInfo));

    PCollection<FlinkSqlTestUtils.NonPojoProduct> output =
        getSingletonPCollection("NonPojoProductFromInput", pipeline, NON_POJO_PRODUCT, typeInfo)
            .apply(transform);

    output.apply(
        MapElements.into(TypeDescriptors.nulls())
            .via(
                r -> {
                  assertEquals(NON_POJO_PRODUCT, r);
                  return null;
                }));

    pipeline.run(getPipelineOptions());
  }

  @Test(expected = IllegalStateException.class)
  public void testEmptyQueries() {
    Pipeline pipeline = Pipeline.create();
    SingleOutputSqlTransform<Integer, FlinkSqlTestUtils.Order> transform =
        FlinkSql.of(Integer.class, FlinkSqlTestUtils.Order.class).withDDL(ORDERS_DDL);

    pipeline
        .apply(Create.empty(TextualIntegerCoder.of()))
        .apply(transform)
        .apply(
            MapElements.into(TypeDescriptors.nulls())
                .via(
                    r -> {
                      fail();
                      return null;
                    }));

    pipeline.run(getPipelineOptions());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSpecifyNonExistingOutputTable() {
    Pipeline pipeline = Pipeline.create();
    SingleOutputSqlTransform<Integer, FlinkSqlTestUtils.Order> transform =
        FlinkSql.of(Integer.class, FlinkSqlTestUtils.Order.class)
            .withDDL(ORDERS_DDL)
            .withQuery(
                "OrdersForOutput",
                "SELECT orderNumber, product, amount, price, buyer, orderTime FROM Orders")
            .withMainOutputTable("SomeNonExistingTableName");

    pipeline
        .apply(Create.empty(TextualIntegerCoder.of()))
        .apply(transform)
        .apply(
            MapElements.into(TypeDescriptors.nulls())
                .via(
                    r -> {
                      fail();
                      return null;
                    }));

    pipeline.run(getPipelineOptions());
  }

  @Test(expected = IllegalStateException.class)
  public void testOnlySetAdditionalInputForMultiOutputSqlTransform() {
    Pipeline pipeline = Pipeline.create();
    MultiOutputSqlTransform<Integer, FlinkSqlTestUtils.CountryAndSales> transform =
        FlinkSql.of(Integer.class, FlinkSqlTestUtils.CountryAndSales.class)
            .withDDL(ORDERS_DDL)
            .withDDL(PRODUCTS_DDL)
            .withQuery("OrdersTable", "SELECT * FROM Orders")
            .withQuery("ProductsTable", "SELECT * FROM Products")
            .withAdditionalOutputTable(
                new TupleTag<FlinkSqlTestUtils.ProductAndSales>("ProductsTable") {});

    pipeline
        .apply(Create.empty(TextualIntegerCoder.of()))
        .apply(transform)
        .get("ProductsTable")
        .apply(
            MapElements.into(TypeDescriptors.nulls())
                .via(
                    r -> {
                      fail();
                      return null;
                    }));

    pipeline.run(getPipelineOptions());
  }

  @Test(expected = IllegalStateException.class)
  public void testApplySqlToStreamingJobThrowException() {
    Pipeline pipeline = Pipeline.create();
    SingleOutputSqlTransform<Integer, FlinkSqlTestUtils.Order> transform =
        FlinkSql.of(Integer.class, FlinkSqlTestUtils.Order.class)
            .withDDL(ORDERS_DDL)
            .withQuery(
                "OrdersForOutput",
                "SELECT orderNumber, product, amount, price, buyer, orderTime FROM Orders");
    pipeline.apply(Create.empty(TextualIntegerCoder.of())).apply(transform);

    FlinkPipelineOptions options = getPipelineOptions();
    options.setStreaming(true);
    pipeline.run(options);
  }

  // ---------------- private helper methods -----------------------

  private static FlinkPipelineOptions getPipelineOptions() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(FlinkRunner.class);
    options.setUseDataStreamForBatch(true);
    options.setParallelism(2);
    return options;
  }

  private static <T> void verifyRecords(PCollection<T> pCollection, String file, Class<T> clazz)
      throws IOException {
    RecordsVerifier<T> recordsVerifier = new RecordsVerifier<>(file, clazz);
    final int expectedNumRecords = recordsVerifier.expectedRecords.size();

    pCollection
        .apply(
            "PrintToConsole",
            MapElements.into(TypeDescriptors.integers())
                .via(
                    record -> {
                      recordsVerifier.verifyRecord(record);
                      return 1;
                    }))
        .apply(Count.globally())
        .apply(
            MapElements.into(TypeDescriptors.nulls())
                .via(
                    count -> {
                      assertEquals(expectedNumRecords, count.intValue());
                      return null;
                    }));
  }

  // -------------------------- private helper class ------------------------

  private static final class RecordsVerifier<T> implements Serializable {
    private final Set<T> expectedRecords;

    RecordsVerifier(String file, Class<T> clazz) throws IOException {
      this.expectedRecords = getExpectedRecords(file, clazz);
    }

    public void verifyRecord(T record) {
      assertTrue("The expected records does not contain " + record, expectedRecords.remove(record));
    }

    private static <T> Set<T> getExpectedRecords(String fileName, Class<T> clazz)
        throws IOException {
      File file =
          new File(
              FlinkSqlPTransformTest.class
                  .getClassLoader()
                  .getResource("tables/" + fileName)
                  .getFile());

      CsvMapper csvMapper = new CsvMapper();
      csvMapper.disable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);

      CsvSchema csvSchema = csvMapper.typedSchemaFor(clazz).withColumnSeparator(',').withComments();

      try (MappingIterator<T> complexUsersIter =
          csvMapper.readerWithTypedSchemaFor(clazz).with(csvSchema).readValues(file)) {
        return new HashSet<>(complexUsersIter.readAll());
      }
    }
  }
}
