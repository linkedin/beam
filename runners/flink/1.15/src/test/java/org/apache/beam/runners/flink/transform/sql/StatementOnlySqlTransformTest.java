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

import static org.apache.beam.runners.flink.transform.sql.FlinkSqlTestUtils.ORDERS_VERIFYING_SINK_2_DDL;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.junit.Test;

/** Unit tests for {@link StatementOnlySqlTransform}. */
public class StatementOnlySqlTransformTest {
  @Test
  public void testBatch() {
    testBasics(false);
  }

  @Test
  public void testStreaming() {
    testBasics(true);
  }

  @Test
  public void testCreateCatalogViaDDL() {
    Pipeline pipeline = Pipeline.create();
    StatementOnlySqlTransform transform =
        SqlTransform.ofStatements()
            .addStatement(
                String.format(
                    "CREATE CATALOG MyCatalog with ( 'type' = '%s' )",
                    TestingInMemCatalogFactory.IDENTIFIER))
            .addStatement(
                "INSERT INTO MyCatalog.TestDatabase.OrdersVerify SELECT * FROM MyCatalog.TestDatabase.Orders;");

    pipeline.apply(transform);
    pipeline.run(getPipelineOptions());
  }

  @Test
  public void testDDLAndMultipleInsertStatements() {
    SerializableCatalog catalog = TestingInMemCatalogFactory.getCatalog("TestCatalog");

    Pipeline pipeline = Pipeline.create();
    StatementOnlySqlTransform transform =
        SqlTransform.ofStatements()
            .withCatalog("MyCatalog", catalog)
            .addStatement(ORDERS_VERIFYING_SINK_2_DDL)
            .addStatement(
                "CREATE TEMPORARY VIEW MyView AS SELECT * FROM MyCatalog.TestDatabase.Orders;")
            .addStatement("INSERT INTO MyCatalog.TestDatabase.OrdersVerify SELECT * FROM MyView;")
            .addStatement("INSERT INTO OrdersVerify2 SELECT * FROM MyView;");

    pipeline.apply(transform);
    pipeline.run(getPipelineOptions());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyStatements() {
    StatementOnlySqlTransform transform = SqlTransform.ofStatements();
    Pipeline pipeline = Pipeline.create();
    pipeline.apply(transform);
    pipeline.run(getPipelineOptions());
  }

  @Test
  public void testInsertOverwrite() {
    SerializableCatalog catalog = TestingInMemCatalogFactory.getCatalog("TestCatalog");

    Pipeline pipeline = Pipeline.create();
    StatementOnlySqlTransform transform =
        SqlTransform.ofStatements()
            .withCatalog("MyCatalog", catalog)
            .addStatement(
                "INSERT OVERWRITE MyCatalog.TestDatabase.OrdersVerify SELECT * FROM MyCatalog.TestDatabase.Orders;");

    pipeline.apply(transform);
    pipeline.run(getPipelineOptions());
  }

  @Test
  public void testWithFunction() {
    SerializableCatalog catalog = TestingInMemCatalogFactory.getCatalog("TestCatalog");
    Pipeline pipeline = Pipeline.create();

    StatementOnlySqlTransform transform = SqlTransform.ofStatements();
    transform
        .withCatalog("MyCatalog", catalog)
        .withFunction("udfViaClass", FlinkSqlTestUtils.ToUpperCaseAndReplaceString.class)
        .withFunction("udfViaInstance", new FlinkSqlTestUtils.ToUpperCaseAndReplaceString())
        .addStatement(
            "INSERT INTO MyCatalog.TestDatabase.OrdersVerifyWithModifiedBuyerNames "
                + "SELECT orderNumber, product, amount, price, udfViaClass(buyer), orderTime FROM MyCatalog.TestDatabase.Orders")
        .addStatement(
            "INSERT INTO MyCatalog.TestDatabase.OrdersVerifyWithModifiedBuyerNames "
                + "SELECT orderNumber, product, amount, price, udfViaInstance(buyer), orderTime FROM MyCatalog.TestDatabase.Orders");

    pipeline.apply(transform);
    pipeline.run(getPipelineOptions());
  }

  // ----------------
  private void testBasics(boolean isStreaming) {
    SerializableCatalog catalog = TestingInMemCatalogFactory.getCatalog("TestCatalog");

    Pipeline pipeline = Pipeline.create();
    StatementOnlySqlTransform transform =
        SqlTransform.ofStatements()
            .withCatalog("MyCatalog", catalog)
            .addStatement(
                "CREATE TEMPORARY VIEW MyView AS SELECT * FROM MyCatalog.TestDatabase.Orders;")
            .addStatement("INSERT INTO MyCatalog.TestDatabase.OrdersVerify SELECT * FROM MyView;");

    pipeline.apply(transform);
    FlinkPipelineOptions options = getPipelineOptions();
    options.setStreaming(isStreaming);
    pipeline.run(options);
  }

  private FlinkPipelineOptions getPipelineOptions() {
    FlinkPipelineOptions options = FlinkSqlTestUtils.getPipelineOptions();
    options.setParallelism(1);
    return options;
  }
}
