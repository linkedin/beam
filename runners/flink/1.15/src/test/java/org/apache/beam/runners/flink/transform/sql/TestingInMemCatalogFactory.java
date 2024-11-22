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

import static org.apache.beam.runners.flink.transform.sql.FlinkSqlTestUtils.getOrdersCatalogTable;
import static org.apache.beam.runners.flink.transform.sql.FlinkSqlTestUtils.getOrdersVerifyCatalogTable;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.factories.CatalogFactory;

/** A testing {@link Catalog Catalog} for SQL PTransform Test. */
public class TestingInMemCatalogFactory implements CatalogFactory {
  public static final String DEFAULT_DATABASE_NAME = "TestDatabase";
  public static final String IDENTIFIER = "TestingInMemoryCatalogFactory";

  public static TestingInMemCatalog getCatalog(String name) {
    TestingInMemCatalog catalog = new TestingInMemCatalog(name, DEFAULT_DATABASE_NAME);
    try {
      catalog.createTable(new ObjectPath("TestDatabase", "Orders"), getOrdersCatalogTable(), true);
      catalog.createTable(
          new ObjectPath("TestDatabase", "OrdersVerify"),
          getOrdersVerifyCatalogTable("Orders"),
          true);
      catalog.createTable(
          new ObjectPath("TestDatabase", "OrdersVerifyWithModifiedBuyerNames"),
          getOrdersVerifyCatalogTable("OrdersWithConvertedBuyerNames"),
          true);
    } catch (TableAlreadyExistException | DatabaseNotExistException e) {
      throw new RuntimeException(e);
    }
    return catalog;
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Catalog createCatalog(Context context) {
    return getCatalog(context.getName());
  }

  public static class TestingInMemCatalog extends GenericInMemoryCatalog
      implements SerializableCatalog {
    public TestingInMemCatalog(String name, String defaultDatabase) {
      super(name, defaultDatabase);
    }
  }
}
