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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PDone;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.util.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Beam PTransform that only take a complete SQL statements with INSERT INTO clause. */
public class StatementOnlySqlTransform extends PTransform<PBegin, PDone> {
  private static final Logger LOG = LoggerFactory.getLogger(StatementOnlySqlTransform.class);

  private final List<String> statements;
  private final Map<String, SerializableCatalog> catalogs;
  private final Map<String, UserDefinedFunction> functionInstances;
  private final Map<String, Class<? extends UserDefinedFunction>> functionClasses;

  StatementOnlySqlTransform() {
    this.statements = new ArrayList<>();
    this.catalogs = new HashMap<>();
    this.functionInstances = new HashMap<>();
    this.functionClasses = new HashMap<>();
  }

  @Override
  public PDone expand(PBegin input) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("User statements:");
      for (String statement : statements) {
        LOG.debug("{}\n", statement);
      }
    }
    return PDone.in(input.getPipeline());
  }

  @Override
  public void validate(@Nullable PipelineOptions options) {
    Preconditions.checkArgument(
        !statements.isEmpty(), "No statement is provided for the SqlPtransform..");
  }

  /**
   * Add any Flink SQL statement to this transform. Note that there must be a <code>INSERT INTO
   * </code> statement. Otherwise, an exception will be thrown.
   *
   * @param statement the statement to be added.
   * @return this {@link StatementOnlySqlTransform}.
   */
  public StatementOnlySqlTransform addStatement(String statement) {
    statements.add(cleanUp(statement));
    return this;
  }

  /**
   * Define add a new {@link Catalog} to be used by the SQL query.
   *
   * @param name the name of the catalog.
   * @param catalog the catalog to use.
   * @return this {@link MultiOutputSqlTransformWithInput} itself.
   */
  public StatementOnlySqlTransform withCatalog(String name, SerializableCatalog catalog) {
    catalogs.put(name, catalog);
    return this;
  }

  /**
   * Register a temporary user defined function for this SQL transform. The function will be
   * registered as a <i>System Function</i> which means it will temporarily override other functions
   * with the same name, if such function exists.
   *
   * @param name the name of the function.
   * @param functionClass the class of the user defined function.
   * @return this {@link StatementOnlySqlTransform} itself.
   */
  public StatementOnlySqlTransform withFunction(
      String name, Class<? extends UserDefinedFunction> functionClass) {
    functionClasses.put(name, functionClass);
    return this;
  }

  /**
   * Register a temporary user defined function for this SQL transform. The function will be
   * registered as a <i>System Function</i> which means it will temporarily override other functions
   * with the same name, if such function exists.
   *
   * @param name the name of the function.
   * @param functionInstance the user defined function instance.
   * @return this {@link StatementOnlySqlTransform} itself.
   */
  public StatementOnlySqlTransform withFunction(String name, UserDefinedFunction functionInstance) {
    functionInstances.put(name, functionInstance);
    return this;
  }

  // --------------------- package private getters -----------------
  List<String> getStatements() {
    return Collections.unmodifiableList(statements);
  }

  Map<String, SerializableCatalog> getCatalogs() {
    return Collections.unmodifiableMap(catalogs);
  }

  Map<String, UserDefinedFunction> getFunctionInstances() {
    return functionInstances;
  }

  Map<String, Class<? extends UserDefinedFunction>> getFunctionClasses() {
    return functionClasses;
  }

  // --------------------- private helpers ------------------------
  private static String cleanUp(String s) {
    return s.trim().endsWith(";") ? s : s + ";";
  }
}
