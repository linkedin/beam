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
import java.util.Map;
import java.util.StringJoiner;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.runners.flink.FlinkCustomTransformTranslatorRegistrar;
import org.apache.beam.runners.flink.FlinkStreamingPipelineTranslator;
import org.apache.beam.runners.flink.FlinkStreamingTranslationContext;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatementOnlyFlinkSqlTransformTranslator
    extends FlinkStreamingPipelineTranslator.StreamTransformTranslator<PTransform<PBegin, PDone>> {
  private static final Logger LOG =
      LoggerFactory.getLogger(StatementOnlyFlinkSqlTransformTranslator.class);
  public static final String FLINK_STATEMENT_ONLY_SQL_URN =
      "beam:transform:flink:sql-statements-only:v1";
  private static final String INSERT = "INSERT";

  @Override
  public void translateNode(
      PTransform<PBegin, PDone> transform, FlinkStreamingTranslationContext context) {
    StatementOnlySqlTransform sqlTransform = (StatementOnlySqlTransform) transform;

    StreamTableEnvironment tEnv = StreamTableEnvironment.create(context.getExecutionEnvironment());
    sqlTransform.getCatalogs().forEach(tEnv::registerCatalog);
    sqlTransform.getFunctionClasses().forEach(tEnv::createTemporarySystemFunction);
    sqlTransform.getFunctionInstances().forEach(tEnv::createTemporarySystemFunction);

    StringJoiner combinedStatements = new StringJoiner("\n\n");
    StreamStatementSet ss = tEnv.createStatementSet();
    for (String statement : sqlTransform.getStatements()) {
      combinedStatements.add(statement);
      try {
        if (isInsertStatement(statement)) {
          ss.addInsertSql(statement);
        } else {
          // Not an insert into statement. Treat it as a DDL.
          tEnv.executeSql(statement);
        }
      } catch (Exception e) {
        LOG.error("Encountered exception when executing statement: {}", statement);
        throw new RuntimeException(e);
      }
    }
    // Now attach everything to StreamExecutionEnv.
    ss.attachAsDataStream();
    LOG.info("Executing SQL statements:\n {}", combinedStatements);
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
              StatementOnlySqlTransform.class,
              PTransformTranslation.TransformPayloadTranslator.NotSerializable.forUrn(
                  FLINK_STATEMENT_ONLY_SQL_URN))
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
          .put(FLINK_STATEMENT_ONLY_SQL_URN, new StatementOnlyFlinkSqlTransformTranslator())
          .build();
    }
  }

  // ------------------- private helper methods -----------------
  private static boolean isInsertStatement(String statement) {
    return statement.substring(0, INSERT.length()).toUpperCase().startsWith(INSERT);
  }
}
