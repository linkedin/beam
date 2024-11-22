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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * A table factory class that verifies the output of {@link StatementOnlySqlTransform}. It reads
 * from a specified table file as the expected output, and then compare it with the records sent to
 * this Sink.
 */
public class VerifyingTableSinkFactory implements DynamicTableSinkFactory, Serializable {
  public static final String IDENTIFIER = "verifyingSink";
  public static final ConfigOption<String> EXPECTED_RESULT_FILE_PATH_OPTION =
      ConfigOptions.key("expected.result.file.path").stringType().noDefaultValue();
  public static final ConfigOption<Boolean> HAS_HEADER_OPTION =
      ConfigOptions.key("has.header").booleanType().defaultValue(true);

  @Override
  public DynamicTableSink createDynamicTableSink(Context tableSinkFactoryContext) {
    // We need to make sure the sink is serializable. So we need to get rid of the non-serializable
    // objects here and extract the information into serializable objects.
    Configuration config =
        Configuration.fromMap(tableSinkFactoryContext.getCatalogTable().getOptions());
    ResolvedSchema schema = tableSinkFactoryContext.getCatalogTable().getResolvedSchema();
    List<LogicalType> logicalTypes =
        schema.getColumns().stream()
            .map(c -> c.getDataType().getLogicalType())
            .collect(Collectors.toList());

    final String expectedResultFilePath = config.get(EXPECTED_RESULT_FILE_PATH_OPTION);
    final boolean hasHeader = config.get(HAS_HEADER_OPTION);
    final List<String> columnsToVerify = schema.getColumnNames();

    return new SerializableDynamicTableSink() {

      @Override
      public void applyOverwrite(boolean b) {
        // Do nothing.
      }

      @Override
      public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.all();
      }

      @Override
      public SinkRuntimeProvider getSinkRuntimeProvider(
          org.apache.flink.table.connector.sink.DynamicTableSink.Context tableSinkContext) {
        return new SerializableSinkV2Provider() {
          @Override
          public Sink<RowData> createSink() {
            return new SerializableSink<RowData>() {
              @Override
              public SinkWriter<RowData> createWriter(InitContext sinkInitContext)
                  throws IOException {
                return new VerifyingSinkWriter(
                    logicalTypes, expectedResultFilePath, hasHeader, columnsToVerify);
              }
            };
          }
        };
      }

      @Override
      public DynamicTableSink copy() {
        return this;
      }

      @Override
      public String asSummaryString() {
        return "VerifyingDynamicTableSink";
      }
    };
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.singleton(EXPECTED_RESULT_FILE_PATH_OPTION);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.singleton(HAS_HEADER_OPTION);
  }

  private static class VerifyingSinkWriter implements SinkWriter<RowData>, Serializable {

    private final List<LogicalType> logicalTypes;
    private final ExpectedResultTable expectedRows;
    private final int[] indexToVerify;
    private int rowNumber;

    private VerifyingSinkWriter(
        List<LogicalType> logicalTypes,
        String expectedRowsFile,
        boolean hasHeaderLine,
        List<String> columnsToVerify)
        throws FileNotFoundException {
      this.logicalTypes = logicalTypes;
      this.rowNumber = 0;
      this.expectedRows = readRowsFromFile(expectedRowsFile, hasHeaderLine);

      indexToVerify = new int[columnsToVerify.size()];
      for (int i = 0; i < columnsToVerify.size(); i++) {
        indexToVerify[i] =
            Preconditions.checkStateNotNull(
                expectedRows.headerNameToIndex.get(columnsToVerify.get(i)));
      }
    }

    @Override
    public void write(
        RowData element, org.apache.flink.api.connector.sink2.SinkWriter.Context sinkContext)
        throws IOException, InterruptedException {
      List<String> actual =
          Splitter.on(',').splitToList(element.toString().replace("+I(", "").replace(")", ""));
      for (int index : indexToVerify) {
        String expected = expectedRows.rows.get(rowNumber).get(index);
        if (!compareValue(logicalTypes.get(index), expected, element, index)) {
          throw new RuntimeException(
              String.format(
                  "The following Rows are unequal:\n expected: %s\n actual: %s\n",
                  expected, actual));
        }
      }
      rowNumber++;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
      // No op.
    }

    @Override
    public void close() throws Exception {
      if (rowNumber != expectedRows.rows.size()) {
        throw new RuntimeException(
            String.format(
                "Expect %d rows, but only received %d rows", expectedRows.rows.size(), rowNumber));
      }
    }
  }

  private static boolean compareValue(
      LogicalType logicalType, String expected, RowData row, int index) {
    if (logicalType instanceof TimestampType) {
      LocalDateTime actual = row.getTimestamp(index, 3).toLocalDateTime();
      return LocalDateTime.parse(expected.replace(" ", "T")).equals(actual);
    } else if (logicalType instanceof BigIntType) {
      long actual = row.getLong(index);
      return Long.parseLong(expected) == actual;
    } else if (logicalType instanceof IntType) {
      int actual = row.getInt(index);
      return Integer.parseInt(expected) == actual;
    } else if (logicalType instanceof DecimalType) {
      BigDecimal expectedBigDecimal = BigDecimal.valueOf(Double.parseDouble(expected));
      return row.getDecimal(index, 8, 2).toBigDecimal().compareTo(expectedBigDecimal) == 0;
    } else if (logicalType instanceof DoubleType) {
      return row.getDouble(index) == Double.parseDouble(expected);
    } else if (logicalType instanceof VarCharType) {
      return row.getString(index).toString().equals(expected);
    } else {
      throw new RuntimeException("Unrecognized logical type." + logicalType);
    }
  }

  private static ExpectedResultTable readRowsFromFile(String path, boolean hasHeaderLine)
      throws FileNotFoundException {
    ExpectedResultTable expected = new ExpectedResultTable();
    File file = new File(path);
    try (Scanner scanner = new Scanner(file, Charsets.UTF_8.name())) {
      boolean readHeaderLine = hasHeaderLine;
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        List<String> columns = Splitter.on(',').splitToList(line);
        if (readHeaderLine) {
          for (int i = 0; i < columns.size(); i++) {
            expected.headerNameToIndex.put(columns.get(i).replace("#", ""), i);
          }
          readHeaderLine = false;
        } else {
          expected.rows.add(columns);
        }
      }
      return expected;
    }
  }

  interface SerializableDynamicTableSink
      extends DynamicTableSink, SupportsOverwrite, Serializable {}

  interface SerializableSinkV2Provider extends SinkV2Provider, Serializable {}

  interface SerializableSink<T> extends Sink<T>, Serializable {}

  private static class ExpectedResultTable {
    Map<String, Integer> headerNameToIndex = new HashMap<>();
    List<List<String>> rows = new ArrayList<>();
  }
}
