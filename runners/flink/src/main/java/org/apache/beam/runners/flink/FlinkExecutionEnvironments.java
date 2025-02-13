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
package org.apache.beam.runners.flink;

import static org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.getDefaultLocalParallelism;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.expansion.ExternalConfigRegistrar;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.net.HostAndPort;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for Flink execution environments. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class FlinkExecutionEnvironments {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkExecutionEnvironments.class);

  /**
   * If the submitted job is a batch processing job, this method creates the adequate Flink {@link
   * org.apache.flink.api.java.ExecutionEnvironment} depending on the user-specified options.
   */
  public static ExecutionEnvironment createBatchExecutionEnvironment(FlinkPipelineOptions options) {
    return createBatchExecutionEnvironment(
        options,
        MoreObjects.firstNonNull(options.getFilesToStage(), Collections.emptyList()),
        options.getFlinkConfDir());
  }

  static ExecutionEnvironment createBatchExecutionEnvironment(
      FlinkPipelineOptions options, List<String> filesToStage, @Nullable String confDir) {

    LOG.info("Creating a Batch Execution Environment.");

    // Although Flink uses Rest, it expects the address not to contain a http scheme
    String flinkMasterHostPort = stripHttpSchema(options.getFlinkMaster());
    Configuration flinkConfiguration = getFlinkConfiguration(confDir, options);
    ExecutionEnvironment flinkBatchEnv;

    // depending on the master, create the right environment.
    if ("[local]".equals(flinkMasterHostPort)) {
      setManagedMemoryByFraction(flinkConfiguration);
      disableClassLoaderLeakCheck(flinkConfiguration);
      flinkBatchEnv = ExecutionEnvironment.createLocalEnvironment(flinkConfiguration);
    } else if ("[collection]".equals(flinkMasterHostPort)) {
      flinkBatchEnv = new CollectionEnvironment();
    } else if ("[auto]".equals(flinkMasterHostPort)) {
      flinkBatchEnv = ExecutionEnvironment.getExecutionEnvironment();
      if (flinkBatchEnv instanceof LocalEnvironment) {
        disableClassLoaderLeakCheck(flinkConfiguration);
        flinkBatchEnv = ExecutionEnvironment.createLocalEnvironment(flinkConfiguration);
        flinkBatchEnv.setParallelism(getDefaultLocalParallelism());
      }
    } else {
      int defaultPort = flinkConfiguration.getInteger(RestOptions.PORT);
      HostAndPort hostAndPort =
          HostAndPort.fromString(flinkMasterHostPort).withDefaultPort(defaultPort);
      flinkConfiguration.setInteger(RestOptions.PORT, hostAndPort.getPort());
      flinkBatchEnv =
          ExecutionEnvironment.createRemoteEnvironment(
              hostAndPort.getHost(),
              hostAndPort.getPort(),
              flinkConfiguration,
              filesToStage.toArray(new String[filesToStage.size()]));
      LOG.info("Using Flink Master URL {}:{}.", hostAndPort.getHost(), hostAndPort.getPort());
    }

    // Set the execution mode for data exchange.
    flinkBatchEnv
        .getConfig()
        .setExecutionMode(ExecutionMode.valueOf(options.getExecutionModeForBatch()));

    // set the correct parallelism.
    if (options.getParallelism() != -1 && !(flinkBatchEnv instanceof CollectionEnvironment)) {
      flinkBatchEnv.setParallelism(options.getParallelism());
    }
    // Set the correct parallelism, required by UnboundedSourceWrapper to generate consistent
    // splits.
    final int parallelism;
    if (flinkBatchEnv instanceof CollectionEnvironment) {
      parallelism = 1;
    } else {
      parallelism =
          determineParallelism(
              options.getParallelism(), flinkBatchEnv.getParallelism(), flinkConfiguration);
    }

    flinkBatchEnv.setParallelism(parallelism);
    // set parallelism in the options (required by some execution code)
    options.setParallelism(parallelism);

    if (options.getObjectReuse()) {
      flinkBatchEnv.getConfig().enableObjectReuse();
    } else {
      flinkBatchEnv.getConfig().disableObjectReuse();
    }

    applyLatencyTrackingInterval(flinkBatchEnv.getConfig(), options);

    return flinkBatchEnv;
  }

  @VisibleForTesting
  static StreamExecutionEnvironment createStreamExecutionEnvironment(FlinkPipelineOptions options) {
    return createStreamExecutionEnvironment(
        options,
        MoreObjects.firstNonNull(options.getFilesToStage(), Collections.emptyList()),
        options.getFlinkConfDir());
  }

  /**
   * If the submitted job is a stream processing job, this method creates the adequate Flink {@link
   * org.apache.flink.streaming.api.environment.StreamExecutionEnvironment} depending on the
   * user-specified options.
   */
  public static StreamExecutionEnvironment createStreamExecutionEnvironment(
      FlinkPipelineOptions options, List<String> filesToStage, @Nullable String confDir) {

    LOG.info("Creating a Streaming Environment.");

    // Although Flink uses Rest, it expects the address not to contain a http scheme
    String masterUrl = stripHttpSchema(options.getFlinkMaster());
    Configuration flinkConfiguration = getFlinkConfiguration(confDir, options);
    StreamExecutionEnvironment flinkStreamEnv;

    // depending on the master, create the right environment.
    if ("[local]".equals(masterUrl)) {
      setManagedMemoryByFraction(flinkConfiguration);
      disableClassLoaderLeakCheck(flinkConfiguration);
      flinkStreamEnv =
          StreamExecutionEnvironment.createLocalEnvironment(
              getDefaultLocalParallelism(), flinkConfiguration);
    } else if ("[auto]".equals(masterUrl)) {
      flinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
      if (flinkStreamEnv instanceof LocalStreamEnvironment) {
        disableClassLoaderLeakCheck(flinkConfiguration);
        flinkStreamEnv =
            StreamExecutionEnvironment.createLocalEnvironment(
                getDefaultLocalParallelism(), flinkConfiguration);
      }
    } else {
      int defaultPort = flinkConfiguration.getInteger(RestOptions.PORT);
      HostAndPort hostAndPort = HostAndPort.fromString(masterUrl).withDefaultPort(defaultPort);
      flinkConfiguration.setInteger(RestOptions.PORT, hostAndPort.getPort());
      final SavepointRestoreSettings savepointRestoreSettings;
      if (options.getSavepointPath() != null) {
        savepointRestoreSettings =
            SavepointRestoreSettings.forPath(
                options.getSavepointPath(), options.getAllowNonRestoredState());
      } else {
        savepointRestoreSettings = SavepointRestoreSettings.none();
      }
      flinkStreamEnv =
          new RemoteStreamEnvironment(
              hostAndPort.getHost(),
              hostAndPort.getPort(),
              flinkConfiguration,
              filesToStage.toArray(new String[filesToStage.size()]),
              null,
              savepointRestoreSettings);
      LOG.info("Using Flink Master URL {}:{}.", hostAndPort.getHost(), hostAndPort.getPort());
    }

    // Set the parallelism, required by UnboundedSourceWrapper to generate consistent splits.
    final int parallelism =
        determineParallelism(
            options.getParallelism(), flinkStreamEnv.getParallelism(), flinkConfiguration);
    flinkStreamEnv.setParallelism(parallelism);
    if (options.getMaxParallelism() > 0) {
      flinkStreamEnv.setMaxParallelism(options.getMaxParallelism());
    }
    // set parallelism in the options (required by some execution code)
    options.setParallelism(parallelism);

    if (options.getObjectReuse()) {
      flinkStreamEnv.getConfig().enableObjectReuse();
    } else {
      flinkStreamEnv.getConfig().disableObjectReuse();
    }

    // default to event time
    flinkStreamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // for the following 2 parameters, a value of -1 means that Flink will use
    // the default values as specified in the configuration.
    int numRetries = options.getNumberOfExecutionRetries();
    if (numRetries != -1) {
      flinkStreamEnv.setNumberOfExecutionRetries(numRetries);
    }
    long retryDelay = options.getExecutionRetryDelay();
    if (retryDelay != -1) {
      flinkStreamEnv.getConfig().setExecutionRetryDelay(retryDelay);
    }

    configureCheckpointing(options, flinkStreamEnv);

    applyLatencyTrackingInterval(flinkStreamEnv.getConfig(), options);

    if (options.getAutoWatermarkInterval() != null) {
      flinkStreamEnv.getConfig().setAutoWatermarkInterval(options.getAutoWatermarkInterval());
    }

    configureStateBackend(options, flinkStreamEnv);

    return flinkStreamEnv;
  }

  private static void configureCheckpointing(
      FlinkPipelineOptions options, StreamExecutionEnvironment flinkStreamEnv) {
    // A value of -1 corresponds to disabled checkpointing (see CheckpointConfig in Flink).
    // If the value is not -1, then the validity checks are applied.
    // By default, checkpointing is disabled.
    long checkpointInterval = options.getCheckpointingInterval();
    if (checkpointInterval != -1) {
      if (checkpointInterval < 1) {
        throw new IllegalArgumentException("The checkpoint interval must be positive");
      }
      flinkStreamEnv.enableCheckpointing(
          checkpointInterval, CheckpointingMode.valueOf(options.getCheckpointingMode()));

      if (options.getShutdownSourcesAfterIdleMs() == -1) {
        // If not explicitly configured, we never shutdown sources when checkpointing is enabled.
        options.setShutdownSourcesAfterIdleMs(Long.MAX_VALUE);
      }

      if (options.getCheckpointTimeoutMillis() != -1) {
        flinkStreamEnv
            .getCheckpointConfig()
            .setCheckpointTimeout(options.getCheckpointTimeoutMillis());
      }

      boolean externalizedCheckpoint = options.isExternalizedCheckpointsEnabled();
      boolean retainOnCancellation = options.getRetainExternalizedCheckpointsOnCancellation();
      if (externalizedCheckpoint) {
        flinkStreamEnv
            .getCheckpointConfig()
            .enableExternalizedCheckpoints(
                retainOnCancellation
                    ? ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
                    : ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
      }

      long minPauseBetweenCheckpoints = options.getMinPauseBetweenCheckpoints();
      if (minPauseBetweenCheckpoints != -1) {
        flinkStreamEnv
            .getCheckpointConfig()
            .setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
      }
      boolean failOnCheckpointingErrors = options.getFailOnCheckpointingErrors();
      flinkStreamEnv.getCheckpointConfig().setFailOnCheckpointingErrors(failOnCheckpointingErrors);

      flinkStreamEnv
          .getCheckpointConfig()
          .setMaxConcurrentCheckpoints(options.getNumConcurrentCheckpoints());
    } else {
      if (options.getShutdownSourcesAfterIdleMs() == -1) {
        // If not explicitly configured, we never shutdown sources when checkpointing is enabled.
        options.setShutdownSourcesAfterIdleMs(0L);
      }
    }
  }

  private static void configureStateBackend(
      FlinkPipelineOptions options, StreamExecutionEnvironment env) {
    final StateBackend stateBackend;
    if (options.getStateBackend() != null) {
      final String storagePath = options.getStateBackendStoragePath();
      Preconditions.checkArgument(
          storagePath != null,
          "State backend was set to '%s' but no storage path was provided.",
          options.getStateBackend());

      if (options.getStateBackend().equalsIgnoreCase("rocksdb")) {
        try {
          stateBackend = new RocksDBStateBackend(storagePath);
        } catch (Exception e) {
          throw new RuntimeException(
              "Could not create RocksDB state backend. Make sure it is included in the path.", e);
        }
      } else if (options.getStateBackend().equalsIgnoreCase("filesystem")) {
        stateBackend = new FsStateBackend(storagePath);
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Unknown state backend '%s'. Use 'rocksdb' or 'filesystem' or configure via Flink config file.",
                options.getStateBackend()));
      }
    } else if (options.getStateBackendFactory() != null) {
      // Legacy way of setting the state backend
      stateBackend =
          InstanceBuilder.ofType(FlinkStateBackendFactory.class)
              .fromClass(options.getStateBackendFactory())
              .build()
              .createStateBackend(options);
    } else {
      stateBackend = null;
    }
    if (stateBackend != null) {
      env.setStateBackend(stateBackend);
    }
  }

  /**
   * Removes the http:// or https:// schema from a url string. This is commonly used with the
   * flink_master address which is expected to be of form host:port but users may specify a URL;
   * Python code also assumes a URL which may be passed here.
   */
  private static String stripHttpSchema(String url) {
    return url.trim().replaceFirst("^http[s]?://", "");
  }

  private static int determineParallelism(
      final int pipelineOptionsParallelism,
      final int envParallelism,
      final Configuration configuration) {
    if (pipelineOptionsParallelism > 0) {
      return pipelineOptionsParallelism;
    }
    if (envParallelism > 0) {
      // If the user supplies a parallelism on the command-line, this is set on the execution
      // environment during creation
      return envParallelism;
    }

    final int flinkConfigParallelism =
        configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM.key(), -1);
    if (flinkConfigParallelism > 0) {
      return flinkConfigParallelism;
    }
    LOG.warn(
        "No default parallelism could be found. Defaulting to parallelism 1. "
            + "Please set an explicit parallelism with --parallelism");
    return 1;
  }

  @VisibleForTesting
  static Configuration getFlinkConfiguration(
      @Nullable String flinkConfDir, FlinkPipelineOptions flinkPipelineOptions) {
    Configuration dynamicProperties = null;
    final Map<String, String> flinkConfMap =
        ExternalConfigRegistrar.getConfig(flinkPipelineOptions);
    if (!flinkConfMap.isEmpty()) {
      dynamicProperties = Configuration.fromMap(flinkConfMap);
    }
    if (flinkConfDir != null && !flinkConfDir.isEmpty()) {
      return GlobalConfiguration.loadConfiguration(flinkConfDir, dynamicProperties);
    } else if (dynamicProperties != null) {
      return GlobalConfiguration.loadConfiguration(dynamicProperties);
    } else {
      return GlobalConfiguration.loadConfiguration();
    }
  }

  private static void applyLatencyTrackingInterval(
      ExecutionConfig config, FlinkPipelineOptions options) {
    long latencyTrackingInterval = options.getLatencyTrackingInterval();
    config.setLatencyTrackingInterval(latencyTrackingInterval);
  }

  private static void setManagedMemoryByFraction(final Configuration config) {
    if (!config.containsKey("taskmanager.memory.managed.size")) {
      float managedMemoryFraction = config.getFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION);
      long freeHeapMemory = EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag();
      long managedMemorySize = (long) (freeHeapMemory * managedMemoryFraction);
      config.setString("taskmanager.memory.managed.size", String.valueOf(managedMemorySize));
    }
  }

  /**
   * Disables classloader.check-leaked-classloader unless set by the user. See
   * https://github.com/apache/beam/issues/20783.
   */
  private static void disableClassLoaderLeakCheck(final Configuration config) {
    if (!config.containsKey("classloader.check-leaked-classloader")) {
      config.setBoolean("classloader.check-leaked-classloader", false);
    }
  }
}
