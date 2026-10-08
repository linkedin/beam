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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.expansion.ExternalConfigRegistrar;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Tests external configuration discovery and precedence in Flink environment creation. */
@RunWith(Parameterized.class)
public class FlinkExternalConfigurationTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Parameterized.Parameter public boolean batch;

  @Parameterized.Parameters(name = "batch = {0}")
  public static Collection<Object[]> modes() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  private @Nullable ClassLoader originalClassLoader;
  private @Nullable URLClassLoader serviceClassLoader;

  @Before
  public void saveClassLoader() {
    originalClassLoader = Thread.currentThread().getContextClassLoader();
  }

  @After
  public void restoreClassLoader() throws IOException {
    Thread.currentThread().setContextClassLoader(originalClassLoader);
    if (serviceClassLoader != null) {
      serviceClassLoader.close();
    }
  }

  @Test
  public void externalConfigOverridesFile() throws Exception {
    registerProvider(TestRegistrar.class);
    assertEquals(31, createEnvironment(optionsWithParallelism(31), createConfigDirectory()));
  }

  @Test
  public void externalConfigWithoutDirectory() throws Exception {
    registerProvider(TestRegistrar.class);
    assertEquals(31, createEnvironment(optionsWithParallelism(31), null));
  }

  @Test
  public void externalConfigWithEmptyDirectory() throws Exception {
    registerProvider(TestRegistrar.class);
    assertEquals(31, createEnvironment(optionsWithParallelism(31), ""));
  }

  @Test
  public void explicitBeamOptionOverridesExternalConfig() throws Exception {
    registerProvider(TestRegistrar.class);
    ExternalConfigOptions options = optionsWithParallelism(31);
    options.setParallelism(42);
    assertEquals(42, createEnvironment(options, createConfigDirectory()));
    assertEquals(42, options.getParallelism().intValue());
  }

  @Test
  public void externalConfigUsesEachPipelinesOptions() throws Exception {
    registerProvider(TestRegistrar.class);
    assertEquals(31, createEnvironment(optionsWithParallelism(31), null));
    assertEquals(43, createEnvironment(optionsWithParallelism(43), null));
  }

  @Test
  public void emptyExternalConfigPreservesFile() throws Exception {
    registerProvider(TestRegistrar.class);
    assertEquals(23, createEnvironment(options(), createConfigDirectory()));
  }

  @Test
  public void emptyExternalConfigPreservesDefaults() throws Exception {
    registerProvider(TestRegistrar.class);
    assertEquals(1, createEnvironment(options(), null));
  }

  @Test
  public void noProviderPreservesFile() throws Exception {
    assertEquals(23, createEnvironment(options(), createConfigDirectory()));
  }

  @Test
  public void noProviderPreservesDefaults() {
    assertEquals(1, createEnvironment(options(), null));
  }

  @Test
  public void providerFailureIsPropagated() throws Exception {
    registerProvider(FailingRegistrar.class);
    IllegalStateException error =
        assertThrows(IllegalStateException.class, () -> createEnvironment(options(), null));
    assertEquals("External config failed", error.getMessage());
  }

  private int createEnvironment(FlinkPipelineOptions options, @Nullable String confDir) {
    return batch
        ? FlinkExecutionEnvironments.createBatchExecutionEnvironment(
                options, Collections.emptyList(), confDir)
            .getParallelism()
        : FlinkExecutionEnvironments.createStreamExecutionEnvironment(
                options, Collections.emptyList(), confDir)
            .getParallelism();
  }

  private ExternalConfigOptions options() {
    ExternalConfigOptions options = PipelineOptionsFactory.as(ExternalConfigOptions.class);
    options.setFlinkMaster("host:80");
    options.setExternalConfig(Collections.emptyMap());
    return options;
  }

  private ExternalConfigOptions optionsWithParallelism(int parallelism) {
    ExternalConfigOptions options = options();
    options.setExternalConfig(
        Collections.singletonMap("parallelism.default", Integer.toString(parallelism)));
    return options;
  }

  private String createConfigDirectory() throws IOException {
    Path directory = temporaryFolder.newFolder().toPath();
    Files.write(
        directory.resolve("flink-conf.yaml"),
        Collections.singletonList("parallelism.default: 23"),
        StandardCharsets.UTF_8);
    Files.write(
        directory.resolve("config.yaml"),
        Arrays.asList("parallelism:", "  default: 23"),
        StandardCharsets.UTF_8);
    return directory.toString();
  }

  private void registerProvider(Class<? extends ExternalConfigRegistrar> provider)
      throws IOException {
    Path directory = temporaryFolder.newFolder().toPath();
    Path service =
        directory.resolve("META-INF/services/" + ExternalConfigRegistrar.class.getName());
    Files.createDirectories(service.getParent());
    Files.write(service, Collections.singletonList(provider.getName()), StandardCharsets.UTF_8);
    serviceClassLoader =
        new URLClassLoader(new URL[] {directory.toUri().toURL()}, originalClassLoader);
    Thread.currentThread().setContextClassLoader(serviceClassLoader);
  }

  /** Pipeline-scoped external configuration used by the test provider. */
  public interface ExternalConfigOptions extends FlinkPipelineOptions {
    Map<String, String> getExternalConfig();

    void setExternalConfig(Map<String, String> config);
  }

  /** Discovered only through each test's temporary service resource. */
  public static class TestRegistrar implements ExternalConfigRegistrar {
    @Override
    @SuppressWarnings("unchecked")
    public Map<String, String> getExternalConfig(PipelineOptions options) {
      return options.as(ExternalConfigOptions.class).getExternalConfig();
    }
  }

  /** Provider failures must not silently fall back to default Flink configuration. */
  public static class FailingRegistrar implements ExternalConfigRegistrar {
    @Override
    public <K, V> Map<K, V> getExternalConfig(PipelineOptions options) {
      throw new IllegalStateException("External config failed");
    }
  }
}
