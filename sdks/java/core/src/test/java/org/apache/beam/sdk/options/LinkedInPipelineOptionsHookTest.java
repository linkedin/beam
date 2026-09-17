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
package org.apache.beam.sdk.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.expansion.ExternalConfigRegistrar;
import org.apache.beam.sdk.testing.PropertyCustomizationHandler;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for LinkedIn-specific pipeline options extension hooks. */
@RunWith(JUnit4.class)
public class LinkedInPipelineOptionsHookTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  /** Test options used by the LinkedIn hook tests. */
  public interface TestOptions extends PipelineOptions {
    String getTestValue();

    void setTestValue(String value);

    Object getObjectValue();

    void setObjectValue(Object value);
  }

  @Test
  public void runnerPipelineOptionsFactoryInterceptsTopLevelCreation() throws Exception {
    TestRunnerPipelineOptionsFactory.calls = 0;
    withService(
        RunnerPipelineOptionsFactory.Registrar.class,
        TestRunnerPipelineOptionsFactory.Registrar.class,
        () -> {
          TestOptions options =
              PipelineOptionsFactory.fromArgs("--testValue=fromArgs").as(TestOptions.class);

          assertEquals("fromArgs", options.getTestValue());
          assertEquals("fromRunnerFactory", options.as(ApplicationNameOptions.class).getAppName());
          assertEquals(1, TestRunnerPipelineOptionsFactory.calls);
        });
  }

  @Test
  public void customPipelineOptionsInitializerRunsOnAsConversion() throws Exception {
    TestCustomPipelineOptionsInitializer.calls = 0;
    TestCustomPipelineOptionsInitializer.initializedClass = null;
    withService(
        CustomPipelineOptionsInitializer.Registrar.class,
        TestCustomPipelineOptionsInitializer.Registrar.class,
        () -> {
          PipelineOptions options = PipelineOptionsFactory.create();
          TestCustomPipelineOptionsInitializer.calls = 0;
          TestCustomPipelineOptionsInitializer.initializedClass = null;
          TestOptions initialized = options.as(TestOptions.class);

          assertNotNull(initialized);
          assertEquals(TestOptions.class, TestCustomPipelineOptionsInitializer.initializedClass);
          assertEquals(1, TestCustomPipelineOptionsInitializer.calls);
        });
  }

  @Test
  public void propertyCustomizationHandlerCanRestoreExternallyStoredValues() throws Exception {
    TestPropertyCustomizationHandler.clearValues();
    Object nonSerializableValue = new Object();
    withService(
        PropertyCustomizationHandler.Registrar.class,
        TestPropertyCustomizationHandler.Registrar.class,
        () -> {
          new ProxyInvocationHandler(new HashMap<>())
              .as(TestOptions.class)
              .setObjectValue(nonSerializableValue);

          Object restored =
              new ProxyInvocationHandler(new HashMap<>()).as(TestOptions.class).getObjectValue();
          assertSame(nonSerializableValue, restored);
        });
  }

  @Test
  public void externalConfigRegistrarLoadsRunnerConfig() throws Exception {
    withService(
        ExternalConfigRegistrar.class,
        TestExternalConfigRegistrar.class,
        () -> {
          Map<String, String> config =
              ExternalConfigRegistrar.getConfig(PipelineOptionsFactory.create());
          assertEquals(Collections.singletonMap("flink.key", "flink.value"), config);
        });
  }

  private void withService(
      Class<?> serviceClass, Class<?> implementationClass, ThrowingRunnable runnable)
      throws Exception {
    Path serviceRoot = temporaryFolder.newFolder().toPath();
    Path servicesDir = serviceRoot.resolve("META-INF/services");
    Files.createDirectories(servicesDir);
    Files.write(
        servicesDir.resolve(serviceClass.getName()),
        Collections.singletonList(implementationClass.getName()),
        StandardCharsets.UTF_8);

    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    URL[] urls = new URL[] {serviceRoot.toUri().toURL()};
    try (URLClassLoader classLoader = new URLClassLoader(urls, previous)) {
      Thread.currentThread().setContextClassLoader(classLoader);
      runnable.run();
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  /** Test runner factory loaded via a temporary ServiceLoader resource. */
  public static class TestRunnerPipelineOptionsFactory implements RunnerPipelineOptionsFactory {
    private static int calls;

    @Override
    public <T extends PipelineOptions> T getPipelineOptions(String[] args, Class<T> clazz) {
      assertEquals(
          TestRunnerPipelineOptionsFactory.class, RunnerPipelineOptionsFactory.findFactoryCaller());
      calls++;
      T options = PipelineOptionsFactory.fromArgs(args).as(clazz);
      options.as(ApplicationNameOptions.class).setAppName("fromRunnerFactory");
      return options;
    }

    /** Test registrar for {@link TestRunnerPipelineOptionsFactory}. */
    public static class Registrar implements RunnerPipelineOptionsFactory.Registrar {
      @Override
      public RunnerPipelineOptionsFactory create() {
        return new TestRunnerPipelineOptionsFactory();
      }
    }
  }

  /** Test custom options initializer loaded via a temporary ServiceLoader resource. */
  public static class TestCustomPipelineOptionsInitializer
      implements CustomPipelineOptionsInitializer<PipelineOptions> {
    private static int calls;
    private static @Nullable Class<?> initializedClass;

    @Override
    public PipelineOptions init(PipelineOptions pipelineOptions, Class<PipelineOptions> clazz) {
      calls++;
      initializedClass = clazz;
      return pipelineOptions;
    }

    /** Test registrar for {@link TestCustomPipelineOptionsInitializer}. */
    public static class Registrar implements CustomPipelineOptionsInitializer.Registrar {
      @Override
      public CustomPipelineOptionsInitializer<?> create() {
        return new TestCustomPipelineOptionsInitializer();
      }
    }
  }

  /** Test property customization handler loaded via a temporary ServiceLoader resource. */
  public static class TestPropertyCustomizationHandler implements PropertyCustomizationHandler {
    private static final ConcurrentHashMap<Class<?>, Map<String, Object>> VALUES =
        new ConcurrentHashMap<>();

    static void clearValues() {
      VALUES.clear();
    }

    @Override
    public Boolean isCustomizationEnabled() {
      return true;
    }

    @Override
    public Boolean containsProperty(Method method, String propertyName) {
      return VALUES.containsKey(method.getDeclaringClass())
          && VALUES.get(method.getDeclaringClass()).containsKey(propertyName);
    }

    @Override
    public Object getProperty(Method method, String propertyName) {
      return VALUES.get(method.getDeclaringClass()).get(propertyName);
    }

    @Override
    public void setProperty(Method method, String propertyName, @Nullable Object value) {
      VALUES
          .computeIfAbsent(method.getDeclaringClass(), ignored -> new ConcurrentHashMap<>())
          .put(propertyName, value);
    }

    @Override
    public void clear() {
      clearValues();
    }

    /** Test registrar for {@link TestPropertyCustomizationHandler}. */
    public static class Registrar implements PropertyCustomizationHandler.Registrar {
      @Override
      public PropertyCustomizationHandler create() {
        return new TestPropertyCustomizationHandler();
      }
    }
  }

  /** Test external config registrar loaded via a temporary ServiceLoader resource. */
  public static class TestExternalConfigRegistrar implements ExternalConfigRegistrar {
    @Override
    public <K, V> Map<K, V> getExternalConfig(PipelineOptions options) {
      @SuppressWarnings("unchecked")
      Map<K, V> config = (Map<K, V>) Collections.singletonMap("flink.key", "flink.value");
      return config;
    }
  }
}
