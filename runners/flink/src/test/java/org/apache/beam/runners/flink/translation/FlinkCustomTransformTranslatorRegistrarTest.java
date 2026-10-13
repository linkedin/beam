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
package org.apache.beam.runners.flink.translation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.runners.flink.FlinkCustomTransformTranslatorRegistrar;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkStreamingPipelineTranslator;
import org.apache.beam.runners.flink.FlinkStreamingPipelineTranslator.StreamTransformTranslator;
import org.apache.beam.runners.flink.FlinkStreamingTranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Tests the translator extension from outside the runner's package. */
@RunWith(Parameterized.class)
public class FlinkCustomTransformTranslatorRegistrarTest {
  private static final String CUSTOM_URN = "beam:test:custom_flink_source:v1";
  private static String translatedBy = "builtin";

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Parameterized.Parameter public boolean streaming;

  @Parameterized.Parameters(name = "streaming = {0}")
  public static Collection<Object[]> modes() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  @Test
  public void builtinsWorkWithoutProviders() throws Exception {
    assertEquals("builtin:" + streaming, translate(true));
  }

  @Test
  public void customTranslatorIsDiscoveredAndDispatched() throws Exception {
    assertEquals("first:" + streaming, translate(false, FirstRegistrar.class));
  }

  @Test
  public void unrelatedBuiltinRemainsAvailable() throws Exception {
    assertEquals("builtin:" + streaming, translate(true, FirstRegistrar.class));
  }

  @Test
  public void customTranslatorCanOverrideBuiltin() throws Exception {
    assertEquals("override:" + streaming, translate(true, ImpulseRegistrar.class));
  }

  @Test
  public void laterProviderOverridesEarlierProvider() throws Exception {
    assertEquals(
        "second:" + streaming, translate(false, FirstRegistrar.class, SecondRegistrar.class));
  }

  @Test
  public void providerOrderIsPreserved() throws Exception {
    assertEquals(
        "first:" + streaming, translate(false, SecondRegistrar.class, FirstRegistrar.class));
  }

  @Test
  public void unknownTransformStillFailsWithoutProvider() {
    InvocationTargetException error =
        assertThrows(InvocationTargetException.class, () -> translate(false));
    assertThat(error.getCause(), instanceOf(UnsupportedOperationException.class));
  }

  @Test
  public void providerFailureIsPropagated() {
    InvocationTargetException error =
        assertThrows(
            InvocationTargetException.class, () -> translate(true, FailingRegistrar.class));
    assertThat(error.getCause(), instanceOf(ExceptionInInitializerError.class));
    assertEquals("Translator provider failed", error.getCause().getCause().getMessage());
  }

  private String translate(boolean impulse, Class<?>... providers) throws Exception {
    // Initialize shared SDK registries before using an isolated runner classloader.
    assertEquals(
        PTransformTranslation.IMPULSE_TRANSFORM_URN,
        PTransformTranslation.urnForTransformOrNull(Impulse.create()));
    Path directory = temporaryFolder.newFolder().toPath();
    Path service =
        directory.resolve(
            "META-INF/services/" + FlinkCustomTransformTranslatorRegistrar.class.getName());
    Files.createDirectories(service.getParent());
    Files.write(
        service,
        Arrays.stream(providers).map(Class::getName).collect(Collectors.toList()),
        StandardCharsets.UTF_8);

    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    // The production registry is static; isolate it instead of modifying global state in tests.
    try (URLClassLoader loader =
        new RegistryClassLoader(
            new URL[] {
              directory.toUri().toURL(),
              FlinkStreamingPipelineTranslator.class
                  .getProtectionDomain()
                  .getCodeSource()
                  .getLocation(),
              getClass().getProtectionDomain().getCodeSource().getLocation()
            },
            getClass().getClassLoader())) {
      Thread.currentThread().setContextClassLoader(loader);
      return (String)
          loader
              .loadClass(getClass().getName())
              .getMethod("translatePipeline", boolean.class, boolean.class)
              .invoke(null, impulse, streaming);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  /**
   * Invoked in a fresh runner classloader to exercise real registry lookup and pipeline dispatch.
   */
  public static String translatePipeline(boolean impulse, boolean streaming) {
    translatedBy = "builtin:" + streaming;
    Pipeline pipeline = Pipeline.create();
    if (impulse) {
      pipeline.apply(Impulse.create());
    } else {
      pipeline.apply(new CustomSource());
    }
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.setParallelism(1);
    new FlinkStreamingPipelineTranslator(environment, pipeline.getOptions(), streaming)
        .translate(pipeline);
    return translatedBy;
  }

  private static class RegistryClassLoader extends URLClassLoader {
    RegistryClassLoader(URL[] urls, ClassLoader parent) {
      super(urls, parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      // Keep options compatible with the shared SDK's global proxy cache.
      if (!name.startsWith("org.apache.beam.runners.flink.")
          || name.equals(FlinkPipelineOptions.class.getName())) {
        return super.loadClass(name, resolve);
      }
      synchronized (getClassLoadingLock(name)) {
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          loaded = findClass(name);
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }
  }

  private static class CustomSource
      extends PTransformTranslation.RawPTransform<PBegin, PCollection<byte[]>> {
    @Override
    public FunctionSpec getSpec() {
      return FunctionSpec.newBuilder().setUrn(CUSTOM_URN).build();
    }

    @Override
    public PCollection<byte[]> expand(PBegin input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(),
          WindowingStrategy.globalDefault(),
          PCollection.IsBounded.BOUNDED,
          ByteArrayCoder.of());
    }
  }

  private static class RecordingTranslator
      extends StreamTransformTranslator<PTransform<PBegin, PCollection<byte[]>>> {
    private final String name;

    RecordingTranslator(String name) {
      this.name = name;
    }

    @Override
    public void translateNode(
        PTransform<PBegin, PCollection<byte[]>> transform,
        FlinkStreamingTranslationContext context) {
      PCollection<byte[]> output = context.getOutput(transform);
      context.setOutputDataStream(
          output,
          context
              .getExecutionEnvironment()
              .fromElements(WindowedValues.valueInGlobalWindow(new byte[0]))
              .returns(context.getTypeInfo(output))
              .name(name));
      translatedBy = name + ":" + context.isStreaming();
    }
  }

  /** First provider for the custom source. */
  public static class FirstRegistrar implements FlinkCustomTransformTranslatorRegistrar {
    @Override
    public Map<String, StreamTransformTranslator<?>> getTransformTranslators() {
      return Collections.singletonMap(CUSTOM_URN, new RecordingTranslator("first"));
    }
  }

  /** Second provider for the same custom source. */
  public static class SecondRegistrar implements FlinkCustomTransformTranslatorRegistrar {
    @Override
    public Map<String, StreamTransformTranslator<?>> getTransformTranslators() {
      return Collections.singletonMap(CUSTOM_URN, new RecordingTranslator("second"));
    }
  }

  /** Provider replacing the built-in impulse translator. */
  public static class ImpulseRegistrar implements FlinkCustomTransformTranslatorRegistrar {
    @Override
    public Map<String, StreamTransformTranslator<?>> getTransformTranslators() {
      return Collections.singletonMap(
          PTransformTranslation.IMPULSE_TRANSFORM_URN, new RecordingTranslator("override"));
    }
  }

  /** Provider failures must remain visible during registry initialization. */
  public static class FailingRegistrar implements FlinkCustomTransformTranslatorRegistrar {
    @Override
    public Map<String, StreamTransformTranslator<?>> getTransformTranslators() {
      throw new IllegalStateException("Translator provider failed");
    }
  }
}
