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
package org.apache.beam.runners.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.runners.spark.translation.BoundedDataset;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.TransformEvaluator;
import org.apache.beam.runners.spark.translation.TransformEvaluatorRegistrar;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Exercises custom evaluators from outside the translation package. */
public class SparkRunnerExtensionsTest {
  private static final String CUSTOM_URN = "beam:test:custom_spark_source:v1";

  @ClassRule public static SparkContextRule contextRule = new SparkContextRule();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  @After
  public void clearSessions() {
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
  }

  @Test
  public void builtinWorksWithoutProviders() throws Exception {
    assertEquals("", translate(true));
  }

  @Test
  public void customEvaluatorIsDiscoveredAndDispatched() throws Exception {
    assertEquals("first", translate(false, FirstRegistrar.class));
  }

  @Test
  public void unrelatedBuiltinRemainsAvailable() throws Exception {
    assertEquals("", translate(true, FirstRegistrar.class));
  }

  @Test
  public void customEvaluatorCanOverrideBuiltin() throws Exception {
    assertEquals("override", translate(true, ImpulseRegistrar.class));
  }

  @Test
  public void laterProviderOverridesEarlierProvider() throws Exception {
    assertEquals("second", translate(false, FirstRegistrar.class, SecondRegistrar.class));
  }

  @Test
  public void providerOrderIsPreserved() throws Exception {
    assertEquals("first", translate(false, SecondRegistrar.class, FirstRegistrar.class));
  }

  @Test
  public void unknownTransformStillFailsWithoutProvider() {
    assertThrows(IllegalStateException.class, () -> translate(false));
  }

  @Test
  public void providerFailureIsPropagated() {
    ExceptionInInitializerError error =
        assertThrows(
            ExceptionInInitializerError.class, () -> translate(true, FailingRegistrar.class));
    assertEquals("Evaluator provider failed", error.getCause().getMessage());
  }

  @Test
  public void collectionDatasetConstructorPreservesValues() {
    BoundedDataset<String> dataset =
        new BoundedDataset<>(
            Arrays.asList("first", "second"), contextRule.getSparkContext(), StringUtf8Coder.of());
    assertEquals(
        Arrays.asList("first", "second"), dataset.getRDD().map(WindowedValue::getValue).collect());
  }

  @Test
  public void evaluationContextDoesNotCreateAnUnusedSession() {
    Pipeline pipeline = Pipeline.create();
    new EvaluationContext(
        contextRule.getSparkContext(), pipeline, contextRule.createPipelineOptions());
    assertFalse(SparkSession.getDefaultSession().isDefined());
  }

  @Test
  public void sessionUsesProvidedContextAndIsReused() {
    Pipeline pipeline = Pipeline.create();
    EvaluationContext context =
        new EvaluationContext(
            contextRule.getSparkContext(), pipeline, contextRule.createPipelineOptions());
    SparkSession session = context.getSparkSession();
    assertSame(contextRule.getSparkContext().sc(), session.sparkContext());
    assertSame(session, context.getSparkSession());
    assertEquals(3L, session.range(3).count());
  }

  private String translate(boolean impulse, Class<?>... providers) throws Exception {
    Path directory = temporaryFolder.newFolder().toPath();
    Path service =
        directory.resolve("META-INF/services/" + TransformEvaluatorRegistrar.class.getName());
    Files.createDirectories(service.getParent());
    Files.write(
        service,
        Arrays.stream(providers).map(Class::getName).collect(Collectors.toList()),
        StandardCharsets.UTF_8);

    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader loader =
        new RegistryClassLoader(
            new URL[] {
              directory.toUri().toURL(),
              TransformTranslator.class.getProtectionDomain().getCodeSource().getLocation()
            },
            getClass().getClassLoader())) {
      Thread.currentThread().setContextClassLoader(loader);
      SparkPipelineTranslator translator =
          (SparkPipelineTranslator)
              loader
                  .loadClass(TransformTranslator.Translator.class.getName())
                  .getDeclaredConstructor()
                  .newInstance();
      SparkPipelineOptions options = contextRule.createPipelineOptions();
      Pipeline pipeline = Pipeline.create(options);
      PCollection<byte[]> output = pipeline.apply(impulse ? Impulse.create() : new CustomSource());
      EvaluationContext context =
          new EvaluationContext(contextRule.getSparkContext(), pipeline, options);
      SparkRunner.initAccumulators(options, context.getSparkContext());
      pipeline.traverseTopologically(new SparkRunner.Evaluator(translator, context));
      @SuppressWarnings("unchecked")
      BoundedDataset<byte[]> dataset = (BoundedDataset<byte[]>) context.borrowDataset(output);
      return new String(
          dataset.getRDD().map(WindowedValue::getValue).collect().get(0), StandardCharsets.UTF_8);
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  private static class RegistryClassLoader extends URLClassLoader {
    RegistryClassLoader(URL[] urls, ClassLoader parent) {
      super(urls, parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      String registry = TransformTranslator.class.getName();
      // Isolate the static registry, not the SDK/options caches or evaluator API.
      if (!name.equals(registry) && !name.startsWith(registry + "$")) {
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

  private static class RecordingEvaluator
      implements TransformEvaluator<PTransform<PBegin, PCollection<byte[]>>> {
    private final String name;

    RecordingEvaluator(String name) {
      this.name = name;
    }

    @Override
    public void evaluate(
        PTransform<PBegin, PCollection<byte[]>> transform, EvaluationContext context) {
      context.putDataset(
          transform,
          new BoundedDataset<>(
              context
                  .getSparkContext()
                  .parallelize(Collections.singletonList(name.getBytes(StandardCharsets.UTF_8)))
                  .map(WindowedValues::valueInGlobalWindow)));
    }

    @Override
    public String toNativeString() {
      return name;
    }
  }

  public static class FirstRegistrar implements TransformEvaluatorRegistrar {
    @Override
    public Map<String, TransformEvaluator<?>> getTransformEvaluators() {
      return Collections.singletonMap(CUSTOM_URN, new RecordingEvaluator("first"));
    }
  }

  public static class SecondRegistrar implements TransformEvaluatorRegistrar {
    @Override
    public Map<String, TransformEvaluator<?>> getTransformEvaluators() {
      return Collections.singletonMap(CUSTOM_URN, new RecordingEvaluator("second"));
    }
  }

  public static class ImpulseRegistrar implements TransformEvaluatorRegistrar {
    @Override
    public Map<String, TransformEvaluator<?>> getTransformEvaluators() {
      return Collections.singletonMap(
          PTransformTranslation.IMPULSE_TRANSFORM_URN, new RecordingEvaluator("override"));
    }
  }

  public static class FailingRegistrar implements TransformEvaluatorRegistrar {
    @Override
    public Map<String, TransformEvaluator<?>> getTransformEvaluators() {
      throw new IllegalStateException("Evaluator provider failed");
    }
  }
}
