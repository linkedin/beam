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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.apache.beam.runners.flink.translation.wrappers.streaming.StreamRecordStripper.stripStreamRecordFromWindowedValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.LateDataDroppingDoFnRunner;
import org.apache.beam.runners.core.SimpleDoFnRunner;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.TestFlinkRunner;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerDeclaration;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.powermock.reflect.Whitebox;

/** Tests for {@link DoFnRunnerWrapper} and {@link DoFnRunnerWrappers}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "unchecked"
})
public class DoFnRunnerWrapperTest {

  /** Events recorded by {@link EnterExitRunner} and {@link OrderedFn}, in order. */
  private static final List<String> EVENTS = new CopyOnWriteArrayList<>();

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @After
  public void tearDown() {
    DoFnRunnerWrappers.resetForTesting();
    EVENTS.clear();
  }

  @Test
  public void noWrapperIsRegisteredOnTheTestClassPath() {
    // A wrapper registered in META-INF/services would wrap every operator in every test.
    assertThat(DoFnRunnerWrappers.getWrappers(), empty());
  }

  @Test
  public void noWrappersReturnsTheSameRunner() {
    DoFnRunnerWrappers.setForTesting(emptyList());
    DoFnRunner<String, String> runner = new TaggingRunner<>("inner", null);

    assertThat(
        DoFnRunnerWrappers.apply(runner, context(DoFnRunnerWrapper.Kind.PAR_DO)),
        sameInstance(runner));
  }

  @Test
  public void wrappersApplyByOrderThenClassName() {
    DoFnRunnerWrappers.setForTesting(
        Arrays.asList(new WrapperHigh(), new WrapperB(), new WrapperLow(), new WrapperA()));

    assertThat(
        DoFnRunnerWrappers.getWrappers().stream()
            .map(wrapper -> wrapper.getClass().getSimpleName())
            .collect(Collectors.toList()),
        contains("WrapperLow", "WrapperA", "WrapperB", "WrapperHigh"));

    DoFnRunner<String, String> inner = new TaggingRunner<>("inner", null);
    DoFnRunner<String, String> outer =
        DoFnRunnerWrappers.apply(inner, context(DoFnRunnerWrapper.Kind.PAR_DO));
    List<String> chain = new ArrayList<>();
    for (DoFnRunner<String, String> r = outer; r instanceof TaggingRunner; ) {
      TaggingRunner<String, String> tagging = (TaggingRunner<String, String>) r;
      chain.add(tagging.tag);
      r = tagging.delegate;
    }
    // Outermost first: a higher order wraps further outside.
    assertThat(chain, contains("WrapperHigh", "WrapperB", "WrapperA", "WrapperLow", "inner"));
  }

  @Test
  public void wrapperReturningNullFails() {
    DoFnRunnerWrappers.setForTesting(Collections.singletonList(new NullWrapper()));

    assertThrows(
        NullPointerException.class,
        () ->
            DoFnRunnerWrappers.apply(
                new TaggingRunner<>("inner", null), context(DoFnRunnerWrapper.Kind.PAR_DO)));
  }

  @Test
  public void parDoRunnerIsWrapped() throws Exception {
    RecordingWrapper wrapper = new RecordingWrapper();
    DoFnRunnerWrappers.setForTesting(Collections.singletonList(wrapper));
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setMaxBundleSize(2L);

    OneInputStreamOperatorTestHarness<WindowedValue<String>, WindowedValue<String>> testHarness =
        new OneInputStreamOperatorTestHarness<>(statelessOperator(new IdentityFn(), options));
    try {
      testHarness.open();

      assertThat(wrapper.contexts.size(), is(1));
      DoFnRunnerWrapper.Context context = wrapper.contexts.get(0);
      assertThat(context.getStepName(), is("stepName"));
      assertThat(context.getKind(), is(DoFnRunnerWrapper.Kind.PAR_DO));
      assertThat(
          context.getPipelineOptions().as(FlinkPipelineOptions.class).getMaxBundleSize(), is(2L));
      assertThat(wrapper.wrappedRunners, contains(instanceOf(SimpleDoFnRunner.class)));

      testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("a")));
      testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("b")));

      assertThat(
          wrapper.calls,
          contains(
              "PAR_DO startBundle",
              "PAR_DO processElement a",
              "PAR_DO processElement b",
              "PAR_DO finishBundle"));
      assertThat(
          stripStreamRecordFromWindowedValue(testHarness.getOutput()),
          contains(WindowedValue.valueInGlobalWindow("a"), WindowedValue.valueInGlobalWindow("b")));
    } finally {
      testHarness.close();
    }
  }

  @Test
  public void wrapperReturningTheRunnerUnchangedChangesNothing() throws Exception {
    List<DoFnRunnerWrapper.Context> contexts = new CopyOnWriteArrayList<>();
    DoFnRunnerWrappers.setForTesting(
        Collections.singletonList(
            new DoFnRunnerWrapper() {
              @Override
              public <InputT, OutputT> DoFnRunner<InputT, OutputT> wrap(
                  DoFnRunner<InputT, OutputT> runner, Context context) {
                contexts.add(context);
                return runner;
              }
            }));

    DoFnOperator<String, String> operator =
        statelessOperator(new IdentityFn(), FlinkPipelineOptions.defaults());
    OneInputStreamOperatorTestHarness<WindowedValue<String>, WindowedValue<String>> testHarness =
        new OneInputStreamOperatorTestHarness<>(operator);
    try {
      testHarness.open();
      testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("a")));

      assertThat(contexts.size(), is(1));
      assertThat(
          stripStreamRecordFromWindowedValue(testHarness.getOutput()),
          contains(WindowedValue.valueInGlobalWindow("a")));
    } finally {
      testHarness.close();
    }
  }

  @Test
  public void statefulRunnerIsInsideTheWrapperAndTimersGoThroughIt() throws Exception {
    RecordingWrapper wrapper = new RecordingWrapper();
    DoFnRunnerWrappers.setForTesting(Collections.singletonList(wrapper));

    DoFnOperator<KV<Integer, String>, String> operator =
        keyedOperator(new StatefulTimerFn(), FlinkPipelineOptions.defaults(), false);
    KeyedOneInputStreamOperatorTestHarness<
            ByteBuffer, WindowedValue<KV<Integer, String>>, WindowedValue<String>>
        testHarness = keyedHarness(operator);
    try {
      testHarness.open();

      assertThat(wrapper.wrappedRunners, contains(instanceOf(StatefulDoFnRunner.class)));
      // The wrapper does not hide the stateful runner's system state from early binding.
      Map<String, ?> registeredStates =
          Whitebox.getInternalState(operator.getKeyedStateBackend(), "registeredKVStates");
      for (StateTag<?> tag :
          ((StatefulDoFnRunner<?, ?, ?>) wrapper.wrappedRunners.get(0)).getSystemStateTags()) {
        assertTrue(tag.getId(), registeredStates.containsKey(tag.getId()));
      }

      IntervalWindow window = new IntervalWindow(new Instant(0), Duration.millis(10));
      testHarness.processWatermark(0);
      testHarness.processElement(
          new StreamRecord<>(
              WindowedValue.of(KV.of(1, "a"), new Instant(1), window, PaneInfo.NO_FIRING)));
      testHarness.processWatermark(20);

      assertThat(wrapper.calls, hasItem("PAR_DO processElement KV{1, a}"));
      // User timer ids reach the runner with the DoFnSignature timer prefix.
      assertThat(
          wrapper.calls,
          hasItem("PAR_DO onTimer " + TimerDeclaration.PREFIX + StatefulTimerFn.TIMER_ID));
      // The state cleanup timer is fired through the wrapper too.
      assertThat(
          wrapper.calls,
          hasItem("PAR_DO onTimer " + StatefulDoFnRunner.TimeInternalsCleanupTimer.GC_TIMER_ID));
      assertThat(
          stripStreamRecordFromWindowedValue(testHarness.getOutput()),
          hasItem(WindowedValue.of("timer", new Instant(5), window, PaneInfo.NO_FIRING)));
    } finally {
      testHarness.close();
    }
  }

  @Test
  public void wrapperIsInsideTheStableInputBuffer() throws Exception {
    RecordingWrapper wrapper = new RecordingWrapper();
    DoFnRunnerWrappers.setForTesting(Collections.singletonList(wrapper));
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setCheckpointingInterval(1L);
    Supplier<DoFnOperator<String, String>> operatorSupplier =
        () -> statelessOperator(new StableInputFn(), options);

    DoFnOperator<String, String> operator = operatorSupplier.get();
    OneInputStreamOperatorTestHarness<WindowedValue<String>, WindowedValue<String>> testHarness =
        new OneInputStreamOperatorTestHarness<>(operator);
    OperatorSubtaskState snapshot;
    try {
      testHarness.open();
      testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("a")));
      testHarness.processElement(new StreamRecord<>(WindowedValue.valueInGlobalWindow("b")));

      // Buffered until the checkpoint completes: not processed yet, so not seen by the wrapper.
      assertThat(wrapper.calls, empty());

      snapshot = testHarness.snapshot(0, 0);
      operator.notifyCheckpointComplete(0L);
      assertThat(
          wrapper.calls,
          contains(
              "PAR_DO startBundle",
              "PAR_DO processElement a",
              "PAR_DO processElement b",
              "PAR_DO finishBundle"));
    } finally {
      testHarness.close();
    }

    // Elements replayed after a restore go through the restored operator's wrapper.
    wrapper.calls.clear();
    operator = operatorSupplier.get();
    testHarness = new OneInputStreamOperatorTestHarness<>(operator);
    try {
      testHarness.initializeState(snapshot);
      testHarness.open();
      operator.notifyCheckpointComplete(0L);

      assertThat(wrapper.contexts.size(), is(2));
      assertThat(
          wrapper.calls,
          contains(
              "PAR_DO startBundle",
              "PAR_DO processElement a",
              "PAR_DO processElement b",
              "PAR_DO finishBundle"));
    } finally {
      testHarness.close();
    }
  }

  @Test
  public void windowRunnerIsWrapped() throws Exception {
    RecordingWrapper wrapper = new RecordingWrapper();
    DoFnRunnerWrappers.setForTesting(Collections.singletonList(wrapper));

    WindowingStrategy<Object, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)));
    Coder<IntervalWindow> windowCoder = windowingStrategy.getWindowFn().windowCoder();
    TupleTag<KV<Long, Long>> outputTag = new TupleTag<>("main-output");
    SystemReduceFn<Long, Long, long[], Long, BoundedWindow> reduceFn =
        SystemReduceFn.combining(
            VarLongCoder.of(),
            AppliedCombineFn.withInputCoder(
                Sum.ofLongs(),
                CoderRegistry.createDefault(),
                KvCoder.of(VarLongCoder.of(), VarLongCoder.of())));
    WindowDoFnOperator<Long, Long, Long> operator =
        new WindowDoFnOperator<Long, Long, Long>(
            reduceFn,
            "gbkStep",
            (Coder)
                WindowedValue.getFullCoder(
                    SingletonKeyedWorkItemCoder.of(
                        VarLongCoder.of(), VarLongCoder.of(), windowCoder),
                    windowCoder),
            outputTag,
            emptyList(),
            new DoFnOperator.MultiOutputOutputManagerFactory<>(
                outputTag,
                WindowedValue.getFullCoder(
                    KvCoder.of(VarLongCoder.of(), VarLongCoder.of()), windowCoder),
                new SerializablePipelineOptions(FlinkPipelineOptions.defaults())),
            windowingStrategy,
            emptyMap(),
            emptyList(),
            FlinkPipelineOptions.defaults(),
            VarLongCoder.of(),
            new WorkItemKeySelector(
                VarLongCoder.of(),
                new SerializablePipelineOptions(FlinkPipelineOptions.defaults())));
    KeyedOneInputStreamOperatorTestHarness<
            ByteBuffer, WindowedValue<KeyedWorkItem<Long, Long>>, WindowedValue<KV<Long, Long>>>
        testHarness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                operator,
                (KeySelector<WindowedValue<KeyedWorkItem<Long, Long>>, ByteBuffer>)
                    item -> FlinkKeyUtils.encodeKey(item.getValue().key(), VarLongCoder.of()),
                new CoderTypeInformation<>(
                    FlinkKeyUtils.ByteBufferCoder.of(), FlinkPipelineOptions.defaults()));
    try {
      testHarness.open();

      assertThat(wrapper.contexts.size(), is(1));
      assertThat(wrapper.contexts.get(0).getStepName(), is("gbkStep"));
      assertThat(wrapper.contexts.get(0).getKind(), is(DoFnRunnerWrapper.Kind.WINDOW));
      // Outside the late-data-dropping runner that WindowDoFnOperator adds.
      assertThat(wrapper.wrappedRunners, contains(instanceOf(LateDataDroppingDoFnRunner.class)));

      IntervalWindow window = new IntervalWindow(new Instant(0), Duration.millis(10));
      testHarness.processWatermark(0);
      WindowedValue<Long> value = WindowedValue.of(5L, new Instant(1), window, PaneInfo.NO_FIRING);
      testHarness.processElement(
          new StreamRecord<>(
              WindowedValue.of(
                  new SingletonKeyedWorkItem<>(1L, value),
                  new Instant(1),
                  window,
                  PaneInfo.NO_FIRING)));
      testHarness.processWatermark(10);

      // One call for the element and one for the fired end-of-window timer.
      assertThat(
          wrapper.calls.stream().filter(call -> call.startsWith("WINDOW processElement")).count(),
          greaterThanOrEqualTo(2L));
      assertThat(
          stripStreamRecordFromWindowedValue(testHarness.getOutput()),
          contains(
              WindowedValue.of(
                  KV.of(1L, 5L),
                  new Instant(9),
                  window,
                  PaneInfo.createPane(true, true, PaneInfo.Timing.ON_TIME))));
    } finally {
      testHarness.close();
    }
  }

  @Test
  public void pipelineOperatorsAreWrappedWithTheirKind() throws Exception {
    RecordingWrapper wrapper = new RecordingWrapper();
    DoFnRunnerWrappers.setForTesting(Collections.singletonList(wrapper));
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setRunner(TestFlinkRunner.class);
    options.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("Impulse", Impulse.create())
        .apply("Split", ParDo.of(new RangeFn()))
        .apply("Key", WithKeys.of("k"))
        .apply("Group", GroupByKey.create());
    pipeline.run().waitUntilFinish();

    List<String> contexts =
        wrapper.contexts.stream()
            .map(context -> context.getKind() + " " + context.getStepName())
            .collect(Collectors.toList());
    assertTrue(contexts.toString(), contexts.contains("WINDOW Group"));
    assertTrue(
        contexts.toString(), contexts.stream().anyMatch(c -> c.startsWith("SPLITTABLE Split/")));
    assertTrue(contexts.toString(), contexts.stream().anyMatch(c -> c.startsWith("PAR_DO Key/")));
    assertThat(wrapper.calls, hasItem("SPLITTABLE startBundle"));
    assertTrue(
        wrapper.calls.toString(),
        wrapper.calls.stream().anyMatch(call -> call.startsWith("SPLITTABLE processElement")));
    // RangeFn outputs 0, 1 and 2, and each reaches the WithKeys ParDo through the wrapper.
    assertThat(
        wrapper.stepCalls.stream()
            .filter(call -> call.startsWith("Key/") && call.contains(" processElement "))
            .count(),
        is(3L));
    assertTrue(
        wrapper.stepCalls.toString(),
        wrapper.stepCalls.stream().anyMatch(call -> call.startsWith("Group processElement")));
  }

  @Test
  public void wrapperIsOutsideTheRunnerASubclassAdds() throws Exception {
    // beam-runner's DlqDoFnOperator overrides createWrappingDoFnRunner, and its runner must run
    // inside the extension's wrapper.
    EnterExitWrapper wrapper = new EnterExitWrapper();
    DoFnRunnerWrappers.setForTesting(Collections.singletonList(wrapper));
    DoFnOperator<KV<Integer, String>, String> operator =
        keyedOperator(new OrderedFn(), FlinkPipelineOptions.defaults(), true);
    KeyedOneInputStreamOperatorTestHarness<
            ByteBuffer, WindowedValue<KV<Integer, String>>, WindowedValue<String>>
        testHarness = keyedHarness(operator);
    try {
      testHarness.open();

      // wrapper -> subclass runner -> stateful runner -> ... -> DoFn
      assertThat(wrapper.wrappedRunners, contains(instanceOf(EnterExitRunner.class)));
      EnterExitRunner<?, ?> subclassRunner = (EnterExitRunner<?, ?>) wrapper.wrappedRunners.get(0);
      assertThat(subclassRunner.tag, is("subclass"));
      assertThat(subclassRunner.delegate, instanceOf(StatefulDoFnRunner.class));

      IntervalWindow window = new IntervalWindow(new Instant(0), Duration.millis(10));
      testHarness.processWatermark(0);
      testHarness.processElement(
          new StreamRecord<>(
              WindowedValue.of(KV.of(1, "a"), new Instant(1), window, PaneInfo.NO_FIRING)));
      String element = "processElement " + KV.of(1, "a");
      assertThat(
          EVENTS.stream().filter(event -> event.contains(element)).collect(Collectors.toList()),
          contains(
              "wrapper enter " + element,
              "subclass enter " + element,
              "DoFn " + element,
              "subclass exit " + element,
              "wrapper exit " + element));

      testHarness.processWatermark(20);
      String timer = "onTimer " + TimerDeclaration.PREFIX + OrderedFn.TIMER_ID;
      assertThat(
          EVENTS.stream().filter(event -> event.contains(timer)).collect(Collectors.toList()),
          contains(
              "wrapper enter " + timer,
              "subclass enter " + timer,
              "DoFn " + timer,
              "subclass exit " + timer,
              "wrapper exit " + timer));
    } finally {
      testHarness.close();
    }
  }

  @Test
  public void earlyBindingOfKeyedStateIsTheSameWithAndWithoutWrappers() throws Exception {
    BoundState wrapped = statesBoundAtOpen(true, false);
    BoundState wrappedBuffered = statesBoundAtOpen(true, true);
    BoundState plain = statesBoundAtOpen(false, false);
    BoundState plainBuffered = statesBoundAtOpen(false, true);

    // The StatefulDoFnRunner's system state: the sort buffer and its minimum timestamp.
    List<String> systemStateIds = wrapped.systemStateIds;
    assertThat(systemStateIds, is(Arrays.asList("sortBuffer", "sortBufferMinStamp")));
    assertThat(wrappedBuffered.systemStateIds, is(systemStateIds));

    // Without stable-input buffering, the user state and the system state are bound at open.
    assertThat(plain.bound, hasItem("state"));
    assertTrue(plain.toString(), plain.bound.containsAll(systemStateIds));
    assertThat(wrapped.bound, is(plain.bound));

    // With buffering, the existing check sees the BufferingDoFnRunner and binds only the user
    // state (upstream behavior); a wrapper keeps that.
    assertThat(plainBuffered.bound, hasItem("state"));
    for (String id : systemStateIds) {
      assertThat(plainBuffered.bound, not(hasItem(id)));
    }
    assertThat(wrappedBuffered.bound, is(plainBuffered.bound));
  }

  @Test
  public void wrappersAreDiscoveredThroughTheBeamClassLoader() throws Exception {
    ClassLoader beamLoader =
        new IsolatedBeamClassLoader(servicesDescriptor(ServiceLoadedWrapper.class));
    assertThat(
        Class.forName(DoFnRunnerWrappers.class.getName(), false, beamLoader),
        not(sameInstance(DoFnRunnerWrappers.class)));

    List<?> wrappers = getWrappersIn(beamLoader);
    assertThat(wrappers.size(), is(1));
    assertThat(wrappers.get(0).getClass().getName(), is(ServiceLoadedWrapper.class.getName()));
    assertThat(wrappers.get(0).getClass().getClassLoader(), sameInstance(beamLoader));
    // Loaded once per Beam class loader.
    assertThat(getWrappersIn(beamLoader), sameInstance(wrappers));
  }

  @Test
  public void wrapperVisibleOnlyToTheThreadContextClassLoaderIsNotLoaded() throws Exception {
    // Beam in a parent class loader, the wrapper only in the user class loader: not supported.
    ClassLoader beamLoader = new IsolatedBeamClassLoader(null);
    URL userClassPath = servicesDescriptor(ServiceLoadedWrapper.class);
    ClassLoader userLoader = new URLClassLoader(new URL[] {userClassPath}, beamLoader);
    assertTrue(userLoader.getResources(SERVICES_FILE).hasMoreElements());

    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    thread.setContextClassLoader(userLoader);
    try {
      assertThat(getWrappersIn(beamLoader).size(), is(0));
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  @Test
  public void wrapperThatFailsToConstructFailsFast() throws Exception {
    ClassLoader beamLoader =
        new IsolatedBeamClassLoader(servicesDescriptor(FailingToConstructWrapper.class));

    InvocationTargetException failure =
        assertThrows(InvocationTargetException.class, () -> getWrappersIn(beamLoader));
    assertThat(failure.getCause(), instanceOf(ServiceConfigurationError.class));
    // Not cached as "no wrappers": the next operator open() fails the same way.
    assertThrows(InvocationTargetException.class, () -> getWrappersIn(beamLoader));
  }

  private static DoFnRunnerWrapper.Context context(DoFnRunnerWrapper.Kind kind) {
    return DoFnRunnerWrappers.context("stepName", FlinkPipelineOptions.defaults(), kind);
  }

  private static final String SERVICES_FILE =
      "META-INF/services/" + DoFnRunnerWrapper.class.getName();

  /** Returns a class path directory whose services file lists {@code provider}. */
  private URL servicesDescriptor(Class<? extends DoFnRunnerWrapper> provider) throws IOException {
    File classPath = temporaryFolder.newFolder();
    File servicesFile = new File(classPath, SERVICES_FILE);
    Files.createDirectories(servicesFile.getParentFile().toPath());
    Files.write(servicesFile.toPath(), provider.getName().getBytes(StandardCharsets.UTF_8));
    return classPath.toURI().toURL();
  }

  private static List<?> getWrappersIn(ClassLoader beamLoader) throws Exception {
    Method getWrappers =
        Class.forName(DoFnRunnerWrappers.class.getName(), true, beamLoader)
            .getDeclaredMethod("getWrappers");
    getWrappers.setAccessible(true);
    return (List<?>) getWrappers.invoke(null);
  }

  /** The keyed state bound by a stateful operator's open(), before any element. */
  private static BoundState statesBoundAtOpen(boolean withWrapper, boolean withBuffering)
      throws Exception {
    RecordingWrapper wrapper = new RecordingWrapper();
    DoFnRunnerWrappers.setForTesting(
        withWrapper ? Collections.singletonList(wrapper) : emptyList());
    // @RequiresStableInput needs checkpointing; set it in every case so only buffering differs.
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setCheckpointingInterval(1L);
    DoFnOperator<KV<Integer, String>, String> operator =
        keyedOperator(
            withBuffering ? new StableStatefulFn() : new StatefulTimerFn(), options, false);
    KeyedOneInputStreamOperatorTestHarness<
            ByteBuffer, WindowedValue<KV<Integer, String>>, WindowedValue<String>>
        testHarness = keyedHarness(operator);
    try {
      testHarness.open();
      assertThat(operator.bufferingDoFnRunner != null, is(withBuffering));
      Map<String, ?> registeredStates =
          Whitebox.getInternalState(operator.getKeyedStateBackend(), "registeredKVStates");
      List<String> systemStateIds = null;
      if (withWrapper) {
        assertThat(wrapper.wrappedRunners, contains(instanceOf(StatefulDoFnRunner.class)));
        systemStateIds =
            ((StatefulDoFnRunner<?, ?, ?>) wrapper.wrappedRunners.get(0))
                .getSystemStateTags().stream().map(StateTag::getId).collect(Collectors.toList());
      } else {
        assertThat(wrapper.contexts, empty());
      }
      return new BoundState(new HashSet<>(registeredStates.keySet()), systemStateIds);
    } finally {
      testHarness.close();
      DoFnRunnerWrappers.resetForTesting();
    }
  }

  private static class BoundState {
    final Set<String> bound;
    final @Nullable List<String> systemStateIds;

    BoundState(Set<String> bound, @Nullable List<String> systemStateIds) {
      this.bound = bound;
      this.systemStateIds = systemStateIds;
    }

    @Override
    public String toString() {
      return "BoundState{bound=" + bound + ", systemStateIds=" + systemStateIds + "}";
    }
  }

  private static KeySelector<WindowedValue<KV<Integer, String>>, ByteBuffer> keySelector() {
    return element -> FlinkKeyUtils.encodeKey(element.getValue().getKey(), VarIntCoder.of());
  }

  /** A keyed operator on 10 ms fixed windows, optionally a subclass that adds its own runner. */
  private static DoFnOperator<KV<Integer, String>, String> keyedOperator(
      DoFn<KV<Integer, String>, String> fn,
      FlinkPipelineOptions options,
      boolean withSubclassRunner) {
    WindowingStrategy<Object, IntervalWindow> windowingStrategy =
        WindowingStrategy.of(FixedWindows.of(Duration.millis(10)));
    Coder<IntervalWindow> windowCoder = windowingStrategy.getWindowFn().windowCoder();
    Coder<WindowedValue<KV<Integer, String>>> inputCoder =
        WindowedValue.getFullCoder(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()), windowCoder);
    TupleTag<String> outputTag = new TupleTag<>("main-output");
    DoFnOperator.MultiOutputOutputManagerFactory<String> outputManagerFactory =
        new DoFnOperator.MultiOutputOutputManagerFactory<>(
            outputTag,
            WindowedValue.getFullCoder(StringUtf8Coder.of(), windowCoder),
            new SerializablePipelineOptions(options));
    if (withSubclassRunner) {
      return new SubclassRunnerOperator<>(
          fn,
          inputCoder,
          outputTag,
          outputManagerFactory,
          windowingStrategy,
          options,
          VarIntCoder.of(),
          keySelector());
    }
    return new DoFnOperator<>(
        fn,
        "stepName",
        inputCoder,
        emptyMap(),
        outputTag,
        emptyList(),
        outputManagerFactory,
        windowingStrategy,
        emptyMap(), /* side-input mapping */
        emptyList(), /* side inputs */
        options,
        VarIntCoder.of(),
        keySelector(),
        DoFnSchemaInformation.create(),
        emptyMap());
  }

  private static KeyedOneInputStreamOperatorTestHarness<
          ByteBuffer, WindowedValue<KV<Integer, String>>, WindowedValue<String>>
      keyedHarness(DoFnOperator<KV<Integer, String>, String> operator) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator,
        keySelector(),
        new CoderTypeInformation<>(
            FlinkKeyUtils.ByteBufferCoder.of(), FlinkPipelineOptions.defaults()));
  }

  private static DoFnOperator<String, String> statelessOperator(
      DoFn<String, String> fn, FlinkPipelineOptions options) {
    TupleTag<String> outputTag = new TupleTag<>("main-output");
    return new DoFnOperator<>(
        fn,
        "stepName",
        WindowedValue.getValueOnlyCoder(StringUtf8Coder.of()),
        emptyMap(),
        outputTag,
        emptyList(),
        new DoFnOperator.MultiOutputOutputManagerFactory<>(
            outputTag,
            WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE),
            new SerializablePipelineOptions(options)),
        WindowingStrategy.globalDefault(),
        emptyMap(), /* side-input mapping */
        emptyList(), /* side inputs */
        options,
        null,
        null,
        DoFnSchemaInformation.create(),
        emptyMap());
  }

  private static class IdentityFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(context.element());
    }
  }

  private static class StableInputFn extends DoFn<String, String> {
    @ProcessElement
    @RequiresStableInput
    public void processElement(ProcessContext context) {
      context.output(context.element());
    }
  }

  private static class StatefulTimerFn extends DoFn<KV<Integer, String>, String> {
    static final String TIMER_ID = "timer";

    @SuppressWarnings("unused")
    @StateId("state")
    private final StateSpec<ValueState<String>> stateSpec = StateSpecs.value(StringUtf8Coder.of());

    @SuppressWarnings("unused")
    @TimerId(TIMER_ID)
    private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void processElement(
        ProcessContext context,
        @StateId("state") ValueState<String> state,
        @TimerId(TIMER_ID) Timer timer) {
      state.write(context.element().getValue());
      timer.set(new Instant(5));
    }

    @OnTimer(TIMER_ID)
    public void onTimer(OnTimerContext context) {
      context.output("timer");
    }
  }

  /** Like beam-runner's DlqDoFnOperator: adds its own runner in createWrappingDoFnRunner. */
  private static class SubclassRunnerOperator<InputT, OutputT>
      extends DoFnOperator<InputT, OutputT> {
    SubclassRunnerOperator(
        DoFn<InputT, OutputT> doFn,
        Coder<WindowedValue<InputT>> inputCoder,
        TupleTag<OutputT> mainOutputTag,
        OutputManagerFactory<OutputT> outputManagerFactory,
        WindowingStrategy<?, ?> windowingStrategy,
        PipelineOptions options,
        Coder<?> keyCoder,
        KeySelector<WindowedValue<InputT>, ?> keySelector) {
      super(
          doFn,
          "stepName",
          inputCoder,
          emptyMap(),
          mainOutputTag,
          emptyList(),
          outputManagerFactory,
          windowingStrategy,
          Collections.<Integer, PCollectionView<?>>emptyMap(),
          Collections.<PCollectionView<?>>emptyList(),
          options,
          keyCoder,
          keySelector,
          DoFnSchemaInformation.create(),
          emptyMap());
    }

    @Override
    protected DoFnRunner<InputT, OutputT> createWrappingDoFnRunner(
        DoFnRunner<InputT, OutputT> wrappedRunner, StepContext stepContext) {
      return new EnterExitRunner<>(
          "subclass", super.createWrappingDoFnRunner(wrappedRunner, stepContext));
    }
  }

  private static class EnterExitWrapper implements DoFnRunnerWrapper {
    final List<DoFnRunner<?, ?>> wrappedRunners = new CopyOnWriteArrayList<>();

    @Override
    public <InputT, OutputT> DoFnRunner<InputT, OutputT> wrap(
        DoFnRunner<InputT, OutputT> runner, Context context) {
      wrappedRunners.add(runner);
      return new EnterExitRunner<>("wrapper", runner);
    }
  }

  /** Records {@code "<tag> enter <call>"} and {@code "<tag> exit <call>"} in {@link #EVENTS}. */
  private static class EnterExitRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {
    final String tag;
    final DoFnRunner<InputT, OutputT> delegate;

    EnterExitRunner(String tag, DoFnRunner<InputT, OutputT> delegate) {
      this.tag = tag;
      this.delegate = delegate;
    }

    private void around(String call, Runnable body) {
      EVENTS.add(tag + " enter " + call);
      body.run();
      EVENTS.add(tag + " exit " + call);
    }

    @Override
    public void startBundle() {
      around("startBundle", delegate::startBundle);
    }

    @Override
    public void processElement(WindowedValue<InputT> elem) {
      around("processElement " + elem.getValue(), () -> delegate.processElement(elem));
    }

    @Override
    public <KeyT> void onTimer(
        String timerId,
        String timerFamilyId,
        KeyT key,
        BoundedWindow window,
        Instant timestamp,
        Instant outputTimestamp,
        TimeDomain timeDomain) {
      around(
          "onTimer " + timerId,
          () ->
              delegate.onTimer(
                  timerId, timerFamilyId, key, window, timestamp, outputTimestamp, timeDomain));
    }

    @Override
    public void finishBundle() {
      around("finishBundle", delegate::finishBundle);
    }

    @Override
    public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
      around("onWindowExpiration", () -> delegate.onWindowExpiration(window, timestamp, key));
    }

    @Override
    public DoFn<InputT, OutputT> getFn() {
      return delegate.getFn();
    }
  }

  /** Records its element and timer calls in {@link #EVENTS}. */
  private static class OrderedFn extends DoFn<KV<Integer, String>, String> {
    static final String TIMER_ID = "timer";

    @SuppressWarnings("unused")
    @TimerId(TIMER_ID)
    private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void processElement(ProcessContext context, @TimerId(TIMER_ID) Timer timer) {
      EVENTS.add("DoFn processElement " + context.element());
      timer.set(new Instant(5));
    }

    @OnTimer(TIMER_ID)
    public void onTimer() {
      EVENTS.add("DoFn onTimer " + TimerDeclaration.PREFIX + TIMER_ID);
    }
  }

  private static class StableStatefulFn extends DoFn<KV<Integer, String>, String> {
    @SuppressWarnings("unused")
    @StateId("state")
    private final StateSpec<ValueState<String>> stateSpec = StateSpecs.value(StringUtf8Coder.of());

    @ProcessElement
    @RequiresStableInput
    public void processElement(ProcessContext context, @StateId("state") ValueState<String> state) {
      state.write(context.element().getValue());
    }
  }

  /** Listed in a services file only on an isolated class loader. */
  public static class ServiceLoadedWrapper implements DoFnRunnerWrapper {
    @Override
    public <InputT, OutputT> DoFnRunner<InputT, OutputT> wrap(
        DoFnRunner<InputT, OutputT> runner, Context context) {
      return runner;
    }
  }

  /** Listed in a services file only on an isolated class loader. */
  public static class FailingToConstructWrapper implements DoFnRunnerWrapper {
    public FailingToConstructWrapper() {
      throw new IllegalStateException("cannot construct");
    }

    @Override
    public <InputT, OutputT> DoFnRunner<InputT, OutputT> wrap(
        DoFnRunner<InputT, OutputT> runner, Context context) {
      return runner;
    }
  }

  /**
   * Stands for the class loader that loaded Beam: defines its own copy of the wrapper API and of
   * the test providers, delegates every other class to the test class loader, and optionally serves
   * a services file. So the copy's static wrapper cache starts empty and loads for real.
   */
  private static class IsolatedBeamClassLoader extends ClassLoader {
    private static final String API_PREFIX = DoFnRunnerWrapper.class.getName();
    private static final String TEST_PREFIX = DoFnRunnerWrapperTest.class.getName();

    private final @Nullable URL classPathWithServicesFile;

    IsolatedBeamClassLoader(@Nullable URL classPathWithServicesFile) {
      super(DoFnRunnerWrapperTest.class.getClassLoader());
      this.classPathWithServicesFile = classPathWithServicesFile;
    }

    private static boolean isolated(String name) {
      return (name.startsWith(API_PREFIX) && !name.startsWith(TEST_PREFIX))
          || name.equals(ServiceLoadedWrapper.class.getName())
          || name.equals(FailingToConstructWrapper.class.getName());
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (!isolated(name)) {
        return super.loadClass(name, resolve);
      }
      synchronized (getClassLoadingLock(name)) {
        Class<?> loadedClass = findLoadedClass(name);
        if (loadedClass == null) {
          byte[] bytes = readClass(name);
          loadedClass = defineClass(name, bytes, 0, bytes.length);
        }
        if (resolve) {
          resolveClass(loadedClass);
        }
        return loadedClass;
      }
    }

    private byte[] readClass(String name) throws ClassNotFoundException {
      try (InputStream in = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
        if (in == null) {
          throw new ClassNotFoundException(name);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        for (int n = in.read(buffer); n >= 0; n = in.read(buffer)) {
          out.write(buffer, 0, n);
        }
        return out.toByteArray();
      } catch (IOException e) {
        throw new ClassNotFoundException(name, e);
      }
    }

    @Override
    protected Enumeration<URL> findResources(String name) throws IOException {
      if (classPathWithServicesFile != null && name.equals(SERVICES_FILE)) {
        return Collections.enumeration(
            Collections.singletonList(new URL(classPathWithServicesFile, SERVICES_FILE)));
      }
      return Collections.emptyEnumeration();
    }
  }

  private static class RangeFn extends DoFn<byte[], Long> {
    @ProcessElement
    public void processElement(
        RestrictionTracker<OffsetRange, Long> tracker, OutputReceiver<Long> receiver) {
      for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {
        receiver.output(i);
      }
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction() {
      return new OffsetRange(0, 3);
    }
  }

  /** Records what it wraps and every call made to the runners it returns. */
  private static class RecordingWrapper implements DoFnRunnerWrapper {
    final List<Context> contexts = new CopyOnWriteArrayList<>();
    final List<DoFnRunner<?, ?>> wrappedRunners = new CopyOnWriteArrayList<>();
    /** Calls as {@code "<kind> <call>"}. */
    final List<String> calls = new CopyOnWriteArrayList<>();
    /** Calls as {@code "<step name> <call>"}. */
    final List<String> stepCalls = new CopyOnWriteArrayList<>();

    @Override
    public <InputT, OutputT> DoFnRunner<InputT, OutputT> wrap(
        DoFnRunner<InputT, OutputT> runner, Context context) {
      contexts.add(context);
      wrappedRunners.add(runner);
      return new RecordingRunner<>(
          runner,
          call -> {
            calls.add(context.getKind() + " " + call);
            stepCalls.add(context.getStepName() + " " + call);
          });
    }
  }

  private static class RecordingRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {
    private final DoFnRunner<InputT, OutputT> delegate;
    private final Consumer<String> calls;

    RecordingRunner(DoFnRunner<InputT, OutputT> delegate, Consumer<String> calls) {
      this.delegate = delegate;
      this.calls = calls;
    }

    @Override
    public void startBundle() {
      calls.accept("startBundle");
      delegate.startBundle();
    }

    @Override
    public void processElement(WindowedValue<InputT> elem) {
      calls.accept("processElement " + elem.getValue());
      delegate.processElement(elem);
    }

    @Override
    public <KeyT> void onTimer(
        String timerId,
        String timerFamilyId,
        KeyT key,
        BoundedWindow window,
        Instant timestamp,
        Instant outputTimestamp,
        TimeDomain timeDomain) {
      calls.accept("onTimer " + timerId);
      delegate.onTimer(timerId, timerFamilyId, key, window, timestamp, outputTimestamp, timeDomain);
    }

    @Override
    public void finishBundle() {
      calls.accept("finishBundle");
      delegate.finishBundle();
    }

    @Override
    public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
      calls.accept("onWindowExpiration");
      delegate.onWindowExpiration(window, timestamp, key);
    }

    @Override
    public DoFn<InputT, OutputT> getFn() {
      return delegate.getFn();
    }
  }

  /** A runner that only records its tag and what it wraps. */
  private static class TaggingRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {
    final String tag;
    final DoFnRunner<InputT, OutputT> delegate;

    TaggingRunner(String tag, DoFnRunner<InputT, OutputT> delegate) {
      this.tag = tag;
      this.delegate = delegate;
    }

    @Override
    public void startBundle() {}

    @Override
    public void processElement(WindowedValue<InputT> elem) {}

    @Override
    public <KeyT> void onTimer(
        String timerId,
        String timerFamilyId,
        KeyT key,
        BoundedWindow window,
        Instant timestamp,
        Instant outputTimestamp,
        TimeDomain timeDomain) {}

    @Override
    public void finishBundle() {}

    @Override
    public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {}

    @Override
    public DoFn<InputT, OutputT> getFn() {
      return null;
    }
  }

  private abstract static class TaggingWrapper implements DoFnRunnerWrapper {
    @Override
    public <InputT, OutputT> DoFnRunner<InputT, OutputT> wrap(
        DoFnRunner<InputT, OutputT> runner, Context context) {
      return new TaggingRunner<>(getClass().getSimpleName(), runner);
    }
  }

  private static class WrapperA extends TaggingWrapper {}

  private static class WrapperB extends TaggingWrapper {}

  private static class WrapperLow extends TaggingWrapper {
    @Override
    public int order() {
      return -1;
    }
  }

  private static class WrapperHigh extends TaggingWrapper {
    @Override
    public int order() {
      return 1;
    }
  }

  private static class NullWrapper implements DoFnRunnerWrapper {
    @Override
    public <InputT, OutputT> DoFnRunner<InputT, OutputT> wrap(
        DoFnRunner<InputT, OutputT> runner, Context context) {
      return null;
    }
  }
}
