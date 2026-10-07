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
package org.apache.beam.runners.flink.translation.wrappers.streaming.state;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FlinkStateBinders}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class FlinkStateBindersTest {
  @Test
  public void defaultRegistrarUsesBeamEarlyBinder() {
    FlinkStateInternals.EarlyBinder binder =
        FlinkStateBinders.getEarlyBinder(
            mock(KeyedStateBackend.class),
            new SerializablePipelineOptions(PipelineOptionsFactory.as(FlinkPipelineOptions.class)),
            "step",
            GlobalWindow.Coder.INSTANCE);

    assertThat(binder, instanceOf(FlinkStateInternals.EarlyBinder.class));
  }

  @Test
  public void earlyBinderAllowsCustomStateCreation() {
    TestEarlyBinder binder =
        new TestEarlyBinder(
            mock(KeyedStateBackend.class),
            new SerializablePipelineOptions(PipelineOptionsFactory.as(FlinkPipelineOptions.class)));
    StateSpec<ValueState<String>> stateSpec = StateSpecs.value(StringUtf8Coder.of());

    binder.bindValue("test-state", stateSpec, StringUtf8Coder.of());

    assertThat(binder.createdStateNames, contains("test-state"));
  }

  private static class TestEarlyBinder extends FlinkStateInternals.EarlyBinder {
    private final List<String> createdStateNames = new ArrayList<>();

    private TestEarlyBinder(
        KeyedStateBackend keyedStateBackend, SerializablePipelineOptions pipelineOptions) {
      super(keyedStateBackend, pipelineOptions, GlobalWindow.Coder.INSTANCE);
    }

    @Override
    protected <NamespaceT, StateT extends org.apache.flink.api.common.state.State, T>
        StateT getOrCreateKeyedState(
            TypeSerializer<NamespaceT> namespaceSerializer,
            StateDescriptor<StateT, T> stateDescriptor) {
      createdStateNames.add(stateDescriptor.getName());
      return null;
    }
  }
}
