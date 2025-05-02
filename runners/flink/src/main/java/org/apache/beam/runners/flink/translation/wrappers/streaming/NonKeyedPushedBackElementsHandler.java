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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.OperatorStateBackend;

/** {@link PushedBackElementsHandler} that stores elements in a Flink operator state list. */
class NonKeyedPushedBackElementsHandler<T> implements PushedBackElementsHandler<T> {

  static <T> NonKeyedPushedBackElementsHandler<T> create(
      OperatorStateBackend backend, ListStateDescriptor<T> stateDescriptor) {
    return new NonKeyedPushedBackElementsHandler<>(backend, stateDescriptor);
  }

  private final OperatorStateBackend backend;
  private final ListStateDescriptor<T> stateDescriptor;
  @Nullable private ListState<T> elementState;

  private NonKeyedPushedBackElementsHandler(
      OperatorStateBackend backend, ListStateDescriptor<T> stateDescriptor) {
    this.backend = checkNotNull(backend);
    this.stateDescriptor = checkNotNull(stateDescriptor);
  }

  @Override
  public Stream<T> getElements() throws Exception {
    if (elementState != null) {
      return StreamSupport.stream(elementState.get().spliterator(), false);
    } else {
      return Stream.empty();
    }
  }

  @Override
  public void clear() throws Exception {
    if (elementState != null) {
      elementState.clear();
    }
  }

  @Override
  public void pushBack(T element) throws Exception {
    if (elementState == null) {
      elementState = backend.getListState(stateDescriptor);
    }
    elementState.add(element);
  }

  @Override
  public void pushBackAll(Iterable<T> elements) throws Exception {
    if (elementState == null) {
      elementState = backend.getListState(stateDescriptor);
    }
    final ListState<T> state = elementState;
    for (T e : elements) {
      // TODO: use addAll() once Flink has addAll(Iterable<T>)
      state.add(e);
    }
  }
}
