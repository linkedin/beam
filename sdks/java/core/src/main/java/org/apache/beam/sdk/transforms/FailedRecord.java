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
package org.apache.beam.sdk.transforms;

import java.io.Serializable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.joda.time.Instant;

/**
 * Wraps a record that failed processing, carrying the original input alongside error context.
 *
 * <p>Used as the failure type in {@link ParDoWithDlq} when an element's {@code @ProcessElement}
 * handler throws an exception.
 *
 * @param <T> the input element type
 */
public final class FailedRecord<T> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final T input;
  private final String errorType;
  private final String stackTrace;
  private final Instant failedAt;

  private FailedRecord(T input, String errorType, String stackTrace, Instant failedAt) {
    this.input = input;
    this.errorType = errorType;
    this.stackTrace = stackTrace;
    this.failedAt = failedAt;
  }

  /**
   * Creates a {@link FailedRecord} from the original input, the thrown exception, and the element's
   * event-time timestamp.
   *
   * @param input the original input element that caused the failure
   * @param throwable the exception that was thrown
   * @param failedAt the event-time timestamp of the element (not wall-clock time)
   */
  public static <T> FailedRecord<T> of(T input, Throwable throwable, Instant failedAt) {
    return new FailedRecord<>(
        input,
        throwable.getClass().getName(),
        Throwables.getStackTraceAsString(throwable),
        failedAt);
  }

  /** The original input element that caused the failure. */
  public T input() {
    return input;
  }

  /**
   * Fully-qualified class name of the exception (e.g. {@code java.lang.IllegalArgumentException}).
   */
  public String errorType() {
    return errorType;
  }

  /** Full stack trace string, including the exception message and cause chain. */
  public String stackTrace() {
    return stackTrace;
  }

  /**
   * The event-time timestamp of the failed element, taken from the element's watermark position.
   * This is NOT wall-clock time.
   */
  public Instant failedAt() {
    return failedAt;
  }

  @Override
  public String toString() {
    return "FailedRecord{errorType=" + errorType + ", failedAt=" + failedAt + "}";
  }
}
