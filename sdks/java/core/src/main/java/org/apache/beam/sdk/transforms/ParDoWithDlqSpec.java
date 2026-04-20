package org.apache.beam.sdk.transforms;

import javax.annotation.Nullable;

/**
 * Public accessor interface for the package-private {@link ParDoWithDlq} transform.
 *
 * <p>This interface allows the Flink runner (which lives in a different package) to
 * inspect transform properties — {@link #getFn()}, {@link #getDlqSink()},
 * {@link #getDlqFilter()} — and reference the {@link #URN} constant, without
 * needing to import the package-private implementation class.
 *
 * <p>Callers outside {@code org.apache.beam.sdk.transforms} should interact with this
 * transform only through {@link ParDo.SingleOutput#withDlq(DlqSink)} and
 * {@link ParDo.SingleOutput#withDlq(DlqSink, SerializableFunction)}.
 *
 * @param <InputT>  the input element type
 * @param <OutputT> the output element type (success path only)
 */
public interface ParDoWithDlqSpec<InputT, OutputT> {

  /** URN used by the Flink runner to identify and translate this transform. */
  String URN = "beam:transform:li:pardo_with_dlq:v1";

  /** Returns the wrapped {@link DoFn}. */
  DoFn<InputT, OutputT> getFn();

  /** Returns the {@link DlqSink} that receives failed elements. */
  DlqSink<InputT> getDlqSink();

  /**
   * Returns the optional filter predicate, or {@code null} if all exceptions are routed to
   * the DLQ.
   */
  @Nullable
  SerializableFunction<Throwable, Boolean> getDlqFilter();

  /**
   * Returns the implementation {@link PTransform} class for use in payload translator
   * registration. Accessible here because this interface is in the same package as the
   * package-private {@link ParDoWithDlq}.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static Class<? extends PTransform<?, ?>> implementationClass() {
    return (Class<? extends PTransform<?, ?>>) (Class) ParDoWithDlq.class;
  }
}
