package org.apache.beam.sdk.transforms;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * Public accessor interface for the package-private {@link ParDoWithDlqMultiOutput} transform.
 *
 * <p>Multi-output counterpart to {@link ParDoWithDlqSpec}. Mirrors the relationship between
 * {@link ParDo.SingleOutput} and {@link ParDo.MultiOutput}: a {@code ParDo.MultiOutput} can be
 * wrapped with {@code .withDlq(DlqSink)} to route per-element user-code throws to the sink
 * while still emitting successful elements to each of the configured output tags.
 *
 * <p>This interface allows the Flink runner (which lives in a different package) to inspect
 * transform properties — {@link #getFn()}, {@link #getMainOutputTag()},
 * {@link #getAdditionalOutputTags()}, {@link #getSideInputs()}, {@link #getDlqSink()},
 * {@link #getDlqFilter()} — and reference the {@link #URN} constant, without needing to import
 * the package-private implementation class.
 *
 * <p>Callers outside {@code org.apache.beam.sdk.transforms} should interact with this transform
 * only through {@link ParDo.MultiOutput#withDlq(DlqSink)} and
 * {@link ParDo.MultiOutput#withDlq(DlqSink, SerializableFunction)}.
 *
 * @param <InputT> the input element type
 */
public interface ParDoWithDlqMultiOutputSpec<InputT> {

  /** URN used by the Flink runner to identify and translate this transform. */
  String URN = "beam:transform:li:pardo_with_dlq_multi_output:v1";

  /** Returns the wrapped {@link DoFn}. */
  DoFn<InputT, ?> getFn();

  /** Returns the main output tag from the wrapped {@code ParDo.MultiOutput}. */
  TupleTag<?> getMainOutputTag();

  /** Returns the additional output tags from the wrapped {@code ParDo.MultiOutput}. */
  TupleTagList getAdditionalOutputTags();

  /**
   * Returns the side inputs threaded through from the wrapped {@code ParDo.MultiOutput} (an
   * empty map when none were declared).
   */
  Map<String, PCollectionView<?>> getSideInputs();

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
   * package-private {@link ParDoWithDlqMultiOutput}.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static Class<? extends PTransform<?, ?>> implementationClass() {
    return (Class<? extends PTransform<?, ?>>) (Class) ParDoWithDlqMultiOutput.class;
  }
}
