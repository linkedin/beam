package org.apache.beam.sdk.transforms;

import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollection;

/**
 * A variant of {@link ParDo} that routes elements whose {@code @ProcessElement} handler throws to
 * a {@link DlqSink} instead of crashing the pipeline.
 *
 * <p>Created via {@link ParDo.SingleOutput#withDlq(DlqSink)}:
 *
 * <pre>{@code
 * PCollection<Output> results = input.apply(
 *     ParDo.of(new MyFn())
 *          .withDlq(myDlqSink)
 *          .withDlqFilter(e -> e instanceof MyBusinessException));
 * }</pre>
 *
 * <p>The returned {@link PCollection} contains only successfully processed elements. Failed
 * elements are written to the {@link DlqSink} by the runner — no side output or tuple tag is
 * visible to the caller.
 *
 * <p>Currently only supported in the Flink runner.
 *
 * @param <InputT>  the input element type
 * @param <OutputT> the output element type (success path only)
 */
public class ParDoWithDlq<InputT, OutputT>
    extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {

  /** URN used by the Flink runner to identify and translate this transform. */
  public static final String URN = "beam:transform:li:pardo_with_dlq:v1";

  private static final List<String> ALLOWED_RUNNERS =
      ImmutableList.of(
          "org.apache.beam.runners.flink.FlinkRunner",
          "com.linkedin.beam.runners.LineageFlinkRunner");

  private final DoFn<InputT, OutputT> fn;
  private final DlqSink<InputT> dlqSink;
  @Nullable private final SerializableFunction<Throwable, Boolean> dlqFilter;

  ParDoWithDlq(
      DoFn<InputT, OutputT> fn,
      DlqSink<InputT> dlqSink,
      @Nullable SerializableFunction<Throwable, Boolean> dlqFilter) {
    this.fn = fn;
    this.dlqSink = dlqSink;
    this.dlqFilter = dlqFilter;
  }

  /**
   * Returns a new {@link ParDoWithDlq} that additionally applies the given filter predicate.
   * Only exceptions for which the predicate returns {@code true} are routed to the DLQ; others
   * re-throw and crash the pipeline.
   */
  public ParDoWithDlq<InputT, OutputT> withDlqFilter(
      SerializableFunction<Throwable, Boolean> dlqFilter) {
    Preconditions.checkArgument(dlqFilter != null, "dlqFilter must not be null");
    return new ParDoWithDlq<>(fn, dlqSink, dlqFilter);
  }

  public DoFn<InputT, OutputT> getFn() {
    return fn;
  }

  public DlqSink<InputT> getDlqSink() {
    return dlqSink;
  }

  @Nullable
  public SerializableFunction<Throwable, Boolean> getDlqFilter() {
    return dlqFilter;
  }

  @Override
  public void validate(@Nullable PipelineOptions options) {
    if (options == null) {
      return;
    }
    Preconditions.checkState(
        ALLOWED_RUNNERS.contains(options.getRunner().getName()),
        "ParDoWithDlq is not supported in runner: %s",
        options.getRunner().getName());
  }

  /**
   * Creates a single primitive output {@link PCollection} for the success path. The DLQ path is
   * handled entirely within the Flink runner via the registered {@link DlqSink} — no side output
   * is added to the Beam pipeline graph.
   */
  @Override
  public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
    return PCollection.createPrimitiveOutputInternal(
        input.getPipeline(), input.getWindowingStrategy(), input.isBounded(), null);
  }
}
