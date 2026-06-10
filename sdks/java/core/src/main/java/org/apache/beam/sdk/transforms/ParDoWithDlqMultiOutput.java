package org.apache.beam.sdk.transforms;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * Package-private implementation of the multi-output DLQ-enabled ParDo transform.
 *
 * <p>Multi-output counterpart to {@link ParDoWithDlq}. Instances are created exclusively via
 * {@link ParDo.MultiOutput#withDlq(DlqSink)} or
 * {@link ParDo.MultiOutput#withDlq(DlqSink, SerializableFunction)}. Callers outside this package
 * interact with this transform through the {@link ParDoWithDlqMultiOutputSpec} interface.
 *
 * <p>The Beam pipeline graph still exposes the same {@link PCollectionTuple} the wrapped
 * {@code ParDo.MultiOutput} would produce — the DLQ side output is handled entirely within the
 * Flink runner via the registered {@link DlqSink} and is not visible in the user-facing tuple.
 *
 * @param <InputT> the input element type
 */
class ParDoWithDlqMultiOutput<InputT>
    extends PTransform<PCollection<? extends InputT>, PCollectionTuple>
    implements ParDoWithDlqMultiOutputSpec<InputT> {

  private static final List<String> ALLOWED_RUNNERS =
      ImmutableList.of(
          "org.apache.beam.runners.flink.FlinkRunner",
          "com.linkedin.beam.runners.LineageFlinkRunner");

  private final DoFn<InputT, ?> fn;
  private final TupleTag<?> mainOutputTag;
  private final TupleTagList additionalOutputTags;
  private final Map<String, PCollectionView<?>> sideInputs;
  private final DlqSink<InputT> dlqSink;
  @Nullable private final SerializableFunction<Throwable, Boolean> dlqFilter;

  ParDoWithDlqMultiOutput(
      DoFn<InputT, ?> fn,
      TupleTag<?> mainOutputTag,
      TupleTagList additionalOutputTags,
      Map<String, PCollectionView<?>> sideInputs,
      DlqSink<InputT> dlqSink,
      @Nullable SerializableFunction<Throwable, Boolean> dlqFilter) {
    this.fn = fn;
    this.mainOutputTag = mainOutputTag;
    this.additionalOutputTags = additionalOutputTags;
    this.sideInputs = sideInputs;
    this.dlqSink = dlqSink;
    this.dlqFilter = dlqFilter;
  }

  @Override
  public DoFn<InputT, ?> getFn() {
    return fn;
  }

  @Override
  public TupleTag<?> getMainOutputTag() {
    return mainOutputTag;
  }

  @Override
  public TupleTagList getAdditionalOutputTags() {
    return additionalOutputTags;
  }

  @Override
  public Map<String, PCollectionView<?>> getSideInputs() {
    return sideInputs;
  }

  @Override
  public DlqSink<InputT> getDlqSink() {
    return dlqSink;
  }

  @Override
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
        "ParDoWithDlqMultiOutput is not supported in runner: %s",
        options.getRunner().getName());
  }

  /**
   * Constructs a {@link PCollectionTuple} with one primitive output per tag (main + additional),
   * matching {@link ParDo.MultiOutput#expand}.
   *
   * <p>Sets coders on each output via the {@code CoderRegistry} (same logic as
   * {@code ParDo.MultiOutput.expand}) so that wrapping a {@code ParDo.MultiOutput} with
   * {@code .withDlq(...)} does not erase coder inference — anonymous-class DoFns continue to
   * have their output types resolved automatically.
   */
  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public PCollectionTuple expand(PCollection<? extends InputT> input) {
    PCollectionTuple outputs =
        PCollectionTuple.ofPrimitiveOutputsInternal(
            input.getPipeline(),
            TupleTagList.of(mainOutputTag).and(additionalOutputTags.getAll()),
            Collections.emptyMap(),
            input.getWindowingStrategy(),
            input.isBounded());
    CoderRegistry coderRegistry = input.getPipeline().getCoderRegistry();
    Coder<InputT> inputCoder = ((PCollection<InputT>) input).getCoder();
    for (PCollection<?> out : outputs.getAll().values()) {
      try {
        out.setCoder(
            (Coder)
                coderRegistry.getCoder(
                    out.getTypeDescriptor(), fn.getInputTypeDescriptor(), inputCoder));
      } catch (CannotProvideCoderException e) {
        // Leave coder unset for this tag — caller can .setCoder explicitly. Matches
        // ParDo.MultiOutput's expand behavior.
      }
    }
    return outputs;
  }
}
