package org.apache.beam.sdk.transforms;

import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Package-private implementation of the DLQ-enabled ParDo transform.
 *
 * <p>Instances are created exclusively via {@link ParDo.SingleOutput#withDlq(DlqSink)} or
 * {@link ParDo.SingleOutput#withDlq(DlqSink, SerializableFunction)}. Callers outside this package
 * interact with this transform through the {@link ParDoWithDlqSpec} interface.
 *
 * @param <InputT>  the input element type
 * @param <OutputT> the output element type (success path only)
 */
class ParDoWithDlq<InputT, OutputT>
    extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>>
    implements ParDoWithDlqSpec<InputT, OutputT> {

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

  @Override
  public DoFn<InputT, OutputT> getFn() {
    return fn;
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
        "ParDoWithDlq is not supported in runner: %s",
        options.getRunner().getName());
  }

  /**
   * Creates a single primitive output {@link PCollection} for the success path. The DLQ path is
   * handled entirely within the Flink runner via the registered {@link DlqSink} — no side output
   * is added to the Beam pipeline graph.
   *
   * <p>Mirrors {@link ParDo.SingleOutput#expand}'s schema/coder inference so the wrapping does
   * not erase output type info. Without this, callers that wrap an anonymous-class DoFn would
   * see "Unable to return a default Coder" downstream because plain ParDo's coder inference is
   * bypassed by the wrapping PTransform.
   */
  @Override
  @SuppressWarnings("unchecked")
  public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
    PCollection<OutputT> output = PCollection.createPrimitiveOutputInternal(
        input.getPipeline(), input.getWindowingStrategy(), input.isBounded(), null);
    SchemaRegistry schemaRegistry = input.getPipeline().getSchemaRegistry();
    CoderRegistry coderRegistry = input.getPipeline().getCoderRegistry();
    TypeDescriptor<OutputT> outputTypeDescriptor = fn.getOutputTypeDescriptor();
    try {
      output.setSchema(
          schemaRegistry.getSchema(outputTypeDescriptor),
          outputTypeDescriptor,
          schemaRegistry.getToRowFunction(outputTypeDescriptor),
          schemaRegistry.getFromRowFunction(outputTypeDescriptor));
    } catch (NoSuchSchemaException e) {
      try {
        output.setCoder(
            coderRegistry.getCoder(
                outputTypeDescriptor,
                fn.getInputTypeDescriptor(),
                ((PCollection<InputT>) input).getCoder()));
      } catch (CannotProvideCoderException e2) {
        // Ignore and leave coder unset — caller can still .setCoder explicitly. Matches
        // ParDo.SingleOutput's behavior.
      }
    }
    return output;
  }
}
