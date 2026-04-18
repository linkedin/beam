package org.apache.beam.sdk.transforms;

import java.io.Serializable;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * A sink for failed elements in a {@link ParDoWithDlq} transform.
 *
 * <p>Implementations are responsible for durably storing {@link FailedRecord}s so they can be
 * inspected or replayed later. The Flink runner calls {@link #setup} once when the operator opens,
 * {@link #write} for every failed element, and {@link #teardown} when the operator closes.
 *
 * <p><b>Contract:</b> {@link #write} must never throw. Any write failure should be caught
 * internally and logged — the element is then silently dropped from the DLQ path rather than
 * crashing the pipeline.
 *
 * @param <T> the input element type whose failures are being captured
 */
public interface DlqSink<T> extends Serializable {

  /**
   * Called once when the Flink operator opens. Use this to initialise connections or producers.
   */
  void setup(PipelineOptions options);

  /**
   * Writes a failed record to the DLQ. This method must never throw — implementations must catch
   * and suppress all exceptions internally (best-effort semantics).
   */
  void write(FailedRecord<T> record);

  /**
   * Called once when the Flink operator closes. Use this to flush and close connections.
   */
  void teardown();
}
