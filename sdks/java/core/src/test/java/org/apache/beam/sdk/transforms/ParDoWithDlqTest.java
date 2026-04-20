package org.apache.beam.sdk.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.io.Serializable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link ParDoWithDlq} and related types.
 *
 * <p>These tests cover API construction, field accessors, and the {@link FailedRecord} /
 * {@link DlqSink} contracts. Pipeline execution tests (which require the Flink runner) live in
 * {@code beam-runner/runtime-flink}.
 */
@RunWith(JUnit4.class)
public class ParDoWithDlqTest implements Serializable {

  // ---------------------------------------------------------------------------
  // Test helpers
  // ---------------------------------------------------------------------------

  static class PassThroughFn extends DoFn<Integer, Integer> {
    @ProcessElement
    public void processElement(@Element Integer input, OutputReceiver<Integer> out) {
      out.output(input);
    }
  }

  static class ThrowForEvenFn extends DoFn<Integer, Integer> {
    @ProcessElement
    public void processElement(@Element Integer input, OutputReceiver<Integer> out) {
      if (input % 2 == 0) {
        throw new IllegalArgumentException("Even input rejected: " + input);
      }
      out.output(input);
    }
  }

  /** A no-op {@link DlqSink} that records the last written record for assertions. */
  static class CapturingDlqSink<T> implements DlqSink<T> {
    FailedRecord<T> lastRecord;
    int writeCount = 0;

    @Override
    public void setup(PipelineOptions options) {}

    @Override
    public void write(FailedRecord<T> record) {
      lastRecord = record;
      writeCount++;
    }

    @Override
    public void teardown() {}
  }

  // ---------------------------------------------------------------------------
  // ParDoWithDlq construction
  // ---------------------------------------------------------------------------

  @Test
  public void testWithDlqReturnedFromParDoOf() {
    CapturingDlqSink<Integer> sink = new CapturingDlqSink<>();
    assertNotNull(ParDo.of(new PassThroughFn()).withDlq(sink));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetFnReturnsSameFn() {
    PassThroughFn fn = new PassThroughFn();
    ParDoWithDlqSpec<Integer, Integer> transform =
        (ParDoWithDlqSpec<Integer, Integer>) ParDo.of(fn).withDlq(new CapturingDlqSink<>());
    assertSame(fn, transform.getFn());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetDlqSinkReturnsSameSink() {
    CapturingDlqSink<Integer> sink = new CapturingDlqSink<>();
    ParDoWithDlqSpec<Integer, Integer> transform =
        (ParDoWithDlqSpec<Integer, Integer>) ParDo.of(new PassThroughFn()).withDlq(sink);
    assertSame(sink, transform.getDlqSink());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDlqFilterIsNullByDefault() {
    ParDoWithDlqSpec<Integer, Integer> transform =
        (ParDoWithDlqSpec<Integer, Integer>) ParDo.of(new PassThroughFn()).withDlq(new CapturingDlqSink<>());
    assertNull(transform.getDlqFilter());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWithDlqFilterSetsFilter() {
    SerializableFunction<Throwable, Boolean> filter = t -> t instanceof IllegalArgumentException;
    ParDoWithDlqSpec<Integer, Integer> transform =
        (ParDoWithDlqSpec<Integer, Integer>) ParDo.of(new PassThroughFn())
            .withDlq(new CapturingDlqSink<>(), filter);
    assertSame(filter, transform.getDlqFilter());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWithDlqFilterPreservesFnAndSink() {
    PassThroughFn fn = new PassThroughFn();
    CapturingDlqSink<Integer> sink = new CapturingDlqSink<>();
    ParDoWithDlqSpec<Integer, Integer> transform =
        (ParDoWithDlqSpec<Integer, Integer>) ParDo.of(fn).withDlq(sink, t -> true);
    assertSame(fn, transform.getFn());
    assertSame(sink, transform.getDlqSink());
  }

  @Test
  public void testUrnConstant() {
    assertEquals("beam:transform:li:pardo_with_dlq:v1", ParDoWithDlqSpec.URN);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithDlqRejectsNullSink() {
    ParDo.of(new PassThroughFn()).withDlq(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithDlqFilterRejectsNullFilter() {
    ParDo.of(new PassThroughFn()).withDlq(new CapturingDlqSink<>(), null);
  }

  // ---------------------------------------------------------------------------
  // FailedRecord
  // ---------------------------------------------------------------------------

  @Test
  public void testFailedRecordFields() {
    Instant ts = Instant.ofEpochMilli(12345L);
    IllegalArgumentException ex = new IllegalArgumentException("bad input");
    FailedRecord<Integer> record = FailedRecord.of(42, ex, ts);

    assertEquals(Integer.valueOf(42), record.input());
    assertEquals(IllegalArgumentException.class.getName(), record.errorType());
    assertNotNull(record.stackTrace());
    org.junit.Assert.assertTrue(record.stackTrace().contains("bad input"));
    assertEquals(ts, record.failedAt());
  }

  @Test
  public void testFailedRecordCauseChainIsIncluded() {
    RuntimeException cause = new RuntimeException("root cause");
    RuntimeException wrapper = new RuntimeException("wrapper", cause);
    FailedRecord<String> record = FailedRecord.of("input", wrapper, Instant.now());
    org.junit.Assert.assertTrue(
        "Stack trace should include the cause", record.stackTrace().contains("root cause"));
  }

  // ---------------------------------------------------------------------------
  // DlqSink contract: write must not throw
  // ---------------------------------------------------------------------------

  @Test
  public void testDlqSinkWriteDoesNotThrow() {
    DlqSink<Integer> throwingSink =
        new DlqSink<Integer>() {
          @Override
          public void setup(PipelineOptions options) {}

          @Override
          public void write(FailedRecord<Integer> record) {
            // A well-behaved sink catches its own errors — simulate a failure being swallowed
            try {
              throw new RuntimeException("simulated write failure");
            } catch (RuntimeException ignored) {
              // best-effort: do not propagate
            }
          }

          @Override
          public void teardown() {}
        };

    // Calling write should not throw even if the sink itself encounters an error
    FailedRecord<Integer> record =
        FailedRecord.of(1, new IllegalStateException("test"), Instant.now());
    throwingSink.write(record); // must not throw
  }
}
