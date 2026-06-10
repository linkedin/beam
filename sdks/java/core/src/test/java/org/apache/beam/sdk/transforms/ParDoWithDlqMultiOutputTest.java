package org.apache.beam.sdk.transforms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link ParDoWithDlqMultiOutput} and the {@link ParDo.MultiOutput#withDlq}
 * factory methods.
 *
 * <p>These tests cover construction, field accessors, and the URN contract. Pipeline execution
 * tests (which require the Flink runner) live in {@code beam-runner/runtime-flink} alongside the
 * translator. The DLQ-side contract for {@link FailedRecord} and {@link DlqSink} is exercised by
 * {@link ParDoWithDlqTest}; this file focuses on the multi-output-specific surface.
 */
@RunWith(JUnit4.class)
public class ParDoWithDlqMultiOutputTest implements Serializable {

  // ---------------------------------------------------------------------------
  // Test helpers
  // ---------------------------------------------------------------------------

  /** A trivial multi-output DoFn used as a stand-in for user logic. */
  static class PassThroughMultiOutputFn extends DoFn<Integer, Integer> {
    @ProcessElement
    public void processElement(@Element Integer input, OutputReceiver<Integer> out) {
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
  // ParDoWithDlqMultiOutput construction
  // ---------------------------------------------------------------------------

  @Test
  public void testWithDlqReturnedFromMultiOutput() {
    TupleTag<Integer> mainTag = new TupleTag<Integer>("main") {};
    CapturingDlqSink<Integer> sink = new CapturingDlqSink<>();
    assertNotNull(
        ParDo.of(new PassThroughMultiOutputFn())
            .withOutputTags(mainTag, TupleTagList.empty())
            .withDlq(sink));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetFnReturnsSameFn() {
    PassThroughMultiOutputFn fn = new PassThroughMultiOutputFn();
    TupleTag<Integer> mainTag = new TupleTag<Integer>("main") {};
    ParDoWithDlqMultiOutputSpec<Integer> transform =
        (ParDoWithDlqMultiOutputSpec<Integer>) ParDo.of(fn)
            .withOutputTags(mainTag, TupleTagList.empty())
            .withDlq(new CapturingDlqSink<>());
    assertSame(fn, transform.getFn());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetMainOutputTagReturnsSameTag() {
    TupleTag<Integer> mainTag = new TupleTag<Integer>("main") {};
    ParDoWithDlqMultiOutputSpec<Integer> transform =
        (ParDoWithDlqMultiOutputSpec<Integer>) ParDo.of(new PassThroughMultiOutputFn())
            .withOutputTags(mainTag, TupleTagList.empty())
            .withDlq(new CapturingDlqSink<>());
    assertSame(mainTag, transform.getMainOutputTag());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetAdditionalOutputTagsReturnsSameTags() {
    TupleTag<Integer> mainTag = new TupleTag<Integer>("main") {};
    TupleTag<String> sideA = new TupleTag<String>("sideA") {};
    TupleTag<String> sideB = new TupleTag<String>("sideB") {};
    TupleTagList additional = TupleTagList.of(sideA).and(sideB);
    ParDoWithDlqMultiOutputSpec<Integer> transform =
        (ParDoWithDlqMultiOutputSpec<Integer>) ParDo.of(new PassThroughMultiOutputFn())
            .withOutputTags(mainTag, additional)
            .withDlq(new CapturingDlqSink<>());
    assertEquals(2, transform.getAdditionalOutputTags().size());
    assertTrue(transform.getAdditionalOutputTags().getAll().contains(sideA));
    assertTrue(transform.getAdditionalOutputTags().getAll().contains(sideB));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetSideInputsIsEmptyByDefault() {
    TupleTag<Integer> mainTag = new TupleTag<Integer>("main") {};
    ParDoWithDlqMultiOutputSpec<Integer> transform =
        (ParDoWithDlqMultiOutputSpec<Integer>) ParDo.of(new PassThroughMultiOutputFn())
            .withOutputTags(mainTag, TupleTagList.empty())
            .withDlq(new CapturingDlqSink<>());
    assertNotNull(transform.getSideInputs());
    assertTrue(transform.getSideInputs().isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetDlqSinkReturnsSameSink() {
    TupleTag<Integer> mainTag = new TupleTag<Integer>("main") {};
    CapturingDlqSink<Integer> sink = new CapturingDlqSink<>();
    ParDoWithDlqMultiOutputSpec<Integer> transform =
        (ParDoWithDlqMultiOutputSpec<Integer>) ParDo.of(new PassThroughMultiOutputFn())
            .withOutputTags(mainTag, TupleTagList.empty())
            .withDlq(sink);
    assertSame(sink, transform.getDlqSink());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDlqFilterIsNullByDefault() {
    TupleTag<Integer> mainTag = new TupleTag<Integer>("main") {};
    ParDoWithDlqMultiOutputSpec<Integer> transform =
        (ParDoWithDlqMultiOutputSpec<Integer>) ParDo.of(new PassThroughMultiOutputFn())
            .withOutputTags(mainTag, TupleTagList.empty())
            .withDlq(new CapturingDlqSink<>());
    assertNull(transform.getDlqFilter());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWithDlqFilterSetsFilter() {
    TupleTag<Integer> mainTag = new TupleTag<Integer>("main") {};
    SerializableFunction<Throwable, Boolean> filter = t -> t instanceof IllegalArgumentException;
    ParDoWithDlqMultiOutputSpec<Integer> transform =
        (ParDoWithDlqMultiOutputSpec<Integer>) ParDo.of(new PassThroughMultiOutputFn())
            .withOutputTags(mainTag, TupleTagList.empty())
            .withDlq(new CapturingDlqSink<>(), filter);
    assertSame(filter, transform.getDlqFilter());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWithDlqFilterPreservesFnTagsAndSink() {
    PassThroughMultiOutputFn fn = new PassThroughMultiOutputFn();
    TupleTag<Integer> mainTag = new TupleTag<Integer>("main") {};
    TupleTag<String> sideA = new TupleTag<String>("sideA") {};
    CapturingDlqSink<Integer> sink = new CapturingDlqSink<>();
    ParDoWithDlqMultiOutputSpec<Integer> transform =
        (ParDoWithDlqMultiOutputSpec<Integer>) ParDo.of(fn)
            .withOutputTags(mainTag, TupleTagList.of(sideA))
            .withDlq(sink, t -> true);
    assertSame(fn, transform.getFn());
    assertSame(mainTag, transform.getMainOutputTag());
    assertEquals(1, transform.getAdditionalOutputTags().size());
    assertTrue(transform.getAdditionalOutputTags().getAll().contains(sideA));
    assertSame(sink, transform.getDlqSink());
  }

  @Test
  public void testUrnConstant() {
    assertEquals(
        "beam:transform:li:pardo_with_dlq_multi_output:v1", ParDoWithDlqMultiOutputSpec.URN);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithDlqRejectsNullSink() {
    TupleTag<Integer> mainTag = new TupleTag<Integer>("main") {};
    ParDo.of(new PassThroughMultiOutputFn())
        .withOutputTags(mainTag, TupleTagList.empty())
        .withDlq(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithDlqFilterRejectsNullFilter() {
    TupleTag<Integer> mainTag = new TupleTag<Integer>("main") {};
    ParDo.of(new PassThroughMultiOutputFn())
        .withOutputTags(mainTag, TupleTagList.empty())
        .withDlq(new CapturingDlqSink<>(), null);
  }
}
