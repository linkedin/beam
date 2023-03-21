package org.apache.beam.runners.samza.translation;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

public class SamzaInputGBKMetricOp<T> extends SamzaMetricOp<T> {
  private Map<BoundedWindow, BigInteger> sumOfTimestampsPerWindowId;
  private Map<BoundedWindow, Long> sumOfCountPerWindowId;

  public SamzaInputGBKMetricOp(String pValue, String transformFullName, SamzaOpMetricRegistry samzaOpMetricRegistry) {
    super(pValue, transformFullName, samzaOpMetricRegistry);
    this.sumOfTimestampsPerWindowId = new HashMap<>();
    this.sumOfCountPerWindowId = new HashMap<>();
  }

  @Override
  public void processElement(WindowedValue<T> inputElement, OpEmitter<T> emitter) {
    for (BoundedWindow windowId : inputElement.getWindows()) {
      BigInteger sumTimestampsForId = sumOfTimestampsPerWindowId.getOrDefault(windowId, BigInteger.ZERO);
      sumOfTimestampsPerWindowId.put(windowId, sumTimestampsForId.add(BigInteger.valueOf(System.nanoTime())));
      Long count = sumOfCountPerWindowId.getOrDefault(windowId, 0L);
      sumOfCountPerWindowId.put(windowId, count + 1);
    }
    samzaOpMetricRegistry.getSamzaOpMetrics().getTransformInputThroughput(transformFullName).inc();
    emitter.emitElement(inputElement);
  }

  @Override
  public void processWatermark(Instant watermark, OpEmitter<T> emitter) {
    sumOfTimestampsPerWindowId.forEach((windowId, sumOfTimestamps) -> {
      System.out.println(
          String.format("Input [%s] Processing watermark: %s for task: %s", transformFullName, watermark.getMillis(),
              taskContext.getTaskModel().getTaskName().getTaskName()));
      // if the window is closed and there is some data
      if (watermark.isAfter(windowId.maxTimestamp()) && sumOfTimestamps.compareTo(BigInteger.ZERO) > 0) {
        samzaOpMetricRegistry.updateAvgStartTimeMapGBK(transformFullName, windowId,
            Math.floorDiv(sumOfTimestamps.longValue(), sumOfCountPerWindowId.get(windowId)));
      }
      // todo: cleanup
      else {
        // Empty data case - you don't need to handle
        System.out.println(
            String.format("Input [%s] SumOfTimestamps: %s zero for watermark: %s for task: %s", transformFullName, sumOfTimestamps.longValue(), watermark.getMillis(),
                taskContext.getTaskModel().getTaskName().getTaskName()));
      }
    });
    super.processWatermark(watermark, emitter);
  }
}
