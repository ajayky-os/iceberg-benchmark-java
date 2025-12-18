package com.google.cloud.gcs;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;

public class CustomMetricListener extends SparkListener {

  private static final Deque<StageInfo> stageInfoDeque = new LinkedBlockingDeque<>();
  private static final ConcurrentHashMap<Integer, Long> stageToExecutionId =
      new ConcurrentHashMap<>();
  private CountDownLatch latch = new CountDownLatch(1);
  private final AtomicLong capturedId = new AtomicLong(-1);

  public static Deque<StageInfo> getStageInfoDeque() {
    return stageInfoDeque;
  }

  public static ConcurrentHashMap<Integer, Long> getStageToExecutionId() {
    return stageToExecutionId;
  }

  public void reset() {
    latch = new CountDownLatch(1);
    capturedId.set(-1L);
  }

  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    String executionId = jobStart.properties().getProperty("spark.sql.execution.id");
    if (executionId == null) {
      return;
    }
    capturedId.set(Long.parseLong(executionId));
    latch.countDown();
    List<StageInfo> stages = scala.collection.JavaConverters.seqAsJavaList(jobStart.stageInfos());
    for (StageInfo stage : stages) {
      stageToExecutionId.put(stage.stageId(), Long.parseLong(executionId));
      // System.out.println("--- Listener: Mapped Stage " + stage.stageId() + " to execution_id "
      // + executionId);
    }
  }

  @Override
  public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {

    StageInfo info = stageCompleted.stageInfo();
    if (info == null) {
      return;
    }
    System.out.println(
        "--- Listener: Stage Completed: id="
            + info.stageId()
            + " execution_id="
            + stageToExecutionId.get(info.stageId())
            + " ---");
    stageInfoDeque.offer(info);
  }

  public long waitForExecutionId() {
    try {
      boolean found = latch.await(10, TimeUnit.SECONDS);
      if (found) {
        return capturedId.get();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return -1; // Timed out or failed
  }
}
