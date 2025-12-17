package com.google.cloud.gcs;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.AccumulableInfo;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import scala.jdk.javaapi.CollectionConverters;

public class CustomMetricListener extends SparkListener {

  private final Map<String, String> currentQueryMetrics = new ConcurrentHashMap<>();
  private final LongAdder totalCustomScanTimeMs = new LongAdder();
  private final LongAdder totalExecutorRunTime = new LongAdder();
  private final LongAdder totalExecutorCpuTime = new LongAdder();
  private final LongAdder totalExecutorGcTime = new LongAdder();
  private final LongAdder totalBatchNodeExecutorRunTime = new LongAdder();
  private final LongAdder totalBatchNodeExecutorCpuTime = new LongAdder();
  private final LongAdder totalBatchNodeExecutorGcTime = new LongAdder();

  public Map<String, String> getMetrics() {
    // Add aggregated metrics before returning
    currentQueryMetrics.put(
        "total_batch_scan_time_ms", String.valueOf(totalCustomScanTimeMs.sum()));
    currentQueryMetrics.put(
        "total_executor_run_time_ms", String.valueOf(totalExecutorRunTime.sum()));
    currentQueryMetrics.put(
        "total_executor_cpu_time_ms", String.valueOf(totalExecutorCpuTime.sum()));
    currentQueryMetrics.put("total_executor_gc_time_ms", String.valueOf(totalExecutorGcTime.sum()));
    currentQueryMetrics.put(
        "total_batch_scan_node_executor_run_time_ms",
        String.valueOf(totalBatchNodeExecutorRunTime.sum()));
    currentQueryMetrics.put(
        "total_batch_scan_node_cpu_time_ms", String.valueOf(totalBatchNodeExecutorCpuTime.sum()));
    currentQueryMetrics.put(
        "total_batch_scan_node_gc_time_ms", String.valueOf(totalBatchNodeExecutorGcTime.sum()));
    return new HashMap<>(currentQueryMetrics);
  }

  public void resetMetrics() {
    currentQueryMetrics.clear();
    totalCustomScanTimeMs.reset();
    totalExecutorRunTime.reset();
    totalExecutorCpuTime.reset();
    totalExecutorGcTime.reset();
    totalBatchNodeExecutorRunTime.reset();
    totalBatchNodeExecutorCpuTime.reset();
    totalBatchNodeExecutorGcTime.reset();
  }

  @Override
  public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {

    StageInfo info = stageCompleted.stageInfo();
    if (info == null) {
      return;
    }
    System.out.println("--- Listener: Stage Completed: id=" + info.stageId() + " ---");

    TaskMetrics taskMetrics = info.taskMetrics();
    if (taskMetrics != null) {
      totalExecutorRunTime.add(taskMetrics.executorRunTime());
      totalExecutorCpuTime.add(taskMetrics.executorCpuTime());
      totalExecutorGcTime.add(taskMetrics.jvmGCTime());
      addMetric("executorRunTime_" + info.stageId(), String.valueOf(taskMetrics.executorRunTime()));
      addMetric("executorCpuTime_" + info.stageId(), String.valueOf(taskMetrics.executorCpuTime()));
      addMetric("jvmGCTime_" + info.stageId(), String.valueOf(taskMetrics.jvmGCTime()));
      addMetric(
          "diskBytesSpilled_" + info.stageId(), String.valueOf(taskMetrics.diskBytesSpilled()));
      addMetric(
          "memoryBytesSpilled_" + info.stageId(), String.valueOf(taskMetrics.memoryBytesSpilled()));
      if (taskMetrics.shuffleReadMetrics() != null) {
        addMetric(
            "shuffleReadFetchWaitTime_" + info.stageId(),
            String.valueOf(taskMetrics.shuffleReadMetrics().fetchWaitTime()));
        addMetric(
            "shuffleReadTotalBytesRead_" + info.stageId(),
            String.valueOf(taskMetrics.shuffleReadMetrics().totalBytesRead()));
        addMetric(
            "shuffleReadRecordsRead_" + info.stageId(),
            String.valueOf(taskMetrics.shuffleReadMetrics().recordsRead()));
      }
    }
    if (info.accumulables() == null || info.accumulables().isEmpty()) {
      return;
    }
    Map<Object, AccumulableInfo> accumulables = CollectionConverters.asJava(info.accumulables());
    for (AccumulableInfo accumInfo : accumulables.values()) {
      if (accumInfo.name().isDefined() && accumInfo.value().isDefined()) {
        String name = accumInfo.name().get();
        Object value = accumInfo.value().get();
        String metricName = "accumulable_" + name.replaceAll("[^a-zA-Z0-9_]", "_");
        metricName += "_" + info.stageId();

        // Capture custom scan time
        if (!metricName.startsWith("accumulable_custom_scan_time")) {
          continue;
        }
        System.out.println("    --- accumulable_custom_scan_time=" + value.toString());
        addMetric(metricName, Long.parseLong(value.toString()));
        if (taskMetrics != null) {
          totalBatchNodeExecutorRunTime.add(taskMetrics.executorRunTime());
          totalBatchNodeExecutorCpuTime.add(taskMetrics.executorCpuTime());
          totalBatchNodeExecutorGcTime.add(taskMetrics.jvmGCTime());
        }
        try {
          long scanTime = Long.parseLong(value.toString());
          totalCustomScanTimeMs.add(scanTime);
        } catch (NumberFormatException e) {
          System.err.println("WARN: Could not parse custom_scan_time_of_batch_ value: " + value);
        }
      }
    }
  }

  private void addMetric(String key, long value) {
    if (!currentQueryMetrics.containsKey(key)) {
      currentQueryMetrics.put(key, String.valueOf(value));
      return;
    }
    long currentValue = Long.parseLong(currentQueryMetrics.get(key));
    currentQueryMetrics.put(key, String.valueOf(currentValue + value));
  }

  private void addMetric(String key, String value) {
      currentQueryMetrics.put(key, value);
  }
}
