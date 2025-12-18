package com.google.cloud.gcs;

import static java.lang.Boolean.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.Deque;
import java.util.stream.Collectors;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.AccumulableInfo;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import scala.jdk.javaapi.CollectionConverters;

@Command(
    name = "IcebergBenchmark",
    mixinStandardHelpOptions = true,
    version = "1.0",
    description = "Runs TPC-DS and TPC-H benchmarks against Iceberg on GCS.")
public class IcebergBenchmark implements Runnable {

  @Option(
      names = {"--tpcds-dir"},
      required = true,
      description = "TPC-DS query directory")
  private String tpcdsDir;

  @Option(
      names = {"--tpch-dir"},
      required = true,
      description = "TPC-H query directory")
  private String tpchDir;

  @Option(
      names = {"--tpcds-data-db"},
      required = true,
      description = "TPC-DS data database")
  private String tpcdsDataDb;

  @Option(
      names = {"--tpch-data-db"},
      required = true,
      description = "TPC-H data database")
  private String tpchDataDb;

  @Option(
      names = {"--catalog-name"},
      required = true,
      description = "Iceberg catalog name")
  private String catalogName;

  @Option(
      names = {"--output-gcs-path"},
      required = true,
      description = "GCS path to write results CSV files (e.g., gs://bucket/path)")
  private String outputGcsPath;

  @Option(
      names = {"--run-id"},
      required = true,
      description = "Unique ID for this benchmark run")
  private String runId;

  private final List<Map<String, Object>> resultsBuffer = new ArrayList<>();
  private SparkSession spark;
  private com.google.cloud.gcs.CustomMetricListener listener;
  private ObjectMapper mapper = new ObjectMapper();
  private String clientType;
  private boolean analyticsCoreEnabled;

  public static void main(String[] args) {
    int exitCode = new CommandLine(new IcebergBenchmark()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public void run() {
    spark =
        SparkSession.builder()
            .appName("Iceberg GCS Benchmark Java")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.debug.maxToStringFields", 1000)
            .getOrCreate();

    spark.sparkContext().setLogLevel("WARN");

    listener = new CustomMetricListener();
    spark.sparkContext().addSparkListener(listener);
    System.out.println("--- CustomMetricListener Registered ---");

    analyticsCoreEnabled =
        parseBoolean(
            spark
                .conf()
                .get("spark.sql.catalog." + catalogName + ".gcs.analytics-core.enabled", "false"));
    clientType = "HTTP";
    if (spark
        .conf()
        .get("spark.sql.catalog." + catalogName + ".gcs.client.type", "HTTP_CLIENT")
        .equals("GRPC_CLIENT")) {
      clientType = "GRPC";
    }

    try {
      runBenchmark("TPC-DS", tpcdsDir, tpcdsDataDb, catalogName);
      runBenchmark("TPC-H", tpchDir, tpchDataDb, catalogName);

      flushResultsToCsv(outputGcsPath);
    } finally {
      spark.sparkContext().removeSparkListener(listener);
      spark.stop();
    }
  }

  private StructType getResultsSchema() {
    return DataTypes.createStructType(
        new StructField[] {
          DataTypes.createStructField("run_id", DataTypes.StringType, false),
          DataTypes.createStructField("schema_size", DataTypes.StringType, true),
          DataTypes.createStructField("benchmark_type", DataTypes.StringType, true),
          DataTypes.createStructField("query_name", DataTypes.StringType, true),
          DataTypes.createStructField("execution_time_sec", DataTypes.DoubleType, true),
          DataTypes.createStructField("status", DataTypes.StringType, true),
          DataTypes.createStructField("error_message", DataTypes.StringType, true),
          DataTypes.createStructField("metrics_json", DataTypes.StringType, true),
          DataTypes.createStructField("analytics_core_enabled", DataTypes.BooleanType, true),
          DataTypes.createStructField("client_type", DataTypes.StringType, true),
          DataTypes.createStructField("total_batch_scan_time_ms", DataTypes.LongType, true),
          DataTypes.createStructField("timestamp", DataTypes.TimestampType, false)
        });
  }

  private void runBenchmark(
      String benchmarkName, String queriesPath, String dbName, String catalogName) {
    System.out.println(
        " Running " + benchmarkName + " benchmark from " + queriesPath + " on " + dbName);
    spark.sql("USE " + catalogName + "." + dbName);

    Path queryDir = Paths.get(queriesPath);
    if (!Files.exists(queryDir)) {
      System.out.println("Warning: Query directory not found: " + queriesPath);
      return;
    }

    try {
      List<Path> sqlFiles =
          Files.list(queryDir)
              .filter(Files::isRegularFile)
              .filter(p -> p.toString().endsWith(".sql"))
              .sorted(Comparator.comparing(p -> p.getFileName().toString()))
              .collect(Collectors.toList());

      System.out.println("Found " + sqlFiles.size() + " queries for " + benchmarkName);

      for (Path sqlFile : sqlFiles) {
        String queryName = sqlFile.getFileName().toString();
        System.out.println("Running " + dbName + " query: " + queryName);
        String querySql = new String(Files.readAllBytes(sqlFile));
        querySql = querySql.replace("${database}", catalogName).replace("${schema}", dbName);

        String status = "SUCCESS";
        String errorMessage = null;
        long queryStartTimeMs = System.currentTimeMillis();

        try {
          listener.reset();
          spark.sql(querySql).write().format("noop").mode(SaveMode.Overwrite).save();
        } catch (Exception e) {
          status = "FAILED";
          errorMessage = e.toString().substring(0, Math.min(e.toString().length(), 2000));
          System.out.println("  -> FAILED: " + queryName + ". Error: " + errorMessage);
        }
        long queryEndTimeMs = System.currentTimeMillis();
        double durationSec = (queryEndTimeMs - queryStartTimeMs) / 1000.0;
        long executionId = listener.waitForExecutionId();

        Map<String, Object> result = new HashMap<>();
        result.put("run_id", runId);
        result.put("schema_size", dbName);
        result.put("benchmark_type", benchmarkName);
        result.put("query_name", queryName);
        result.put("execution_id", executionId);
        result.put("query_start_time", queryStartTimeMs);
        result.put("query_end_time", queryEndTimeMs);
        result.put("execution_time_sec", durationSec);
        result.put("status", status);
        result.put("error_message", errorMessage);
        result.put("analytics_core_enabled", analyticsCoreEnabled);
        result.put("client_type", clientType);
        result.put("timestamp", Timestamp.from(Instant.now()));
        resultsBuffer.add(result);
        System.out.println(
            "-> Buffered result for query="
                + queryName
                + ", status="
                + status
                + ", execution_id="
                + executionId);
      }
      System.out.println("Waiting for 10 sec to synchronize SparkListener events");
      Thread.sleep(10000);
      processStageInfoFromDeque();
    } catch (IOException | InterruptedException e) {
      System.err.println("Error listing SQL files: " + e.getMessage());
    }
  }

  private void processStageInfoFromDeque() {
    Deque<StageInfo> stageInfoDeque = CustomMetricListener.getStageInfoDeque();
    System.out.println(
        "Processing "
            + stageInfoDeque.size()
            + " completed stages from Deque for current queries...");

    while (!stageInfoDeque.isEmpty()) {
      StageInfo stageInfo = stageInfoDeque.poll();
      Long executionId = listener.getStageToExecutionId().get(stageInfo.stageId());
      if (executionId == null) {
        System.out.println(
            "  -> Warning: Stage ID " + stageInfo.stageId() + " has no execution_id.");
        continue;
      }
      Optional<Map<String, Object>> matchingResultOpt =
          resultsBuffer.stream()
              .filter(
                  result -> {
                    long queryExecutionId = ((Long) result.get("execution_id")).longValue();
                    return queryExecutionId == executionId;
                  })
              .findFirst();
      matchingResultOpt.ifPresentOrElse(
          matchingResult -> {
            @SuppressWarnings("unchecked")
            List<StageInfo> stages =
                (List<StageInfo>) matchingResult.computeIfAbsent("stages", k -> new ArrayList<>());
            stages.add(stageInfo);
          },
          () -> {
            System.out.println(
                "  -> Warning: Stage ID "
                    + stageInfo.stageId()
                    + " (execution_id "
                    + executionId
                    + ") not found in any query result buffer..");
          });
    }

    for (Map<String, Object> result : resultsBuffer) {
      updateQueryMetricFromStageInfo(result);
    }
  }

  void updateQueryMetricFromStageInfo(Map<String, Object> queryMetric) {
    if (!queryMetric.containsKey("stages")) {
      return;
    }
    @SuppressWarnings("unchecked")
    List<StageInfo> stages = (List<StageInfo>) queryMetric.get("stages");
    if (stages.isEmpty()) {
      return;
    }
    Map<String, String> metricJson = new HashMap<>();
    long total_batch_scan_time_ms = 0;
    long total_executor_run_time_ms = 0;
    long total_executor_cpu_time_ms = 0;
    long total_executor_gc_time_ms = 0;
    long total_batch_scan_node_executor_run_time_ms = 0;
    long total_batch_scan_node_cpu_time_ms = 0;
    long total_batch_scan_node_gc_time_ms = 0;
    for (StageInfo stageInfo : stages) {
      TaskMetrics taskMetrics = stageInfo.taskMetrics();
      if (taskMetrics != null) {
        total_executor_run_time_ms += taskMetrics.executorRunTime();
        total_executor_cpu_time_ms += taskMetrics.executorCpuTime();
        total_executor_gc_time_ms += taskMetrics.jvmGCTime();
      }
      if (stageInfo.accumulables() == null || stageInfo.accumulables().isEmpty()) {
        continue;
      }
      Map<Object, AccumulableInfo> accumulables =
          CollectionConverters.asJava(stageInfo.accumulables());
      for (AccumulableInfo accumInfo : accumulables.values()) {
        if (accumInfo.name().isDefined() && accumInfo.value().isDefined()) {
          String name = accumInfo.name().get();
          Object value = accumInfo.value().get();
          String metricName = "accumulable_" + name.replaceAll("[^a-zA-Z0-9_]", "_");
          metricName += "_" + stageInfo.stageId();

          // Capture custom scan time
          if (!metricName.startsWith("accumulable_custom_scan_time")) {
            continue;
          }
          total_batch_scan_time_ms += Long.parseLong(value.toString());
          if (metricJson.containsKey(metricName)) {
            metricJson.put(
                metricName,
                String.valueOf(
                    Long.parseLong(metricJson.get(metricName)) + Long.parseLong(value.toString())));
          } else {
            metricJson.put(metricName, value.toString());
          }
          if (taskMetrics != null) {
            total_batch_scan_node_executor_run_time_ms += taskMetrics.executorRunTime();
            total_batch_scan_node_cpu_time_ms += taskMetrics.executorCpuTime();
            total_batch_scan_node_gc_time_ms += taskMetrics.jvmGCTime();
          }
        }
      }
      metricJson.put("total_executor_run_time_ms", String.valueOf(total_executor_run_time_ms));
      metricJson.put("total_executor_cpu_time_ms", String.valueOf(total_executor_cpu_time_ms));
      metricJson.put("total_executor_gc_time_ms", String.valueOf(total_executor_gc_time_ms));
      metricJson.put(
          "total_batch_scan_node_executor_run_time_ms",
          String.valueOf(total_batch_scan_node_executor_run_time_ms));
      metricJson.put(
          "total_batch_scan_node_cpu_time_ms", String.valueOf(total_batch_scan_node_cpu_time_ms));
      metricJson.put(
          "total_batch_scan_node_gc_time_ms", String.valueOf(total_batch_scan_node_gc_time_ms));

      metricJson.put(
          "gcs.analytics-core.small-file.cache.threshold-bytes",
          spark
              .conf()
              .get(
                  "spark.sql.catalog."
                      + catalogName
                      + ".gcs.analytics-core.small-file.cache.threshold-bytes",
                  "default"));
      metricJson.put("execution_id", String.valueOf(queryMetric.get("execution_id")));
      String json = "{}";
      try {
        json = mapper.writeValueAsString(metricJson);
      } catch (Exception e) {
        System.err.println("Error serializing metrics to JSON: " + e.getMessage());
      }
      queryMetric.put("metric_json", json);
      queryMetric.put("total_batch_scan_time_ms", total_batch_scan_time_ms);
    }
  }

  private List<Row> createRowsFromBuffer() {
    return resultsBuffer.stream()
        .map(
            map -> {
              return RowFactory.create(
                  map.get("run_id"),
                  map.get("schema_size"),
                  map.get("benchmark_type"),
                  map.get("query_name"),
                  map.get("execution_time_sec"),
                  map.get("status"),
                  map.get("error_message"),
                  map.get("metric_json"),
                  map.get("analytics_core_enabled"),
                  map.get("client_type"),
                  map.get("total_batch_scan_time_ms"),
                  map.get("timestamp"));
            })
        .collect(Collectors.toList());
  }

  private void flushResultsToCsv(String outputGcsPath) {
    if (resultsBuffer.isEmpty()) {
      System.out.println("No results to flush to CSV.");
      return;
    }
    System.out.println("Flushing " + resultsBuffer.size() + " results to CSV file");

    List<Row> rows = createRowsFromBuffer();
    Dataset<Row> resultsDF = spark.createDataFrame(rows, getResultsSchema());

    String finalOutputPath = outputGcsPath + "/" + runId;
    System.out.println("  -> Writing results to: " + finalOutputPath);

    resultsDF
        .repartition(1)
        .write()
        .option("header", "true")
        .option("delimiter", ",")
        .option("quoteAll", "true")
        .option("escape", "\"")
        .mode(SaveMode.Append)
        .csv(finalOutputPath);

    System.out.println("  -> Flushed " + resultsBuffer.size() + " results to " + finalOutputPath);
    resultsBuffer.clear();
  }
}
