#!/bin/bash
set -e

$SPARK_HOME/bin/spark-submit \
  --master $SPARK_MASTER_URL \
  --jars $ICEBERG_SPARK_JAR \
  --packages ch.cern.sparkmeasure:spark-measure_2.13:0.27 \
  --conf spark.hadoop.hive.cli.print.header=true \
  --class com.google.cloud.gcs.IcebergBenchmark \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.$CATALOG_NAME=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.$CATALOG_NAME.type=hadoop \
  --conf spark.sql.catalog.$CATALOG_NAME.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
  --conf spark.sql.catalog.$CATALOG_NAME.warehouse=$CATALOG_WAREHOUSE \
  --conf spark.sql.catalog.$CATALOG_NAME.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.sql.catalog.$CATALOG_NAME.gcs.analytics-core.enabled=true \
  --conf spark.sql.iceberg.vectorization.enabled=true \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  --conf spark.history.fs.logDirectory=file:///tmp/spark-events \
  --conf spark.sql.shuffle.partitions=2000 \
  --conf spark.dynamicAllocation.enabled=false \
  --driver-memory 32g \
  --executor-memory 32g \
  --executor-cores 5 \
  --num-executors 29 \
  $BENCHMARK_JAR_PATH \
  --tpcds-dir $TPCDS_QUERIES_FOLDER \
  --tpch-dir $TPCH_QUERIES_FOLDER \
  --tpcds-data-db $TPCDS_DATA_DB \
  --tpch-data-db $TPCH_DATA_DB \
  --catalog-name $CATALOG_NAME \
  --run-id ${BENCHMARK_RUN_ID} \
  --output-gcs-path $RESULT_OUTPUT_GCS_PATH