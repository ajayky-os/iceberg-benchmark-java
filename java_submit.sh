#!/bin/bash

# Exit on error
set -e


OUTPUT_GCS_PATH=gs://gcs-hyd-iceberg-benchmark-results/


# Build the Java project
echo "--- Building Java Benchmark --- "
cd $JAVA_BENCHMARK_DIR
mvn clean package
cd ..

JAR_PATH="$(find target -name 'iceberg-benchmark-java-*-jar-with-dependencies.jar')"

if [ -z "$JAR_PATH" ]; then
    echo "Error: Benchmark JAR not found!"
    exit 1
fi
echo "--- JAR Path: $JAR_PATH ---"

export SPARK_HOME=/opt/spark
export ICEBERG_SPARK_JAR="gs://gcs-hyd-iceberg-benchmark-warehouse/jars/iceberg-spark-runtime-4.0_2.13-1.11.0-SNAPSHOT.jar,gs://gcs-hyd-iceberg-benchmark-warehouse/jars/iceberg-gcp-bundle-1.11.0-SNAPSHOT.jar"

CATALOG_NAME="gcs_prod"
TPCDS_DATA_DB="tpcds_sf10"
TPCH_DATA_DB="tpch_sf10"

echo "--- Output GCS Path: $OUTPUT_GCS_PATH ---"

# Spark-submit command (based on submit2.sh)
$SPARK_HOME/bin/spark-submit \
  --master spark://10.182.0.93:7077 \
  --jars $ICEBERG_SPARK_JAR \
  --packages ch.cern.sparkmeasure:spark-measure_2.13:0.27 \
  --conf spark.hadoop.hive.cli.print.header=true \
  --class com.google.cloud.gcs.IcebergBenchmark \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gcs_prod=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gcs_prod.type=hadoop \
  --conf spark.sql.catalog.gcs_prod.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
  --conf spark.sql.catalog.gcs_prod.warehouse=gs://gcs-hyd-iceberg-benchmark-warehouse/warehouse \
  --conf spark.sql.catalog.gcs_prod.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.sql.catalog.gcs_prod.gcs.analytics-core.enabled=true \
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
  $JAR_PATH \
  --tpcds-dir /home/ajayky_google_com/iceberg-gcs-benchmark/queries/tpcds/ \
  --tpch-dir /home/ajayky_google_com/iceberg-gcs-benchmark/queries/tpch/ \
  --tpcds-data-db $TPCDS_DATA_DB \
  --tpch-data-db $TPCH_DATA_DB \
  --catalog-name $CATALOG_NAME \
  --output-gcs-path $OUTPUT_GCS_PATH

echo "Java benchmark submission finished."
"
