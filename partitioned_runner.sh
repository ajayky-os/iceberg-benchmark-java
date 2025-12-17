#!/bin/bash
set -e

export TPCH_QUERIES_FOLDER=/home/ajayky_google_com/java-benchmark-iceberg/queries/tpch
export TPCDS_QUERIES_FOLDER=/home/ajayky_google_com/java-benchmark-iceberg/queries/tpcds
export JAVA_BENCHMARK_DIR=/home/ajayky_google_com/java-benchmark-iceberg
export SPARK_MASTER_URL=spark://10.182.0.93:7077
export RESULT_OUTPUT_GCS_PATH=gs://gcs-hyd-iceberg-benchmark-results/partitioned
export SPARK_HOME=/opt/spark
export ICEBERG_SPARK_JAR="gs://gcs-hyd-iceberg-benchmark-warehouse/jars/iceberg-spark-runtime-4.0_2.13-1.11.0-SNAPSHOT.jar,gs://gcs-hyd-iceberg-benchmark-warehouse/jars/iceberg-gcp-bundle-1.11.0-SNAPSHOT.jar"
export CATALOG_NAME="gcs_prod_partitioned"
export CATALOG_WAREHOUSE="gs://gcs-hyd-iceberg-benchmark-warehouse/partitioned_warehouse"

# Set unique benchmark run id
export BENCHMARK_RUN_ID=$(date +%Y%m%d%H%M%S)-$(uuidgen)
echo "BENCHMARK_RUN_ID: ${BENCHMARK_RUN_ID}"

# Compile benchmark runner
cd $JAVA_BENCHMARK_DIR
mvn clean package
export BENCHMARK_JAR_PATH="$(find $JAVA_BENCHMARK_DIR/target -name 'java-benchmarking-iceberg-*-jar-with-dependencies.jar')"
if [ -z "$BENCHMARK_JAR_PATH" ]; then
    echo "Error: Benchmark JAR not found!"
    exit 1
fi

# Analytics Core enabled with JSON client
#TPCDS_DATA_DB="tpcds_sf1" TPCH_DATA_DB="tpch_sf1" ./spark_submit_with_analytics_core.sh
#TPCDS_DATA_DB="tpcds_sf10" TPCH_DATA_DB="tpch_sf10" ./spark_submit_with_analytics_core.sh
#TPCDS_DATA_DB="tpcds_sf100" TPCH_DATA_DB="tpch_sf100" ./spark_submit_with_analytics_core.sh
TPCDS_DATA_DB="tpcds_sf1000" TPCH_DATA_DB="tpch_sf1000" ./spark_submit_with_analytics_core.sh

# Analytics core disabled
#TPCDS_DATA_DB="tpcds_sf1" TPCH_DATA_DB="tpch_sf1" ./spark_submit_without_analytics_core.sh
#TPCDS_DATA_DB="tpcds_sf10" TPCH_DATA_DB="tpch_sf10" ./spark_submit_without_analytics_core.sh
#TPCDS_DATA_DB="tpcds_sf100" TPCH_DATA_DB="tpch_sf100" ./spark_submit_without_analytics_core.sh
TPCDS_DATA_DB="tpcds_sf1000" TPCH_DATA_DB="tpch_sf1000" ./spark_submit_without_analytics_core.sh

# Analytics core enabled with GRPC client
#TPCDS_DATA_DB="tpcds_sf1" TPCH_DATA_DB="tpch_sf1" ./spark_submit_grpc_enabled.sh
#TPCDS_DATA_DB="tpcds_sf10" TPCH_DATA_DB="tpch_sf10" ./spark_submit_grpc_enabled.sh
#TPCDS_DATA_DB="tpcds_sf100" TPCH_DATA_DB="tpch_sf100" ./spark_submit_grpc_enabled.sh
#TPCDS_DATA_DB="tpcds_sf1000" TPCH_DATA_DB="tpch_sf1000" ./spark_submit_grpc_enabled.sh