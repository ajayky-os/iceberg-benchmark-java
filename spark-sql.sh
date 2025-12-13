export SPARK_HOME=/opt/spark
export ICEBERG_SPARK_JAR="gs://gcs-hyd-iceberg-benchmark-warehouse/jars/iceberg-spark-runtime-4.0_2.13-1.11.0-SNAPSHOT.jar,gs://gcs-hyd-iceberg-benchmark-warehouse/jars/iceberg-gcp-bundle-1.11.0-SNAPSHOT.jar"

CATALOG_NAME="gcs_prod"
TPCDS_DATA_DB="tpcds_sf100"
TPCH_DATA_DB="tpch_sf100"
RESULTS_DB="analytics_core_results"
# RESULTS_DB="fileio_results"

export POLARIS_URL="htttp://10.182.15.225:8181/api/catalog"
export CREDENTIALS="root:my-secret-password" # format: clientId:clientSecret
#export CREDENTIALS="8ea011b87b5e736d:a1ccc96791128c334c91164d19bb5bdd"
export CATALOG_NAME_IN_POLARIS="gcp_lakehouse"


# The spark-submit command
$SPARK_HOME/bin/spark-sql \
  --master spark://10.182.0.93:7077 \
  --jars $ICEBERG_SPARK_JAR \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.gcs_prod=org.apache.iceberg.spark.SparkCatalog  \
  --conf spark.sql.catalog.gcs_prod.type=hadoop  \
  --conf spark.sql.catalog.gcs_prod.warehouse=gs://gcs-hyd-iceberg-benchmark-warehouse/warehouse \
  --conf spark.sql.catalog.gcs_prod.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.sql.catalog.gcs_prod.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
  --conf spark.sql.catalog.gcs_prod.gcs.analytics.core.enabled=true \
  --conf spark.sql.catalog.gcs_prod_partitioned=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gcs_prod_partitioned.type=hadoop \
  --conf spark.sql.catalog.gcs_prod_partitioned.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
  --conf spark.sql.catalog.gcs_prod_partitioned.warehouse=gs://gcs-hyd-iceberg-benchmark-warehouse/partitioned_warehouse \
  --conf spark.sql.catalog.gcs_prod_partitioned.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.sql.catalog.gcs_prod_partitioned.gcs.analytics-core.enabled=true \
  --conf spark.hadoop.hive.cli.print.header=true \
  --conf spark.sql.iceberg.vectorization.enabled=true \
  --driver-memory 32g \
  --executor-memory 32g \
  --executor-cores 5 \
  --num-executors 29