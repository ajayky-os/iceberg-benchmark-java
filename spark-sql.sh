export SPARK_MASTER_URL=spark://10.182.0.93:7077
export ICEBERG_SPARK_JAR="gs://gcs-hyd-iceberg-benchmark-warehouse/jars/iceberg-spark-runtime-4.0_2.13-1.11.0-SNAPSHOT.jar,gs://gcs-hyd-iceberg-benchmark-warehouse/jars/iceberg-gcp-bundle-1.11.0-SNAPSHOT.jar"
export CATALOG_NAME="gcs_prod"
export PARTITIONED_CATALOG_NAME="gcs_prod_partitioned"
export CATALOG_WAREHOUSE="gs://gcs-hyd-iceberg-benchmark-warehouse/warehouse"
export PARTITIONED_CATALOG_WAREHOUSE="gs://gcs-hyd-iceberg-benchmark-warehouse/partitioned_warehouse"

/opt/spark/bin/spark-sql \
  --master $SPARK_MASTER_URL \
  --jars $ICEBERG_SPARK_JAR \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.$CATALOG_NAME=org.apache.iceberg.spark.SparkCatalog  \
  --conf spark.sql.catalog.$CATALOG_NAME.type=hadoop  \
  --conf spark.sql.catalog.$CATALOG_NAME.warehouse=$CATALOG_WAREHOUSE \
  --conf spark.sql.catalog.$CATALOG_NAME.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.sql.catalog.$CATALOG_NAME.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
  --conf spark.sql.catalog.$CATALOG_NAME.gcs.analytics.core.enabled=true \
  --conf spark.sql.catalog.$PARTITIONED_CATALOG_NAME=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.$PARTITIONED_CATALOG_NAME.type=hadoop \
  --conf spark.sql.catalog.$PARTITIONED_CATALOG_NAME.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO \
  --conf spark.sql.catalog.$PARTITIONED_CATALOG_NAME.warehouse=$PARTITIONED_CATALOG_WAREHOUSE \
  --conf spark.sql.catalog.$PARTITIONED_CATALOG_NAME.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.sql.catalog.$PARTITIONED_CATALOG_NAME.gcs.analytics-core.enabled=true \
  --conf spark.hadoop.hive.cli.print.header=true \
  --conf spark.sql.iceberg.vectorization.enabled=true \
  --driver-memory 32g \
  --executor-memory 32g \
  --executor-cores 5 \
  --num-executors 29