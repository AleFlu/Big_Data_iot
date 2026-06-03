#!/bin/bash
set -e

mkdir -p /spark/checkpoints/iot_stream

exec /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory "${SPARK_DRIVER_MEMORY:-512m}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-512m}" \
  --total-executor-cores 3 \
  --conf spark.driver.host=spark-job \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.ui.enabled=false \
  --conf spark.sql.shuffle.partitions=16 \
  --conf spark.streaming.stopGracefullyOnShutdown=true \
  /spark/app/spark_stream_job.py
