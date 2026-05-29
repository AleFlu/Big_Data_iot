#!/bin/bash
set -e

mkdir -p /spark/checkpoints/iot_stream

exec /opt/spark/bin/spark-submit \
  --master local[4] \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.ui.enabled=false \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.streaming.stopGracefullyOnShutdown=true \
  /spark/app/spark_stream_job.py
