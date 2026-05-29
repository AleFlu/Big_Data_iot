#!/bin/bash
set -e

case "$SPARK_ROLE" in
  master)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
        --host 0.0.0.0 --port 7077 --webui-port 8082
    ;;
  worker)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
        spark://spark-master:7077 \
        --cores "${SPARK_WORKER_CORES:-1}" \
        --memory "${SPARK_WORKER_MEMORY:-512m}"
    ;;
  driver)
    exec /spark/app/submit.sh
    ;;
  *)
    echo "[ERROR] SPARK_ROLE='${SPARK_ROLE}' non riconosciuto. Valori validi: master | worker | driver" >&2
    exit 1
    ;;
esac
