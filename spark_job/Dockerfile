FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

RUN pip install --no-cache-dir elasticsearch==7.17.9 pymongo==4.6.3

# Download Kafka and MongoDB connector JARs via Maven (curl) at build time
# Avoids Spark's broken --packages + Python script NPE on ARM64/Spark 3.5.3
RUN mkdir -p /opt/spark/jars && \
    BASE=https://repo1.maven.org/maven2 && \
    # spark-sql-kafka connector and its deps
    curl -fsSL "$BASE/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar" \
         -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSL "$BASE/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar" \
         -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSL "$BASE/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" \
         -o /opt/spark/jars/kafka-clients-3.4.1.jar && \
    curl -fsSL "$BASE/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
         -o /opt/spark/jars/commons-pool2-2.11.1.jar && \
    curl -fsSL "$BASE/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar" \
         -o /opt/spark/jars/lz4-java-1.8.0.jar && \
    curl -fsSL "$BASE/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar" \
         -o /opt/spark/jars/snappy-java-1.1.10.5.jar && \
    curl -fsSL "$BASE/org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar" \
         -o /opt/spark/jars/hadoop-client-api-3.3.4.jar && \
    curl -fsSL "$BASE/org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar" \
         -o /opt/spark/jars/hadoop-client-runtime-3.3.4.jar && \
    # mongo-spark-connector and its deps
    curl -fsSL "$BASE/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/mongo-spark-connector_2.12-10.3.0.jar" \
         -o /opt/spark/jars/mongo-spark-connector_2.12-10.3.0.jar && \
    curl -fsSL "$BASE/org/mongodb/mongodb-driver-sync/4.8.2/mongodb-driver-sync-4.8.2.jar" \
         -o /opt/spark/jars/mongodb-driver-sync-4.8.2.jar && \
    curl -fsSL "$BASE/org/mongodb/mongodb-driver-core/4.8.2/mongodb-driver-core-4.8.2.jar" \
         -o /opt/spark/jars/mongodb-driver-core-4.8.2.jar && \
    curl -fsSL "$BASE/org/mongodb/bson/4.8.2/bson-4.8.2.jar" \
         -o /opt/spark/jars/bson-4.8.2.jar && \
    curl -fsSL "$BASE/org/mongodb/bson-record-codec/4.8.2/bson-record-codec-4.8.2.jar" \
         -o /opt/spark/jars/bson-record-codec-4.8.2.jar && \
    echo "All JARs downloaded: $(ls /opt/spark/jars/ | grep -E '(kafka|mongo)' | wc -l) kafka+mongo JARs"

# Create app dir with spark ownership
RUN mkdir -p /spark/app && chown spark:spark /spark/app

COPY spark_stream_job.py submit.sh entrypoint.sh /spark/app/
RUN chmod +x /spark/app/submit.sh /spark/app/entrypoint.sh \
    && chown spark:spark /spark/app/spark_stream_job.py /spark/app/submit.sh /spark/app/entrypoint.sh

USER spark
WORKDIR /spark/app
ENTRYPOINT ["/spark/app/entrypoint.sh"]
