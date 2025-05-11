#!/bin/bash
set -e

echo "Starting Spark master in the background..."
/opt/bitnami/spark/sbin/start-master.sh

echo "Environment is ready!"
echo "Spark UI available at http://localhost:8090"
echo "Application UI available at http://localhost:4040 when an application is running"
echo ""
echo "You can run a processing job by executing:"
echo "/opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 /opt/bitnami/spark/src/data_processing/main.py process-reddit"
echo ""

# Keep the container running
tail -f /dev/null 