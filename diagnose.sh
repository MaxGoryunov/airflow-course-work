#!/bin/bash

echo "=== Airflow Spark Diagnostics ==="
echo ""

echo "1. Spark installation in Airflow:"
docker exec airflow_webserver /opt/spark/bin/spark-submit --version 2>&1 | head -5

echo ""
echo "2. Spark Master connectivity:"
docker exec airflow_webserver ping -c 1 spark-master 2>&1

echo ""
echo "3. Spark Master port 7077:"
docker exec airflow_webserver nc -zv spark-master 7077 2>&1

echo ""
echo "4. HDFS connectivity:"
docker exec airflow_webserver hdfs dfs -ls / 2>&1 | head -5

echo ""
echo "5. Test Spark job (local mode):"
docker exec airflow_webserver /opt/spark/bin/spark-submit --master local[2] /opt/spark-apps/spark_job.py 2>&1 | head -20

echo ""
echo "6. Spark Master logs (last 20 lines):"
docker logs spark_master 2>&1 | tail -20

echo ""
echo "7. Spark Worker logs (last 20 lines):"
docker logs spark_worker 2>&1 | tail -20