#!/bin/bash

echo "=== Hadoop Initialization ==="

# Создание директорий
echo "Creating directories..."
mkdir -p hadoop_namenode
mkdir -p hadoop_datanode
mkdir -p hadoop_config
chmod -R 777 hadoop_namenode hadoop_datanode

# Проверка наличия конфигурационных файлов
if [ ! -f hadoop_config/core-site.xml ]; then
    echo "ERROR: hadoop_config/core-site.xml not found!"
    exit 1
fi

if [ ! -f hadoop_config/hdfs-site.xml ]; then
    echo "ERROR: hadoop_config/hdfs-site.xml not found!"
    exit 1
fi

echo "Configuration files found."

# Очистка старых данных (опционально)
read -p "Do you want to clean old Hadoop data? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Cleaning old data..."
    rm -rf hadoop_namenode/*
    rm -rf hadoop_datanode/*
fi

echo "Starting Hadoop cluster..."
docker-compose up -d namenode

echo "Waiting for NameNode to initialize (20 seconds)..."
sleep 20

echo "Starting DataNode..."
docker-compose up -d datanode

sleep 10

echo ""
echo "=== Checking Hadoop cluster status ==="
docker exec namenode hdfs dfsadmin -report

echo ""
echo "Creating HDFS directories for Airflow..."
docker exec namenode hdfs dfs -mkdir -p /user/airflow
docker exec namenode hdfs dfs -chmod 777 /user/airflow
docker exec namenode hdfs dfs -mkdir -p /tmp
docker exec namenode hdfs dfs -chmod 777 /tmp

echo ""
echo "Testing HDFS write operation..."
echo "Test file" | docker exec -i namenode hdfs dfs -put - /tmp/test.txt
docker exec namenode hdfs dfs -ls /tmp

echo ""
echo "=== Hadoop initialization completed! ==="
echo "NameNode UI: http://localhost:9870"
echo "HDFS defaultFS: hdfs://namenode:9000"