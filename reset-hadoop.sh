#!/bin/bash

echo "=== Complete Hadoop Reset ==="

# Остановка контейнеров
echo "1. Stopping containers..."
docker-compose down

# Удаление старых данных
echo "2. Removing old data..."
sudo rm -rf hadoop_namenode hadoop_datanode

# Создание новых директорий с правильными правами
echo "3. Creating fresh directories..."
mkdir -p hadoop_namenode hadoop_datanode
chmod -R 777 hadoop_namenode hadoop_datanode

# Проверка конфигурационных файлов
echo "4. Checking configuration files..."
if [ ! -f hadoop_config/core-site.xml ]; then
    echo "ERROR: hadoop_config/core-site.xml not found!"
    exit 1
fi

if [ ! -f hadoop_config/hdfs-site.xml ]; then
    echo "ERROR: hadoop_config/hdfs-site.xml not found!"
    exit 1
fi

echo "Configuration files OK."

# Запуск NameNode для форматирования
echo "5. Starting and formatting NameNode..."
docker-compose up -d namenode

echo "Waiting for NameNode to format and start (30 seconds)..."
sleep 30

# Проверка NameNode
echo "6. Checking NameNode status..."
docker exec namenode hdfs dfsadmin -report || {
    echo "NameNode not ready, checking logs..."
    docker logs namenode
    exit 1
}

# Запуск DataNode
echo "7. Starting DataNode..."
docker-compose up -d datanode

echo "Waiting for DataNode to register (20 seconds)..."
sleep 20

# Проверка кластера
echo "8. Checking cluster status..."
docker exec namenode hdfs dfsadmin -report

echo ""
echo "=== Creating HDFS directories ==="
docker exec namenode hdfs dfs -mkdir -p /user/airflow
docker exec namenode hdfs dfs -chmod 777 /user/airflow
docker exec namenode hdfs dfs -mkdir -p /tmp
docker exec namenode hdfs dfs -chmod 777 /tmp

echo ""
echo "=== Testing HDFS ==="
echo "Test file content" | docker exec -i namenode hdfs dfs -put - /tmp/test.txt
docker exec namenode hdfs dfs -ls /tmp

echo ""
echo "=== Hadoop cluster is ready! ==="
echo "NameNode UI: http://localhost:9870"
echo "HDFS defaultFS: hdfs://namenode:9000"