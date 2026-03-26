#!/bin/bash

echo "=== Full Restart of All Services ==="

# Остановка всех контейнеров
echo "1. Stopping all containers..."
docker-compose down

# Очистка папки логов
echo "2. Cleaning logs..."
sudo rm -rf logs
mkdir -p logs
chmod -R 777 logs

# Проверка и создание папок Hadoop
echo "3. Preparing Hadoop directories..."
mkdir -p hadoop_namenode hadoop_datanode
chmod -R 777 hadoop_namenode hadoop_datanode

# Проверка конфигурационных файлов
if [ ! -f hadoop_config/core-site.xml ] || [ ! -f hadoop_config/hdfs-site.xml ]; then
    echo "ERROR: Hadoop configuration files not found in hadoop_config/"
    echo "Please create core-site.xml and hdfs-site.xml"
    exit 1
fi

# Запуск всех сервисов
echo "4. Starting all services..."
docker-compose up -d

# Ожидание запуска
echo "5. Waiting for services to start (45 seconds)..."
sleep 45

# Проверка статуса
echo ""
echo "=== Services Status ==="
docker-compose ps

echo ""
echo "=== Checking Airflow ==="
docker exec airflow_webserver airflow users list 2>/dev/null || echo "Airflow still starting..."

echo ""
echo "=== Checking Hadoop ==="
docker exec namenode hdfs dfsadmin -report 2>/dev/null || echo "Hadoop still starting..."

echo ""
echo "=== Access URLs ==="
echo "Airflow: http://localhost:8081"
echo "Hadoop NameNode: http://localhost:9870"
echo ""
echo "Airflow credentials:"
echo "  Username: admin"
echo "  Password: ${AIRFLOW_ADMIN_PASSWORD:-admin}"