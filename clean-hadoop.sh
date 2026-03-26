#!/bin/bash

echo "=== Cleaning Hadoop Setup ==="

# Остановка контейнеров
echo "Stopping containers..."
docker-compose stop namenode datanode

# Удаление контейнеров
echo "Removing containers..."
docker-compose rm -f namenode datanode

# Удаление данных
read -p "Delete Hadoop data directories? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Removing data directories..."
    rm -rf hadoop_namenode
    rm -rf hadoop_datanode
fi

echo "Cleanup completed!"