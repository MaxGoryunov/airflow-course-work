#!/bin/bash
# init-airflow.sh

set -e

# Инициализация базы данных
airflow db init

# Создание пользователя admin
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password ${AIRFLOW_ADMIN_PASSWORD:-admin} || true

echo "Airflow initialization completed!"