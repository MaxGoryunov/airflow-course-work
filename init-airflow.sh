#!/bin/bash

set -e

echo "=== Initializing Airflow ==="

# mkdir -p /opt/airflow/logs /opt/airflow/logs/scheduler /opt/airflow/dags
# chmod -R 777 /opt/airflow/logs

echo "Initializing database..."
airflow db migrate

if ! airflow users list | grep -q admin; then
    echo "Creating admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password ${AIRFLOW_ADMIN_PASSWORD:-admin}
else
    echo "Admin user already exists."
fi

echo "=== Airflow initialization completed ==="

# #!/bin/bash
# # init-airflow.sh

# set -e

# # Инициализация базы данных
# airflow db init

# # Создание пользователя admin
# airflow users create \
#     --username admin \
#     --firstname Admin \
#     --lastname User \
#     --role Admin \
#     --email admin@example.com \
#     --password ${AIRFLOW_ADMIN_PASSWORD:-admin} || true

# echo "Airflow initialization completed!"