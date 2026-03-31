#!/bin/bash
set -e

if [ ! -w "/app/superset_home" ]; then
  echo "Ошибка: Нет прав на запись в /app/superset_home. Добавьте права"
fi

echo "Миграции..."
superset db upgrade

echo "Создание администратора..."
superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@fab.org \
    --password ${SUPERSET_ADMIN_PASSWORD:-admin}

echo "Инициализация ролей..."
superset init

echo "Запуск сервера..."
/usr/bin/run-server.sh
# superset db upgrade

# superset fab create-admin \
#     --username admin \
#     --firstname Superset \
#     --lastname Admin \
#     --email admin@fab.org \
#     --password Admin123

# superset init

# /usr/bin/run-server.sh
