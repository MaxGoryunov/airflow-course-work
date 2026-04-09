#!/bin/bash
set -e

if [ ! -w "/app/superset_home" ]; then
  echo "Error: No write permission for /app/superset_home. Add permissions"
fi

echo "Migrate/upgrade..."
superset db upgrade

echo "Create admin..."
if ! superset fab list-users | grep -q "admin"; then
    echo "Creating Superset admin..."
    superset fab create-admin \
        --username admin \
        --firstname Superset \
        --lastname Admin \
        --email admin@fab.org \
        --password ${SUPERSET_ADMIN_PASSWORD:-admin}
    
    superset db upgrade
else
    echo "Superset admin already exists. Skipping..."
fi

echo "Role init..."
superset init

echo "Server start..."
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
