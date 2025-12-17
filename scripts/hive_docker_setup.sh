#!/bin/bash

set -e

echo "Starting Hive Docker Cluster"

IIT_REPO_PATH="$HOME/MSC/Big Data Programming/Assessment/weather-analytics/iit"
COMPOSE_DIR="$IIT_REPO_PATH/lab5/hadoop-hive-dockercompose"
COMPOSE_FILE="$COMPOSE_DIR/docker-compose-new.yaml"

echo "Checking repository..."
if [ ! -d "$IIT_REPO_PATH" ]; then
    echo "Cloning repository..."
    mkdir -p "$(dirname "$IIT_REPO_PATH")"
    git clone https://github.com/ramindu-msc/iit "$IIT_REPO_PATH"
else
    echo "Updating repository..."
    cd "$IIT_REPO_PATH" && git pull origin main
fi

if [ ! -f "$COMPOSE_FILE" ]; then
    echo "ERROR: docker-compose file not found at: $COMPOSE_FILE"
    exit 1
fi

echo "Cleaning up existing containers..."
cd "$COMPOSE_DIR"
sudo docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true

echo "Starting Hadoop and Hive services..."
sudo docker compose -f "$COMPOSE_FILE" up -d

echo "Waiting for services to initialize..."
echo "Waiting for NameNode..."
for i in {1..30}; do
    if sudo docker exec namenode hdfs dfsadmin -report &>/dev/null; then
        echo "✓ NameNode is ready"
        break
    fi
    sleep 2
done

echo "Waiting for Hive Metastore..."
sleep 15

echo "Container Status:"
sudo docker ps --format "table {{.Names}}\t{{.Status}}"

echo "Cluster Ready!"
echo "Access Points:"
echo "  • NameNode UI:        http://localhost:50070"
echo "  • ResourceManager UI: http://localhost:8088"
echo "  • Hive Server:        localhost:10000"
echo ""
echo "Run queries with: ./scripts/run_all_hive.sh"
echo "Stop cluster with: ./scripts/stop-hive.sh"