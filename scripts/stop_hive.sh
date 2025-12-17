#!/bin/bash

set -e

echo "Stopping Hive Docker Cluster"

IIT_REPO_PATH="$HOME/MSC/Big Data Programming/Assessment/weather-analytics/iit"
COMPOSE_DIR="$IIT_REPO_PATH/lab5/hadoop-hive-dockercompose"
COMPOSE_FILE="$COMPOSE_DIR/docker-compose-new.yaml"

if [ ! -f "$COMPOSE_FILE" ]; then
    echo "ERROR: docker-compose file not found at: $COMPOSE_FILE"
    echo "Attempting to stop containers by name instead..."
    
    CONTAINERS=("hive-server" "hive-metastore" "hive-metastore-postgresql" "nodemanager" "resourcemanager" "datanode" "namenode")
    
    for container in "${CONTAINERS[@]}"; do
        if sudo docker ps -a --format '{{.Names}}' | grep -q "^${container}$"; then
            echo "Stopping $container..."
            sudo docker stop "$container" 2>/dev/null || true
            sudo docker rm "$container" 2>/dev/null || true
        fi
    done
    
    echo "Containers stopped"
    exit 0
fi

cd "$COMPOSE_DIR"

echo "Checking running containers..."
if sudo docker ps --format '{{.Names}}' | grep -E "(namenode|hive-server|hive-metastore)" > /dev/null; then
    echo "Found running Hive/Hadoop containers"
else
    echo "No Hive/Hadoop containers currently running"
fi

echo "Stopping all containers..."
sudo docker compose -f "$COMPOSE_FILE" down

echo "Cluster stopped successfully!"
echo ""
echo "Options:"
echo "  To remove volumes (deletes all data): sudo docker compose -f "$COMPOSE_FILE" down -v"
echo "  To restart cluster: ./scripts/hive_docker_setup.sh"