#!/bin/bash

echo "Starting Spark Docker Containers"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCKER_COMPOSE_FILE="/Users/rajitha/MSC/Big Data Programming/Assessment/iit/lab7/hadoop-spark-dockercompose/docker-compose.yaml"

if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
    echo "ERROR: docker-compose.yaml not found at: $DOCKER_COMPOSE_FILE"
    exit 1
fi

echo "Step 1: Stopping and removing existing containers (if any)..."
cd "$(dirname "$DOCKER_COMPOSE_FILE")"
sudo docker compose -f "$DOCKER_COMPOSE_FILE" down 2>/dev/null || true

CONTAINERS=("namenode" "datanode" "resourcemanager" "nodemanager" "spark-master" "spark-worker-1")
for container in "${CONTAINERS[@]}"; do
    if sudo docker ps -a --format '{{.Names}}' | grep -q "^${container}$"; then
        echo "Removing existing container: $container"
        sudo docker stop "$container" 2>/dev/null || true
        sudo docker rm -f "$container" 2>/dev/null || true
    fi
done

echo "Existing containers cleaned up"

echo ""
echo "Step 2: Starting Docker containers..."
sudo docker compose -f "$DOCKER_COMPOSE_FILE" up -d

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to start Docker containers"
    exit 1
fi

echo ""
echo "Waiting for containers to start..."
sleep 10

echo ""
echo "Step 3: Checking container status..."
sudo docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "Step 4: Checking datanode connection to namenode..."
echo "Checking datanode logs..."
sudo docker logs datanode | tail -20

echo ""
echo "Waiting for datanode to fully start and connect to namenode..."
sleep 15

echo ""
echo "Checking if datanode is connected..."
if sudo docker exec namenode bash -c "hdfs dfsadmin -report" 2>/dev/null | grep -q "datanode"; then
    echo "Datanode is connected to namenode"
else
    echo "Warning: Datanode connection status unclear. Checking logs..."
    sudo docker logs datanode | tail -10
fi

echo ""
echo "Step 5: Checking HDFS status..."
sudo docker exec namenode bash -c "hdfs dfsadmin -report" 2>/dev/null | head -30

echo ""
echo "Docker containers started successfully!"
echo ""
echo "Container URLs:"
echo "  - Namenode UI: http://localhost:9870"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - Spark Worker UI: http://localhost:8081"
echo ""
echo "To view datanode logs continuously, run:"
echo "  sudo docker logs -f datanode"
