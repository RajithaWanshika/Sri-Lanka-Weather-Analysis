#!/bin/bash

echo "Sri Lanka Weather Analytics - Docker Hadoop Setup"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
IIT_REPO_PATH="/Users/rajitha/MSC/Big Data Programming/Assessment/iit"
DOCKER_COMPOSE_FILE="$IIT_REPO_PATH/lab1/docker-compose.yaml"

echo "Step 1: Cloning/Updating Hadoop Docker repository..."
if [ ! -d "$IIT_REPO_PATH" ]; then
    cd "$(dirname "$IIT_REPO_PATH")"
    git clone https://github.com/ramindu-msc/iit
    echo "Repository cloned"
else
    cd "$IIT_REPO_PATH"
    git pull origin main
    echo "Repository updated"
fi

if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
    echo "ERROR: docker-compose.yaml not found at: $DOCKER_COMPOSE_FILE"
    exit 1
fi

echo ""
echo "Step 2: Setting up permissions..."
sudo chmod 755 "$IIT_REPO_PATH/lab1/"

echo ""
echo "Step 3: Stopping and removing existing containers (if any)..."
cd "$(dirname "$DOCKER_COMPOSE_FILE")"
sudo docker compose -f "$DOCKER_COMPOSE_FILE" down 2>/dev/null || true

CONTAINERS=("namenode" "datanode1" "datanode2" "resourcemanager" "nodemanager1" "nodemanager2")
for container in "${CONTAINERS[@]}"; do
    if sudo docker ps -a --format '{{.Names}}' | grep -q "^${container}$"; then
        echo "Removing existing container: $container"
        sudo docker stop "$container" 2>/dev/null || true
        sudo docker rm -f "$container" 2>/dev/null || true
    fi
done

echo "Existing containers cleaned up"

echo ""
echo "Step 4: Starting Docker Hadoop cluster..."
sudo docker compose -f "$DOCKER_COMPOSE_FILE" up -d

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to start Docker containers"
    exit 1
fi

echo ""
echo "Waiting for containers to initialize (30 seconds)..."
sleep 30

echo ""
echo "Step 5: Checking container status..."
sudo docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "Step 6: Checking datanode connections to namenode..."
echo "Waiting for datanodes to fully start and connect..."
sleep 15

echo ""
echo "Checking HDFS status..."
if sudo docker exec namenode bash -c "hdfs dfsadmin -report" 2>/dev/null | grep -q "datanode"; then
    echo "Datanodes are connected to namenode"
    sudo docker exec namenode bash -c "hdfs dfsadmin -report" 2>/dev/null | head -40
else
    echo "Warning: Datanode connection status unclear. Checking logs..."
    sudo docker logs datanode1 | tail -10
    sudo docker logs datanode2 | tail -10
fi

echo ""
echo "Docker Hadoop setup complete!"
echo ""
echo "Access cluster dashboard at:"
echo "  - Namenode UI: http://localhost:9870/dfshealth.html#tab-overview"
echo "  - ResourceManager UI: http://localhost:8088"
echo ""
echo "To access namenode shell:"
echo "  sudo docker exec -it namenode bash"
echo ""
echo "To view datanode logs:"
echo "  sudo docker logs -f datanode1"
echo "  sudo docker logs -f datanode2"
