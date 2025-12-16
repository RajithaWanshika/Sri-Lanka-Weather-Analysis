#!/bin/bash

echo "Sri Lanka Weather Analytics - Docker Hadoop Setup"

echo "Step 1: Cloning Hadoop Docker repository..."
if [ ! -d "iit" ]; then
    git clone https://github.com/ramindu-msc/iit
    echo "Repository cloned"
else
    echo "Repository already exists"
fi

echo ""
echo "Step 2: Setting up permissions..."
cd iit/
sudo chmod 755 lab1/

echo ""
echo "Step 3: Starting Docker Hadoop cluster..."
sudo docker compose -f lab1/docker-compose.yaml up -d

echo ""
echo "Waiting for containers to initialize (30 seconds)..."
sleep 30

echo ""
echo "Step 4: Checking container status..."
sudo docker ps

echo ""
echo "Docker Hadoop setup complete!"
echo ""
echo "Access cluster dashboard at:"
echo "http://localhost:9870/dfshealth.html#tab-overview"
echo ""
echo "To access namenode shell:"
echo "sudo docker exec -it namenode bash"
