#!/bin/bash

echo "Uploading Weather Data to HDFS"

echo "Step 1: Creating temporary directory in namenode..."
sudo docker exec -it namenode bash -c "mkdir -p /tmp/weather_data"

echo ""
echo "Step 2: Copying CSV files to namenode container..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
sudo docker cp "$PROJECT_ROOT/data/weatherData.csv" namenode:/tmp/weather_data/
sudo docker cp "$PROJECT_ROOT/data/locationData.csv" namenode:/tmp/weather_data/

echo "Files copied to container"

echo ""
echo "Step 3: Creating HDFS directories..."
sudo docker exec -it namenode bash -c "
    hdfs dfs -mkdir -p /user/weather/input/weather
    hdfs dfs -mkdir -p /user/weather/input/location
    hdfs dfs -mkdir -p /user/weather/output
"

echo "HDFS directories created"

echo ""
echo "Step 4: Uploading files to HDFS..."
sudo docker exec -it namenode bash -c "
    hdfs dfs -put /tmp/weather_data/weatherData.csv /user/weather/input/weather/
    hdfs dfs -put /tmp/weather_data/locationData.csv /user/weather/input/location/
"

echo "Files uploaded to HDFS"

echo ""
echo "Step 5: Verifying upload..."
echo ""
echo "Weather data:"
sudo docker exec -it namenode bash -c "hdfs dfs -ls /user/weather/input/weather/"

echo ""
echo "Location data:"
sudo docker exec -it namenode bash -c "hdfs dfs -ls /user/weather/input/location/"

echo ""
echo "Data upload complete!"
