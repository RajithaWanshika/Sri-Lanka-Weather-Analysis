#!/bin/bash

echo "Uploading Weather Data to HDFS"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WEATHER_FILE="$PROJECT_ROOT/data/weatherData.csv"
LOCATION_FILE="$PROJECT_ROOT/data/locationData.csv"

# Check if data files exist
if [ ! -f "$WEATHER_FILE" ]; then
    echo "ERROR: Weather data file not found at: $WEATHER_FILE"
    exit 1
fi

if [ ! -f "$LOCATION_FILE" ]; then
    echo "ERROR: Location data file not found at: $LOCATION_FILE"
    exit 1
fi

# Check if namenode container is running
if ! sudo docker ps --format '{{.Names}}' | grep -q "^namenode$"; then
    echo "ERROR: namenode container is not running"
    echo "Please start the Docker containers first: bash scripts/spark_docker_setup.sh"
    exit 1
fi

echo "Step 1: Creating temporary directory in namenode..."
sudo docker exec namenode bash -c "mkdir -p /tmp/weather_data"

echo ""
echo "Step 2: Copying CSV files to namenode container..."
sudo docker cp "$WEATHER_FILE" namenode:/tmp/weather_data/weatherData.csv
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to copy weather data file"
    exit 1
fi

sudo docker cp "$LOCATION_FILE" namenode:/tmp/weather_data/locationData.csv
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to copy location data file"
    exit 1
fi

echo "Files copied to container"

echo ""
echo "Step 3: Creating HDFS directories..."
sudo docker exec namenode bash -c "
    hdfs dfs -mkdir -p /user/weather/input/weather
    hdfs dfs -mkdir -p /user/weather/input/location
    hdfs dfs -mkdir -p /user/weather/output
"

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to create HDFS directories"
    exit 1
fi

echo "HDFS directories created"

echo ""
echo "Step 4: Uploading files to HDFS..."
echo "Uploading weather data..."
sudo docker exec namenode bash -c "hdfs dfs -put -f /tmp/weather_data/weatherData.csv /user/weather/input/weather/weatherData.csv"
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to upload weather data file to HDFS"
    exit 1
fi
echo "Weather data uploaded"

echo "Uploading location data..."
sudo docker exec namenode bash -c "hdfs dfs -put -f /tmp/weather_data/locationData.csv /user/weather/input/location/locationData.csv"
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to upload location data file to HDFS"
    exit 1
fi
echo "Location data uploaded"

echo "Files uploaded to HDFS"
echo "Waiting for HDFS to sync..."
sleep 2

echo ""
echo "Step 5: Verifying upload..."
echo ""
echo "Weather data:"
sudo docker exec namenode bash -c "hdfs dfs -ls /user/weather/input/weather/"

echo ""
echo "Location data:"
sudo docker exec namenode bash -c "hdfs dfs -ls /user/weather/input/location/"

echo ""
echo "Verifying file existence..."
# Retry verification up to 3 times with delay
for i in {1..3}; do
    WEATHER_CHECK=$(sudo docker exec namenode bash -c "hdfs dfs -test -e /user/weather/input/weather/weatherData.csv && echo 'OK' || echo 'MISSING'" 2>/dev/null | tr -d '\r\n')
    LOCATION_CHECK=$(sudo docker exec namenode bash -c "hdfs dfs -test -e /user/weather/input/location/locationData.csv && echo 'OK' || echo 'MISSING'" 2>/dev/null | tr -d '\r\n')
    
    if [ "$WEATHER_CHECK" = "OK" ] && [ "$LOCATION_CHECK" = "OK" ]; then
        echo "✓ Both files uploaded successfully!"
        echo "  Weather data: OK"
        echo "  Location data: OK"
        echo ""
        echo "Data upload complete!"
        exit 0
    fi
    
    if [ $i -lt 3 ]; then
        echo "Verification attempt $i failed. Retrying in 2 seconds..."
        echo "  Weather data: $WEATHER_CHECK"
        echo "  Location data: $LOCATION_CHECK"
        sleep 2
    fi
done

echo "✗ Upload verification failed after 3 attempts!"
echo "  Weather data: $WEATHER_CHECK"
echo "  Location data: $LOCATION_CHECK"
echo ""
echo "Checking HDFS directory contents..."
echo "Weather directory:"
sudo docker exec namenode bash -c "hdfs dfs -ls /user/weather/input/weather/" 2>/dev/null || true
echo ""
echo "Location directory:"
sudo docker exec namenode bash -c "hdfs dfs -ls /user/weather/input/location/" 2>/dev/null || true
echo ""
echo "ERROR: One or more files failed to upload"
exit 1
