#!/bin/bash

set -e

echo "Running Hive Queries"

QUERY_FILES=(
    "src/main/java/com/weather/analytics/hive/create_tables.hql"
    "src/main/java/com/weather/analytics/hive/seasonal_evapotranspiration.hql"
    "src/main/java/com/weather/analytics/hive/top_temperate_cities.hql"
)

echo "Checking query files..."
for query in "${QUERY_FILES[@]}"; do
    if [ ! -f "$query" ]; then
        echo "ERROR: Query file not found: $query"
        echo "Please ensure the file exists in the current directory"
        exit 1
    fi
    echo "Found: $query"
done

echo "Checking if Hive cluster is running..."
if ! sudo docker ps | grep -q "hive-server"; then
    echo "ERROR: Hive server is not running!"
    echo "Please start the cluster first: ./scripts/hive_docker_setup.sh"
    exit 1
fi
echo "Hive cluster is running"

echo ""
echo "Preparing data for Hive tables..."
echo "Checking if data exists in HDFS..."

# Check if data exists in the upload location
WEATHER_EXISTS=$(sudo docker exec namenode bash -c "hdfs dfs -test -e /user/weather/input/weather/weatherData.csv && echo 'exists' || echo 'not found'" 2>/dev/null | tr -d '\r\n')
LOCATION_EXISTS=$(sudo docker exec namenode bash -c "hdfs dfs -test -e /user/weather/input/location/locationData.csv && echo 'exists' || echo 'not found'" 2>/dev/null | tr -d '\r\n')

if [ "$WEATHER_EXISTS" != "exists" ] || [ "$LOCATION_EXISTS" != "exists" ]; then
    echo "ERROR: Data files not found in HDFS!"
    echo "  Weather data: $WEATHER_EXISTS"
    echo "  Location data: $LOCATION_EXISTS"
    echo "Please upload data first: bash scripts/upload_data.sh"
    exit 1
fi

echo "Data found in HDFS. Copying to Hive table locations..."

# Create Hive data directories
sudo docker exec namenode bash -c "hdfs dfs -mkdir -p /user/hive/weather" 2>/dev/null || true
sudo docker exec namenode bash -c "hdfs dfs -mkdir -p /user/hive/location" 2>/dev/null || true

# Remove existing data if any
sudo docker exec namenode bash -c "hdfs dfs -rm -r -f /user/hive/weather/*" 2>/dev/null || true
sudo docker exec namenode bash -c "hdfs dfs -rm -r -f /user/hive/location/*" 2>/dev/null || true

# Copy data to Hive table locations
echo "Copying weather data..."
sudo docker exec namenode bash -c "hdfs dfs -cp /user/weather/input/weather/weatherData.csv /user/hive/weather/" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to copy weather data to Hive location"
    exit 1
fi

echo "Copying location data..."
sudo docker exec namenode bash -c "hdfs dfs -cp /user/weather/input/location/locationData.csv /user/hive/location/" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to copy location data to Hive location"
    exit 1
fi

echo "Data prepared for Hive tables"
echo ""

echo "Executing queries..."
for i in "${!QUERY_FILES[@]}"; do
    query="${QUERY_FILES[$i]}"
    query_num=$((i + 1))
    total=${#QUERY_FILES[@]}
    
    echo ""
    echo "Query [$query_num/$total]: $query"
    
    echo "Copying query to container..."
    sudo docker cp "$query" hive-server:/tmp/current_query.hql
    
    echo "Executing query..."
    if sudo docker exec hive-server hive -f /tmp/current_query.hql; then
        echo "Query completed successfully: $query"
    else
        echo "Query failed: $query"
        exit 1
    fi
done

echo "Retrieving results..."
mkdir -p ./results

echo "Checking for results in hive-server container..."
if sudo docker exec hive-server test -d /results/seasonal_evapotranspiration 2>/dev/null; then
    echo "Results found in hive-server container. Listing:"
    sudo docker exec hive-server ls -la /results/seasonal_evapotranspiration/ 2>/dev/null || true
    sudo docker exec hive-server ls -la /results/top_temperate_cities/ 2>/dev/null || true
    
    echo "Copying results from hive-server container to local directory..."
    sudo docker cp hive-server:/results/seasonal_evapotranspiration/. ./results/seasonal_evapotranspiration/ 2>/dev/null || true
    sudo docker cp hive-server:/results/top_temperate_cities/. ./results/top_temperate_cities/ 2>/dev/null || true
    
    echo "Results saved to ./results/"
    echo ""
    echo "Results structure:"
    ls -lR ./results/ 2>/dev/null || echo "No results copied yet"
else
    echo "No /results directory found in hive-server container"
    echo "Checking if results exist in different location..."
    sudo docker exec hive-server find / -name "000000_0" -type f 2>/dev/null | head -5 || true
fi

echo "All Queries Completed Successfully!"
echo ""
echo "Results location: ./results/"
echo ""
echo "View results:"
echo "  cat ./results/seasonal_evapotranspiration/000000_0"
echo "  cat ./results/top_temperate_cities/000000_0"