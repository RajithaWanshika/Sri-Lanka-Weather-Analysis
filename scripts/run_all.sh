#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Weather Analytics - Complete Workflow"
echo ""

echo "Step 0: Starting Docker containers..."
bash scripts/spark_docker_setup.sh

if [ $? -ne 0 ]; then
    echo "ERROR: Docker containers failed to start"
    exit 1
fi

echo ""

cd "$PROJECT_ROOT"

echo "Step 1: Building MapReduce JAR..."
bash scripts/mvn_buid.sh

if [ $? -ne 0 ]; then
    echo "ERROR: Build failed"
    exit 1
fi

echo ""
echo "Step 2: Checking if data is uploaded to HDFS..."
WEATHER_EXISTS=$(sudo docker exec -it namenode bash -c "hdfs dfs -test -e /user/weather/input/weather/weatherData.csv && echo 'exists' || echo 'not found'" 2>/dev/null | tr -d '\r\n')

if [ "$WEATHER_EXISTS" != "exists" ]; then
    echo "Data not found in HDFS. Uploading data..."
    bash scripts/upload_data.sh
else
    echo "Data already exists in HDFS. Skipping upload."
fi

echo ""
echo "Step 3: Cleaning old output directories..."
bash scripts/cleanup.sh

echo ""
echo "Step 4: Running MapReduce Jobs..."
bash scripts/run_jobs.sh

if [ $? -ne 0 ]; then
    echo "ERROR: MapReduce jobs failed"
    exit 1
fi

echo ""
echo "Step 5: Downloading results..."
bash scripts/download_mapreduce_results.sh

echo ""
echo "Workflow completed successfully!"
echo ""
echo "Results are available in: $PROJECT_ROOT/results/"
echo ""
echo "To view results:"
echo "  cat results/analysis1_district_monthly/part-r-00000 | head -10"
echo "  cat results/analysis2_highest_precipitation/part-r-00000 | head -10"
echo ""
echo "To find the highest precipitation:"
echo "  grep HIGHEST results/analysis2_highest_precipitation/part-r-00000"
echo ""
