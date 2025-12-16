#!/bin/bash

echo "Downloading MapReduce Results from HDFS"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$PROJECT_ROOT/results"

mkdir -p "$RESULTS_DIR"

echo "Step 1: Downloading District Monthly Analysis results..."
sudo docker exec -it namenode bash -c "hdfs dfs -get /user/weather/output/analysis1_district_monthly /tmp/analysis1_district_monthly"
sudo docker cp namenode:/tmp/analysis1_district_monthly "$RESULTS_DIR/"
sudo docker exec -it namenode bash -c "rm -rf /tmp/analysis1_district_monthly"
echo "Downloaded to: $RESULTS_DIR/analysis1_district_monthly"

echo ""
echo "Step 2: Downloading Highest Precipitation Analysis results..."
sudo docker exec -it namenode bash -c "hdfs dfs -get /user/weather/output/analysis2_highest_precipitation /tmp/analysis2_highest_precipitation"
sudo docker cp namenode:/tmp/analysis2_highest_precipitation "$RESULTS_DIR/"
sudo docker exec -it namenode bash -c "rm -rf /tmp/analysis2_highest_precipitation"
echo "Downloaded to: $RESULTS_DIR/analysis2_highest_precipitation"

echo ""
echo "Results downloaded successfully!"
echo ""
echo "Results location: $RESULTS_DIR"
echo ""
echo "To view results:"
echo "  cat $RESULTS_DIR/analysis1_district_monthly/part-r-00000"
echo "  cat $RESULTS_DIR/analysis2_highest_precipitation/part-r-00000"
