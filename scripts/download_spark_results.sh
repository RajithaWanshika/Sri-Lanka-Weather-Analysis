#!/bin/bash

echo "Downloading Spark Results from HDFS"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$PROJECT_ROOT/results"

mkdir -p "$RESULTS_DIR"

echo "Step 1: Downloading Shortwave Radiation Analysis results..."
sudo docker exec namenode bash -c "hdfs dfs -get /user/weather/output/analysis3_shortwave_radiation /tmp/analysis3_shortwave_radiation" 2>/dev/null || true
sudo docker cp namenode:/tmp/analysis3_shortwave_radiation "$RESULTS_DIR/" 2>/dev/null || true
sudo docker exec namenode bash -c "rm -rf /tmp/analysis3_shortwave_radiation" 2>/dev/null || true
echo "Downloaded to: $RESULTS_DIR/analysis3_shortwave_radiation"

echo ""
echo "Step 2: Downloading Weekly Maximum Temperature Analysis results..."
sudo docker exec namenode bash -c "hdfs dfs -get /user/weather/output/analysis4_weekly_max_temp /tmp/analysis4_weekly_max_temp" 2>/dev/null || true
sudo docker cp namenode:/tmp/analysis4_weekly_max_temp "$RESULTS_DIR/" 2>/dev/null || true
sudo docker exec namenode bash -c "rm -rf /tmp/analysis4_weekly_max_temp" 2>/dev/null || true
echo "Downloaded to: $RESULTS_DIR/analysis4_weekly_max_temp"

echo ""
echo "Spark results downloaded successfully!"
echo ""
echo "Results location: $RESULTS_DIR"
echo ""
echo "To view results:"
echo "  cat $RESULTS_DIR/analysis3_shortwave_radiation/part-*.csv | head -10"
echo "  cat $RESULTS_DIR/analysis4_weekly_max_temp/part-*.csv | head -10"
