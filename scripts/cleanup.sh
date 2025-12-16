#!/bin/bash

echo "Cleaning up HDFS output directories"

sudo docker exec -it namenode bash -c "
    hdfs dfs -rm -r /user/weather/output/analysis1_district_monthly 2>/dev/null || true
    hdfs dfs -rm -r /user/weather/output/analysis2_highest_precipitation 2>/dev/null || true
"

echo ""
echo "Output directories cleaned"
echo "You can now re-run the analyses"
