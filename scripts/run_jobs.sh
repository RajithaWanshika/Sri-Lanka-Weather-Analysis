#!/bin/bash

echo "Running Weather Analytics MapReduce Jobs"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
JAR_FILE="$PROJECT_ROOT/target/weather-analytics-0.0.1-SNAPSHOT.jar"
CONTAINER_JAR_PATH="/tmp/weather-analytics-0.0.1-SNAPSHOT.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: JAR file not found at: $JAR_FILE"
    echo "Please build the project first: bash scripts/mvn_buid.sh"
    exit 1
fi

echo "Step 1: Copying JAR to namenode container..."
sudo docker cp "$JAR_FILE" namenode:"$CONTAINER_JAR_PATH"
echo "JAR copied to container"

echo ""
echo "Step 2: Running District Monthly Weather Analysis..."
sudo docker exec namenode bash -c "
    hadoop jar $CONTAINER_JAR_PATH \
    com.weather.analytics.district.monthly.WeatherDriver \
    /user/weather/input/weather/weatherData.csv \
    /user/weather/input/location/locationData.csv \
    /user/weather/output/analysis1_district_monthly
"

if [ $? -eq 0 ]; then
    echo ""
    echo "District Monthly Analysis completed successfully!"
    echo ""
    echo "Viewing results:"
    sudo docker exec namenode bash -c "hdfs dfs -cat /user/weather/output/analysis1_district_monthly/part-r-00000 | head -20"
else
    echo ""
    echo "ERROR: District Monthly Analysis failed"
    exit 1
fi

echo ""
echo "Step 3: Running Highest Precipitation Analysis..."
sudo docker exec namenode bash -c "
    hadoop jar $CONTAINER_JAR_PATH \
    com.weather.analytics.highest.precipitation.MaxPrecipitationDriver \
    /user/weather/input/weather/weatherData.csv \
    /user/weather/output/analysis2_highest_precipitation
"

if [ $? -eq 0 ]; then
    echo ""
    echo "Highest Precipitation Analysis completed successfully!"
    echo ""
    echo "Viewing results:"
    sudo docker exec namenode bash -c "hdfs dfs -cat /user/weather/output/analysis2_highest_precipitation/part-r-00000 | head -20"
    echo ""
    echo "To find the month/year with highest precipitation, check the output above."
else
    echo ""
    echo "ERROR: Highest Precipitation Analysis failed"
    exit 1
fi

echo ""
echo "All MapReduce jobs completed successfully!"
echo ""
echo "Step 4: Downloading MapReduce results to local results folder..."
bash "$SCRIPT_DIR/download_mapreduce_results.sh"
