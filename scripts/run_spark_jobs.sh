#!/bin/bash

echo "Running Weather Analytics Spark Jobs"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
JAR_FILE="$PROJECT_ROOT/target/weather-analytics-0.0.1-SNAPSHOT.jar"
CONTAINER_JAR_PATH="/tmp/weather-analytics-0.0.1-SNAPSHOT.jar"

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
WEATHER_EXISTS=$(sudo docker exec namenode bash -c "hdfs dfs -test -e /user/weather/input/weather/weatherData.csv && echo 'exists' || echo 'not found'" 2>/dev/null | tr -d '\r\n')
LOCATION_EXISTS=$(sudo docker exec namenode bash -c "hdfs dfs -test -e /user/weather/input/location/locationData.csv && echo 'exists' || echo 'not found'" 2>/dev/null | tr -d '\r\n')

if [ "$WEATHER_EXISTS" != "exists" ] || [ "$LOCATION_EXISTS" != "exists" ]; then
    echo "Data not found in HDFS. Weather: $WEATHER_EXISTS, Location: $LOCATION_EXISTS"
    echo "Uploading data..."
    bash scripts/upload_data.sh
    if [ $? -ne 0 ]; then
        echo "ERROR: Data upload failed"
        exit 1
    fi
else
    echo "Data already exists in HDFS. Skipping upload."
fi

echo ""
echo "Step 3: Cleaning old output directories..."
bash scripts/cleanup.sh

echo ""

echo "Step 4: Checking if JAR file exists..."
if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: JAR file not found at: $JAR_FILE"
    echo "Please build the project first: bash scripts/mvn_buid.sh"
    exit 1
fi

echo ""
echo "Step 5: Checking if spark-master container is running..."
if ! sudo docker ps --format '{{.Names}}' | grep -q "^spark-master$"; then
    echo "ERROR: spark-master container is not running"
    echo "Please start the Docker containers first: bash scripts/spark_docker_setup.sh"
    exit 1
fi

echo ""
echo "Step 6: Checking if data files exist in HDFS..."
echo "Checking if data files exist in HDFS..."
WEATHER_EXISTS=$(sudo docker exec namenode bash -c "hdfs dfs -test -e /user/weather/input/weather/weatherData.csv && echo 'exists' || echo 'not found'" 2>/dev/null | tr -d '\r\n')
LOCATION_EXISTS=$(sudo docker exec namenode bash -c "hdfs dfs -test -e /user/weather/input/location/locationData.csv && echo 'exists' || echo 'not found'" 2>/dev/null | tr -d '\r\n')

if [ "$WEATHER_EXISTS" != "exists" ] || [ "$LOCATION_EXISTS" != "exists" ]; then
    echo "ERROR: Data files not found in HDFS"
    echo "Weather data exists: $WEATHER_EXISTS"
    echo "Location data exists: $LOCATION_EXISTS"
    echo ""
    echo "Please upload data first: bash scripts/upload_data.sh"
    exit 1
fi

echo "Data files found in HDFS"
echo ""

echo "Step 7: Copying JAR to spark-master container..."
sudo docker cp "$JAR_FILE" spark-master:"$CONTAINER_JAR_PATH"
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to copy JAR to container"
    exit 1
fi
echo "JAR copied to container"

echo ""
echo "Step 8: Running Shortwave Radiation Analysis..."
sudo docker exec spark-master bash -c "
    spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 2 \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.default.parallelism=200 \
    --conf spark.driver.maxResultSize=1g \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --class com.weather.analytics.spark.shortwave.ShortwaveRadiationAnalysis \
    $CONTAINER_JAR_PATH \
    hdfs://namenode:9000/user/weather/input/weather/weatherData.csv \
    hdfs://namenode:9000/user/weather/input/location/locationData.csv \
    hdfs://namenode:9000/user/weather/output/analysis3_shortwave_radiation
"

if [ $? -eq 0 ]; then
    echo ""
    echo "Shortwave Radiation Analysis completed successfully!"
    echo ""
    echo "Viewing results:"
    sudo docker exec namenode bash -c "hdfs dfs -cat /user/weather/output/analysis3_shortwave_radiation/part-*.csv | head -20"
else
    echo ""
    echo "ERROR: Shortwave Radiation Analysis failed"
    exit 1
fi

echo ""
echo "Step 9: Running Weekly Maximum Temperature Analysis..."
sudo docker exec spark-master bash -c "
    spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 2 \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.default.parallelism=200 \
    --conf spark.driver.maxResultSize=1g \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --class com.weather.analytics.spark.weeklytemp.WeeklyMaxTemperatureAnalysis \
    $CONTAINER_JAR_PATH \
    hdfs://namenode:9000/user/weather/input/weather/weatherData.csv \
    hdfs://namenode:9000/user/weather/input/location/locationData.csv \
    hdfs://namenode:9000/user/weather/output/analysis4_weekly_max_temp
"

if [ $? -eq 0 ]; then
    echo ""
    echo "Weekly Maximum Temperature Analysis completed successfully!"
    echo ""
    echo "Viewing results:"
    sudo docker exec namenode bash -c "hdfs dfs -cat /user/weather/output/analysis4_weekly_max_temp/part-*.csv | head -20"
else
    echo ""
    echo "ERROR: Weekly Maximum Temperature Analysis failed"
    exit 1
fi

echo ""
echo "All Spark jobs completed successfully!"
echo ""
echo "Step 10: Downloading Spark results to local results folder..."
bash "$SCRIPT_DIR/download_spark_results.sh"

echo ""
echo "All Spark results are available in: $PROJECT_ROOT/results/"
echo ""
echo "To view results:"
echo "  cat $PROJECT_ROOT/results/analysis3_shortwave_radiation/part-*.csv | head -10"
echo "  cat $PROJECT_ROOT/results/analysis4_weekly_max_temp/part-*.csv | head -10"
