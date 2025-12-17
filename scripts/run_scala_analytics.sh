#!/bin/bash

echo "Running Weather Analytics (Scala)"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$PROJECT_ROOT/results"
SCALA_DIR="$PROJECT_ROOT/src/main/scala/weather/analytics"

echo "Step 0: Starting Docker containers..."
bash scripts/spark_docker_setup.sh

if [ $? -ne 0 ]; then
    echo "ERROR: Docker containers failed to start"
    exit 1
fi

echo ""
echo "Step 1: Checking data in HDFS..."
WEATHER_EXISTS=$(sudo docker exec namenode bash -c "hdfs dfs -test -e /user/weather/input/weather/weatherData.csv && echo 'exists' || echo 'not found'" 2>/dev/null | tr -d '\r\n')
LOCATION_EXISTS=$(sudo docker exec namenode bash -c "hdfs dfs -test -e /user/weather/input/location/locationData.csv && echo 'exists' || echo 'not found'" 2>/dev/null | tr -d '\r\n')

if [ "$WEATHER_EXISTS" != "exists" ] || [ "$LOCATION_EXISTS" != "exists" ]; then
    echo "Uploading data..."
    bash scripts/upload_data.sh || exit 1
fi

echo ""
echo "Step 2: Cleaning old outputs..."
bash scripts/cleanup.sh

echo ""
echo "Step 3: Building project with Scala support..."
cd "$PROJECT_ROOT"
bash scripts/mvn_buid.sh

if [ $? -ne 0 ]; then
    echo "ERROR: Build failed"
    exit 1
fi

JAR_FILE="$PROJECT_ROOT/target/weather-analytics-0.0.1-SNAPSHOT.jar"
CONTAINER_JAR_PATH="/tmp/weather-analytics-0.0.1-SNAPSHOT.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "ERROR: JAR file not found at: $JAR_FILE"
    exit 1
fi

echo ""
echo "Step 4: Checking if spark-master container is running..."
MAX_RETRIES=5
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if sudo docker ps --format '{{.Names}}' | grep -q "^spark-master$"; then
        echo "spark-master container is running"
        break
    else
        RETRY_COUNT=$((RETRY_COUNT + 1))
        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
            echo "Waiting for spark-master container to start... (attempt $RETRY_COUNT/$MAX_RETRIES)"
            sleep 5
        fi
    fi
done

if ! sudo docker ps --format '{{.Names}}' | grep -q "^spark-master$"; then
    echo "ERROR: spark-master container is not running after $MAX_RETRIES attempts"
    echo ""
    echo "Checking container status..."
    sudo docker ps -a | grep spark
    echo ""
    echo "Checking spark-master logs..."
    sudo docker logs spark-master 2>&1 | tail -20
    echo ""
    echo "Please check the logs above and start the Docker containers: bash scripts/spark_docker_setup.sh"
    exit 1
fi

echo ""
echo "Step 5: Copying JAR to spark-master container..."
sudo docker cp "$JAR_FILE" spark-master:"$CONTAINER_JAR_PATH"

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to copy JAR to container"
    exit 1
fi
echo "JAR copied to container"

echo ""
echo "Step 6: Running Shortwave Radiation Analysis..."
sudo docker exec spark-master bash -c "
    cd /opt/spark && \
    bin/spark-submit \
        --master spark://spark-master:7077 \
        --driver-memory 1g \
        --executor-memory 1g \
        --executor-cores 2 \
        --conf spark.sql.shuffle.partitions=200 \
        --conf spark.default.parallelism=200 \
        --conf spark.driver.maxResultSize=1g \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --class weather.analytics.ShortwaveRadiationAnalysis \
        $CONTAINER_JAR_PATH \
        hdfs://namenode:9000/user/weather/input/weather/weatherData.csv \
        hdfs://namenode:9000/user/weather/input/location/locationData.csv \
        hdfs://namenode:9000/user/weather/output/analysis3_shortwave_radiation
"

if [ $? -ne 0 ]; then
    echo "ERROR: Shortwave Radiation Analysis failed"
    exit 1
fi

echo ""
echo "Step 7: Running Weekly Maximum Temperature Analysis..."
sudo docker exec spark-master bash -c "
    cd /opt/spark && \
    bin/spark-submit \
        --master spark://spark-master:7077 \
        --driver-memory 2g \
        --executor-memory 2g \
        --executor-cores 2 \
        --conf spark.sql.shuffle.partitions=200 \
        --conf spark.default.parallelism=200 \
        --conf spark.driver.maxResultSize=1g \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --class weather.analytics.WeeklyMaxTemperatureAnalysis \
        $CONTAINER_JAR_PATH \
        hdfs://namenode:9000/user/weather/input/weather/weatherData.csv \
        hdfs://namenode:9000/user/weather/input/location/locationData.csv \
        hdfs://namenode:9000/user/weather/output/analysis4_weekly_max_temp
"

if [ $? -ne 0 ]; then
    echo "ERROR: Weekly Maximum Temperature Analysis failed"
    exit 1
fi

echo ""
echo "Step 8: Downloading results to local results folder..."
mkdir -p "$RESULTS_DIR"

echo "Downloading Shortwave Radiation results..."
sudo docker exec namenode bash -c "hdfs dfs -get /user/weather/output/analysis3_shortwave_radiation /tmp/analysis3_shortwave_radiation" 2>/dev/null || true
sudo docker cp namenode:/tmp/analysis3_shortwave_radiation "$RESULTS_DIR/" 2>/dev/null || true
sudo docker exec namenode bash -c "rm -rf /tmp/analysis3_shortwave_radiation" 2>/dev/null || true

echo "Downloading Weekly Max Temperature results..."
sudo docker exec namenode bash -c "hdfs dfs -get /user/weather/output/analysis4_weekly_max_temp /tmp/analysis4_weekly_max_temp" 2>/dev/null || true
sudo docker cp namenode:/tmp/analysis4_weekly_max_temp "$RESULTS_DIR/" 2>/dev/null || true
sudo docker exec namenode bash -c "rm -rf /tmp/analysis4_weekly_max_temp" 2>/dev/null || true

echo ""
echo "All Scala analyses completed successfully!"
echo ""
echo "Results downloaded successfully!"
echo "Results are available in: $RESULTS_DIR"
echo ""
echo "Available results:"
echo "  - $RESULTS_DIR/analysis3_shortwave_radiation/"
echo "  - $RESULTS_DIR/analysis4_weekly_max_temp/"
echo ""
echo "To view results:"
echo "  cat $RESULTS_DIR/analysis3_shortwave_radiation/part-*.csv | head -10"
echo "  cat $RESULTS_DIR/analysis4_weekly_max_temp/2_weekly_temps_by_district/part-*.csv | head -10"
echo "  cat $RESULTS_DIR/analysis4_weekly_max_temp/1_hottest_months_summary/part-*.csv | head -10"
echo "  cat $RESULTS_DIR/analysis4_weekly_max_temp/3_overall_weekly_temps/part-*.csv | head -10"
