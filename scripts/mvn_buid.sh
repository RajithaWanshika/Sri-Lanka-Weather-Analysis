#!/bin/bash

echo "Building MapReduce JAR"

if ! command -v mvn &> /dev/null; then
    echo "ERROR: Maven is not installed"
    echo "Please install Maven: sudo apt-get install maven"
    exit 1
fi

echo "Step 1: Cleaning previous builds..."
mvn clean

echo ""
echo "Step 2: Compiling and packaging..."
mvn package

JAR_FILE="target/weather-analytics-0.0.1-SNAPSHOT.jar"
if [ -f "$JAR_FILE" ]; then
    echo ""
    echo "JAR file created successfully!"
    echo "Location: $JAR_FILE"
else
    echo ""
    echo "ERROR: JAR file not created"
    exit 1
fi
