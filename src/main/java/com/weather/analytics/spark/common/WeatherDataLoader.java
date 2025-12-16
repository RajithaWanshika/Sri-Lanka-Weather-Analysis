package com.weather.analytics.spark.common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public final class WeatherDataLoader {

  private WeatherDataLoader() {}

  public static Dataset<Row> loadWeatherDataWithShortwave(SparkSession spark, String inputPath) {
    Dataset<Row> rawDF = spark.read()
      .option("header", "true")
      .option("inferSchema", "false")
      .csv(inputPath);

    String[] columns = rawDF.columns();
    String shortwaveCol = null;
    for (String col : columns) {
      if (col.contains("shortwave_radiation_sum")) {
        shortwaveCol = col;
        break;
      }
    }

    if (shortwaveCol == null) {
      throw new IllegalArgumentException("Could not find shortwave_radiation_sum column in CSV");
    }

    return rawDF.select(
      functions.col("location_id").cast(DataTypes.IntegerType).alias("location_id"),
      functions.col("date").alias("date"),
      functions.col(shortwaveCol).cast(DataTypes.DoubleType)
        .alias("shortwave_radiation")
    );
  }

  public static Dataset<Row> loadWeatherDataWithTemperature(SparkSession spark, String inputPath) {
    Dataset<Row> rawDF = spark.read()
      .option("header", "true")
      .option("inferSchema", "false")
      .csv(inputPath);

    String[] columns = rawDF.columns();
    String tempMaxCol = null;
    for (String col : columns) {
      if (col.contains("temperature_2m_max")) {
        tempMaxCol = col;
        break;
      }
    }

    if (tempMaxCol == null) {
      throw new IllegalArgumentException("Could not find temperature_2m_max column in CSV");
    }

    return rawDF.select(
      functions.col("location_id").cast(DataTypes.IntegerType).alias("location_id"),
      functions.col("date").alias("date"),
      functions.col(tempMaxCol).cast(DataTypes.DoubleType)
        .alias("temperature_max")
    );
  }
}
