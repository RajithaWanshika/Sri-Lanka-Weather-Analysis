package com.weather.analytics.spark.common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public final class LocationDataLoader {

  private LocationDataLoader() {}

  public static Dataset<Row> loadLocationData(SparkSession spark, String inputPath) {
    StructType locationSchema = DataTypes.createStructType(new StructField[]{
      DataTypes.createStructField("location_id", DataTypes.StringType, false),
      DataTypes.createStructField("latitude", DataTypes.StringType, true),
      DataTypes.createStructField("longitude", DataTypes.StringType, true),
      DataTypes.createStructField("elevation", DataTypes.StringType, true),
      DataTypes.createStructField("utc_offset_seconds", DataTypes.StringType, true),
      DataTypes.createStructField("timezone", DataTypes.StringType, true),
      DataTypes.createStructField("timezone_abbreviation", DataTypes.StringType, true),
      DataTypes.createStructField("city_name", DataTypes.StringType, true)
    });

    return spark.read()
      .option("header", "true")
      .schema(locationSchema)
      .csv(inputPath)
      .withColumnRenamed("location_id", "loc_id");
  }
}
