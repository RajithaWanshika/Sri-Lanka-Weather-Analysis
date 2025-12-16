package com.weather.analytics.spark.shortwave;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import com.weather.analytics.spark.common.DateParserUDF;
import com.weather.analytics.spark.common.LocationDataLoader;
import com.weather.analytics.spark.common.SparkService;
import com.weather.analytics.spark.common.WeatherDataLoader;

public class ShortwaveRadiationAnalysis {

  private static final double RADIATION_THRESHOLD = 15.0;

  public static void main(String[] args) {
    if (args.length < 3) {
      System.err.println("Usage: ShortwaveRadiationAnalysis <weather_input> <location_input> <output>");
      System.exit(1);
    }

    String weatherInputPath = args[0];
    String locationInputPath = args[1];
    String outputPath = args[2];

    SparkSession spark = SparkService.createSparkSession("Shortwave Radiation Analysis");

    try {
      DateParserUDF.registerUDFs(spark);

      Dataset<Row> locationDF = LocationDataLoader.loadLocationData(spark, locationInputPath);
      Dataset<Row> weatherDF = WeatherDataLoader.loadWeatherDataWithShortwave(spark, weatherInputPath);

      Dataset<Row> joinedDF = weatherDF.join(
        locationDF,
        weatherDF.col("location_id").equalTo(locationDF.col("loc_id")),
        "left"
      ).select(
        weatherDF.col("location_id"),
        weatherDF.col("date"),
        weatherDF.col("shortwave_radiation"),
        functions.coalesce(locationDF.col("city_name"),
          functions.concat(functions.lit("Unknown_"), weatherDF.col("location_id"))).alias("district")
      );

      Dataset<Row> withDateParsed = joinedDF
        .withColumn("year", functions.callUDF("parseYear", joinedDF.col("date")))
        .withColumn("month", functions.callUDF("parseMonth", joinedDF.col("date")))
        .filter(functions.col("year").isNotNull())
        .filter(functions.col("month").isNotNull())
        .filter(functions.col("shortwave_radiation").isNotNull());

      Dataset<Row> monthlyStats = withDateParsed
        .groupBy("district", "year", "month")
        .agg(
          functions.sum("shortwave_radiation").alias("total_radiation"),
          functions.sum(
            functions.when(functions.col("shortwave_radiation").gt(RADIATION_THRESHOLD),
              functions.col("shortwave_radiation")).otherwise(0.0)
          ).alias("radiation_above_threshold"),
          functions.count("shortwave_radiation").alias("record_count")
        );

      Dataset<Row> result = monthlyStats
        .withColumn("percentage_above_threshold",
          functions.when(functions.col("total_radiation").gt(0),
            functions.col("radiation_above_threshold")
              .divide(functions.col("total_radiation"))
              .multiply(100))
            .otherwise(0.0))
        .select(
          functions.col("district"),
          functions.col("year"),
          functions.col("month"),
          functions.col("total_radiation"),
          functions.col("radiation_above_threshold"),
          functions.col("percentage_above_threshold"),
          functions.col("record_count")
        )
        .orderBy("district", "year", "month");

      result.coalesce(1)
        .write()
        .mode("overwrite")
        .option("header", "true")
        .csv(outputPath);

      System.out.println("Analysis completed successfully. Results written to: " + outputPath);

    } catch (Exception e) {
      System.err.println("Error during analysis: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    } finally {
      SparkService.stopSparkSession(spark);
    }
  }
}
