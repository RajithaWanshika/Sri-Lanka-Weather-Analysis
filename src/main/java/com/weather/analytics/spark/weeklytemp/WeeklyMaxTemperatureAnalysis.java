package com.weather.analytics.spark.weeklytemp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import com.weather.analytics.spark.common.DateParserUDF;
import com.weather.analytics.spark.common.LocationDataLoader;
import com.weather.analytics.spark.common.SparkService;
import com.weather.analytics.spark.common.WeatherDataLoader;

public class WeeklyMaxTemperatureAnalysis {

  public static void main(String[] args) {
    if (args.length < 3) {
      System.err.println("Usage: WeeklyMaxTemperatureAnalysis <weather_input> <location_input> <output>");
      System.exit(1);
    }

    String weatherInputPath = args[0];
    String locationInputPath = args[1];
    String outputPath = args[2];

    SparkSession spark = SparkService.createSparkSession("Weekly Maximum Temperature Analysis");

    try {
      DateParserUDF.registerUDFs(spark);

      Dataset<Row> locationDF = LocationDataLoader.loadLocationData(spark, locationInputPath);
      Dataset<Row> weatherDF = WeatherDataLoader.loadWeatherDataWithTemperature(spark, weatherInputPath);

      Dataset<Row> joinedDF = weatherDF.join(
        locationDF,
        weatherDF.col("location_id").equalTo(locationDF.col("loc_id")),
        "left"
      ).select(
        weatherDF.col("location_id"),
        weatherDF.col("date"),
        weatherDF.col("temperature_max"),
        functions.coalesce(locationDF.col("city_name"),
          functions.concat(functions.lit("Unknown_"), weatherDF.col("location_id"))).alias("district")
      );

      Dataset<Row> withDateParsed = joinedDF
        .withColumn("year", functions.callUDF("parseYear", joinedDF.col("date")))
        .withColumn("month", functions.callUDF("parseMonth", joinedDF.col("date")))
        .filter(functions.col("year").isNotNull())
        .filter(functions.col("month").isNotNull())
        .filter(functions.col("temperature_max").isNotNull())
        .withColumn("week_of_year",
          functions.coalesce(
            functions.callUDF("parseWeekOfYear", joinedDF.col("date")),
            functions.lit(1)
          ));

      Dataset<Row> monthlyAvgTemp = withDateParsed
        .groupBy("district", "year", "month")
        .agg(functions.avg("temperature_max").alias("avg_max_temp"));

      Dataset<Row> maxTempPerYear = monthlyAvgTemp
        .groupBy("district", "year")
        .agg(functions.max("avg_max_temp").alias("max_avg_temp"));

      Dataset<Row> hottestMonths = monthlyAvgTemp
        .join(maxTempPerYear,
          monthlyAvgTemp.col("district").equalTo(maxTempPerYear.col("district"))
            .and(monthlyAvgTemp.col("year").equalTo(maxTempPerYear.col("year")))
            .and(monthlyAvgTemp.col("avg_max_temp").equalTo(maxTempPerYear.col("max_avg_temp"))),
          "inner")
        .select(monthlyAvgTemp.col("district"),
          monthlyAvgTemp.col("year"),
          monthlyAvgTemp.col("month").alias("hottest_month"))
        .groupBy("district", "year")
        .agg(functions.min("hottest_month").alias("hottest_month"));

      Dataset<Row> hottestMonthRecords = withDateParsed
        .join(
          hottestMonths,
          withDateParsed.col("district").equalTo(hottestMonths.col("district"))
            .and(withDateParsed.col("year").equalTo(hottestMonths.col("year")))
            .and(withDateParsed.col("month").equalTo(hottestMonths.col("hottest_month"))),
          "inner"
        )
        .select(
          withDateParsed.col("district"),
          withDateParsed.col("year"),
          withDateParsed.col("month"),
          withDateParsed.col("week_of_year"),
          withDateParsed.col("temperature_max")
        );

      Dataset<Row> weeklyMaxTemp = hottestMonthRecords
        .groupBy("district", "year", "month", "week_of_year")
        .agg(
          functions.max("temperature_max").alias("weekly_max_temp"),
          functions.count("temperature_max").alias("days_in_week")
        )
        .orderBy("district", "year", "month", "week_of_year");

      Dataset<Row> result = weeklyMaxTemp
        .withColumn("month_name",
          functions.when(functions.col("month").equalTo(1), "January")
            .when(functions.col("month").equalTo(2), "February")
            .when(functions.col("month").equalTo(3), "March")
            .when(functions.col("month").equalTo(4), "April")
            .when(functions.col("month").equalTo(5), "May")
            .when(functions.col("month").equalTo(6), "June")
            .when(functions.col("month").equalTo(7), "July")
            .when(functions.col("month").equalTo(8), "August")
            .when(functions.col("month").equalTo(9), "September")
            .when(functions.col("month").equalTo(10), "October")
            .when(functions.col("month").equalTo(11), "November")
            .when(functions.col("month").equalTo(12), "December")
            .otherwise("Unknown"))
        .select(
          functions.col("district"),
          functions.col("year"),
          functions.col("month"),
          functions.col("month_name"),
          functions.col("week_of_year"),
          functions.col("weekly_max_temp"),
          functions.col("days_in_week")
        )
        .orderBy("district", "year", "week_of_year");

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
