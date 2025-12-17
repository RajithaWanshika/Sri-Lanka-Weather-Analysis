package weather.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Try

object ShortwaveRadiationAnalysis {

  private val RADIATION_THRESHOLD = 15.0

  private val dateFormatters: List[DateTimeFormatter] = List(
    DateTimeFormatter.ofPattern("M/d/yyyy"),
    DateTimeFormatter.ofPattern("MM/dd/yyyy"),
    DateTimeFormatter.ofPattern("d/M/yyyy"),
    DateTimeFormatter.ofPattern("dd/MM/yyyy"),
    DateTimeFormatter.ofPattern("yyyy-MM-dd"),
    DateTimeFormatter.ofPattern("yyyy-M-d")
  )

  private def parseDate(dateStr: String): Option[LocalDate] = {
    if (dateStr == null || dateStr.trim.isEmpty) {
      None
    } else {
      val trimmedDate = dateStr.trim
      dateFormatters.view
        .flatMap(formatter => Try(LocalDate.parse(trimmedDate, formatter)).toOption)
        .headOption
    }
  }

  private def parseYearFunc(dateStr: String): Integer = {
    parseDate(dateStr).map(_.getYear).map(Integer.valueOf).orNull
  }

  private def parseMonthFunc(dateStr: String): Integer = {
    parseDate(dateStr).map(_.getMonthValue).map(Integer.valueOf).orNull
  }

  private def loadLocationData(spark: SparkSession, inputPath: String): DataFrame = {
    val locationSchema = StructType(Array(
      StructField("location_id", StringType, nullable = false),
      StructField("latitude", StringType, nullable = true),
      StructField("longitude", StringType, nullable = true),
      StructField("elevation", StringType, nullable = true),
      StructField("utc_offset_seconds", StringType, nullable = true),
      StructField("timezone", StringType, nullable = true),
      StructField("timezone_abbreviation", StringType, nullable = true),
      StructField("city_name", StringType, nullable = true)
    ))

    spark.read
      .option("header", "true")
      .schema(locationSchema)
      .csv(inputPath)
      .withColumnRenamed("location_id", "loc_id")
  }

  private def loadWeatherData(spark: SparkSession, inputPath: String): DataFrame = {
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "false")
      .csv(inputPath)

    val shortwaveCol = rawDF.columns.find(_.contains("shortwave_radiation_sum"))
      .getOrElse(throw new IllegalArgumentException("Could not find shortwave_radiation_sum column in CSV"))

    rawDF.select(
      col("location_id").cast(IntegerType).alias("location_id"),
      col("date").alias("date"),
      col(shortwaveCol).cast(DoubleType).alias("shortwave_radiation")
    )
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: ShortwaveRadiationAnalysis <weather_input> <location_input> <output>")
      System.exit(1)
    }

    val weatherInputPath = args(0)
    val locationInputPath = args(1)
    val outputPath = args(2)

    val spark = SparkSession.builder()
      .appName("Shortwave Radiation Analysis")
      .getOrCreate()

    try {
      spark.udf.register("parseYear", parseYearFunc _)
      spark.udf.register("parseMonth", parseMonthFunc _)

      val locationDF = loadLocationData(spark, locationInputPath)
      val weatherDF = loadWeatherData(spark, weatherInputPath)

      val joinedDF = weatherDF.join(
        locationDF,
        weatherDF("location_id") === locationDF("loc_id"),
        "left"
      ).select(
        weatherDF("location_id"),
        weatherDF("date"),
        weatherDF("shortwave_radiation"),
        coalesce(
          locationDF("city_name"),
          concat(lit("Unknown_"), weatherDF("location_id"))
        ).alias("district")
      )

      val withDateParsed = joinedDF
        .withColumn("year", callUDF("parseYear", col("date")))
        .withColumn("month", callUDF("parseMonth", col("date")))
        .filter(col("year").isNotNull)
        .filter(col("month").isNotNull)
        .filter(col("shortwave_radiation").isNotNull)

      val monthlyStats = withDateParsed
        .groupBy("district", "year", "month")
        .agg(
          sum("shortwave_radiation").alias("total_radiation"),
          sum(
            when(col("shortwave_radiation") > RADIATION_THRESHOLD, col("shortwave_radiation"))
              .otherwise(0.0)
          ).alias("radiation_above_threshold"),
          count("shortwave_radiation").alias("record_count")
        )

      val result = monthlyStats
        .withColumn("percentage_above_threshold",
          when(col("total_radiation") > 0,
            (col("radiation_above_threshold") / col("total_radiation")) * 100
          ).otherwise(0.0)
        )
        .withColumn("month_name",
          when(col("month") === 1, "January")
            .when(col("month") === 2, "February")
            .when(col("month") === 3, "March")
            .when(col("month") === 4, "April")
            .when(col("month") === 5, "May")
            .when(col("month") === 6, "June")
            .when(col("month") === 7, "July")
            .when(col("month") === 8, "August")
            .when(col("month") === 9, "September")
            .when(col("month") === 10, "October")
            .when(col("month") === 11, "November")
            .when(col("month") === 12, "December")
            .otherwise("Unknown")
        )
        .select(
          col("district"),
          col("year"),
          col("month"),
          col("month_name"),
          round(col("total_radiation"), 2).alias("total_radiation_MJ_m2"),
          round(col("radiation_above_threshold"), 2).alias("radiation_above_15MJ_m2"),
          round(col("percentage_above_threshold"), 2).alias("percentage_above_15MJ"),
          col("record_count").alias("days_in_month")
        )
        .orderBy("district", "year", "month")

      result.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(outputPath)

      println("\n" + "=" * 40)
      println("SHORTWAVE RADIATION ANALYSIS COMPLETE")
      println("=" * 40)
      println(s"Analysis: Percentage of radiation >$RADIATION_THRESHOLD MJ/m² per month per district")
      println(s"Threshold: $RADIATION_THRESHOLD MJ/m²")
      println(s"Output: $outputPath")
      println("=" * 40 + "\n")

      println("Sample Results (Top 10):")
      result.show(10, truncate = false)

    } catch {
      case e: Exception =>
        System.err.println(s"Error during analysis: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
}
