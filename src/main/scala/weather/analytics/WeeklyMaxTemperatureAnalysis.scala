package weather.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Try

object WeeklyMaxTemperatureAnalysis {

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

  private def parseWeekOfYearFunc(dateStr: String): Integer = {
    parseDate(dateStr).map { date =>
      val dayOfYear = date.getDayOfYear
      (dayOfYear - 1) / 7 + 1
    }.map(Integer.valueOf).orNull
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

    val tempMaxCol = rawDF.columns.find(_.contains("temperature_2m_max"))
      .getOrElse(throw new IllegalArgumentException("Could not find temperature_2m_max column in CSV"))

    rawDF.select(
      col("location_id").cast(IntegerType).alias("location_id"),
      col("date").alias("date"),
      col(tempMaxCol).cast(DoubleType).alias("temperature_max")
    )
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: WeeklyMaxTemperatureAnalysis <weather_input> <location_input> <output>")
      System.exit(1)
    }

    val weatherInputPath = args(0)
    val locationInputPath = args(1)
    val outputPath = args(2)

    val spark = SparkSession.builder()
      .appName("Weekly Maximum Temperature Analysis")
      .getOrCreate()

    try {
      spark.udf.register("parseYear", parseYearFunc _)
      spark.udf.register("parseMonth", parseMonthFunc _)
      spark.udf.register("parseWeekOfYear", parseWeekOfYearFunc _)

      val locationDF = loadLocationData(spark, locationInputPath)
      val weatherDF = loadWeatherData(spark, weatherInputPath)

      val joinedDF = weatherDF.join(
        locationDF,
        weatherDF("location_id") === locationDF("loc_id"),
        "left"
      ).select(
        weatherDF("location_id"),
        weatherDF("date"),
        weatherDF("temperature_max"),
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
        .filter(col("temperature_max").isNotNull)
        .withColumn("week_of_year",
          coalesce(
            callUDF("parseWeekOfYear", col("date")),
            lit(1)
          )
        )

      val monthlyAvgTempOverall = withDateParsed
        .groupBy("year", "month")
        .agg(avg("temperature_max").alias("avg_max_temp"))

      val maxTempPerYear = monthlyAvgTempOverall
        .groupBy("year")
        .agg(max("avg_max_temp").alias("max_avg_temp"))

      val hottestMonthsGlobal = monthlyAvgTempOverall.as("m")
        .join(
          maxTempPerYear.as("max"),
          col("m.year") === col("max.year") &&
            col("m.avg_max_temp") === col("max.max_avg_temp"),
          "inner"
        )
        .select(
          col("m.year").alias("year"),
          col("m.month").alias("hottest_month"),
          round(col("m.avg_max_temp"), 2).alias("avg_temperature")
        )
        .groupBy("year")
        .agg(
          min("hottest_month").alias("hottest_month"),
          max("avg_temperature").alias("avg_temperature")
        )

      val hottestMonthRecords = withDateParsed.as("w")
        .join(
          hottestMonthsGlobal.as("h"),
          col("w.year") === col("h.year") &&
            col("w.month") === col("h.hottest_month"),
          "inner"
        )
        .select(
          col("w.district"),
          col("w.year"),
          col("w.month"),
          col("w.week_of_year"),
          col("w.temperature_max")
        )

      val weeklyMaxTemp = hottestMonthRecords
        .groupBy("district", "year", "month", "week_of_year")
        .agg(
          max("temperature_max").alias("weekly_max_temp"),
          count("temperature_max").alias("days_in_week")
        )
        .orderBy("district", "year", "month", "week_of_year")

      val addMonthName = (df: DataFrame) => df.withColumn("month_name",
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

      val hottestMonthsSummary = hottestMonthsGlobal
        .withColumn("month_name",
          when(col("hottest_month") === 1, "January")
            .when(col("hottest_month") === 2, "February")
            .when(col("hottest_month") === 3, "March")
            .when(col("hottest_month") === 4, "April")
            .when(col("hottest_month") === 5, "May")
            .when(col("hottest_month") === 6, "June")
            .when(col("hottest_month") === 7, "July")
            .when(col("hottest_month") === 8, "August")
            .when(col("hottest_month") === 9, "September")
            .when(col("hottest_month") === 10, "October")
            .when(col("hottest_month") === 11, "November")
            .when(col("hottest_month") === 12, "December")
            .otherwise("Unknown")
        )
        .select(
          col("year"),
          col("hottest_month").alias("month"),
          col("month_name").alias("hottest_month_name"),
          col("avg_temperature").alias("avg_max_temperature_C")
        )
        .orderBy("year")

      val weeklyMaxTempDistrictWise = addMonthName(weeklyMaxTemp)
        .select(
          col("district"),
          col("year"),
          col("month"),
          col("month_name").alias("hottest_month_name"),
          col("week_of_year"),
          round(col("weekly_max_temp"), 2).alias("weekly_max_temperature_C"),
          col("days_in_week")
        )
        .orderBy("district", "year", "week_of_year")

      val overallWeeklyMaxTemp = hottestMonthRecords
        .groupBy("year", "month", "week_of_year")
        .agg(
          max("temperature_max").alias("overall_weekly_max_temp"),
          countDistinct("district").alias("districts_count"),
          count("temperature_max").alias("total_days")
        )
        .orderBy("year", "week_of_year")

      val overallWeeklyMaxTempFormatted = addMonthName(overallWeeklyMaxTemp)
        .select(
          col("year"),
          col("month"),
          col("month_name").alias("hottest_month_name"),
          col("week_of_year"),
          round(col("overall_weekly_max_temp"), 2).alias("overall_weekly_max_temperature_C"),
          col("districts_count"),
          col("total_days")
        )
        .orderBy("year", "week_of_year")

      val baseOutputPath = outputPath.stripSuffix("/")
      
      hottestMonthsSummary.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"${baseOutputPath}/1_hottest_months_summary")

      weeklyMaxTempDistrictWise.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"${baseOutputPath}/2_weekly_temps_by_district")

      overallWeeklyMaxTempFormatted.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(s"${baseOutputPath}/3_overall_weekly_temps")

      println("\n" + "=" * 70)
      println("WEEKLY MAXIMUM TEMPERATURE ANALYSIS COMPLETE")
      println("=" * 70)
      println("Three output tables generated:")
      println(s"  1. Hottest month per year (global): ${baseOutputPath}/1_hottest_months_summary")
      println(s"  2. Weekly temps by district: ${baseOutputPath}/2_weekly_temps_by_district")
      println(s"  3. Overall weekly temps (all districts): ${baseOutputPath}/3_overall_weekly_temps")
      println("=" * 70 + "\n")

      println("TABLE 1: Hottest Month per Year (Across All Districts):")
      println("-" * 70)
      hottestMonthsSummary.show(false)

      println("\nTABLE 2: Weekly Max Temps by District (Sample - Top 15):")
      println("-" * 70)
      weeklyMaxTempDistrictWise.show(15, truncate = false)

      println("\nTABLE 3: Overall Weekly Max Temps Across All Districts (Sample - Top 15):")
      println("-" * 70)
      overallWeeklyMaxTempFormatted.show(15, truncate = false)

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
