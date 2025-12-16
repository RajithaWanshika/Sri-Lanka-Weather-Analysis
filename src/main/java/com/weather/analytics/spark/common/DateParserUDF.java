package com.weather.analytics.spark.common;

import com.weather.analytics.common.DateParser;
import com.weather.analytics.common.DateParser.DateComponents;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

public class DateParserUDF {

  private static final List<DateTimeFormatter> DATE_FORMATTERS = new ArrayList<>();

  static {
    DATE_FORMATTERS.add(DateTimeFormatter.ofPattern("M/d/yyyy"));
    DATE_FORMATTERS.add(DateTimeFormatter.ofPattern("MM/dd/yyyy"));
    DATE_FORMATTERS.add(DateTimeFormatter.ofPattern("d/M/yyyy"));
    DATE_FORMATTERS.add(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
    DATE_FORMATTERS.add(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    DATE_FORMATTERS.add(DateTimeFormatter.ofPattern("yyyy-M-d"));
  }

  public static class ParseYearUDF implements UDF1<String, Integer> {
    @Override
    public Integer call(String date) throws Exception {
      DateComponents components = DateParser.parseDate(date);
      return components != null ? components.getYear() : null;
    }
  }

  public static class ParseMonthUDF implements UDF1<String, Integer> {
    @Override
    public Integer call(String date) throws Exception {
      DateComponents components = DateParser.parseDate(date);
      return components != null ? components.getMonth() : null;
    }
  }

  public static class ParseDayOfYearUDF implements UDF1<String, Integer> {
    @Override
    public Integer call(String date) throws Exception {
      if (date == null || date.trim().isEmpty()) {
        return null;
      }
      String trimmedDate = date.trim();
      for (DateTimeFormatter formatter : DATE_FORMATTERS) {
        try {
          LocalDate localDate = LocalDate.parse(trimmedDate, formatter);
          return localDate.getDayOfYear();
        } catch (DateTimeParseException e) {
          continue;
        }
      }
      return null;
    }
  }

  public static class ParseWeekOfYearUDF implements UDF1<String, Integer> {
    @Override
    public Integer call(String date) throws Exception {
      if (date == null || date.trim().isEmpty()) {
        return null;
      }
      String trimmedDate = date.trim();
      for (DateTimeFormatter formatter : DATE_FORMATTERS) {
        try {
          LocalDate localDate = LocalDate.parse(trimmedDate, formatter);
          // Calculate week of year (ISO 8601 week numbering)
          int dayOfYear = localDate.getDayOfYear();
          int weekOfYear = (dayOfYear - 1) / 7 + 1;
          return weekOfYear;
        } catch (DateTimeParseException e) {
          continue;
        }
      }
      return null;
    }
  }

  public static void registerUDFs(org.apache.spark.sql.SparkSession spark) {
    spark.udf().register("parseYear", new ParseYearUDF(), DataTypes.IntegerType);
    spark.udf().register("parseMonth", new ParseMonthUDF(), DataTypes.IntegerType);
    spark.udf().register("parseDayOfYear", new ParseDayOfYearUDF(), DataTypes.IntegerType);
    spark.udf().register("parseWeekOfYear", new ParseWeekOfYearUDF(), DataTypes.IntegerType);
  }
}
