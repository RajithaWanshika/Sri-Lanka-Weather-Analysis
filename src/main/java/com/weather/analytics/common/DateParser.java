package com.weather.analytics.common;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

public final class DateParser {

  private static final List<DateTimeFormatter> DATE_FORMATTERS = new ArrayList<>();

  static {
    DATE_FORMATTERS.add(DateTimeFormatter.ofPattern("M/d/yyyy"));
    DATE_FORMATTERS.add(DateTimeFormatter.ofPattern("MM/dd/yyyy"));
    DATE_FORMATTERS.add(DateTimeFormatter.ofPattern("d/M/yyyy"));
    DATE_FORMATTERS.add(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
    DATE_FORMATTERS.add(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    DATE_FORMATTERS.add(DateTimeFormatter.ofPattern("yyyy-M-d"));
  }

  private DateParser() {}

  public static DateComponents parseDate(String date) {
    if (date == null || date.trim().isEmpty()) {
      return null;
    }

    String trimmedDate = date.trim();

    for (DateTimeFormatter formatter : DATE_FORMATTERS) {
      try {
        LocalDate localDate = LocalDate.parse(trimmedDate, formatter);
        return new DateComponents(localDate.getYear(), localDate.getMonthValue());
      } catch (DateTimeParseException e) {
        continue;
      }
    }
    return null;
  }

  public static class DateComponents {
    private final int year;
    private final int month;

    public DateComponents(int year, int month) {
      this.year = year;
      this.month = month;
    }

    public int getYear() {
      return year;
    }

    public int getMonth() {
      return month;
    }
  }
}
