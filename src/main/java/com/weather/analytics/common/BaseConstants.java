package com.weather.analytics.common;

public abstract class BaseConstants {

  protected BaseConstants() {
    throw new AssertionError("Utility class should not be instantiated");
  }

  public static final int DATE_INDEX = 1;
  public static final int PRECIPITATION_SUM_INDEX = 13;

  public static final String CSV_DELIMITER = ",";
  public static final String DATE_DELIMITER_SLASH = "/";
  public static final String DATE_DELIMITER_HYPHEN = "-";
  public static final String COMPOSITE_KEY_DELIMITER = "|";

  public static final int HEADER_ROW_OFFSET = 0;

  public static final String MALFORMED_RECORDS_COUNTER = "MalformedRecords";

  public static final int YEAR_INDEX_SLASH = 2;
  public static final int MONTH_INDEX_SLASH = 0;
  public static final int YEAR_INDEX_HYPHEN = 0;
  public static final int MONTH_INDEX_HYPHEN = 1;
}
