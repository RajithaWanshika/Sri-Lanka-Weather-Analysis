package com.weather.analytics.district.monthly;

import com.weather.analytics.common.BaseConstants;

public final class WeatherConstants extends BaseConstants {
  
  private WeatherConstants() {
    super();
  }

  public static final String LOCATION_FILE_PATH_KEY = "location.file.path";
  public static final String JOB_NAME = "District Monthly Weather Analysis";

  public static final int LOCATION_ID_INDEX = 0;
  public static final int TEMPERATURE_MEAN_INDEX = 5;
  public static final int CITY_NAME_INDEX = 7;

  public static final String DATE_DELIMITER = "-";
  public static final String UNKNOWN_DISTRICT_PREFIX = "Unknown_";

  public static final String COUNTER_GROUP = "WeatherAnalysis";
  public static final String DISTRICT_MONTHLY_RECORDS_COUNTER = "DistrictMonthlyRecords";

  public static final int EXPECTED_ARGUMENTS = 3;
  public static final String USAGE_MESSAGE = "Usage: WeatherDriver <weather_input> <location_input> <output>";

  public static final int YEAR_INDEX = 0;
  public static final int MONTH_INDEX = 1;
  public static final int EXPECTED_DATE_PARTS = 2;
}
