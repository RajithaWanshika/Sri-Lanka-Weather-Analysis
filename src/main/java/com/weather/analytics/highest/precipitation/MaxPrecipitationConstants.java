package com.weather.analytics.highest.precipitation;

import com.weather.analytics.common.BaseConstants;

public final class MaxPrecipitationConstants extends BaseConstants {
  
  private MaxPrecipitationConstants() {
    super();
  }

  public static final String JOB_NAME = "Highest Precipitation Month/Year Analysis";
  public static final String COUNTER_GROUP = "MaxPrecipitation";
  public static final int EXPECTED_ARGUMENTS = 2;
  public static final String USAGE_MESSAGE = "Usage: MaxPrecipitationDriver <weather_input> <output>";
}
