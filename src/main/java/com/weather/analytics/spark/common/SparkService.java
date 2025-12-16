package com.weather.analytics.spark.common;

import org.apache.spark.sql.SparkSession;

public final class SparkService {

  private SparkService() {}

  public static SparkSession createSparkSession(String appName) {
    return SparkSession.builder()
      .appName(appName)
      .getOrCreate();
  }

  public static void stopSparkSession(SparkSession spark) {
    if (spark != null) {
      spark.stop();
    }
  }
}
