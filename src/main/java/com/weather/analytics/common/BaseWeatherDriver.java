package com.weather.analytics.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public abstract class BaseWeatherDriver extends Configured implements Tool {

  protected static final int SUCCESS_EXIT_CODE = 0;
  protected static final int FAILURE_EXIT_CODE = 1;
  protected static final int INVALID_ARGUMENTS_EXIT_CODE = -1;

  protected abstract Job createJob(Configuration conf, String[] args) throws Exception;

  protected abstract int getExpectedArgumentCount();

  protected abstract String getUsageMessage();

  @Override
  public int run(String[] args) throws Exception {
    if (!validateArguments(args)) {
      return INVALID_ARGUMENTS_EXIT_CODE;
    }

    try {
      Configuration conf = getConf();
      Job job = createJob(conf, args);
      boolean success = job.waitForCompletion(true);
      return success ? SUCCESS_EXIT_CODE : FAILURE_EXIT_CODE;
    } catch (Exception e) {
      System.err.println("Error executing MapReduce job: " + e.getMessage());
      e.printStackTrace();
      return FAILURE_EXIT_CODE;
    }
  }

  protected boolean validateArguments(String[] args) {
    if (args == null || args.length != getExpectedArgumentCount()) {
      System.err.println(getUsageMessage());
      return false;
    }

    for (String arg : args) {
      if (arg == null || arg.trim().isEmpty()) {
        System.err.println("Error: All arguments must be non-empty paths");
        System.err.println(getUsageMessage());
        return false;
      }
    }
    return true;
  }
}
