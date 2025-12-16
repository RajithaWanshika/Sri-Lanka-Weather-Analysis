package com.weather.analytics.district.monthly;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import com.weather.analytics.district.monthly.WeatherWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Optional;

public class WeatherDriver extends Configured implements Tool {

  private static final int SUCCESS_EXIT_CODE = 0;
  private static final int FAILURE_EXIT_CODE = 1;
  private static final int INVALID_ARGUMENTS_EXIT_CODE = -1;

  @Override
  public int run(String[] args) throws Exception {
    return validateArguments(args)
      .map(this::configureAndRunJob)
      .orElse(INVALID_ARGUMENTS_EXIT_CODE);
  }

  private Optional<JobArguments> validateArguments(String[] args) {
    if (args == null || args.length != WeatherConstants.EXPECTED_ARGUMENTS) {
      System.err.println(WeatherConstants.USAGE_MESSAGE);
      return Optional.empty();
    }
    
    if (args[0].trim().isEmpty() || args[1].trim().isEmpty() || args[2].trim().isEmpty()) {
      System.err.println("Error: All arguments must be non-empty paths");
      System.err.println(WeatherConstants.USAGE_MESSAGE);
      return Optional.empty();
    }
    
    return Optional.of(new JobArguments(args[0].trim(), args[1].trim(), args[2].trim()));
  }

  private int configureAndRunJob(JobArguments jobArgs) {
    try {
      Configuration conf = getConf();
      conf.set(WeatherConstants.LOCATION_FILE_PATH_KEY, jobArgs.getLocationInput());

      Job job = createJob(conf, jobArgs);
      boolean success = job.waitForCompletion(true);

      return success ? SUCCESS_EXIT_CODE : FAILURE_EXIT_CODE;
    } catch (Exception e) {
      System.err.println("Error executing MapReduce job: " + e.getMessage());
      e.printStackTrace();
      return FAILURE_EXIT_CODE;
    }
  }

  private Job createJob(Configuration conf, JobArguments jobArgs) throws Exception {
    Job job = Job.getInstance(conf, WeatherConstants.JOB_NAME);
    job.setJarByClass(WeatherDriver.class);

    job.setMapperClass(WeatherMapper.class);
    job.setReducerClass(WeatherReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(WeatherWritable.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(jobArgs.getWeatherInput()));
    FileOutputFormat.setOutputPath(job, new Path(jobArgs.getOutput()));

    return job;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new WeatherDriver(), args);
    System.exit(exitCode);
  }

  private static class JobArguments {
    private final String weatherInput;
    private final String locationInput;
    private final String output;

    JobArguments(String weatherInput, String locationInput, String output) {
      this.weatherInput = weatherInput;
      this.locationInput = locationInput;
      this.output = output;
    }

    String getWeatherInput() {
      return weatherInput;
    }

    String getLocationInput() {
      return locationInput;
    }

    String getOutput() {
      return output;
    }
  }
}
