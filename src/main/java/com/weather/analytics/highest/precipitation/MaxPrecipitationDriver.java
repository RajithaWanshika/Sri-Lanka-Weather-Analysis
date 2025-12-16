package com.weather.analytics.highest.precipitation;

import com.weather.analytics.common.BaseWeatherDriver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class MaxPrecipitationDriver extends BaseWeatherDriver {

  @Override
  protected int getExpectedArgumentCount() {
    return MaxPrecipitationConstants.EXPECTED_ARGUMENTS;
  }

  @Override
  protected String getUsageMessage() {
    return MaxPrecipitationConstants.USAGE_MESSAGE;
  }

  @Override
  protected Job createJob(Configuration conf, String[] args) throws Exception {
    Job job = Job.getInstance(conf, MaxPrecipitationConstants.JOB_NAME);
    job.setJarByClass(MaxPrecipitationDriver.class);
    job.setMapperClass(MaxPrecipitationMapper.class);
    job.setReducerClass(MaxPrecipitationReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setNumReduceTasks(1);

    return job;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxPrecipitationDriver(), args);
    System.exit(exitCode);
  }
}
