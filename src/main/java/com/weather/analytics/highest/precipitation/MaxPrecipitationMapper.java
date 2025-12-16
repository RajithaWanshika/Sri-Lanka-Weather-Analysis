package com.weather.analytics.highest.precipitation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

import com.weather.analytics.common.DateParser;
import com.weather.analytics.common.DateParser.DateComponents;

public class MaxPrecipitationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  private static final Text OUTPUT_KEY = new Text();
  private static final DoubleWritable OUTPUT_VALUE = new DoubleWritable();

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    if (key.get() == MaxPrecipitationConstants.HEADER_ROW_OFFSET) {
      return;
    }

    String line = value.toString();
    String[] fields = line.split(MaxPrecipitationConstants.CSV_DELIMITER);

    try {
      if (fields.length <= MaxPrecipitationConstants.PRECIPITATION_SUM_INDEX) {
        context.getCounter(
          MaxPrecipitationConstants.COUNTER_GROUP, 
          MaxPrecipitationConstants.MALFORMED_RECORDS_COUNTER
        ).increment(1);
        return;
      }

      String date = fields[MaxPrecipitationConstants.DATE_INDEX].trim();
      double precipitationSum = Double.parseDouble(
        fields[MaxPrecipitationConstants.PRECIPITATION_SUM_INDEX].trim()
      );

      DateComponents dateComponents = DateParser.parseDate(date);
      if (dateComponents == null) {
        context.getCounter(
          MaxPrecipitationConstants.COUNTER_GROUP, 
          MaxPrecipitationConstants.MALFORMED_RECORDS_COUNTER
        ).increment(1);
        return;
      }

      String compositeKey = String.join(
        MaxPrecipitationConstants.COMPOSITE_KEY_DELIMITER,
        String.valueOf(dateComponents.getYear()),
        String.valueOf(dateComponents.getMonth())
      );

      OUTPUT_KEY.set(compositeKey);
      OUTPUT_VALUE.set(precipitationSum);

      context.write(OUTPUT_KEY, OUTPUT_VALUE);

    } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
      context.getCounter(
        MaxPrecipitationConstants.COUNTER_GROUP, 
        MaxPrecipitationConstants.MALFORMED_RECORDS_COUNTER
      ).increment(1);
    }
  }

}
