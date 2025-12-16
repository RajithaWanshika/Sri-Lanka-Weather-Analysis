package com.weather.analytics.highest.precipitation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import com.weather.analytics.common.BaseReducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class MaxPrecipitationReducer extends BaseReducer<Text, DoubleWritable, Text, Text> {

  private MonthYearPrecipitationWritable maxPrecipitation = new MonthYearPrecipitationWritable();

  @Override
  protected Text getOutputKey() {
    return new Text();
  }

  @Override
  protected Text getOutputValue() {
    return new Text();
  }

  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

    double totalPrecipitation = StreamSupport.stream(values.spliterator(), false)
      .mapToDouble(DoubleWritable::get)
      .sum();

    String[] keyParts = key.toString().split("\\|");
    int year = keyParts.length > 0 ? Integer.parseInt(keyParts[0]) : 0;
    int month = keyParts.length > 1 ? Integer.parseInt(keyParts[1]) : 0;

    String monthName = getMonthName(month);

    String json = String.format(
      "{\"year\":%d,\"month\":%d,\"monthName\":\"%s\",\"totalPrecipitation\":%.2f}",
      year, month, monthName, totalPrecipitation
    );

    Text outputValue = getOutputValue();
    outputValue.set(json);
    context.write(key, outputValue);

    if (totalPrecipitation > maxPrecipitation.getTotalPrecipitation()) {
      maxPrecipitation.setYear(year);
      maxPrecipitation.setMonth(month);
      maxPrecipitation.setTotalPrecipitation(totalPrecipitation);
    }
  }
  
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    if (maxPrecipitation.getTotalPrecipitation() > 0) {
      String monthOrdinal = getMonthOrdinal(maxPrecipitation.getMonth());
      String summary = String.format("%s month in %d had the highest total precipitation of %.2f mm",
        monthOrdinal, maxPrecipitation.getYear(), maxPrecipitation.getTotalPrecipitation());

      String json = String.format(
        "{\"year\":%d,\"month\":%d,\"monthName\":\"%s\",\"totalPrecipitation\":%.2f,\"summary\":\"%s\"}",
        maxPrecipitation.getYear(), 
        maxPrecipitation.getMonth(), 
        getMonthName(maxPrecipitation.getMonth()), 
        maxPrecipitation.getTotalPrecipitation(),
        summary
      );

      Text outputValue = getOutputValue();
      outputValue.set(json);
      context.write(new Text("HIGHEST"), outputValue);
    }
  }
}
