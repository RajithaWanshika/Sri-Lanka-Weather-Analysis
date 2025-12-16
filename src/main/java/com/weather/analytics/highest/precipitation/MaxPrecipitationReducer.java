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
      maxPrecipitation.addPrecipitation(totalPrecipitation);
      maxPrecipitation.setYear(year);
      maxPrecipitation.setMonth(month);
      maxPrecipitation.setTotalPrecipitation(totalPrecipitation);
    }
  }
  
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    Text outputValue = getOutputValue();
    outputValue.set(String.format("{\"year\":%d,\"month\":%d,\"monthName\":\"%s\",\"totalPrecipitation\":%.2f}",
      maxPrecipitation.getYear(), maxPrecipitation.getMonth(), getMonthName(maxPrecipitation.getMonth()), maxPrecipitation.getTotalPrecipitation()));
    context.write(new Text("HIGHEST"), outputValue);
  }
}
