package com.weather.analytics.district.monthly;

import java.io.IOException;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.Text;

import com.weather.analytics.common.BaseReducer;

public class WeatherReducer extends BaseReducer<Text, WeatherWritable, Text, Text> {

  @Override
  protected Text getOutputKey() {
    return new Text();
  }

  @Override
  protected Text getOutputValue() {
    return new Text();
  }

  @Override
  protected void reduce(Text key, Iterable<WeatherWritable> values, Context context) throws IOException, InterruptedException {
    WeatherWritable aggregatedResult = aggregateWeatherData(values);

    String[] keyParts = key.toString().split("\\|");
    String district = keyParts.length > 0 ? keyParts[0] : "Unknown";
    int month = keyParts.length > 1 ? Integer.parseInt(keyParts[1]) : 0;

    String monthOrdinal = getMonthOrdinal(month);
    String monthName = getMonthName(month);

    String json = String.format(
      "{\"district\":\"%s\",\"month\":%d,\"monthName\":\"%s\",\"totalPrecipitation\":%.2f,\"meanTemperature\":%.2f,\"recordCount\":%d,\"summary\":\"%s had a total precipitation of %.2f mm with a mean temperature of %.2f°C for %s month\"}",
      district, month, monthName,
      aggregatedResult.getTotalPrecipitation(),
      aggregatedResult.getMeanTemperature(),
      aggregatedResult.getRecordCount(),
      district,
      aggregatedResult.getTotalPrecipitation(),
      aggregatedResult.getMeanTemperature(),
      monthOrdinal
    );

    Text outputValue = getOutputValue();
    outputValue.set(json);
    context.write(key, outputValue);

    context.getCounter(
      WeatherConstants.COUNTER_GROUP, 
      WeatherConstants.DISTRICT_MONTHLY_RECORDS_COUNTER
    ).increment(1);
  }

  private WeatherWritable aggregateWeatherData(Iterable<WeatherWritable> values) {
    return StreamSupport.stream(values.spliterator(), false)
      .reduce(
        new WeatherWritable(),
        (accumulator, value) -> {
          accumulator.merge(value);
          return accumulator;
        },
        (acc1, acc2) -> {
          acc1.merge(acc2);
          return acc1;
        }
      );
  }
}
