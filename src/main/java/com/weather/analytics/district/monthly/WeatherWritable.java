package com.weather.analytics.district.monthly;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class WeatherWritable implements Writable {
  private double totalPrecipitation;
  private double temperatureSum;
  private long recordCount;

  public WeatherWritable() {
    this.totalPrecipitation = 0.0;
    this.temperatureSum = 0.0;
    this.recordCount = 0;
  }

  public WeatherWritable(double precipitation, double temperature) {
    this.totalPrecipitation = precipitation;
    this.temperatureSum = temperature;
    this.recordCount = 1;
  }

  public void addData(double precipitation, double temperature) {
    this.totalPrecipitation += precipitation;
    this.temperatureSum += temperature;
    this.recordCount++;
  }

  public void merge(WeatherWritable other) {
    this.totalPrecipitation += other.totalPrecipitation;
    this.temperatureSum += other.temperatureSum;
    this.recordCount += other.recordCount;
  }

  public double getTotalPrecipitation() { 
    return totalPrecipitation; 
  }

  public double getMeanTemperature() { 
    return recordCount > 0 ? temperatureSum / recordCount : 0; 
  }

  public long getRecordCount() { 
    return recordCount; 
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeDouble(totalPrecipitation);
    output.writeDouble(temperatureSum);
    output.writeLong(recordCount);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    totalPrecipitation = input.readDouble();
    temperatureSum = input.readDouble();
    recordCount = input.readLong();
  }

  @Override
  public String toString() {
    return String.format(
      "%.2f\t%.2f\t%d", 
      totalPrecipitation,
      getMeanTemperature(),
      recordCount
    );
  }
}
