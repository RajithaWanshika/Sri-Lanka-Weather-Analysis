package com.weather.analytics.highest.precipitation;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MonthYearPrecipitationWritable implements Writable {
  private int year;
  private int month;
  private double totalPrecipitation;

  public MonthYearPrecipitationWritable() {
    this.year = 0;
    this.month = 0;
    this.totalPrecipitation = 0.0;
  }

  public MonthYearPrecipitationWritable(int year, int month, double precipitation) {
    this.year = year;
    this.month = month;
    this.totalPrecipitation = precipitation;
  }

  public void addPrecipitation(double precipitation) {
    this.totalPrecipitation += precipitation;
  }

  public void merge(MonthYearPrecipitationWritable other) {
    this.totalPrecipitation += other.totalPrecipitation;
  }

  public int getYear() {
    return year; 
  }

  public int getMonth() {
    return month;
  }

  public double getTotalPrecipitation() {
    return totalPrecipitation;
  }

  public void setYear(int year) {
    this.year = year; 
  }

  public void setMonth(int month) {
    this.month = month;
  }

  public void setTotalPrecipitation(double precipitation) { 
    this.totalPrecipitation = precipitation; 
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(year);
    output.writeInt(month);
    output.writeDouble(totalPrecipitation);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    year = input.readInt();
    month = input.readInt();
    totalPrecipitation = input.readDouble();
  }

  @Override
  public String toString() {
    return String.format("%d\t%d\t%.2f", year, month, totalPrecipitation);
  }
}
