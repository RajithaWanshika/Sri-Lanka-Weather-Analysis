package com.weather.analytics.district.monthly;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.weather.analytics.common.DateParser;
import com.weather.analytics.common.DateParser.DateComponents;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, WeatherWritable> {
  
  private static final Text OUTPUT_KEY = new Text();
  private Map<Integer, String> locationMap;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    locationMap = loadLocationMap(context);
  }

  private Map<Integer, String> loadLocationMap(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    String locationFile = conf.get(WeatherConstants.LOCATION_FILE_PATH_KEY);
    
    if (locationFile == null || locationFile.trim().isEmpty()) {
      return Collections.emptyMap();
    }

    Path path = new Path(locationFile);
    FileSystem fs = FileSystem.get(conf);
    
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
         Stream<String> lines = reader.lines()) {
      
      return lines
          .skip(1)
          .filter(line -> !line.trim().isEmpty())
          .map(this::parseLocationLine)
          .filter(entry -> entry != null)
          .collect(Collectors.toMap(
              LocationEntry::getLocationId,
              LocationEntry::getCityName,
              (existing, replacement) -> existing
          ));
    }
  }

  private LocationEntry parseLocationLine(String line) {
    try {
      String[] fields = line.split(WeatherConstants.CSV_DELIMITER);
      if (fields.length <= Math.max(WeatherConstants.LOCATION_ID_INDEX, WeatherConstants.CITY_NAME_INDEX)) {
        return null;
      }

      int locationId = Integer.parseInt(fields[WeatherConstants.LOCATION_ID_INDEX].trim());
      String cityName = fields[WeatherConstants.CITY_NAME_INDEX].trim();

      return new LocationEntry(locationId, cityName);
    } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
      return null;
    }
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    if (key.get() == WeatherConstants.HEADER_ROW_OFFSET) {
      return;
    }

    parseAndEmitWeatherRecord(value.toString(), context);
  }

  private void parseAndEmitWeatherRecord(String line, Context context) {
    WeatherRecord record = parseWeatherRecord(line);

    if (record == null) {
      context.getCounter(
        WeatherConstants.COUNTER_GROUP, 
        WeatherConstants.MALFORMED_RECORDS_COUNTER
      ).increment(1);
      return;
    }

    String compositeKey = buildCompositeKey(record);
    OUTPUT_KEY.set(compositeKey);
    
    WeatherWritable outputValue = new WeatherWritable(
      record.getPrecipitationSum(), 
      record.getTemperatureMean()
    );

    try {
      context.write(OUTPUT_KEY, outputValue);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("Error writing mapper output", e);
    }
  }

  private WeatherRecord parseWeatherRecord(String line) {
    try {
      String[] fields = line.split(WeatherConstants.CSV_DELIMITER);
      
      if (fields.length <= Math.max(
          Math.max(WeatherConstants.LOCATION_ID_INDEX, WeatherConstants.DATE_INDEX),
          Math.max(WeatherConstants.TEMPERATURE_MEAN_INDEX, WeatherConstants.PRECIPITATION_SUM_INDEX)
      )) {
        return null;
      }

      int locationId = Integer.parseInt(fields[WeatherConstants.LOCATION_ID_INDEX].trim());
      String date = fields[WeatherConstants.DATE_INDEX].trim();
      double temperatureMean = Double.parseDouble(fields[WeatherConstants.TEMPERATURE_MEAN_INDEX].trim());
      double precipitationSum = Double.parseDouble(fields[WeatherConstants.PRECIPITATION_SUM_INDEX].trim());

      DateComponents dateComponents = DateParser.parseDate(date);
      if (dateComponents == null) {
        return null;
      }

      String district = locationMap.getOrDefault(
          locationId, 
          WeatherConstants.UNKNOWN_DISTRICT_PREFIX + locationId
      );

      return new WeatherRecord(
        locationId, 
        district, 
        dateComponents.getYear(), 
        dateComponents.getMonth(),
        temperatureMean, 
        precipitationSum
      );
    } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
      return null;
    }
  }

  private String buildCompositeKey(WeatherRecord record) {
    return String.join(
      WeatherConstants.COMPOSITE_KEY_DELIMITER,
      record.getDistrict(),
      String.valueOf(record.getMonth())
    );
  }

  private static class LocationEntry {
    private final int locationId;
    private final String cityName;

    LocationEntry(int locationId, String cityName) {
      this.locationId = locationId;
      this.cityName = cityName;
    }

    int getLocationId() {
      return locationId;
    }

    String getCityName() {
      return cityName;
    }
  }

  private static class WeatherRecord {
    private final int locationId;
    private final String district;
    private final int year;
    private final int month;
    private final double temperatureMean;
    private final double precipitationSum;

    WeatherRecord(int locationId, String district, int year, int month, double temperatureMean, double precipitationSum) {
      this.locationId = locationId;
      this.district = district;
      this.year = year;
      this.month = month;
      this.temperatureMean = temperatureMean;
      this.precipitationSum = precipitationSum;
    }

    int getLocationId() { return locationId; }
    String getDistrict() { return district; }
    int getYear() { return year; }
    int getMonth() { return month; }
    double getTemperatureMean() { return temperatureMean; }
    double getPrecipitationSum() { return precipitationSum; }
  }

} 
