package com.weather.station;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonIgnore;
public class MessageGenerator {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static long counter = 0;

    // Problem 1 fix: declare all fields Jackson needs to serialize
    @JsonProperty("station_id")
    private long stationId;

@JsonProperty("s_no")
private long sno; // Change to completely lowercase 'sno'

    @JsonProperty("battery_status")
    private String batteryStatus;

    @JsonProperty("status_timestamp")
    private long statusTimestamp;

    // Problem 3 fix: nested object as its own inner class
    @JsonProperty("weather")
    private WeatherData weather;

    // Constructor
public MessageGenerator(long stationId, long sno, String batteryStatus,
                        long statusTimestamp, WeatherData weather) {
    this.stationId      = stationId;
    this.sno            = sno; // Update here
    this.batteryStatus  = batteryStatus;
    this.statusTimestamp = statusTimestamp;
    this.weather        = weather;
}
    @Override
    public String toString() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (Exception e) {
            e.printStackTrace();
            return "{}";
        }
    }

    public static String generate(long stationId) {

        // 10% drop
        if (Math.random() < 0.10) {
            return null;
        }

        counter++;

        // Weighted battery status
        double r = Math.random();
        String battery = r < 0.30 ? "low" : r < 0.70 ? "medium" : "high";

        // Realistic ranges
        int humidity   = (int) (Math.random() * 101);       // 0-100 %
        int temperature = 50 + (int) (Math.random() * 71);  // 50-120 °F
        int windSpeed  = (int) (Math.random() * 61);         // 0-60 km/h

        long timestamp = System.currentTimeMillis() / 1000;

        WeatherData weather = new WeatherData(humidity, temperature, windSpeed);
        MessageGenerator msg = new MessageGenerator(stationId, counter, battery, timestamp, weather);
        return msg.toString();
    }

    // Problem 3 fix: inner class for the nested weather object
    public static class WeatherData {

        @JsonProperty("humidity")
        private int humidity;

        @JsonProperty("temperature")
        private int temperature;

        @JsonProperty("wind_speed")
        private int windSpeed;

        public WeatherData(int humidity, int temperature, int windSpeed) {
            this.humidity    = humidity;
            this.temperature = temperature;
            this.windSpeed   = windSpeed;
        }

        // Getters (Jackson needs these for serialization)
        public int getHumidity()    { return humidity; }
        public int getTemperature() { return temperature; }
        public int getWindSpeed()   { return windSpeed; }
    }

    // Getters for WeatherStation's outer fields
    public long   getStationId()      { return stationId; }
public long getSno() { // Change from getSNo() to getSno()
    return sno; 
}
    public String getBatteryStatus()  { return batteryStatus; }
    public long   getStatusTimestamp(){ return statusTimestamp; }
    public WeatherData getWeather()   { return weather; }
}