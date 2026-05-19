package com.weather.station;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.weather.station.model.WeatherData;

public class MessageGenerator {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static long counter = 0;

    @JsonProperty("station_id")  private long stationId;
    @JsonProperty("s_no")        private long sno;
    @JsonProperty("battery_status") private String batteryStatus;
    @JsonProperty("status_timestamp") private long statusTimestamp;
    @JsonProperty("weather")     private WeatherData weather;

    public MessageGenerator(long stationId, long sno, String batteryStatus,
                            long statusTimestamp, WeatherData weather) {
        this.stationId       = stationId;
        this.sno             = sno;
        this.batteryStatus   = batteryStatus;
        this.statusTimestamp = statusTimestamp;
        this.weather         = weather;
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
        if (Math.random() < 0.10) return null; // 10% drop

        counter++;

        double r = Math.random();
        String battery = r < 0.30 ? "low" : r < 0.70 ? "medium" : "high";

        int humidity    = (int) (Math.random() * 101);
        int temperature = 50 + (int) (Math.random() * 71);
        int windSpeed   = (int) (Math.random() * 61);
        long timestamp  = System.currentTimeMillis() / 1000;

        WeatherData weather = new WeatherData(humidity, temperature, windSpeed);
        return new MessageGenerator(stationId, counter, battery, timestamp, weather).toString();
    }

    // Getters
    public long       getStationId()       { return stationId; }
    public long       getSno()             { return sno; }
    public String     getBatteryStatus()   { return batteryStatus; }
    public long       getStatusTimestamp() { return statusTimestamp; }
    public WeatherData getWeather()        { return weather; }
}