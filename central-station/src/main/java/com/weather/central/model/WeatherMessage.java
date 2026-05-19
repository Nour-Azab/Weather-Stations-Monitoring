package com.weather.central.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WeatherMessage {
    
    @JsonProperty("station_id")
    private long stationId;

    @JsonProperty("s_no")
    private long sno;

    @JsonProperty("weather")
    private WeatherData weather;

    @JsonProperty("battery_status")
    private String batteryStatus;

    @JsonProperty("status_timestamp")
    private long statusTimestamp;

    // --- Getters & Setters ---

    public long getStationId() {
        return stationId;
    }

    public void setStationId(long stationId) {
        this.stationId = stationId;
    }

    public long getSno() {
        return sno;
    }

    public void setSno(long sno) {
        this.sno = sno;
    }

    public WeatherData getWeather() {
        return weather;
    }

    public void setWeather(WeatherData weather) {
        this.weather = weather;
    }

    public String getBatteryStatus() {
        return batteryStatus;
    }

    public void setBatteryStatus(String batteryStatus) {
        this.batteryStatus = batteryStatus;
    }

    public long getStatusTimestamp() {
        return statusTimestamp;
    }

    public void setStatusTimestamp(long statusTimestamp) {
        this.statusTimestamp = statusTimestamp;
    }

    @Override
    public String toString() {
        return "WeatherMessage{" +
                "stationId=" + stationId +
                ", sno=" + sno +
                ", weather=" + weather +
                ", batteryStatus='" + batteryStatus + '\'' +
                ", statusTimestamp=" + statusTimestamp +
                '}';
    }
}