package com.weather.central.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WeatherData {
    // The nested weather object
    @JsonProperty("humidity") private int humidity;
    @JsonProperty("temperature") private int temperature;
    @JsonProperty("wind_speed") private int windSpeed;
    // getters + setters
    public int getHumidity() {
        return humidity;
    }
    public int getTemperature() {
        return temperature;
    }
    public int getWindSpeed() {
        return windSpeed;
    }
    public void setHumidity(int humidity) {
        this.humidity = humidity;
    }
    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }
    public void setWindSpeed(int windSpeed) {
        this.windSpeed = windSpeed;
    }
}
