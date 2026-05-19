package com.weather.station.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WeatherData {

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

    public int getHumidity()    { return humidity; }
    public int getTemperature() { return temperature; }
    public int getWindSpeed()   { return windSpeed; }
}