package com.weather.central.model;

import org.springframework.kafka.support.Acknowledgment;

public class ArchiveTask {
    private final WeatherMessage message;
    private final Acknowledgment ack;

    public ArchiveTask(WeatherMessage message, Acknowledgment ack) {
        this.message = message;
        this.ack = ack;
    }

    public WeatherMessage getMessage() {
        return message;
    }

    public Acknowledgment getAck() {
        return ack;
    }
}