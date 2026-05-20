package com.weather.central.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.weather.central.model.WeatherMessage;
import com.weather.central.bitcask.BitcaskService; // Import your service

@Component
public class WeatherConsumer {
    private static final Logger logger = LoggerFactory.getLogger(WeatherConsumer.class);
    private final ObjectMapper mapper = new ObjectMapper(); // create once, reuse
    private final BitcaskService bitcaskService;

    // Spring will automatically inject your storage service here
    public WeatherConsumer(BitcaskService bitcaskService) {
        this.bitcaskService = bitcaskService;
    }

    @KafkaListener(topics = "weather-readings", groupId = "central-station-group")
    public void consume(String message) {
        try {
            WeatherMessage weatherMessage = mapper.readValue(message, WeatherMessage.class);
            logger.info("Received message from station {}", weatherMessage.getStationId());
            logger.info("Message details: {}", weatherMessage);

            // --- THE STORAGE INTERFACE CHOICE ---

            // Choice A: Track latest state of the station (Overwrites old logs on
            // compaction)
            String key = "station_" + weatherMessage.getStationId();

            // Choice B: Timeseries Tracking (Keeps historical entries permanently)
            // String key = "station_" + weatherMessage.getStationId() + "_" +
            // weatherMessage.getSno();

            // Store the raw JSON string directly into Bitcask
            bitcaskService.put(key, message);

        } catch (Exception e) {
            logger.error("Failed to consume message: {}", e.getMessage());
        }
    }
}