package com.weather.central.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.weather.central.model.WeatherMessage;
@Component
public class WeatherConsumer {
    Logger logger = LoggerFactory.getLogger(WeatherConsumer.class);
    ObjectMapper mapper = new ObjectMapper(); // create once, reuse
    
    @KafkaListener(topics = "weather-readings", groupId = "central-station-group")
    public void consume(String message) {
        try {
            WeatherMessage weatherMessage = mapper.readValue(message, WeatherMessage.class);
            logger.info("Received message: {} from station {}", 
                        weatherMessage, weatherMessage.getStationId());
        } catch (Exception e) {
            logger.error("Failed to consume message: {}", e.getMessage());
        }
    }
}


