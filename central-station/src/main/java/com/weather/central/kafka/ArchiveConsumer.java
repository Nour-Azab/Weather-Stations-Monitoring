package com.weather.central.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.weather.central.model.WeatherMessage;
import com.weather.central.model.ArchiveTask;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Component
public class ArchiveConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ArchiveConsumer.class);
    private static final int TARGET_BATCH_SIZE = 1000;
    
    private final ObjectMapper mapper = new ObjectMapper();
    private final Object lock = new Object();

    // Now holds the integrated data payload + acknowledgment tracking bundle
    public static final Queue<ArchiveTask> archiveBuffer = new ConcurrentLinkedQueue<>();

    @KafkaListener(
        topics = "weather-readings",
        groupId = "archive-group",
        containerFactory = "batchManualAckContainerFactory"
    )
    public void consume(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        
        synchronized (lock) {
            for (ConsumerRecord<String, String> record : records) {
                try {
                    WeatherMessage weatherMessage = mapper.readValue(record.value(), WeatherMessage.class);
                    
                    // Package the individual record along with the overarching batch acknowledgment token
                    archiveBuffer.add(new ArchiveTask(weatherMessage, ack));
                    
                } catch (Exception e) {
                    logger.error("Failed to parse message into archive queue: {}", e.getMessage());
                }
            }
            
            logger.info("Current archive buffer state: {}/{} tasks accumulated in memory.", 
                    archiveBuffer.size(), TARGET_BATCH_SIZE);
        }
    }
}