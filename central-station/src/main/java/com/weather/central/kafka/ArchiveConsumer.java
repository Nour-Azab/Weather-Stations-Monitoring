package com.weather.central.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.weather.central.model.WeatherMessage;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Component
public class ArchiveConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ArchiveConsumer.class);
    private static final int TARGET_BATCH_SIZE = 1000;
    
    private final ObjectMapper mapper = new ObjectMapper();
    private final Object lock = new Object();

    // The shared thread-safe queue checked by the background ParquetArchiverWorker
    public static final Queue<WeatherMessage> archiveBuffer = new ConcurrentLinkedQueue<>();

    @KafkaListener(
        topics = "weather-readings",
        groupId = "archive-group",
        containerFactory = "batchManualAckContainerFactory"
    )
    public void consume(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        
        synchronized (lock) {
            for (ConsumerRecord<String, String> record : records) {
                try {
                    // Parse raw incoming stream string into standard object schema
                    WeatherMessage weatherMessage = mapper.readValue(record.value(), WeatherMessage.class);
                    archiveBuffer.add(weatherMessage);
                    
                } catch (Exception e) {
                    logger.error("Failed to parse message into archive queue: {}", e.getMessage());
                }
            }
            
            // Acknowledge the Kafka broker immediately after dropping the chunk into our buffer.
            // This ensures streaming throughput doesn't bottleneck on the 10-second archiver clock.
            ack.acknowledge();
            
            logger.info("Current archive buffer state: {}/{} messages accumulated.", 
                    archiveBuffer.size(), TARGET_BATCH_SIZE);
        }
    }
}