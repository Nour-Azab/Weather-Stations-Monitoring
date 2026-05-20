package com.weather.central.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.weather.central.model.WeatherMessage;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Component
public class ArchiveConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ArchiveConsumer.class);
    private static final int TARGET_BATCH_SIZE = 1000;
    
    private final ObjectMapper mapper = new ObjectMapper();
    
    // This persistent memory buffer holds messages across multiple Kafka polls
    private final List<String> persistentBuffer = new ArrayList<>();
    private final Object lock = new Object();

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
                    persistentBuffer.add(record.value());
                    
                    WeatherMessage weatherMessage = mapper.readValue(record.value(), WeatherMessage.class);
                    archiveBuffer.add(weatherMessage);
                    
                } catch (Exception e) {
                    logger.error("Failed to parse message: {}", e.getMessage());
                }

                // Check if we hit exactly 1,000 messages in memory
                if (persistentBuffer.size() >= TARGET_BATCH_SIZE) {
                    try {
                        logger.info("Buffer reached target size ({}/{}). Writing to Archive...", 
                                persistentBuffer.size(), TARGET_BATCH_SIZE);
                        
                        // 1. Write the accumulated 1,000 messages to disk
                        writeToParquet(persistentBuffer);
                        
                        // 2. Commit the offset to Kafka ONLY after a successful file write
                        ack.acknowledge();
                        logger.info("Successfully committed Kafka offsets for the 1000 message batch.");
                        
                        // 3. Reset the memory buffer for the next 1,000 messages
                        persistentBuffer.clear();
                        
                    } catch (Exception e) {
                        logger.error("Critical: Storage write failed. Keeping messages in memory. Error: {}", e.getMessage());
                        // Do NOT clear the buffer and do NOT acknowledge. 
                        // It will try writing again on the next incoming message.
                        return; 
                    }
                }
            }
            
            // If the buffer hasn't reached 1,000 yet, log the progress and exit the method.
            // We intentionally do NOT call ack.acknowledge() here.
            if (!persistentBuffer.isEmpty()) {
                logger.info("Current archive buffer state: {}/{} messages accumulated.", 
                        persistentBuffer.size(), TARGET_BATCH_SIZE);
            }
        }
    }

    private void writeToParquet(List<String> messages) throws Exception {
        String projectRoot = System.getProperty("user.dir");
        Path archiveDirectory = Paths.get(projectRoot, "Archive");

        if (!Files.exists(archiveDirectory)) {
            Files.createDirectories(archiveDirectory);
        }

        String fileName = "weather_batch_" + System.currentTimeMillis() + ".txt";
        Path targetFile = archiveDirectory.resolve(fileName);

        try (PrintWriter out = new PrintWriter(new FileWriter(targetFile.toFile()))) {
            for (String msg : messages) {
                out.println(msg);
            }
        }
        logger.info("Batch file created completely at: {}", targetFile.toAbsolutePath());
    }
}