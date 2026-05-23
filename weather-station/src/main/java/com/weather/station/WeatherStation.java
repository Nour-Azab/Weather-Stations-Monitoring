package com.weather.station;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WeatherStation {

    public static Properties props = new Properties();

    public static void main(String[] args) {
        System.out.println("Weather Station starting...");

        // Problem 2 fix: wrap in try-catch
        try {
            props.load(WeatherStation.class.getClassLoader()
                       .getResourceAsStream("config.properties"));
        } catch (IOException e) {
            System.err.println("Failed to load config.properties");
            e.printStackTrace();
            return;
        }

        String stationIdValue = System.getenv("STATION_ID");
        if (stationIdValue == null || stationIdValue.isBlank()) {
            stationIdValue = props.getProperty("station_id");
        }
        if (stationIdValue == null || stationIdValue.isBlank()) {
            stationIdValue = "1";
        }
        final long stationId = Long.parseLong(stationIdValue);

        String _topic = System.getenv("KAFKA_TOPIC");
        if (_topic == null || _topic.isBlank()) {
            _topic = props.getProperty("kafka.topic");
        }
        if (_topic == null || _topic.isBlank()) {
            _topic = "weather-readings";
        }
        final String topic = _topic;

        String broker = System.getenv("KAFKA_BROKER");
        if (broker == null || broker.isBlank()) {
            broker = props.getProperty("kafka.broker");
        }
        if (broker == null || broker.isBlank()) {
            broker = "localhost:9092";
        }

        final KafkaProducerService producer = new KafkaProducerService(broker, stationId);

        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {

            String message = MessageGenerator.generate(stationId);
            
            if (message == null) {
                System.out.println("Message dropped (10% rate)");
                return;
            }

            producer.send(topic, message);
            System.out.println("Sent: " + message);

        }, 0, 1, TimeUnit.SECONDS);

        // Graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduler.shutdown();
            producer.close();
            System.out.println("Weather Station stopped.");
        }));

        try {
            Thread.currentThread().join(); // block forever until Ctrl+C
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}