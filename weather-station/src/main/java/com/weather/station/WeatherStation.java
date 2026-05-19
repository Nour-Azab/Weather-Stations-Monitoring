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

        long stationId = Long.parseLong(props.getProperty("station_id", "1"));
        String topic   = props.getProperty("kafka.topic");
        String broker  = props.getProperty("kafka.broker");

        // Problem 3 fix: imports added above
        // Problem 1 fix: scheduler is now INSIDE main()
        KafkaProducerService producer = new KafkaProducerService(broker, stationId);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {

            String message = MessageGenerator.generate(stationId);

            // Problem 4 fix: check for null (10% drop returns null)
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