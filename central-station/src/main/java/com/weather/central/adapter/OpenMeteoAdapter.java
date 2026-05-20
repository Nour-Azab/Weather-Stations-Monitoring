package com.weather.central.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class OpenMeteoAdapter {

    private record Station(long id, double lat, double lon, String name) {}

    private static final List<Station> STATIONS = List.of(
        new Station(99,  31.2, 29.9, "Alexandria"),
        new Station(100, 30.0, 31.2, "Cairo"),
        new Station(101, 25.7, 32.6, "Luxor"),
        new Station(102, 31.1, 32.3, "Suez"),
        new Station(103, 29.1, 30.8, "Giza")
    );

    private static final String API_URL_TEMPLATE =
        "https://api.open-meteo.com/v1/forecast" +
        "?latitude=%s&longitude=%s" +
        "&current=temperature_2m,relative_humidity_2m,wind_speed_10m";

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private KafkaProducer<String, String> producer;
    private final HttpClient httpClient      = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper  = new ObjectMapper();
    private ScheduledExecutorService scheduler;
    private final Map<Long, Long> counters   = new HashMap<>();

    @PostConstruct
    public void start() {
        // Set up Kafka producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);

        // Initialize per-station message counters
        STATIONS.forEach(s -> counters.put(s.id(), 0L));

        // Poll all stations every second
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::pollAllStations, 0, 1, TimeUnit.SECONDS);

        System.out.println("OpenMeteoAdapter started — polling " + STATIONS.size() + " real stations.");
    }

    private void pollAllStations() {
        for (Station station : STATIONS) {
            pollAndPublish(station);
        }
    }

    private void pollAndPublish(Station station) {
        try {
            // 1. Build URL with this station's coordinates
            String url = String.format(API_URL_TEMPLATE, station.lat(), station.lon());

            // 2. Call Open-Meteo API
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();
            HttpResponse<String> response =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            // 3. Parse response
            // { "current": { "temperature_2m": 28.5, "relative_humidity_2m": 65, "wind_speed_10m": 14.2 } }
            JsonNode root    = objectMapper.readTree(response.body());
            JsonNode current = root.path("current");

            double tempCelsius  = current.path("temperature_2m").asDouble();
            int humidity        = current.path("relative_humidity_2m").asInt();
            double windSpeedKmh = current.path("wind_speed_10m").asDouble();

            // 4. Convert units to match project spec (Fahrenheit)
            int tempFahrenheit = (int) ((tempCelsius * 9.0 / 5.0) + 32);
            int windSpeed      = (int) windSpeedKmh;

            // 5. Increment this station's counter and build message
            long counter  = counters.merge(station.id(), 1L, Long::sum);
            long timestamp = System.currentTimeMillis() / 1000;

            String message = String.format(
                "{\"station_id\":%d,\"s_no\":%d,\"battery_status\":\"high\"," +
                "\"status_timestamp\":%d," +
                "\"weather\":{\"humidity\":%d,\"temperature\":%d,\"wind_speed\":%d}}",
                station.id(), counter, timestamp,
                humidity, tempFahrenheit, windSpeed
            );

            // 6. Publish to weather-readings — same topic as mock stations
            producer.send(
                new ProducerRecord<>(
                    "weather-readings",
                    String.valueOf(station.id()),
                    message
                ),
                (metadata, exception) -> {
                    if (exception != null)
                        System.err.println("OpenMeteo send failed [" + station.name() + "]: " + exception.getMessage());
                }
            );

            System.out.println("OpenMeteo [" + station.name() + "] " +
                               "humidity=" + humidity + "% " +
                               "temp=" + tempFahrenheit + "F " +
                               "wind=" + windSpeed + "km/h");

        } catch (Exception e) {
            // Never crash the scheduler — log and retry next second
            System.err.println("OpenMeteoAdapter error [" + station.name() + "]: " + e.getMessage());
        }
    }

    @PreDestroy
    public void stop() {
        if (scheduler != null) scheduler.shutdown();
        if (producer != null)  producer.close();
        System.out.println("OpenMeteoAdapter stopped.");
    }
}