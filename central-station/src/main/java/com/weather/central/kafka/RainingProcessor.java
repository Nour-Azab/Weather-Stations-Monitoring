package com.weather.central.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weather.central.model.WeatherMessage;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class RainingProcessor {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private KafkaStreams streams;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void start() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "rain-alert-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                  Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                  Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream("weather-readings")
               .filter((key, value) -> {
                   try {
                       WeatherMessage msg = objectMapper.readValue(value, WeatherMessage.class);
                       return msg.getWeather().getHumidity() > 70;
                   } catch (Exception e) {
                       // Bad message — skip it, never crash the stream
                       System.err.println("RainingProcessor: skipping bad message. " + e.getMessage());
                       return false;
                   }
               })
               .to("rain-alerts");

        streams = new KafkaStreams(builder.build(), props);

        // Log state changes so you can see it's working
        streams.setStateListener((newState, oldState) ->
            System.out.println("RainingProcessor state: " + oldState + " -> " + newState));

        streams.start();
        System.out.println("RainingProcessor started — watching for humidity > 70%");
    }

    @PreDestroy
    public void stop() {
        if (streams != null) {
            streams.close();
            System.out.println("RainingProcessor stopped.");
        }
    }
}