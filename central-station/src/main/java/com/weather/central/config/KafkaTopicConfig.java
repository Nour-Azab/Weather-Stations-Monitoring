package com.weather.central.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic weatherReadingsTopic() {
        return TopicBuilder.name("weather-readings")
                           .partitions(10)  // one per station
                           .replicas(1)
                           .build();
    }

    @Bean
    public NewTopic rainAlertsTopic() {
        return TopicBuilder.name("rain-alerts")
                           .partitions(1)
                           .replicas(1)
                           .build();
    }
}