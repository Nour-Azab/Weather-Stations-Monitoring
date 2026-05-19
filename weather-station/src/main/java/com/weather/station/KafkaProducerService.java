package com.weather.station;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerService {

    private final KafkaProducer<String, String> producer;
    private final String stationKey;

    public KafkaProducerService(String brokerUrl, long stationId) {
        this.stationKey = String.valueOf(stationId);

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                          StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                          StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }

    public void send(String topic, String message) {
        // stationId is the key so all messages from this station
        // go to the same Kafka partition
        ProducerRecord<String, String> record =
            new ProducerRecord<>(topic, stationKey, message);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Failed to send: " + exception.getMessage());
            }
        });
    }

    public void close() {
        producer.close();
    }
}