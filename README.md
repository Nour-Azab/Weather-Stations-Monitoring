# Weather Stations Monitoring

## File Structure
```
Weather-Stations-Monitoring/
│
├── pom.xml                                          ← Parent POM
│
├── weather-station/
│   ├── pom.xml
│   └── src/
│       └── main/
│           ├── java/
│           │   └── com/weather/station/
│           │       ├── WeatherStation.java          ← Main class (entry point) , send every 1 sec
│           │       ├── MessageGenerator.java        ← Builds JSON message + 10% drop logic
│           │       └── KafkaProducerService.java    ← Sends message to Kafka
│           └── resources/
│               └── config.properties               ← station_id, kafka broker URL
│
├── central-station/
│   ├── pom.xml
│   └── src/
│       └── main/
│           ├── java/
│           │   └── com/weather/central/
│           │       ├── CentralStationApplication.java   ← Spring Boot entry point
│           │       │
│           │       ├── model/
│           │       │   ├── WeatherMessage.java           ← Maps the JSON from station
│           │       │   └── WeatherData.java              ← The nested weather object
│           │       │
│           │       ├── kafka/
│           │       │   ├── WeatherConsumer.java          ← @KafkaListener consumes messages
│           │       │   └── RainingProcessor.java         ← Kafka Streams humidity > 70%
│           │       │
│           │       ├── bitcask/
│           │       │   ├── BitcaskStore.java             ← Core engine (get/put)
│           │       │   ├── KeyDir.java                   ← In-memory hash index
│           │       │   ├── Segment.java                  ← Append-only file segment
│           │       │   ├── HintFile.java                 ← For recovery after crash
│           │       │   └── Compactor.java                ← Scheduled compaction
│           │       │
│           │       ├── parquet/
│           │       │   └── ParquetWriter.java            ← Batched writes to parquet files (10K/batch)
│           │       │
│           │       └── api/
│           │           └── BitcaskController.java        ← REST endpoints for client script
│           │
│           └── resources/
│               └── application.yml                  ← Kafka, server, paths config
│
├── bitcask-client/
│   └── bitcask_client.sh                            ← Bash script (view-all, view, perf)
│
├── docker/
│   ├── Dockerfile.weather-station                   ← Docker image for station
│   └── Dockerfile.central-station                   ← Docker image for central
│
└── k8s/
├── weather-station.yaml                         ← 10 station pods
├── central-station.yaml                         ← 1 central station pod
├── kafka.yaml                                   ← Kafka pods
└── persistent-volume.yaml                       ← Shared storage for parquet + bitcask
```
### What talks to who
```
weather-station
    WeatherStation.java
        → MessageGenerator.java       (creates the message)
        → KafkaProducerService.java   (sends to Kafka topic: weather-readings)

central-station
    WeatherConsumer.java
        → receives from Kafka topic: weather-readings
        → BitcaskStore.java           (stores latest per station)
        → ParquetWriter.java          (archives everything)

    RainingProcessor.java
        → reads from Kafka topic: weather-readings
        → if humidity > 70% → writes to Kafka topic: rain-alerts

    BitcaskController.java
        → BitcaskStore.java           (reads data for API responses)
        → called by bitcask_client.sh

bitcask/
    BitcaskStore.java
        → KeyDir.java                 (in-memory index)
        → Segment.java                (file on disk)
        → HintFile.java               (written at compaction for recovery)
        → Compactor.java              (merges segments, runs on schedule)
```
## Data Flow
```
[10 Weather Stations]
  └─► Kafka: weather-readings
        ├─► WeatherConsumer
        │     ├─► BitcaskStore   (latest reading per station)
        │     └─► ParquetWriter  (full history, batched 10K)
        └─► RainingProcessor
              └─► Kafka: rain-alerts  (humidity > 70%)
 
[bitcask_client.sh] ─► HTTP ─► BitcaskController ─► BitcaskStore
```
 cd central-station && mvn spring-boot:run  :: for consumer
 
 java -jar target/weather-station-shaded.jar:: message generator  kafka producer
 
 docker exec -it weather-stations-monitoring-kafka-1 \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic rain-alerts \
  --from-beginning  :: for testing humidity alerts log

  