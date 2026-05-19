# Weather Stations Monitoring — Implementation Plan

> **How to use this doc:** Follow phases in order. Each phase has a goal, the files you'll touch,
> and what to implement. Don't move to the next phase until the current one works.
> At the end of each phase there's a checkpoint — verify it before continuing.

---

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Prerequisites](#2-prerequisites)
3. [Phase 0 — Project Skeleton](#phase-0--project-skeleton)
4. [Phase 1 — Weather Station (Producer)](#phase-1--weather-station-producer)
5. [Phase 2 — Central Station Core (Kafka Consumer)](#phase-2--central-station-core-kafka-consumer)
6. [Phase 3 — Bitcask Storage Engine](#phase-3--bitcask-storage-engine)
7. [Phase 4 — Parquet Archiving](#phase-4--parquet-archiving)
8. [Phase 5 — Rain Alerts (Kafka Streams)](#phase-5--rain-alerts-kafka-streams)
9. [Phase 6 — REST API + Bitcask Client Script](#phase-6--rest-api--bitcask-client-script)
10. [Phase 7 — ElasticSearch + Kibana](#phase-7--elasticsearch--kibana)
11. [Phase 8 — Docker](#phase-8--docker)
12. [Phase 9 — Kubernetes](#phase-9--kubernetes)
13. [Phase 10 — JFR Profiling](#phase-10--jfr-profiling)
14. [Bonus](#bonus)
15. [Deliverables Checklist](#deliverables-checklist)
16. [Dependency Reference](#dependency-reference)
17. [Config Reference](#config-reference)

---

## 1. Project Overview

```
[10 Weather Stations]
      |
      v
 Kafka: weather-readings
      |
      +---> WeatherConsumer
      |         +---> BitcaskStore     (latest reading per station, plain code INSIDE central station)
      |         +---> ParquetWriter    (full history, batched 10K, partitioned by time+station)
      |
      +---> RainingProcessor (Kafka Streams)
                +---> Kafka: rain-alerts  (humidity > 70%)

[Parquet Files] ---> ElasticSearch ---> Kibana

[bitcask_client.sh] ---> HTTP ---> BitcaskController ---> BitcaskStore
```

**Key facts:**
- Each station sends 1 message/second, **10% random drop**
- Battery distribution: **30% low / 40% medium / 30% high** (weighted random, NOT equal thirds)
- Bitcask is **plain Java code inside central-station** — not a separate process, not a separate Maven module, just classes under `com.weather.central.bitcask`
- Parquet files are **partitioned by time AND station ID**
- ElasticSearch indexes the Parquet files so Kibana can visualize them

---

## 2. Prerequisites

| Tool | Version | Check |
|------|---------|-------|
| Java | 17+ | `java -version` |
| Maven | 3.8+ | `mvn -version` |
| Docker | any | `docker -v` |
| kubectl | any | `kubectl version` |

**Local Kafka for development — use the Apache Kafka image (NOT Bitnami):**
```yaml
# docker-compose-dev.yml  (local dev only, not part of final k8s)
version: '3'
services:
  kafka:
    image: apache/kafka:3.7.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```
> `apache/kafka` uses KRaft mode — no Zookeeper needed for local dev.

Run: `docker compose -f docker-compose-dev.yml up -d`

---

## Phase 0 — Project Skeleton

**Goal:** Maven multi-module builds successfully.

### Files to create:

#### `/pom.xml` (Parent POM)
```xml
<groupId>com.weather</groupId>
<artifactId>weather-stations-monitoring</artifactId>
<version>1.0-SNAPSHOT</version>
<packaging>pom</packaging>

<modules>
  <module>weather-station</module>
  <module>central-station</module>
</modules>
```
- Set `<java.version>17</java.version>` in properties
- Use Spring Boot BOM for dependency management
- Pin shared dependency versions here (Kafka, Jackson, etc.)

#### `/weather-station/pom.xml`
- Parent: root POM
- Dependencies: `kafka-clients`, `jackson-databind`
- Plugin: `maven-shade-plugin` to build a fat JAR (needed for Docker — includes all dependencies)

#### `/central-station/pom.xml`
- Parent: root POM
- Dependencies: `spring-boot-starter-web`, `spring-kafka`, `kafka-streams`, `jackson-databind`, `parquet-avro`, `hadoop-client`
- Plugin: `spring-boot-maven-plugin`

### Checkpoint 0
```bash
mvn clean install -DskipTests
# Must print BUILD SUCCESS with no compilation errors
```

---

## Phase 1 — Weather Station (Producer)

**Goal:** Plain Java app sends a JSON weather message to Kafka every second, with correct battery distribution and 10% drop.

### Files:

#### `config.properties`
```properties
station_id=1
kafka.broker=localhost:9092
kafka.topic=weather-readings
```

#### `WeatherStation.java`
- `main()` entry point
- Load `station_id` and broker from `config.properties` via `Properties.load(getResourceAsStream(...))`
- Create `ScheduledExecutorService`, call `scheduleAtFixedRate` at 1-second intervals
- Each tick:
  1. Call `MessageGenerator.generate()`
  2. If result is `null` — print "Message dropped" and skip
  3. Otherwise — call `KafkaProducerService.send(topic, json)`
- Add a shutdown hook: `Runtime.getRuntime().addShutdownHook(...)` to close the producer cleanly

#### `MessageGenerator.java`
- Constructor takes `long stationId`
- Holds a `long counter = 0` incremented each call (this is `s_no`)
- `generate()` returns a JSON String OR null (10% drop)

**Battery status — weighted random (TAs will verify this distribution):**
```java
double r = Math.random();
String battery = r < 0.30 ? "low" : r < 0.70 ? "medium" : "high";
// Result: 30% low, 40% medium, 30% high
```

**10% drop:**
```java
if (Math.random() < 0.10) return null;
```

**Weather value ranges:**
- `humidity`: 0-100 (integer %)
- `temperature`: 50-120 (integer, Fahrenheit)
- `wind_speed`: 0-60 (integer km/h)

**JSON shape (use Jackson ObjectMapper, not string concatenation):**
```json
{
  "station_id": 1,
  "s_no": 42,
  "battery_status": "medium",
  "status_timestamp": 1716000000,
  "weather": {
    "humidity": 65,
    "temperature": 88,
    "wind_speed": 14
  }
}
```

#### `KafkaProducerService.java`
- Wraps `KafkaProducer<String, String>`
- Constructor takes broker URL, configures `StringSerializer` for key and value
- `send(String topic, String json)` calls `producer.send(new ProducerRecord<>(topic, String.valueOf(stationId), json))`
  - Use `station_id` as the Kafka **key** so all messages from one station go to the same partition
- `close()` method

### Checkpoint 1
```bash
cd weather-station && mvn clean package -DskipTests
java -jar target/weather-station-shaded.jar

# In another terminal, verify Kafka receives messages:
docker exec -it <kafka_container> /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic weather-readings --from-beginning

# Expect: JSON messages every ~1 second, ~10% of seconds produce nothing
```

---

## Phase 2 — Central Station Core (Kafka Consumer)

**Goal:** Spring Boot app starts and consumes messages from Kafka. This is the foundation everything else plugs into.

### Files:

#### `application.yml`
```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BROKER:localhost:9092}
    consumer:
      group-id: central-station-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer

server:
  port: 8080

bitcask:
  data-dir: /data/bitcask

parquet:
  output-dir: /data/parquet
  batch-size: 10000
```

#### `CentralStationApplication.java`
- Standard `@SpringBootApplication`
- Add `@EnableScheduling` — needed for Compactor in Phase 3

#### `WeatherMessage.java`
```java
public class WeatherMessage {
    @JsonProperty("station_id")       private long stationId;
    @JsonProperty("s_no")             private long sNo;
    @JsonProperty("battery_status")   private String batteryStatus;
    @JsonProperty("status_timestamp") private long statusTimestamp;
    @JsonProperty("weather")          private WeatherData weather;
    // getters + setters
}
```

#### `WeatherData.java`
```java
public class WeatherData {
    private int humidity;
    private int temperature;
    @JsonProperty("wind_speed") private int windSpeed;
    // getters + setters
}
```

#### `WeatherConsumer.java`
```java
@Component
public class WeatherConsumer {

    @KafkaListener(topics = "weather-readings", groupId = "central-station-group")
    public void consume(String rawMessage) {
        WeatherMessage msg = objectMapper.readValue(rawMessage, WeatherMessage.class);
        log.info("Received from station {}: s_no={}", msg.getStationId(), msg.getSNo());
        // TODO Phase 3: bitcaskStore.put(String.valueOf(msg.getStationId()), rawMessage);
        // TODO Phase 4: parquetWriter.write(msg);
    }
}
```

### Checkpoint 2
```bash
mvn spring-boot:run -pl central-station
# Start the producer in another terminal
# Central station logs should show received messages from each station
```

---

## Phase 3 — Bitcask Storage Engine

**Goal:** Implement Bitcask as plain Java classes directly inside `central-station`. This is the hardest phase — read the design carefully before writing any code.

### What Bitcask is (read this before coding):

| Operation | What happens |
|-----------|-------------|
| **write** | Append record to active segment file. Never seek back, never overwrite. |
| **read** | Look up key in KeyDir (in-memory HashMap) → get file + offset → seek + read. |
| **compaction** | Merge old segments, keep only latest value per key, write a hint file. |
| **recovery** | On startup: rebuild KeyDir from hint files (fast path) or replay segments (fallback). |

### On-disk record format (binary):
```
| timestamp (8 bytes) | key_size (4 bytes) | value_size (4 bytes) | key bytes | value bytes |
```
Write with `DataOutputStream`, read back with `DataInputStream` or `RandomAccessFile`.

### Files (all under `com.weather.central.bitcask`):

#### `KeyDir.java`
The in-memory index. Must be thread-safe.
```java
// Entry per key:
record KeyDirEntry(String fileName, long valueOffset, int valueSize, long timestamp) {}

// The index:
private final ConcurrentHashMap<String, KeyDirEntry> index = new ConcurrentHashMap<>();

public void put(String key, KeyDirEntry entry)           { index.put(key, entry); }
public KeyDirEntry get(String key)                       { return index.get(key); }
public Set<Map.Entry<String, KeyDirEntry>> entries()     { return index.entrySet(); }
```

#### `Segment.java`
One append-only file on disk.
```java
public class Segment {
    public static final long MAX_SIZE = 64 * 1024 * 1024L; // 64 MB

    private final RandomAccessFile raf;
    private final String fileName;
    private long currentOffset = 0;

    // Constructor: open file in "rw" mode, seek to end if file exists
    public Segment(File file) { ... }

    // Append one record, return the byte offset where VALUE bytes start
    public long write(String key, String value, long timestamp) { ... }

    // Seek to valueOffset, read valueSize bytes, return as String
    public String read(long valueOffset, int valueSize) { ... }

    public long getCurrentOffset() { return currentOffset; }
    public String getFileName()    { return fileName; }
    public void close()            { raf.close(); }
}
```

#### `HintFile.java`
Written during compaction. Read during recovery to skip full segment replay.

Binary format per entry:
```
| timestamp (8) | key_size (4) | value_size (4) | value_offset (8) | key bytes |
```

```java
// Write all KeyDir entries to a .hint file
public static void write(String hintFilePath, Map<String, KeyDirEntry> entries) { ... }

// Read a .hint file back into a map
public static Map<String, KeyDirEntry> read(File hintFile) { ... }
```

#### `Compactor.java`
```java
@Component
public class Compactor {

    @Scheduled(fixedDelay = 60_000) // every 60 seconds
    public void compact() {
        // 1. Tell BitcaskStore to give you the list of closed (non-active) segment files
        // 2. Read ALL records from them, oldest segment first
        //    (iterate each record: read timestamp, key_size, value_size, key, value)
        // 3. Build Map<key, latestRecord> -- later records overwrite earlier ones
        // 4. Acquire write lock on BitcaskStore
        // 5. Write all latest records to a single new compacted segment
        // 6. Write a HintFile alongside the compacted segment
        // 7. Update KeyDir entries with new file + offset from the compacted segment
        // 8. Delete the old segment files
        // 9. Release write lock
    }
}
```
**Critical:** Compaction must not block active reads. Use `ReentrantReadWriteLock` in `BitcaskStore`:
- `get()` acquires **read** lock
- Compactor acquires **write** lock only for the brief swap step (steps 4-9)

#### `BitcaskStore.java`
The main class other components call directly.
```java
@Component
public class BitcaskStore {

    private final KeyDir keyDir = new KeyDir();
    private Segment activeSegment;
    private final List<Segment> closedSegments = new ArrayList<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @PostConstruct
    public void init() {
        // 1. Scan data-dir for .hint files
        //    If found: call HintFile.read() -> populate keyDir (fastest recovery)
        // 2. If no hint files: scan .seg files sorted oldest->newest
        //    For each record in each file: update keyDir with latest entry per key
        // 3. Open a new active segment (or reopen the last one if it's not full)
    }

    public void put(String key, String value) {
        lock.writeLock().lock();
        try {
            long ts = System.currentTimeMillis();
            long valueOffset = activeSegment.write(key, value, ts);
            int valueSize = value.getBytes(StandardCharsets.UTF_8).length;
            keyDir.put(key, new KeyDirEntry(activeSegment.getFileName(), valueOffset, valueSize, ts));
            if (activeSegment.getCurrentOffset() > Segment.MAX_SIZE) {
                rollToNewSegment();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public String get(String key) {
        lock.readLock().lock();
        try {
            KeyDirEntry entry = keyDir.get(key);
            if (entry == null) return null;
            Segment seg = findSegmentByName(entry.fileName()); // search active + closed list
            return seg.read(entry.valueOffset(), entry.valueSize());
        } finally {
            lock.readLock().unlock();
        }
    }

    public Map<String, String> getAll() {
        // Iterate keyDir.entries(), call get() for each key
    }
}
```

### Wire into WeatherConsumer:
```java
bitcaskStore.put(String.valueOf(msg.getStationId()), rawMessage);
```

### Checkpoint 3
```bash
# Run system for 30 seconds, then:
curl http://localhost:8080/api/latest/1
# Returns latest JSON for station 1

# STOP the central station process (Ctrl+C), then restart it
curl http://localhost:8080/api/latest/1
# MUST still return data -- TAs will specifically test this
```
**Recovery is tested during the TA discussion. Do not skip hint file implementation.**

---

## Phase 4 — Parquet Archiving

**Goal:** Archive every message to Parquet files, flushed every 10,000 records, partitioned by time and station ID.

### Required directory structure:
```
/data/parquet/
  station_id=1/
    year=2025/month=05/day=19/
      weather_1716123456.parquet
  station_id=2/
    year=2025/month=05/day=19/
      weather_1716123789.parquet
```

### Files:

#### `ParquetWriter.java`
- `@Component`
- Define a **flat** Parquet/Avro schema (flatten the nested `weather` object):
  ```
  station_id (long), s_no (long), battery_status (string),
  status_timestamp (long), humidity (int), temperature (int), wind_speed (int)
  ```
- Internal state: `Map<Long, List<WeatherMessage>> bufferByStation`
- `write(WeatherMessage msg)`:
  - Add to that station's buffer
  - If `buffer.size() >= batchSize` → call `flush(stationId)`
- `flush(long stationId)`:
  - Build partition path: `/data/parquet/station_id={id}/year={y}/month={m}/day={d}/`
  - Create directories if missing (`Files.createDirectories`)
  - Write all buffered records to `weather_{timestamp}.parquet` in that path using the Parquet library
  - Clear that station's buffer
- `@PreDestroy flushAll()`: flush all non-empty buffers on shutdown so you don't lose the last batch
- Inject `batch-size` and `output-dir` via `@Value("${parquet.batch-size}")`

### Wire into WeatherConsumer:
```java
parquetWriter.write(msg);
```

### Checkpoint 4
```bash
# Temporarily set parquet.batch-size=10 in application.yml for fast testing
# After 10 messages per station, check:
ls /data/parquet/station_id=1/
# Parquet files should appear

# Verify content (requires pandas + pyarrow):
python3 -c "
import pandas as pd
df = pd.read_parquet('/data/parquet/', engine='pyarrow')
print(df.head())
print(df.dtypes)
"
```

---

## Phase 5 — Rain Alerts (Kafka Streams)

**Goal:** Stream processor reads `weather-readings`, filters `humidity > 70%`, publishes to `rain-alerts`.

### Files:

#### `RainingProcessor.java`
```java
@Component
public class RainingProcessor {

    private KafkaStreams streams;

    @PostConstruct
    public void start() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,    "rain-alert-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream("weather-readings")
               .filter((key, value) -> {
                   try {
                       WeatherMessage msg = objectMapper.readValue(value, WeatherMessage.class);
                       return msg.getWeather().getHumidity() > 70;
                   } catch (Exception e) {
                       return false; // malformed message, skip
                   }
               })
               .to("rain-alerts");

        streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    @PreDestroy
    public void stop() {
        if (streams != null) streams.close();
    }
}
```
Create the `rain-alerts` topic manually or set `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` in your Kafka config.

### Checkpoint 5
```bash
# Consume rain-alerts in one terminal:
docker exec -it <kafka> /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic rain-alerts

# Temporarily hardcode humidity=85 in MessageGenerator
# Messages should appear on rain-alerts topic within seconds
```

---

## Phase 6 — REST API + Bitcask Client Script

**Goal:** Expose BitcaskStore via HTTP and implement the exact CLI from the project spec.

### Files:

#### `BitcaskController.java`
```java
@RestController
@RequestMapping("/api")
public class BitcaskController {

    // GET /api/latest/{stationId}
    // Used by: --view --key=STATION_ID
    @GetMapping("/latest/{stationId}")
    public ResponseEntity<String> getLatest(@PathVariable String stationId) {
        String value = bitcaskStore.get(stationId);
        return value != null
            ? ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(value)
            : ResponseEntity.notFound().build();
    }

    // GET /api/all
    // Used by: --view-all and --perf
    @GetMapping("/all")
    public ResponseEntity<Map<String, String>> getAll() {
        return ResponseEntity.ok(bitcaskStore.getAll());
    }
}
```

#### `bitcask_client.sh`
Exact CLI from the project spec:

```bash
#!/bin/bash
BASE_URL="http://localhost:8080/api"

case "$1" in

  --view-all)
    # Write all keys+values to a CSV file named with current Unix timestamp
    TIMESTAMP=$(date +%s)
    FILENAME="${TIMESTAMP}.csv"
    echo "key,value" > "$FILENAME"
    curl -s "$BASE_URL/all" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for k, v in data.items():
    # Escape commas in value to keep CSV valid
    v_clean = v.replace('\n','').replace(',',';')
    print(f'{k},{v_clean}')
" >> "$FILENAME"
    echo "Written to $FILENAME"
    ;;

  --view)
    # Parse --key=SOME_KEY from args
    KEY=""
    for arg in "$@"; do
      case $arg in --key=*) KEY="${arg#*=}";; esac
    done
    if [ -z "$KEY" ]; then
      echo "Usage: $0 --view --key=STATION_ID"
      exit 1
    fi
    curl -s "$BASE_URL/latest/$KEY"
    echo
    ;;

  --perf)
    # Parse --clients=N from args
    CLIENTS=1
    for arg in "$@"; do
      case $arg in --clients=*) CLIENTS="${arg#*=}";; esac
    done
    TIMESTAMP=$(date +%s)
    echo "Starting $CLIENTS concurrent clients..."
    for i in $(seq 1 "$CLIENTS"); do
      (
        FILENAME="${TIMESTAMP}_thread_${i}.csv"
        echo "key,value" > "$FILENAME"
        curl -s "$BASE_URL/all" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for k, v in data.items():
    v_clean = v.replace('\n','').replace(',',';')
    print(f'{k},{v_clean}')
" >> "$FILENAME"
      ) &
    done
    wait
    echo "Done. $CLIENTS files written with prefix ${TIMESTAMP}_thread_"
    ;;

  *)
    echo "Usage:"
    echo "  $0 --view-all"
    echo "  $0 --view --key=STATION_ID"
    echo "  $0 --perf --clients=N"
    exit 1
    ;;
esac
```

```bash
chmod +x bitcask-client/bitcask_client.sh
```

### Checkpoint 6
```bash
./bitcask-client/bitcask_client.sh --view-all
# Creates: 1716034451.csv   (2 columns: key,value)

./bitcask-client/bitcask_client.sh --view --key=1
# Prints JSON for station 1 to stdout

./bitcask-client/bitcask_client.sh --perf --clients=100
# Creates: 1716034451_thread_1.csv ... 1716034451_thread_100.csv
```

---

## Phase 7 — ElasticSearch + Kibana

**Goal:** Index Parquet files in ElasticSearch; build Kibana dashboards showing battery % and dropped messages.

### Add to `docker-compose-dev.yml`:
```yaml
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.13.0
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
    ports:
      - "9200:9200"

  kibana:
    image: docker.elastic.co/kibana/kibana:8.13.0
    ports:
      - "5601:5601"
    environment:
      - ELASTICSEARCH_HOSTS=http://elasticsearch:9200
    depends_on: [elasticsearch]
```

### Indexing Parquet into ElasticSearch:

Recommended: Python script (simplest approach)
```python
# index_to_es.py
import pandas as pd
from elasticsearch import Elasticsearch, helpers
import glob, os

es = Elasticsearch("http://localhost:9200")

# Create index with correct mappings
es.indices.create(index="weather-readings", ignore=400, body={
    "mappings": {
        "properties": {
            "station_id":       {"type": "long"},
            "s_no":             {"type": "long"},
            "battery_status":   {"type": "keyword"},
            "status_timestamp": {"type": "long"},
            "humidity":         {"type": "integer"},
            "temperature":      {"type": "integer"},
            "wind_speed":       {"type": "integer"}
        }
    }
})

for parquet_file in glob.glob("/data/parquet/**/*.parquet", recursive=True):
    df = pd.read_parquet(parquet_file)
    actions = [
        {"_index": "weather-readings", "_source": row.to_dict()}
        for _, row in df.iterrows()
    ]
    helpers.bulk(es, actions)
    print(f"Indexed {len(actions)} records from {parquet_file}")
```

### Required Kibana visualizations:

**1. Battery status distribution** (must show ~30% low / ~40% medium / ~30% high)
- Lens or Aggregation: `Terms` on `battery_status` field → Pie or Bar chart

**2. Dropped messages per station**
- Since dropped messages never reach Kafka, detect gaps in `s_no` per station:
  - In `WeatherConsumer`, track the last `s_no` seen per station in a `Map<Long, Long>`
  - If `msg.getSNo() > lastSNo + 1` → `(msg.getSNo() - lastSNo - 1)` messages were dropped
  - Write a "drop event" record to a separate ES index `weather-drops`
  - Visualize: `Terms` on `station_id` with `Sum` of `dropped_count`

### Checkpoint 7
- Open `http://localhost:5601`
- Create index patterns: `weather-readings` and `weather-drops`
- Build both visualizations
- **Take screenshots** — these are required deliverables

---

## Phase 8 — Docker

**Goal:** Package both services as Docker images. Use `apache/kafka` (not Bitnami).

### `Dockerfile.weather-station`
```dockerfile
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY weather-station/target/weather-station-1.0-SNAPSHOT-shaded.jar app.jar
COPY weather-station/src/main/resources/config.properties config.properties
ENV STATION_ID=1
ENV KAFKA_BROKER=kafka:9092
ENTRYPOINT ["java", "-jar", "app.jar"]
```

### `Dockerfile.central-station`
```dockerfile
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY central-station/target/central-station-1.0-SNAPSHOT.jar app.jar
ENV KAFKA_BROKER=kafka:9092
ENTRYPOINT ["java", "-jar", "app.jar"]
```

### Make config accept env vars (do this early, it affects all phases):
In `WeatherStation.java`:
```java
String broker = System.getenv().getOrDefault("KAFKA_BROKER",
                    props.getProperty("kafka.broker"));
String stationIdStr = System.getenv().getOrDefault("STATION_ID",
                    props.getProperty("station_id"));
```

In `application.yml` (already done if you used `${KAFKA_BROKER:localhost:9092}` syntax).

### Build commands:
```bash
mvn clean install -DskipTests
docker build -f docker/Dockerfile.weather-station -t weather-station:latest .
docker build -f docker/Dockerfile.central-station -t central-station:latest .
```

### Checkpoint 8
```bash
docker run --rm --network host \
  -e STATION_ID=1 -e KAFKA_BROKER=localhost:9092 \
  weather-station:latest
# Messages should appear in Kafka

docker run --rm --network host \
  -p 8080:8080 -e KAFKA_BROKER=localhost:9092 \
  -v /tmp/testdata:/data \
  central-station:latest
# Spring Boot starts, consumes messages, API responds
```

---

## Phase 9 — Kubernetes

**Goal:** Full k8s deployment with 10 station pods, 1 central station, Kafka, shared persistent storage.

### `k8s/persistent-volume.yaml`
```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: weather-data-pv
spec:
  capacity:
    storage: 10Gi
  accessModes: [ReadWriteMany]
  hostPath:
    path: /data/weather   # use proper StorageClass for cloud k8s
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: weather-data-pvc
spec:
  accessModes: [ReadWriteMany]
  resources:
    requests:
      storage: 10Gi
```

### `k8s/kafka.yaml`
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka
spec:
  replicas: 1
  selector:
    matchLabels: {app: kafka}
  template:
    metadata:
      labels: {app: kafka}
    spec:
      containers:
        - name: kafka
          image: apache/kafka:3.7.0       # apache/kafka, NOT bitnami
          ports:
            - containerPort: 9092
          env:
            - {name: KAFKA_NODE_ID, value: "1"}
            - {name: KAFKA_PROCESS_ROLES, value: "broker,controller"}
            - {name: KAFKA_LISTENERS, value: "PLAINTEXT://:9092,CONTROLLER://:9093"}
            - {name: KAFKA_ADVERTISED_LISTENERS, value: "PLAINTEXT://kafka:9092"}
            - {name: KAFKA_CONTROLLER_QUORUM_VOTERS, value: "1@localhost:9093"}
            - {name: KAFKA_CONTROLLER_LISTENER_NAMES, value: "CONTROLLER"}
            - {name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, value: "1"}
---
apiVersion: v1
kind: Service
metadata:
  name: kafka
spec:
  selector: {app: kafka}
  ports:
    - port: 9092
      targetPort: 9092
```

### `k8s/central-station.yaml`
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: central-station
spec:
  replicas: 1
  selector:
    matchLabels: {app: central-station}
  template:
    metadata:
      labels: {app: central-station}
    spec:
      containers:
        - name: central-station
          image: central-station:latest
          ports:
            - containerPort: 8080
          env:
            - {name: KAFKA_BROKER, value: "kafka:9092"}
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: weather-data-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: central-station
spec:
  selector: {app: central-station}
  ports:
    - port: 8080
      targetPort: 8080
```

### `k8s/weather-station.yaml`
Use `StatefulSet` (not Deployment) to get stable, unique pod ordinals:
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: weather-station
spec:
  replicas: 10                  # 10 stations
  serviceName: weather-station
  selector:
    matchLabels: {app: weather-station}
  template:
    metadata:
      labels: {app: weather-station}
    spec:
      containers:
        - name: weather-station
          image: weather-station:latest
          env:
            - {name: KAFKA_BROKER, value: "kafka:9092"}
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            # WeatherStation.java parses station ID from pod ordinal:
            # weather-station-0 -> station ID 1
            # weather-station-1 -> station ID 2  ... etc.
```

**Parse station ID in WeatherStation.java:**
```java
String podName = System.getenv("POD_NAME");
long stationId;
if (podName != null && podName.matches(".*-\\d+$")) {
    long ordinal = Long.parseLong(podName.replaceAll(".*-(\\d+)$", "$1"));
    stationId = ordinal + 1;  // 0-indexed ordinal -> 1-indexed station ID
} else {
    stationId = Long.parseLong(
        System.getenv().getOrDefault("STATION_ID",
            props.getProperty("station_id", "1"))
    );
}
```

### Deploy order:
```bash
kubectl apply -f k8s/persistent-volume.yaml
kubectl apply -f k8s/kafka.yaml
kubectl wait --for=condition=available deployment/kafka --timeout=90s
kubectl apply -f k8s/central-station.yaml
kubectl apply -f k8s/weather-station.yaml
```

### Checkpoint 9
```bash
kubectl get pods
# kafka-xxx               1/1  Running
# central-station-xxx     1/1  Running
# weather-station-0       1/1  Running
# ...
# weather-station-9       1/1  Running

kubectl port-forward service/central-station 8080:8080 &
./bitcask-client/bitcask_client.sh --view-all
# CSV should have 10 rows, one per station (station IDs 1-10)
```

---

## Phase 10 — JFR Profiling

**Goal:** Run the central station for 1 minute under load, collect JFR data, report required metrics.

### Enable JFR (add to Dockerfile or run command):
```bash
java -XX:StartFlightRecording=duration=60s,filename=/data/central-station.jfr \
     -jar central-station.jar
```

For Docker/k8s, add to ENTRYPOINT:
```dockerfile
ENTRYPOINT ["java",
  "-XX:StartFlightRecording=duration=60s,filename=/data/profiling/central-station.jfr",
  "-jar", "app.jar"]
```

### Analyze the `.jfr` file:

**Option A: JDK Mission Control (GUI)** — Download from https://adoptium.net/jmc  
Open the .jfr file, navigate the tabs.

**Option B: Command-line `jfr` tool (bundled in JDK 17)**
```bash
# Summary
jfr summary central-station.jfr

# GC pauses
jfr print --events jdk.GCPhasePause central-station.jfr

# Memory / object allocation
jfr print --events jdk.ObjectAllocationInNewTLAB central-station.jfr

# File I/O
jfr print --events jdk.FileRead,jdk.FileWrite central-station.jfr
```

### What to report (required):

| Metric | Where to find |
|--------|--------------|
| Top 10 classes by total memory | JMC: Memory tab → Object Statistics; or `jfr print --events jdk.OldObjectSample` |
| GC pause count | JMC: GC tab → count of GC events; or count lines from `jdk.GCPhasePause` |
| GC max pause duration | JMC: GC tab → max Duration; or `--events jdk.GCPhasePause` max duration field |
| List of I/O operations | JMC: I/O tab; or `--events jdk.FileRead,jdk.FileWrite,jdk.SocketRead,jdk.SocketWrite` |

---

## Bonus

### Bonus 1 — Open-Meteo Integration (Channel Adapter pattern)
Implement a Channel Adapter that polls the free Open-Meteo API and feeds real weather data into Kafka:

```java
@Component
public class OpenMeteoAdapter {

    // Example URL for Alexandria, Egypt:
    // https://api.open-meteo.com/v1/forecast
    //   ?latitude=31.2&longitude=29.9
    //   &current=temperature_2m,relative_humidity_2m,wind_speed_10m

    @Scheduled(fixedDelay = 1000)
    public void poll() {
        // 1. HTTP GET the Open-Meteo API
        // 2. Map the response to WeatherMessage JSON
        //    (temperature in Celsius -> convert to Fahrenheit)
        // 3. Publish to the "weather-readings" Kafka topic
        //    (use a dedicated station_id, e.g. 99 = "open-meteo")
    }
}
```

### Bonus 2 — Enterprise Integration Patterns (use 5-6)

| Pattern | How to apply in this project |
|---------|------------------------------|
| **Dead Letter Channel** | Catch deserialization exceptions in WeatherConsumer → publish bad messages to `dead-letter` topic |
| **Invalid Message Channel** | Messages failing schema validation → route to `invalid-messages` topic |
| **Idempotent Receiver** | Track `s_no` per station; if you see the same `s_no` twice, skip it (prevents duplicate processing) |
| **Envelope Wrapper** | Wrap incoming messages in an envelope that adds `received_at`, `source`, `processing_id` metadata |
| **Polling Consumer** | The Open-Meteo adapter IS a polling consumer — document it as such |
| **Claim Check** | For messages with large payloads: store payload in Bitcask/file, pass only a reference key in the Kafka message |

---

## Deliverables Checklist

- [ ] Source code (all modules committed)
- [ ] `docker/Dockerfile.weather-station`
- [ ] `docker/Dockerfile.central-station`
- [ ] `k8s/weather-station.yaml`
- [ ] `k8s/central-station.yaml`
- [ ] `k8s/kafka.yaml`
- [ ] `k8s/persistent-volume.yaml`
- [ ] Kibana screenshot: battery distribution (~30% low / ~40% medium / ~30% high)
- [ ] Kibana screenshot: dropped messages per station (~10%)
- [ ] Sample `.parquet` file
- [ ] Sample Bitcask LSM directory (contains `.seg` and `.hint` files)
- [ ] Report containing: architecture overview, JFR results (top 10 memory classes, GC count, GC max pause, I/O list), screenshots, explanations

---

## Dependency Reference

### weather-station/pom.xml
```xml
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-clients</artifactId>
  <version>3.7.0</version>
</dependency>
<dependency>
  <groupId>com.fasterxml.jackson.core</groupId>
  <artifactId>jackson-databind</artifactId>
  <version>2.17.0</version>
</dependency>
```

### central-station/pom.xml
```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-web</artifactId>
</dependency>
<dependency>
  <groupId>org.springframework.kafka</groupId>
  <artifactId>spring-kafka</artifactId>
</dependency>
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-streams</artifactId>
</dependency>
<dependency>
  <groupId>org.apache.parquet</groupId>
  <artifactId>parquet-avro</artifactId>
  <version>1.13.1</version>
</dependency>
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-client</artifactId>
  <version>3.3.6</version>
</dependency>
<dependency>
  <groupId>com.fasterxml.jackson.core</groupId>
  <artifactId>jackson-databind</artifactId>
</dependency>
```

---

## Config Reference

### weather-station `config.properties`
| Key | Description | Example |
|-----|-------------|---------|
| `station_id` | Unique station ID | `1` |
| `kafka.broker` | Kafka bootstrap server | `localhost:9092` |
| `kafka.topic` | Topic to publish to | `weather-readings` |

### central-station `application.yml`
| Key | Description | Default |
|-----|-------------|---------|
| `spring.kafka.bootstrap-servers` | Kafka broker | `localhost:9092` |
| `bitcask.data-dir` | Segment file storage path | `/data/bitcask` |
| `parquet.output-dir` | Parquet output root | `/data/parquet` |
| `parquet.batch-size` | Records before flush | `10000` |

---

## Implementation Order

```
Phase 0  - Maven skeleton builds clean
Phase 1  - Station sends weighted-random JSON (30/40/30 battery, 10% drop)
Phase 2  - Central receives and logs messages from Kafka
Phase 3  - Bitcask stores latest per station (HARDEST — test recovery thoroughly)
Phase 4  - Parquet archives all messages, partitioned by time+station
Phase 5  - Rain alerts via Kafka Streams (humidity > 70%)
Phase 6  - REST API + exact CLI client (--view-all, --view --key=, --perf --clients=)
Phase 7  - ElasticSearch + Kibana dashboards (battery %, drop count)
Phase 8  - Docker images using apache/kafka
Phase 9  - Kubernetes: StatefulSet for 10 stations + shared PVC
Phase 10 - JFR profiling report
Bonus    - Open-Meteo channel adapter + 5-6 EIP patterns
```
