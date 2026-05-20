package com.weather.station;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.weather.station.model.WeatherData;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class MessageGenerator {

    // ── Toggle this to switch between real and mock data ──────────────────
    private static final boolean USE_REAL_DATA = true;
    // ─────────────────────────────────────────────────────────────────────

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static long counter = 0;

    // ── Cache: stationId → last fetched WeatherData ───────────────────────
    private static final Map<Long, WeatherData> cachedWeather = new ConcurrentHashMap<>();
    private static final Map<Long, Long> lastFetchTime = new ConcurrentHashMap<>();
    private static final long CACHE_DURATION_MS = 15 * 60 * 1000L; // 15 minutes
    // ─────────────────────────────────────────────────────────────────────

    private static final String API_URL_TEMPLATE = "https://api.open-meteo.com/v1/forecast" +
            "?latitude=%s&longitude=%s" +
            "&current=temperature_2m,relative_humidity_2m,wind_speed_10m";

    // ── Jackson fields ────────────────────────────────────────────────────
    @JsonProperty("station_id")
    private final long stationId;
    @JsonProperty("s_no")
    private final long sno;
    @JsonProperty("battery_status")
    private final String batteryStatus;
    @JsonProperty("status_timestamp")
    private final long statusTimestamp;
    @JsonProperty("weather")
    private final WeatherData weather;

    public MessageGenerator(long stationId, long sno, String batteryStatus,
            long statusTimestamp, WeatherData weather) {
        this.stationId = stationId;
        this.sno = sno;
        this.batteryStatus = batteryStatus;
        this.statusTimestamp = statusTimestamp;
        this.weather = weather;
    }

    // ── Public entry point ────────────────────────────────────────────────
    public static String generate(long stationId) {
        if (Math.random() < 0.10)
            return null; // 10% drop

        if (USE_REAL_DATA) {
            return generateFromApi(stationId);
        } else {
            return generateMock(stationId);
        }
    }

    // ── Real data from Open-Meteo API (with cache) ────────────────────────
    private static String generateFromApi(long stationId) {
        try {
            long now = System.currentTimeMillis();
            Long lastFetch = lastFetchTime.get(stationId);
            WeatherData cached = cachedWeather.get(stationId);

            // Use cache if still fresh
            if (cached != null && lastFetch != null &&
                    (now - lastFetch) < CACHE_DURATION_MS) {

                System.out.println("[OpenMeteo CACHE] Station " + stationId +
                        " [" + getCityName(stationId) + "]" +
                        " humidity=" + cached.getHumidity() + "%" +
                        " temp=" + cached.getTemperature() + "F" +
                        " wind=" + cached.getWindSpeed() + "km/h");

            } else {
                // Cache expired or first call — fetch from API
                System.out.println("[OpenMeteo API] Fetching fresh data for station " +
                        stationId + " [" + getCityName(stationId) + "]");

                double[] coords = getCoordinates(stationId);
                String url = String.format(API_URL_TEMPLATE, coords[0], coords[1]);

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(java.time.Duration.ofSeconds(5))
                        .GET()
                        .build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                String body = response.body();
                JsonNode root = objectMapper.readTree(body);

                // Check for API error
                if (root.has("error") && root.get("error").asBoolean()) {
                    System.err.println("[OpenMeteo] API error: " +
                            root.path("reason").asText() +
                            " — using mock");
                    return generateMock(stationId);
                }

                if (root.path("current").isMissingNode()) {
                    System.err.println("[OpenMeteo] 'current' missing — using mock");
                    return generateMock(stationId);
                }

                JsonNode current = root.path("current");
                double tempCelsius = current.path("temperature_2m").asDouble();
                int humidity = current.path("relative_humidity_2m").asInt();
                int windSpeed = (int) current.path("wind_speed_10m").asDouble();
                int tempFahrenheit = (int) ((tempCelsius * 9.0 / 5.0) + 32);

                // Update cache
                cached = new WeatherData(humidity, tempFahrenheit, windSpeed);
                cachedWeather.put(stationId, cached);
                lastFetchTime.put(stationId, now);

                System.out.println("[OpenMeteo API] Station " + stationId +
                        " [" + getCityName(stationId) + "]" +
                        " humidity=" + humidity + "%" +
                        " temp=" + tempFahrenheit + "F" +
                        " wind=" + windSpeed + "km/h" +
                        " (cached for 15 min)");
            }

            // Build message using cached data
            double r = Math.random();
            String battery = r < 0.30 ? "low" : r < 0.70 ? "medium" : "high";
            counter++;
            long timestamp = System.currentTimeMillis() / 1000;

            return objectMapper.writeValueAsString(
                    new MessageGenerator(stationId, counter, battery, timestamp, cached));

        } catch (Exception e) {
            System.err.println("[OpenMeteo] Exception for station " + stationId +
                    " — falling back to mock: " + e.getMessage());
            return generateMock(stationId);
        }
    }

    // ── Mock / fake data ──────────────────────────────────────────────────
    private static String generateMock(long stationId) {
        counter++;
        double r = Math.random();
        String battery = r < 0.30 ? "low" : r < 0.70 ? "medium" : "high";
        int humidity = (int) (Math.random() * 101);
        int temperature = 50 + (int) (Math.random() * 71);
        int windSpeed = (int) (Math.random() * 61);
        long timestamp = System.currentTimeMillis() / 1000;
        WeatherData weather = new WeatherData(humidity, temperature, windSpeed);

        System.out.println("[Mock] Station " + stationId +
                " battery=" + battery +
                " humidity=" + humidity + "%" +
                " temp=" + temperature + "F");
        try {
            return objectMapper.writeValueAsString(
                    new MessageGenerator(stationId, counter, battery, timestamp, weather));
        } catch (Exception e) {
            return "{}";
        }
    }

    // ── City coordinates by station ID ────────────────────────────────────
    private static double[] getCoordinates(long stationId) {
        return switch ((int) stationId) {
            case 1 -> new double[] { 30.0, 31.2 }; // Cairo
            case 2 -> new double[] { 31.2, 29.9 }; // Alexandria
            case 3 -> new double[] { 25.7, 32.6 }; // Luxor
            case 4 -> new double[] { 29.9, 32.5 }; // Suez
            case 5 -> new double[] { 30.0, 31.1 }; // Giza
            case 6 -> new double[] { 24.1, 32.9 }; // Aswan
            case 7 -> new double[] { 31.3, 32.3 }; // Port Said
            case 8 -> new double[] { 31.0, 31.4 }; // Mansoura
            case 9 -> new double[] { 30.8, 31.0 }; // Tanta
            case 10 -> new double[] { 27.3, 33.8 }; // Hurghada
            default -> new double[] { 30.0, 31.2 }; // fallback Cairo
        };
    }

    private static String getCityName(long stationId) {
        return switch ((int) stationId) {
            case 1 -> "Cairo";
            case 2 -> "Alexandria";
            case 3 -> "Luxor";
            case 4 -> "Suez";
            case 5 -> "Giza";
            case 6 -> "Aswan";
            case 7 -> "Port Said";
            case 8 -> "Mansoura";
            case 9 -> "Tanta";
            case 10 -> "Hurghada";
            default -> "Cairo";
        };
    }

    // ── Getters for Jackson ───────────────────────────────────────────────
    public long getStationId() {
        return stationId;
    }

    public long getSno() {
        return sno;
    }

    public String getBatteryStatus() {
        return batteryStatus;
    }

    public long getStatusTimestamp() {
        return statusTimestamp;
    }

    public WeatherData getWeather() {
        return weather;
    }
}