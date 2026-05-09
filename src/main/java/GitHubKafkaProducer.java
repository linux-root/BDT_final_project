import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * GitHubKafkaProducer — Java Kafka Producer
 * ==========================================
 * CS523 BDT Final Project — Real-Time GitHub Open Source Trend Analytics
 *
 * Polls the GitHub public Events API on a configurable interval,
 * enriches/normalizes each event into a flat JSON record, and sends
 * it to a Kafka topic for downstream Spark Structured Streaming.
 *
 * Configuration via environment variables or system properties:
 *   GITHUB_TOKEN      GitHub Personal Access Token (optional; increases rate limit)
 *                     Without token: 60 req/hr  → use POLL_INTERVAL=60
 *                     With token:  5000 req/hr  → use POLL_INTERVAL=10
 *   KAFKA_BOOTSTRAP   Kafka bootstrap servers     (default: kafka-server:9092)
 *   KAFKA_TOPIC       Kafka topic name            (default: github-events)
 *   POLL_INTERVAL     Seconds between API polls   (default: 10)
 *
 * Usage (inside cs523bdt-lab container):
 *   # Build
 *   mvn clean package -DskipTests
 *
 *   # Run
 *   export GITHUB_TOKEN="ghp_your_token_here"
 *   java -cp target/github-activity-analytics-1.0-SNAPSHOT-jar-with-dependencies.jar \
 *        GitHubKafkaProducer
 *
 *   # Or pass config as system properties:
 *   java -DGITHUB_TOKEN=ghp_xxx -DKAFKA_BOOTSTRAP=kafka-server:9092 \
 *        -cp target/github-activity-analytics-1.0-SNAPSHOT-jar-with-dependencies.jar \
 *        GitHubKafkaProducer
 */
public class GitHubKafkaProducer {

    // ─────────────────────────────────────────────
    // Configuration
    // ─────────────────────────────────────────────
    private static final String GITHUB_TOKEN    = getConfig("GITHUB_TOKEN", "");
    private static final String KAFKA_BOOTSTRAP = getConfig("KAFKA_BOOTSTRAP", "kafka-server:9092");
    private static final String KAFKA_TOPIC     = getConfig("KAFKA_TOPIC", "github-events");
    private static final int    POLL_INTERVAL   = Integer.parseInt(getConfig("POLL_INTERVAL", "10"));

    private static final String GITHUB_EVENTS_URL = "https://api.github.com/events?per_page=100";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // GitHub event type → trend weight (used for trending score in Spark SQL)
    private static final Map<String, Integer> EVENT_WEIGHTS = Map.ofEntries(
        Map.entry("PushEvent",        2),
        Map.entry("WatchEvent",       3),   // WatchEvent = "star"
        Map.entry("ForkEvent",        2),
        Map.entry("IssuesEvent",      1),
        Map.entry("PullRequestEvent", 1),
        Map.entry("CreateEvent",      1),
        Map.entry("ReleaseEvent",     2),
        Map.entry("DeleteEvent",      0),
        Map.entry("GollumEvent",      0),
        Map.entry("MemberEvent",      0),
        Map.entry("PublicEvent",      0),
        Map.entry("CommitCommentEvent",              0),
        Map.entry("IssueCommentEvent",               0),
        Map.entry("PullRequestReviewEvent",          0),
        Map.entry("PullRequestReviewCommentEvent",   0)
    );

    // ─────────────────────────────────────────────
    // Main
    // ─────────────────────────────────────────────
    public static void main(String[] args) throws InterruptedException {

        printBanner();

        // Build Kafka producer
        KafkaProducer<String, String> producer = createProducer();

        // Track seen event IDs to avoid duplicates across polls
        Set<String> seenIds = new HashSet<>();
        int totalSent = 0;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[Producer] Shutdown signal received. Closing Kafka producer...");
            producer.flush();
            producer.close();
            System.out.println("[Producer] Closed. Goodbye.");
        }));

        try {
            while (true) {
                List<JsonNode> events = fetchEvents();
                int newCount = 0;

                for (JsonNode rawEvent : events) {
                    String eventId = rawEvent.path("id").asText("");
                    if (eventId.isEmpty() || seenIds.contains(eventId)) {
                        continue;
                    }

                    seenIds.add(eventId);

                    try {
                        ObjectNode enriched = enrichEvent(rawEvent);
                        String json = MAPPER.writeValueAsString(enriched);

                        ProducerRecord<String, String> record =
                            new ProducerRecord<>(KAFKA_TOPIC, eventId, json);

                        producer.send(record, (metadata, ex) -> {
                            if (ex != null) {
                                System.err.println("[Producer] Kafka send error: " + ex.getMessage());
                            }
                        });

                        newCount++;
                    } catch (Exception e) {
                        System.err.println("[Producer] Error processing event " + eventId + ": " + e.getMessage());
                    }
                }

                // Flush buffered messages
                producer.flush();
                totalSent += newCount;

                if (newCount > 0) {
                    System.out.printf("[Producer] Sent %d new events → Kafka. Session total: %d. Seen IDs: %d%n",
                        newCount, totalSent, seenIds.size());
                } else {
                    System.out.println("[Producer] No new events this cycle.");
                }

                // Prevent unbounded memory growth — reset cache every 50k IDs
                if (seenIds.size() > 50_000) {
                    System.out.println("[Producer] Trimming seen-IDs cache...");
                    seenIds.clear();
                }

                Thread.sleep(POLL_INTERVAL * 1000L);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[Producer] Interrupted. Exiting...");
        }
    }

    // ─────────────────────────────────────────────
    // GitHub API: fetch events
    // ─────────────────────────────────────────────

    private static List<JsonNode> fetchEvents() {
        List<JsonNode> result = new ArrayList<>();
        HttpURLConnection conn = null;
        try {
            URL url = new URL(GITHUB_EVENTS_URL);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(15_000);
            conn.setReadTimeout(15_000);
            conn.setRequestProperty("Accept", "application/vnd.github+json");
            conn.setRequestProperty("X-GitHub-Api-Version", "2022-11-28");
            conn.setRequestProperty("User-Agent", "CS523-BDT-Project/1.0");
            if (!GITHUB_TOKEN.isEmpty()) {
                conn.setRequestProperty("Authorization", "Bearer " + GITHUB_TOKEN);
            }

            int status = conn.getResponseCode();
            String remaining = conn.getHeaderField("X-RateLimit-Remaining");
            String reset     = conn.getHeaderField("X-RateLimit-Reset");

            if (status == 200) {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(conn.getInputStream()))) {
                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) sb.append(line);
                    JsonNode arr = MAPPER.readTree(sb.toString());
                    if (arr.isArray()) {
                        arr.forEach(result::add);
                    }
                }
                System.out.printf("[Producer] Fetched %d events — rate-limit remaining: %s%n",
                    result.size(), remaining != null ? remaining : "?");

            } else if (status == 403) {
                System.out.printf("[Producer] Rate limited (403). Remaining: %s. Reset: %s. Sleeping 60s...%n",
                    remaining, reset);
                Thread.sleep(60_000);

            } else if (status == 304) {
                System.out.println("[Producer] 304 Not Modified — no new events.");

            } else {
                System.err.println("[Producer] GitHub API error: HTTP " + status);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            System.err.println("[Producer] Network error fetching GitHub events: " + e.getMessage());
        } finally {
            if (conn != null) conn.disconnect();
        }
        return result;
    }

    // ─────────────────────────────────────────────
    // Event enrichment / normalization
    // Produces a flat JSON matching GitHubStreamingApp schema
    // ─────────────────────────────────────────────

    private static ObjectNode enrichEvent(JsonNode raw) {
        String eventType = raw.path("type").asText("Unknown");
        JsonNode actor   = raw.path("actor");
        JsonNode repo    = raw.path("repo");
        JsonNode payload = raw.path("payload");

        ObjectNode out = MAPPER.createObjectNode();

        // ── Core fields ───────────────────────────────────
        out.put("event_id",     raw.path("id").asText(""));
        out.put("event_type",   eventType);
        out.put("actor_login",  actor.path("login").asText(""));
        out.put("actor_id",     actor.path("id").asLong(0));
        out.put("repo_id",      repo.path("id").asLong(0));
        out.put("repo_name",    repo.path("name").asText(""));
        out.put("created_at",   raw.path("created_at").asText(""));
        out.put("ingest_time",  Instant.now().toString());
        out.put("trend_weight", EVENT_WEIGHTS.getOrDefault(eventType, 0));

        // ── Type-specific fields (default: empty / 0) ────
        out.put("push_size",    0);
        out.put("push_ref",     "");
        out.put("ref_type",     "");
        out.put("fork_repo",    "");
        out.put("release_tag",  "");
        out.put("pr_action",    "");
        out.put("issue_action", "");
        out.put("language",     "");

        switch (eventType) {

            case "PushEvent":
                // Number of commits in this push + the git ref
                out.put("push_size", payload.path("size").asInt(0));
                out.put("push_ref",  payload.path("ref").asText(""));
                break;

            case "CreateEvent":
                // repository / branch / tag
                out.put("ref_type", payload.path("ref_type").asText(""));
                break;

            case "ForkEvent":
                // Full name of the newly created fork
                out.put("fork_repo", payload.path("forkee").path("full_name").asText(""));
                break;

            case "ReleaseEvent":
                // Tag of the published release
                out.put("release_tag", payload.path("release").path("tag_name").asText(""));
                break;

            case "PullRequestEvent":
                out.put("pr_action", payload.path("action").asText(""));
                break;

            case "IssuesEvent":
                out.put("issue_action", payload.path("action").asText(""));
                break;

            default:
                break;
        }

        return out;
    }

    // ─────────────────────────────────────────────
    // Kafka producer factory
    // ─────────────────────────────────────────────

    private static KafkaProducer<String, String> createProducer() {
        System.out.println("[Producer] Connecting to Kafka at " + KAFKA_BOOTSTRAP + " ...");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  KAFKA_BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,               "all");
        props.put(ProducerConfig.RETRIES_CONFIG,            3);
        props.put(ProducerConfig.LINGER_MS_CONFIG,          100);      // small batching delay
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,         16384);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,       10_000);
        return new KafkaProducer<>(props);
    }

    // ─────────────────────────────────────────────
    // Utility: read config from env var or system property
    // ─────────────────────────────────────────────

    private static String getConfig(String key, String defaultValue) {
        String env = System.getenv(key);
        if (env != null && !env.isEmpty()) return env;
        String prop = System.getProperty(key);
        if (prop != null && !prop.isEmpty()) return prop;
        return defaultValue;
    }

    private static void printBanner() {
        System.out.println("=================================================");
        System.out.println("  GitHub Activity — Java Kafka Producer");
        System.out.println("  Kafka:    " + KAFKA_BOOTSTRAP + "  →  topic: " + KAFKA_TOPIC);
        System.out.println("  Interval: " + POLL_INTERVAL + " seconds");
        System.out.println("  Token:    " + (GITHUB_TOKEN.isEmpty() ? "NO (60 req/hr max)" : "YES (5000 req/hr)"));
        System.out.println("=================================================");
    }
}
