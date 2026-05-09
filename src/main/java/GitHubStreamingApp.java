import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

/**
 * GitHubStreamingApp — Spark Structured Streaming
 * ================================================
 * CS523 BDT Final Project — Real-Time GitHub Open Source Trend Analytics
 *
 * Pipeline:
 *   Kafka topic "github-events"
 *       │  (JSON records produced by github_producer.py)
 *       ▼
 *   Spark Structured Streaming
 *       │  parse JSON → DataFrame
 *       ▼
 *   Hive table "github_events"  (HDFS Parquet, append mode)
 *
 * Usage (spark-submit inside cs523bdt-lab container):
 *   spark-submit \
 *     --master yarn \
 *     --deploy-mode client \
 *     --class GitHubStreamingApp \
 *     --conf "spark.sql.warehouse.dir=hdfs:///user/hive/warehouse" \
 *     --conf "spark.hadoop.hive.metastore.uris=thrift://localhost:9083" \
 *     target/github-activity-analytics-1.0-SNAPSHOT-jar-with-dependencies.jar \
 *     [kafka-bootstrap-servers] [kafka-topic]
 */
public class GitHubStreamingApp {

    // ─────────────────────────────────────────────
    // Constants
    // ─────────────────────────────────────────────
    static final String HIVE_DB           = "github_analytics";
    static final String HIVE_TABLE        = "github_analytics.github_events";
    static final String CHECKPOINT_DIR    = "hdfs:///tmp/github-streaming-checkpoint";

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

        // ── CLI Arguments ──────────────────────────────────
        String kafkaBootstrap = args.length > 0 ? args[0] : "kafka-server:9092";
        String kafkaTopic     = args.length > 1 ? args[1] : "github-events";

        System.out.println("=================================================");
        System.out.println("  GitHubStreamingApp — Spark Structured Streaming");
        System.out.println("  Kafka: " + kafkaBootstrap + " → topic: " + kafkaTopic);
        System.out.println("  Hive table: " + HIVE_TABLE);
        System.out.println("=================================================");

        // ── Create SparkSession with Hive support ──────────
        SparkSession spark = SparkSession.builder()
                .appName("GitHubStreamingApp")
                .enableHiveSupport()
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // ── Initialize Hive database and table ────────────
        initHiveTable(spark);

        // ── Define JSON schema matching github_producer.py ─
        StructType githubEventSchema = buildEventSchema();

        // ── Read stream from Kafka ─────────────────────────
        Dataset<Row> kafkaDf = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrap)
                .option("subscribe", kafkaTopic)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .option("maxOffsetsPerTrigger", 10000)   // back-pressure
                .load();

        // ── Parse JSON value from Kafka message ────────────
        Dataset<Row> eventsDf = kafkaDf
                .selectExpr("CAST(value AS STRING) AS json_str",
                            "timestamp AS kafka_ts")
                .select(
                        from_json(col("json_str"), githubEventSchema).alias("e"),
                        col("kafka_ts")
                )
                .select(
                        col("e.event_id"),
                        col("e.event_type"),
                        col("e.actor_login"),
                        col("e.actor_id"),
                        col("e.repo_id"),
                        col("e.repo_name"),
                        col("e.created_at"),
                        col("e.ingest_time"),
                        col("e.trend_weight"),
                        col("e.push_size"),
                        col("e.push_ref"),
                        col("e.ref_type"),
                        col("e.fork_repo"),
                        col("e.release_tag"),
                        col("e.pr_action"),
                        col("e.issue_action"),
                        col("e.language"),
                        // Derived convenience columns
                        col("e.created_at").substr(1, 16).alias("created_minute"),  // "YYYY-MM-DDTHH:MM"
                        col("e.created_at").substr(1, 10).alias("created_date"),    // "YYYY-MM-DD"
                        col("e.created_at").substr(1, 7).alias("created_month"),    // "YYYY-MM"
                        split(col("e.repo_name"), "/").getItem(0).alias("repo_owner"),
                        split(col("e.repo_name"), "/").getItem(1).alias("repo_short_name"),
                        current_timestamp().cast(DataTypes.StringType).alias("batch_time")
                )
                // Drop events with missing essential fields
                .filter(col("event_id").isNotNull()
                        .and(col("event_id").notEqual(""))
                        .and(col("event_type").isNotNull())
                        .and(col("repo_name").isNotNull()));

        // ── Write stream to Hive via foreachBatch ──────────
        StreamingQuery query = eventsDf
                .writeStream()
                .outputMode("append")
                .trigger(Trigger.ProcessingTime("30 seconds"))   // micro-batch every 30s
                .option("checkpointLocation", CHECKPOINT_DIR)
                .foreachBatch((batchDf, batchId) -> {
                    long count = batchDf.count();
                    if (count > 0) {
                        System.out.println(
                            "[Batch " + batchId + "] Writing " + count +
                            " events to Hive: " + HIVE_TABLE);

                        batchDf
                            .write()
                            .mode("append")
                            .format("parquet")
                            .saveAsTable(HIVE_TABLE);

                        System.out.println("[Batch " + batchId + "] Done. ✓");
                    } else {
                        System.out.println("[Batch " + batchId + "] Empty batch — skipping.");
                    }
                })
                .start();

        System.out.println("Streaming query started. Awaiting termination...");
        System.out.println("Monitor at Spark UI: http://localhost:4040");
        query.awaitTermination();
    }

    // ─────────────────────────────────────────────
    // Hive table initialization
    // ─────────────────────────────────────────────

    private static void initHiveTable(SparkSession spark) {
        System.out.println("Initializing Hive database and table...");

        spark.sql("CREATE DATABASE IF NOT EXISTS " + HIVE_DB);

        spark.sql(
            "CREATE TABLE IF NOT EXISTS " + HIVE_TABLE + " (" +
            "  event_id        STRING     COMMENT 'GitHub event ID'," +
            "  event_type      STRING     COMMENT 'PushEvent, WatchEvent, ForkEvent, etc.'," +
            "  actor_login     STRING     COMMENT 'GitHub username of the actor'," +
            "  actor_id        BIGINT     COMMENT 'GitHub user ID of the actor'," +
            "  repo_id         BIGINT     COMMENT 'GitHub repository ID'," +
            "  repo_name       STRING     COMMENT 'Full repo name: owner/repo'," +
            "  created_at      STRING     COMMENT 'ISO 8601 timestamp from GitHub API'," +
            "  ingest_time     STRING     COMMENT 'Time this event was ingested by producer'," +
            "  trend_weight    INT        COMMENT 'Weight used in trending score computation'," +
            "  push_size       INT        COMMENT 'Number of commits in PushEvent (0 otherwise)'," +
            "  push_ref        STRING     COMMENT 'Git ref for PushEvent (e.g. refs/heads/main)'," +
            "  ref_type        STRING     COMMENT 'CreateEvent ref type: repository/branch/tag'," +
            "  fork_repo       STRING     COMMENT 'Full name of forked repo in ForkEvent'," +
            "  release_tag     STRING     COMMENT 'Tag name in ReleaseEvent'," +
            "  pr_action       STRING     COMMENT 'PullRequestEvent action: opened/closed/merged'," +
            "  issue_action    STRING     COMMENT 'IssuesEvent action: opened/closed/etc.'," +
            "  language        STRING     COMMENT 'Primary programming language of the repo'," +
            "  created_minute  STRING     COMMENT 'YYYY-MM-DDTHH:MM (for time-series grouping)'," +
            "  created_date    STRING     COMMENT 'YYYY-MM-DD (for daily grouping)'," +
            "  created_month   STRING     COMMENT 'YYYY-MM (for monthly grouping)'," +
            "  repo_owner      STRING     COMMENT 'Repository owner (first part of repo_name)'," +
            "  repo_short_name STRING     COMMENT 'Repository short name (second part)'," +
            "  batch_time      STRING     COMMENT 'Spark batch processing time'" +
            ") " +
            "STORED AS PARQUET " +
            "TBLPROPERTIES ('comment'='GitHub public events streamed from Kafka')"
        );

        System.out.println("Hive table ready: " + HIVE_TABLE);
    }

    // ─────────────────────────────────────────────
    // JSON schema definition
    // Must match the JSON fields produced by github_producer.py
    // ─────────────────────────────────────────────

    private static StructType buildEventSchema() {
        return new StructType()
                .add("event_id",     DataTypes.StringType,  true)
                .add("event_type",   DataTypes.StringType,  true)
                .add("actor_login",  DataTypes.StringType,  true)
                .add("actor_id",     DataTypes.LongType,    true)
                .add("repo_id",      DataTypes.LongType,    true)
                .add("repo_name",    DataTypes.StringType,  true)
                .add("created_at",   DataTypes.StringType,  true)
                .add("ingest_time",  DataTypes.StringType,  true)
                .add("trend_weight", DataTypes.IntegerType, true)
                .add("push_size",    DataTypes.IntegerType, true)
                .add("push_ref",     DataTypes.StringType,  true)
                .add("ref_type",     DataTypes.StringType,  true)
                .add("fork_repo",    DataTypes.StringType,  true)
                .add("release_tag",  DataTypes.StringType,  true)
                .add("pr_action",    DataTypes.StringType,  true)
                .add("issue_action", DataTypes.StringType,  true)
                .add("language",     DataTypes.StringType,  true);
    }
}
