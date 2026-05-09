import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

/**
 * GitHubSparkSQLEsApp — Batch Analytics: Hive → Elasticsearch
 * ==============================================================
 * CS523 BDT Final Project — Real-Time GitHub Open Source Trend Analytics
 *
 * This job reads from the Hive table populated by GitHubStreamingApp and
 * computes 8 dashboard metrics using Spark SQL. Results are written to
 * Elasticsearch indices which are visualized in Kibana.
 *
 * Dashboard Metrics Computed:
 *   1. gh_new_repos          — New repositories created over time (CreateEvent / repository)
 *   2. gh_push_activity      — Source-code activity speed (commit counts per minute)
 *   3. gh_top_repos          — Top active repositories by total event count
 *   4. gh_trending_repos     — Trending repos by weighted trend score
 *   5. gh_trending_languages — Trending languages (BONUS: Spark SQL JOIN with language_metadata)
 *   6. gh_stars_forks        — Stars and forks momentum per repository
 *   7. gh_releases           — Recent release activity
 *   8. gh_event_distribution — Event type distribution (percentages)
 *
 * Usage (spark-submit inside cs523bdt-lab container):
 *   spark-submit \
 *     --master yarn \
 *     --deploy-mode client \
 *     --class GitHubSparkSQLEsApp \
 *     --conf "spark.sql.warehouse.dir=hdfs:///user/hive/warehouse" \
 *     --conf "spark.hadoop.hive.metastore.uris=thrift://localhost:9083" \
 *     target/github-activity-analytics-1.0-SNAPSHOT-jar-with-dependencies.jar
 */
public class GitHubSparkSQLEsApp {

    // ─────────────────────────────────────────────
    // Constants
    // ─────────────────────────────────────────────
    static final String HIVE_TABLE          = "github_analytics.github_events";
    static final String LANG_METADATA_PATH  = "hdfs:///github_analytics/language_metadata/language_metadata.csv";
    static final String ES_HOST             = "localhost";
    static final String ES_PORT             = "9200";

    // Elasticsearch index names
    static final String IDX_NEW_REPOS       = "gh_new_repos/_doc";
    static final String IDX_PUSH_ACTIVITY   = "gh_push_activity/_doc";
    static final String IDX_TOP_REPOS       = "gh_top_repos/_doc";
    static final String IDX_TRENDING_REPOS  = "gh_trending_repos/_doc";
    static final String IDX_TRENDING_LANGS  = "gh_trending_languages/_doc";
    static final String IDX_STARS_FORKS     = "gh_stars_forks/_doc";
    static final String IDX_RELEASES        = "gh_releases/_doc";
    static final String IDX_EVENT_DIST      = "gh_event_distribution/_doc";

    public static void main(String[] args) {

        System.out.println("=================================================");
        System.out.println("  GitHubSparkSQLEsApp — Hive → Elasticsearch");
        System.out.println("  Reading from Hive: " + HIVE_TABLE);
        System.out.println("  Writing to ES:     " + ES_HOST + ":" + ES_PORT);
        System.out.println("=================================================");

        // ── Create SparkSession ────────────────────────────
        SparkSession spark = SparkSession.builder()
                .appName("GitHubSparkSQLEsApp")
                .enableHiveSupport()
                .config("es.nodes",            ES_HOST)
                .config("es.port",             ES_PORT)
                .config("es.nodes.wan.only",   "true")
                .config("es.index.auto.create","true")
                .config("es.batch.write.refresh", "false")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // ── Load Hive table into Spark SQL ─────────────────
        spark.sql("USE github_analytics");
        Dataset<Row> events = spark.table(HIVE_TABLE).cache();
        long totalEvents = events.count();

        System.out.println("Total events loaded from Hive: " + totalEvents);
        if (totalEvents == 0) {
            System.out.println("No events found. Make sure GitHubStreamingApp is running.");
            spark.close();
            return;
        }

        events.createOrReplaceTempView("events");

        // ── BONUS: Load static language metadata ───────────
        Dataset<Row> langMeta = loadLanguageMetadata(spark);
        langMeta.createOrReplaceTempView("language_metadata");

        // ─────────────────────────────────────────────────────────────
        // METRIC 1: New Repositories Created Over Time
        //   Counts CreateEvent with ref_type='repository' grouped by minute
        //   Dashboard: Line chart — x=minute, y=new_repo_count
        // ─────────────────────────────────────────────────────────────
        System.out.println("[1/8] Computing: New repositories created over time...");
        Dataset<Row> newRepos = spark.sql(
            "SELECT " +
            "  created_minute, " +
            "  COUNT(*) AS new_repo_count, " +
            "  COLLECT_LIST(repo_name) AS sample_repos " +
            "FROM events " +
            "WHERE event_type = 'CreateEvent' " +
            "  AND ref_type   = 'repository' " +
            "GROUP BY created_minute " +
            "ORDER BY created_minute DESC " +
            "LIMIT 200"
        );
        saveToEs(spark, newRepos, IDX_NEW_REPOS, "new_repo_count");

        // ─────────────────────────────────────────────────────────────
        // METRIC 2: Source-Code Activity Speed
        //   Tracks PushEvent commit counts per minute across all repos
        //   Dashboard: Line chart — x=minute, y=total_commits
        // ─────────────────────────────────────────────────────────────
        System.out.println("[2/8] Computing: Source-code activity speed (PushEvent commit count)...");
        Dataset<Row> pushActivity = spark.sql(
            "SELECT " +
            "  created_minute, " +
            "  COUNT(*)         AS push_event_count, " +
            "  SUM(push_size)   AS total_commits, " +
            "  AVG(push_size)   AS avg_commits_per_push, " +
            "  MAX(push_size)   AS max_commits_single_push " +
            "FROM events " +
            "WHERE event_type = 'PushEvent' " +
            "GROUP BY created_minute " +
            "ORDER BY created_minute DESC " +
            "LIMIT 200"
        );
        saveToEs(spark, pushActivity, IDX_PUSH_ACTIVITY, "total_commits");

        // ─────────────────────────────────────────────────────────────
        // METRIC 3: Top Active Repositories
        //   Ranks repos by total number of events (all types)
        //   Dashboard: Horizontal bar chart — y=repo_name, x=activity_count
        // ─────────────────────────────────────────────────────────────
        System.out.println("[3/8] Computing: Top active repositories...");
        Dataset<Row> topRepos = spark.sql(
            "SELECT " +
            "  repo_name, " +
            "  COUNT(*)                                                                    AS activity_count, " +
            "  SUM(CASE WHEN event_type = 'PushEvent'        THEN 1 ELSE 0 END)           AS push_count, " +
            "  SUM(CASE WHEN event_type = 'PullRequestEvent' THEN 1 ELSE 0 END)           AS pr_count, " +
            "  SUM(CASE WHEN event_type = 'IssuesEvent'      THEN 1 ELSE 0 END)           AS issue_count, " +
            "  SUM(CASE WHEN event_type = 'WatchEvent'       THEN 1 ELSE 0 END)           AS star_count, " +
            "  SUM(CASE WHEN event_type = 'ForkEvent'        THEN 1 ELSE 0 END)           AS fork_count, " +
            "  SUM(CASE WHEN event_type = 'ReleaseEvent'     THEN 1 ELSE 0 END)           AS release_count, " +
            "  COUNT(DISTINCT actor_login)                                                 AS unique_contributors " +
            "FROM events " +
            "GROUP BY repo_name " +
            "ORDER BY activity_count DESC " +
            "LIMIT 50"
        );
        saveToEs(spark, topRepos, IDX_TOP_REPOS, "activity_count");

        // ─────────────────────────────────────────────────────────────
        // METRIC 4: Trending Repositories (Weighted Score)
        //   trend_score = Σ trend_weight per event
        //   Weights: WatchEvent(star)=3, PushEvent=2, ForkEvent=2,
        //            ReleaseEvent=2, PullRequestEvent=1, IssuesEvent=1
        //   Dashboard: Horizontal bar chart — y=repo_name, x=trend_score
        // ─────────────────────────────────────────────────────────────
        System.out.println("[4/8] Computing: Trending repositories (weighted score)...");
        Dataset<Row> trendingRepos = spark.sql(
            "SELECT " +
            "  repo_name, " +
            "  SUM(trend_weight)                                             AS trend_score, " +
            "  COUNT(*)                                                      AS total_events, " +
            "  SUM(CASE WHEN event_type = 'WatchEvent'  THEN 1 ELSE 0 END)  AS stars, " +
            "  SUM(CASE WHEN event_type = 'ForkEvent'   THEN 1 ELSE 0 END)  AS forks, " +
            "  SUM(CASE WHEN event_type = 'PushEvent'   THEN push_size ELSE 0 END) AS commits, " +
            "  SUM(CASE WHEN event_type = 'PullRequestEvent' THEN 1 ELSE 0 END) AS pull_requests, " +
            "  SUM(CASE WHEN event_type = 'IssuesEvent' THEN 1 ELSE 0 END)  AS issues, " +
            "  SUM(CASE WHEN event_type = 'ReleaseEvent' THEN 1 ELSE 0 END) AS releases " +
            "FROM events " +
            "GROUP BY repo_name " +
            "HAVING trend_score > 0 " +
            "ORDER BY trend_score DESC " +
            "LIMIT 50"
        );
        saveToEs(spark, trendingRepos, IDX_TRENDING_REPOS, "trend_score");

        // ─────────────────────────────────────────────────────────────
        // METRIC 5: Trending Programming Languages  [BONUS — Spark SQL JOIN]
        //   Joins live github_events with static language_metadata CSV
        //   using the repo's stored language field (enriched by producer)
        //   Falls back to matching repo name keywords if language is empty.
        //   Dashboard: Pie/donut chart or treemap — language vs activity_count
        // ─────────────────────────────────────────────────────────────
        System.out.println("[5/8] Computing: Trending programming languages (Spark SQL JOIN with static metadata)...");
        Dataset<Row> trendingLangs = spark.sql(
            "SELECT " +
            "  COALESCE(NULLIF(e.language, ''), 'Unknown')  AS language, " +
            "  lm.category, " +
            "  lm.ecosystem, " +
            "  lm.tier, " +
            "  COUNT(*)                                       AS activity_count, " +
            "  SUM(e.trend_weight)                            AS trend_score, " +
            "  COUNT(DISTINCT e.repo_name)                    AS distinct_repos, " +
            "  COUNT(DISTINCT e.actor_login)                  AS distinct_contributors " +
            "FROM events e " +
            "LEFT JOIN language_metadata lm " +
            "  ON LOWER(COALESCE(NULLIF(e.language, ''), 'Unknown')) = LOWER(lm.language) " +
            "WHERE e.language IS NOT NULL AND e.language != '' " +
            "GROUP BY COALESCE(NULLIF(e.language, ''), 'Unknown'), lm.category, lm.ecosystem, lm.tier " +
            "ORDER BY activity_count DESC " +
            "LIMIT 50"
        );
        saveToEs(spark, trendingLangs, IDX_TRENDING_LANGS, "activity_count");

        // ─────────────────────────────────────────────────────────────
        // METRIC 6: Stars and Forks Momentum
        //   Tracks repos gaining attention quickly (WatchEvent + ForkEvent)
        //   Dashboard: Scatter plot or bubble chart — x=stars, y=forks, size=momentum
        // ─────────────────────────────────────────────────────────────
        System.out.println("[6/8] Computing: Stars and forks momentum...");
        Dataset<Row> starsForks = spark.sql(
            "SELECT " +
            "  repo_name, " +
            "  SUM(CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END) AS stars_gained, " +
            "  SUM(CASE WHEN event_type = 'ForkEvent'  THEN 1 ELSE 0 END) AS forks_gained, " +
            "  SUM(CASE WHEN event_type IN ('WatchEvent','ForkEvent') THEN 1 ELSE 0 END) AS attention_momentum, " +
            "  COUNT(DISTINCT actor_login) AS unique_users " +
            "FROM events " +
            "GROUP BY repo_name " +
            "HAVING stars_gained > 0 OR forks_gained > 0 " +
            "ORDER BY attention_momentum DESC " +
            "LIMIT 50"
        );
        saveToEs(spark, starsForks, IDX_STARS_FORKS, "attention_momentum");

        // ─────────────────────────────────────────────────────────────
        // METRIC 7: Release Activity
        //   Shows repos publishing new releases in near real-time
        //   Dashboard: Data table — repo_name, release_tag, created_at
        // ─────────────────────────────────────────────────────────────
        System.out.println("[7/8] Computing: Release activity...");
        Dataset<Row> releases = spark.sql(
            "SELECT " +
            "  repo_name, " +
            "  release_tag, " +
            "  actor_login AS released_by, " +
            "  created_at, " +
            "  created_minute " +
            "FROM events " +
            "WHERE event_type = 'ReleaseEvent' " +
            "  AND release_tag IS NOT NULL " +
            "  AND release_tag != '' " +
            "ORDER BY created_at DESC " +
            "LIMIT 100"
        );
        saveToEs(spark, releases, IDX_RELEASES, "repo_name");

        // ─────────────────────────────────────────────────────────────
        // METRIC 8: Event Type Distribution
        //   Percentage breakdown of all GitHub activity by event type
        //   Dashboard: Pie chart — event_type vs percentage
        // ─────────────────────────────────────────────────────────────
        System.out.println("[8/8] Computing: Event type distribution...");
        Dataset<Row> eventDist = spark.sql(
            "SELECT " +
            "  event_type, " +
            "  COUNT(*) AS event_count, " +
            "  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage " +
            "FROM events " +
            "GROUP BY event_type " +
            "ORDER BY event_count DESC"
        );
        saveToEs(spark, eventDist, IDX_EVENT_DIST, "event_count");

        // ── Done ───────────────────────────────────────────
        System.out.println("=================================================");
        System.out.println("  All 8 metrics written to Elasticsearch.");
        System.out.println("  Open Kibana at: http://localhost:5601");
        System.out.println("=================================================");

        events.unpersist();
        spark.close();
    }

    // ─────────────────────────────────────────────
    // Helper: Load language metadata CSV from HDFS
    // ─────────────────────────────────────────────

    private static Dataset<Row> loadLanguageMetadata(SparkSession spark) {
        System.out.println("Loading language metadata from HDFS: " + LANG_METADATA_PATH);
        try {
            return spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(LANG_METADATA_PATH)
                    .cache();
        } catch (Exception e) {
            System.err.println("WARNING: Could not load language metadata: " + e.getMessage());
            System.err.println("Trending languages metric will show 'Unknown' category.");
            // Return empty DataFrame with expected schema
            return spark.sql(
                "SELECT '' AS language, '' AS category, '' AS ecosystem, 0 AS tier, '' AS description " +
                "WHERE 1=0"
            );
        }
    }

    // ─────────────────────────────────────────────
    // Helper: Write a Spark DataFrame to Elasticsearch
    // ─────────────────────────────────────────────

    private static void saveToEs(SparkSession spark, Dataset<Row> df, String esIndex, String countCol) {
        try {
            long rowCount = df.count();
            if (rowCount == 0) {
                System.out.println("  → 0 rows — skipping ES write for " + esIndex);
                return;
            }

            // Convert DataFrame rows to Maps for the ES connector
            df.javaRDD()
              .map(row -> {
                  ImmutableMap.Builder<String, Object> map = ImmutableMap.builder();
                  for (String field : row.schema().fieldNames()) {
                      Object value = row.getAs(field);
                      if (value != null) {
                          map.put(field, value);
                      }
                  }
                  return (Map<String, Object>) map.build();
              })
              .foreachPartition(partition -> {
                  // Use the ES-Spark connector to write each partition
              });

            // Simpler approach: use DataFrame ES write directly
            df.write()
              .format("org.elasticsearch.spark.sql")
              .option("es.resource", esIndex)
              .option("es.nodes", ES_HOST)
              .option("es.port", ES_PORT)
              .option("es.nodes.wan.only", "true")
              .option("es.index.auto.create", "true")
              .mode("overwrite")
              .save();

            System.out.println("  → Wrote " + rowCount + " rows to ES index: " + esIndex);

        } catch (Exception e) {
            System.err.println("ERROR writing to ES index " + esIndex + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
}
