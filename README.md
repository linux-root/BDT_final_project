# BigData2026 — Final Project

End-to-end pipeline: **Data Sources → Kafka → Spark Streaming → HBase → Visualization / Insights**.

## Grading Map

| #     | Rubric Part                                              | Pts | Implemented in                                                                  |
|-------|----------------------------------------------------------|----:|---------------------------------------------------------------------------------|
| 1     | Data Ingestion using Kafka                               |   3 | `ingestion/src/main/java/com/bigdata2026/ingestion/Main.java`                   |
| 2     | Distributed Processing using Spark Structured Streaming  |   3 | `streaming/src/main/scala/com/bigdata2026/streaming/Main.scala`                 |
| 3     | Saving Processed Data to HBase                           |   2 | `streaming/src/main/scala/com/bigdata2026/streaming/storage/HBaseSink.scala`    |
| 4     | Dynamic Dashboards (visualize insights and results)      |   2 | `visualization/backend/` (REST API) + `visualization/frontend/` (Tyrian SPA) + `visualization/common/` (shared DTOs) |
| 5 ★   | **Bonus** — Spark SQL join with static datasets          |  +2 | `streaming/src/main/scala/com/bigdata2026/streaming/bonus/SparkSqlJoin.scala`   |
| Total |                                                          |  10 | (10% of course grade)                                                            |

## Architecture

```
Data Sources ──► Kafka ──► Spark Structured Streaming ──► HBase ──► { Backend API ──► Frontend Dashboard }
   (ingestion)              (streaming + bonus join)      (storage)         (visualization — Part 4)
```

## Modules

| Module       | Stack                                         | Role                                          |
|--------------|-----------------------------------------------|-----------------------------------------------|
| `ingestion`  | Java 11 + Kafka clients                       | Part 1 — producers from data sources to Kafka |
| `streaming`  | Scala 2.12 + Spark 3.1.2 Structured Streaming | Parts 2 + 3 + 5 — Kafka → enrich → HBase      |
| `visualization/common`   | Scala 3 crossProject (JVM + JS)      | Shared DTOs / API contracts             |
| `visualization/backend`  | Scala 3 + ZIO + Tapir + HBase client | Part 4 — REST API serving HBase reads   |
| `visualization/frontend` | Scala.js + Tyrian                    | Part 4 — Visualization SPA (dashboards) |

## Prerequisites

- JDK 11
- sbt 1.10.7
- Docker (for local Kafka + HBase)
- Node 20+ (frontend dev server, optional)

## Quickstart

```bash
docker compose up -d                # Kafka + HBase locally
sbt compile                         # builds all modules
sbt ingestion/run                   # Part 1 — runs the Java Kafka producer
sbt streaming/run                   # Part 2 + 3 + 5 — runs the Spark streaming job
sbt backend/run                            # Part 4 — runs the ZIO + Tapir API
sbt frontend/fastLinkJS                    # Part 4 — produces ESM bundle for the dashboard
```
