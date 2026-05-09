# Real-Time GitHub Open Source Trend Analytics Dashboard

## CS523 Big Data Technologies — Final Project

### Architecture Overview

```
GitHub Events API
      │
      ▼ (every 10 seconds)
github_producer.py
      │  JSON events
      ▼
Kafka Topic: github-events
      │
      ▼ (Spark Structured Streaming)
GitHubStreamingApp
      │  foreachBatch → append
      ▼
Hive Table: github_events (HDFS/Parquet)
      │
      ▼ (periodic batch job)
GitHubSparkSQLEsApp
      │  8 dashboard metrics via Spark SQL
      │  Bonus: JOIN with language_metadata.csv
      ▼
Elasticsearch indices
      │
      ▼
Kibana Dashboard
```

### Dashboard Metrics

| # | Metric | ES Index |
|---|--------|----------|
| 1 | New repositories created over time (5-min window) | `gh_new_repos` |
| 2 | Source-code activity speed (push commit count) | `gh_push_activity` |
| 3 | Top active repositories (by total events) | `gh_top_repos` |
| 4 | Trending repositories (weighted score) | `gh_trending_repos` |
| 5 | Trending programming languages (Spark SQL JOIN) | `gh_trending_languages` |
| 6 | Stars and forks momentum | `gh_stars_forks` |
| 7 | Release activity | `gh_releases` |
| 8 | Event type distribution | `gh_event_distribution` |

### Technology Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | GitHub REST API v3 + Python kafka-python |
| Message Queue | Apache Kafka 3.4 |
| Stream Processing | Apache Spark 3.2 Structured Streaming |
| Storage | Apache Hive (on HDFS, Parquet format) |
| Analytics | Spark SQL (with static language metadata JOIN) |
| Visualization | Elasticsearch 7.15.2 + Kibana 7.15.2 |

### Project Structure

```
github_activity/
├── README.md                   ← This file
├── commands.txt                ← Step-by-step run instructions
├── docker-compose.yml          ← Elasticsearch + Kibana
├── pom.xml                     ← Maven build (Spark + Kafka + ES)
├── language_metadata.csv       ← Static dataset for bonus SQL JOIN
├── producer/
│   └── github_producer.py      ← GitHub API → Kafka producer
├── scripts/
│   └── kibana_setup.sh         ← Auto-creates Kibana index patterns
└── src/
    └── main/
        └── java/
            ├── GitHubStreamingApp.java    ← Kafka → Hive (Structured Streaming)
            └── GitHubSparkSQLEsApp.java   ← Hive → Elasticsearch (Spark SQL)
```

### Prerequisites

- Docker + Docker Compose (for the main cs523bdt-lab environment)
- Python 3.x with `kafka-python` and `requests` packages
- GitHub Personal Access Token (optional but recommended for higher rate limits)

### Quick Start

See `commands.txt` for the full step-by-step guide.

### Notes on GitHub API Rate Limits

- **Without token**: 60 requests/hour → poll every 60 seconds
- **With token**: 5,000 requests/hour → poll every 10 seconds
- Set `GITHUB_TOKEN` environment variable to use authentication
- The producer fetches up to 100 events per request from `/events` endpoint
