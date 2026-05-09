#!/usr/bin/env bash
# =============================================================================
# kibana_setup.sh
# CS523 BDT Final Project — GitHub Activity Analytics Dashboard Setup
#
# This script auto-creates all Kibana index patterns via the Kibana REST API.
# Run this AFTER Elasticsearch + Kibana are up and AFTER running
# GitHubSparkSQLEsApp at least once (so ES indices exist).
#
# Usage:
#   bash /opt/my_code/Project/github_activity/scripts/kibana_setup.sh
# =============================================================================

KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"
ES_URL="${ES_URL:-http://localhost:9200}"
RETRIES=10
SLEEP=5

echo "============================================"
echo "  GitHub Analytics — Kibana Setup"
echo "  Kibana: $KIBANA_URL"
echo "  ES:     $ES_URL"
echo "============================================"

# ─────────────────────────────────────────────
# Wait for Kibana to be ready
# ─────────────────────────────────────────────
echo "Waiting for Kibana to be ready..."
for i in $(seq 1 $RETRIES); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$KIBANA_URL/api/status")
  if [ "$STATUS" = "200" ]; then
    echo "Kibana is up!"
    break
  fi
  echo "  Attempt $i/$RETRIES: Kibana not ready (HTTP $STATUS). Retrying in ${SLEEP}s..."
  sleep $SLEEP
done

# ─────────────────────────────────────────────
# Create Kibana index patterns
# ─────────────────────────────────────────────
create_index_pattern() {
  local INDEX_NAME="$1"
  local TITLE="$2"
  echo "Creating index pattern: $INDEX_NAME ..."
  curl -s -X POST "$KIBANA_URL/api/saved_objects/index-pattern" \
    -H "Content-Type: application/json" \
    -H "kbn-xsrf: true" \
    -d "{
      \"attributes\": {
        \"title\": \"${TITLE}\",
        \"timeFieldName\": \"created_at\"
      }
    }" | python3 -c "import sys,json; r=json.load(sys.stdin); print('  → ID:', r.get('id','ERROR'), r.get('error','OK'))"
}

create_index_pattern "gh_new_repos"          "gh_new_repos*"
create_index_pattern "gh_push_activity"      "gh_push_activity*"
create_index_pattern "gh_top_repos"          "gh_top_repos*"
create_index_pattern "gh_trending_repos"     "gh_trending_repos*"
create_index_pattern "gh_trending_languages" "gh_trending_languages*"
create_index_pattern "gh_stars_forks"        "gh_stars_forks*"
create_index_pattern "gh_releases"           "gh_releases*"
create_index_pattern "gh_event_distribution" "gh_event_distribution*"

echo ""
echo "============================================"
echo "  Index patterns created!"
echo ""
echo "  Next steps in Kibana (http://localhost:5601):"
echo ""
echo "  1. Analytics → Discover  → explore raw data"
echo "  2. Analytics → Visualize → create charts:"
echo ""
echo "     Metric 1 - New Repos Over Time:"
echo "       Type: Line | Index: gh_new_repos"
echo "       X-axis: created_minute | Y-axis: SUM(new_repo_count)"
echo ""
echo "     Metric 2 - Code Activity Speed:"
echo "       Type: Line | Index: gh_push_activity"
echo "       X-axis: created_minute | Y-axis: SUM(total_commits)"
echo ""
echo "     Metric 3 - Top Active Repositories:"
echo "       Type: Horizontal Bar | Index: gh_top_repos"
echo "       Y-axis: Terms(repo_name) | X-axis: SUM(activity_count)"
echo ""
echo "     Metric 4 - Trending Repos:"
echo "       Type: Horizontal Bar | Index: gh_trending_repos"
echo "       Y-axis: Terms(repo_name) | X-axis: SUM(trend_score)"
echo ""
echo "     Metric 5 - Trending Languages (BONUS JOIN):"
echo "       Type: Pie/Treemap | Index: gh_trending_languages"
echo "       Slice: Terms(language) | Size: SUM(activity_count)"
echo ""
echo "     Metric 6 - Stars & Forks Momentum:"
echo "       Type: Data Table | Index: gh_stars_forks"
echo "       Cols: repo_name, stars_gained, forks_gained, attention_momentum"
echo ""
echo "     Metric 7 - Release Activity:"
echo "       Type: Data Table | Index: gh_releases"
echo "       Cols: repo_name, release_tag, released_by, created_at"
echo ""
echo "     Metric 8 - Event Type Distribution:"
echo "       Type: Pie | Index: gh_event_distribution"
echo "       Slice: Terms(event_type) | Size: SUM(event_count)"
echo ""
echo "  3. Analytics → Dashboard → Add All → Save as 'GitHub Trends'"
echo "============================================"
