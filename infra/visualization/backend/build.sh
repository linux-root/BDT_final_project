#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
SBT="$(command -v sbt 2>/dev/null || echo "$ROOT/sbt")"
echo "==> Building backend fat JAR..."
"$SBT" backend/assembly
echo "==> Building image localhost/visualization-backend:latest..."
docker build -f infra/visualization/backend/Dockerfile -t localhost/visualization-backend:latest .
echo "==> Done: localhost/visualization-backend:latest"
