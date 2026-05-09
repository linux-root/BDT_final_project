#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
SBT="$(command -v sbt 2>/dev/null || echo "$ROOT/sbt")"
echo "==> Compiling frontend Scala.js (optimised)..."
"$SBT" frontend/fullLinkJS
echo "==> Building image localhost/visualization-frontend:latest..."
docker build -f infra/visualization/frontend/Dockerfile -t localhost/visualization-frontend:latest .
echo "==> Done: localhost/visualization-frontend:latest"
