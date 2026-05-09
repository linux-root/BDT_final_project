#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
SBT="$(command -v sbt 2>/dev/null || echo "$ROOT/sbt")"
echo "==> Building streaming fat JAR..."
"$SBT" streaming/assembly
echo "==> Building image localhost/streaming:latest..."
docker build -f infra/streaming/Dockerfile -t localhost/streaming:latest .
echo "==> Done: localhost/streaming:latest"
