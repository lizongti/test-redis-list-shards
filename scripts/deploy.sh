#!/usr/bin/env bash
set -euo pipefail

# deploy.sh
# - 启动 Redis + server 容器
# - 可选：在本机运行 client（更方便观察日志）

cd "$(dirname "$0")/.."

docker compose up -d --build redis server1 server2

echo "\nServer1 API: http://localhost:8080/shards"
echo "Server2 API: http://localhost:8081/shards"

echo "\nRun client locally (Ctrl+C to stop):"
export REDIS_ADDRS="127.0.0.1:6380"
go run ./cmd/client
