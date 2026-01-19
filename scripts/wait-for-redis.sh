#!/usr/bin/env bash
set -euo pipefail

addr="${1:-127.0.0.1:6379}"
timeoutSec="${2:-30}"

# 使用容器内自带的 redis-cli 做健康检查，避免依赖宿主机安装 nc/redis-cli。
# 依赖 docker compose 中固定的 container_name: test-redis-list-shards-redis
container_name="test-redis-list-shards-redis"

echo "Waiting for redis at $addr via docker exec (timeout ${timeoutSec}s)..."

start=$(date +%s)
while true; do
  if docker exec "$container_name" redis-cli -h 127.0.0.1 -p 6379 ping 2>/dev/null | grep -qi "PONG"; then
    echo "Redis is ready."
    exit 0
  fi

  now=$(date +%s)
  if (( now - start >= timeoutSec )); then
    echo "Timed out waiting for Redis." >&2
    exit 1
  fi
  sleep 1
done
