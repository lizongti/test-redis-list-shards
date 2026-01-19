#!/usr/bin/env bash
set -euo pipefail

# test.sh
# - 启动 Redis 容器
# - 运行 Go 测试（包含动态增减 list 的用例）

cd "$(dirname "$0")/.."

docker compose up -d redis

# 等待 Redis 就绪
bash ./scripts/wait-for-redis.sh "127.0.0.1:6380" 30

export REDIS_ADDRS="127.0.0.1:6380"
go test ./... -count=1 -v
