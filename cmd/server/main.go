package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lizongti/test-redis-list-shards/internal/app"
)

// server 是主程序：
// 1) 周期性扫描 Redis 中的 List key（默认匹配 list:*）
// 2) 将这些 list 分配到若干 Shard（每个 Shard 一个 goroutine）
// 3) Shard 负责对其负责的 lists 做 BLPOP 并打印
// 4) 提供 HTTP 接口查询当前 Shard 状态
func main() {
	var (
		redisAddrs    = flag.String("redis", envOr("REDIS_ADDRS", "127.0.0.1:6379"), "Redis address list, comma-separated. e.g. 127.0.0.1:6379")
		redisPassword = flag.String("redis-password", envOr("REDIS_PASSWORD", ""), "Redis password")
		keyPattern    = flag.String("pattern", envOr("LIST_KEY_PATTERN", "list:*"), "Redis key pattern for lists")
		listsPerShard = flag.Int("lists-per-shard", envOrInt("LISTS_PER_SHARD", 8), "Target list count per shard")
		shardCount    = flag.Int("shard-count", envOrInt("SHARD_COUNT", 0), "Fixed shard count (0 = dynamic by lists-per-shard)")
		refreshEvery  = flag.Duration("refresh", envOrDuration("SHARD_REFRESH", 1*time.Second), "Refresh interval for scanning lists")
		instanceID    = flag.String("instance-id", envOr("INSTANCE_ID", ""), "Unique instance id for multi-server ownership")
		memberKey     = flag.String("member-key", envOr("MEMBER_KEY", "test-redis-list-shards:members"), "Redis ZSET key for membership")
		memberTTL     = flag.Duration("member-ttl", envOrDuration("MEMBER_TTL", 6*time.Second), "Membership TTL")
		httpAddr      = flag.String("http", envOr("HTTP_ADDR", ":8080"), "HTTP listen address")
		popEnabled    = flag.Bool("pop", envOrBool("POP_ENABLED", true), "Enable BLPOP workers")
		popTimeout    = flag.Duration("pop-timeout", envOrDuration("POP_TIMEOUT", 1*time.Second), "BLPOP timeout (per call)")
		logPrefix     = flag.String("log-prefix", envOr("LOG_PREFIX", "server"), "Log prefix")
	)
	flag.Parse()

	log.SetPrefix("[" + *logPrefix + "] ")
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg := app.Config{
		RedisAddrs:    splitCSV(*redisAddrs),
		RedisPassword: *redisPassword,
		ListKeyMatch:  *keyPattern,
		ListsPerShard: *listsPerShard,
		ShardCount:    *shardCount,
		RefreshEvery:  *refreshEvery,
		InstanceID:    *instanceID,
		MemberKey:     *memberKey,
		MemberTTL:     *memberTTL,
		HTTPAddr:      *httpAddr,
		PopEnabled:    *popEnabled,
		PopTimeout:    *popTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := app.RunServer(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	// flag 包会再次解析；这里只做最小实现避免引入 strconv 的重复错误处理
	var n int
	_, err := fmtSscanf(v, "%d", &n)
	if err != nil {
		return fallback
	}
	return n
}

func envOrBool(key string, fallback bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if v == "" {
		return fallback
	}
	return v == "1" || v == "true" || v == "yes" || v == "y" || v == "on"
}

func envOrDuration(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

// fmtSscanf 是为了让 cmd/server/main.go 不需要额外 import fmt
// 这里用一个小的封装，方便后续替换/测试。
func fmtSscanf(str, format string, a ...any) (int, error) {
	return fmt.Sscanf(str, format, a...)
}
