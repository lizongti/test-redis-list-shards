package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lizongti/test-redis-list-shards/internal/app"
)

// client 会随机创建新的 List key，并随机向其中 LPUSH 数据。
// 这样可以模拟生产者不断制造新的 list 和数据，从而驱动 server 的动态分片调整。
func main() {
	var (
		redisAddrs    = flag.String("redis", envOr("REDIS_ADDRS", "127.0.0.1:6379"), "Redis address list, comma-separated")
		redisPassword = flag.String("redis-password", envOr("REDIS_PASSWORD", ""), "Redis password")
		prefix        = flag.String("prefix", envOr("LIST_KEY_PREFIX", "list"), "List key prefix, final key will be prefix:<name>")
		pushEvery     = flag.Duration("every", envOrDuration("CLIENT_PUSH_EVERY", 120*time.Millisecond), "Push interval")
		newListProb   = flag.Float64("new-list-prob", envOrFloat("CLIENT_NEW_LIST_PROB", 0.15), "Probability of creating a new list name")
		deleteProb    = flag.Float64("delete-prob", envOrFloat("CLIENT_DELETE_PROB", 0.05), "Probability of deleting an existing list")
		seed          = flag.Int64("seed", envOrInt64("CLIENT_SEED", time.Now().UnixNano()), "Random seed")
		logPrefix     = flag.String("log-prefix", envOr("LOG_PREFIX", "client"), "Log prefix")
	)
	flag.Parse()

	log.SetPrefix("[" + *logPrefix + "] ")
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	rng := rand.New(rand.NewSource(*seed))

	cfg := app.ClientConfig{
		RedisAddrs:    splitCSV(*redisAddrs),
		RedisPassword: *redisPassword,
		KeyPrefix:     *prefix,
		PushEvery:     *pushEvery,
		NewListProb:   *newListProb,
		DeleteProb:    *deleteProb,
		Rng:           rng,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := app.RunClient(ctx, cfg); err != nil {
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

func envOrInt64(key string, fallback int64) int64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	var n int64
	_, err := fmtSscanf(v, "%d", &n)
	if err != nil {
		return fallback
	}
	return n
}

func envOrFloat(key string, fallback float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	var n float64
	_, err := fmtSscanf(v, "%f", &n)
	if err != nil {
		return fallback
	}
	return n
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

func fmtSscanf(str, format string, a ...any) (int, error) {
	return fmt.Sscanf(str, format, a...)
}
