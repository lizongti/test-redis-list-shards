package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/lizongti/test-redis-list-shards/internal/redisx"
)

// RunClient 周期性向 Redis 写入：
// - 随机决定是否创建新 list key
// - 随机选择一个 list key，LPUSH 一条数据
// - 以一定概率 DEL 掉某个 list key（用于测试 shard 动态缩容）
func RunClient(ctx context.Context, cfg ClientConfig) error {
	if len(cfg.RedisAddrs) == 0 {
		return errors.New("redis addrs is empty")
	}
	if cfg.PushEvery <= 0 {
		cfg.PushEvery = 120 * time.Millisecond
	}
	if cfg.Rng == nil {
		cfg.Rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "list"
	}

	rdb, err := redisx.NewUniversalClient(ctx, redisx.Options{
		Addrs:    cfg.RedisAddrs,
		Password: cfg.RedisPassword,
	})
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()

	known := make([]string, 0, 32)
	nameSeq := 0

	ticker := time.NewTicker(cfg.PushEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// 1) maybe create new list
			if len(known) == 0 || cfg.Rng.Float64() < cfg.NewListProb {
				nameSeq++
				key := fmt.Sprintf("%s:%04d", cfg.KeyPrefix, nameSeq)
				known = append(known, key)
				log.Printf("created list key: %s", key)
			}

			// 2) push to a random list
			idx := cfg.Rng.Intn(len(known))
			key := known[idx]
			value := fmt.Sprintf("ts=%d", time.Now().UnixNano())
			if err := rdb.LPush(ctx, key, value).Err(); err != nil {
				return err
			}
			log.Printf("LPUSH %s %s", key, value)

			// 3) maybe delete a random list
			if len(known) > 1 && cfg.DeleteProb > 0 && cfg.Rng.Float64() < cfg.DeleteProb {
				delIdx := cfg.Rng.Intn(len(known))
				delKey := known[delIdx]
				if err := rdb.Del(ctx, delKey).Err(); err != nil {
					return err
				}
				log.Printf("DEL %s", delKey)

				// remove from known
				known[delIdx] = known[len(known)-1]
				known = known[:len(known)-1]
			}
		}
	}
}
