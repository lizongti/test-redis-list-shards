package redisx

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Options 是创建 Redis 客户端的参数。
// 这里使用 UniversalClient：
// - 单机：Addrs=["127.0.0.1:6379"]
// - 集群：Addrs=["node1:6379","node2:6379",...]
// 这样 server/client 两端代码不需要关心具体类型。
type Options struct {
	Addrs    []string
	Password string
}

// Client 是我们在内部使用的最小接口，便于单元测试替换。
// 这里基本覆盖了本项目需要的 redis 操作。
type Client interface {
	Close() error

	Ping(ctx context.Context) *redis.StatusCmd

	Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd
	LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	BLPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Get(ctx context.Context, key string) *redis.StringCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd

	// Membership tracking (ZSET)
	ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd
	ZRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd
	ZRemRangeByScore(ctx context.Context, key, min, max string) *redis.IntCmd
}

// NewUniversalClient 创建 redis.UniversalClient 并做一次 Ping 校验。
func NewUniversalClient(ctx context.Context, opt Options) (Client, error) {
	if len(opt.Addrs) == 0 {
		return nil, errors.New("redis addrs is empty")
	}

	c := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    opt.Addrs,
		Password: opt.Password,
	})

	// 用较短超时验证连通性，避免启动后静默失败。
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := c.Ping(pingCtx).Err(); err != nil {
		_ = c.Close()
		return nil, err
	}

	return c, nil
}
