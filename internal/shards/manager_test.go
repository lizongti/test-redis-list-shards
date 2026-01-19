package shards

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/lizongti/test-redis-list-shards/internal/redisx"
)

// TestDynamicScale 通过动态增加/减少 list keys，验证 shard 数量会随之变化。
//
// 约定：
// - 测试依赖外部 Redis（由 scripts/test.sh 通过 docker compose 启动）
// - 使用 list:test:* 前缀，避免影响其他数据
func TestDynamicScale(t *testing.T) {
	addrs := os.Getenv("REDIS_ADDRS")
	if addrs == "" {
		t.Skip("REDIS_ADDRS is empty; run via scripts/test.sh")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rdb, err := redisx.NewUniversalClient(ctx, redisx.Options{Addrs: []string{addrs}})
	if err != nil {
		t.Fatalf("new redis client: %v", err)
	}
	defer func() { _ = rdb.Close() }()

	// 清理旧 key
	cleanupKeys(t, ctx, rdb, "list:test:*")
	defer cleanupKeys(t, ctx, rdb, "list:test:*")

	memberKey := "test-redis-list-shards:test:members"
	_ = rdb.Del(ctx, memberKey).Err()
	defer func() { _ = rdb.Del(ctx, memberKey).Err() }()

	mgr := NewManager(ManagerConfig{
		Rdb:           rdb,
		ListKeyMatch:  "list:test:*",
		ListsPerShard: 2,
		RefreshEvery:  150 * time.Millisecond,
		InstanceID:    "test-node-1",
		MemberKey:     memberKey,
		MemberTTL:     2 * time.Second,
		PopEnabled:    false, // 测试只关心伸缩，不需要 pop
		PopTimeout:    100 * time.Millisecond,
	})

	mgrCtx, mgrCancel := context.WithCancel(ctx)
	defer mgrCancel()
	go func() { _ = mgr.Run(mgrCtx) }()

	// 增加 5 个 list -> 期望 shardCount = ceil(5/2)=3
	for i := 0; i < 5; i++ {
		key := keyFor(i)
		if err := rdb.LPush(ctx, key, "v").Err(); err != nil {
			t.Fatalf("lpush: %v", err)
		}
	}
	waitForShardCount(t, ctx, mgr, 3)

	// 删除 4 个 list -> 剩 1 个 -> shardCount 仍至少为 1
	for i := 1; i < 5; i++ {
		if err := rdb.Del(ctx, keyFor(i)).Err(); err != nil {
			t.Fatalf("del: %v", err)
		}
	}
	waitForShardCount(t, ctx, mgr, 1)
}

func keyFor(i int) string {
	return "list:test:" + two(i)
}

func two(i int) string {
	if i < 10 {
		return "0" + itoa(i)
	}
	return itoa(i)
}

func itoa(i int) string {
	// 不引入 strconv，保持测试文件更短
	s := ""
	if i == 0 {
		return "0"
	}
	for i > 0 {
		d := i % 10
		s = string('0'+byte(d)) + s
		i /= 10
	}
	return s
}

func waitForShardCount(t *testing.T, ctx context.Context, mgr *Manager, want int) {
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("context done before reaching want=%d (last=%d)", want, mgr.Snapshot().ShardCount)
		case <-deadline.C:
			t.Fatalf("timeout waiting for shardCount=%d (last=%d)", want, mgr.Snapshot().ShardCount)
		case <-ticker.C:
			if mgr.Snapshot().ShardCount == want {
				return
			}
		}
	}
}

func cleanupKeys(t *testing.T, ctx context.Context, rdb redisx.Client, match string) {
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, match, 1000).Result()
		if err != nil {
			// 不 hard fail：测试环境可能为空或 Redis 刚启动
			return
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				t.Fatalf("cleanup del: %v", err)
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
}
