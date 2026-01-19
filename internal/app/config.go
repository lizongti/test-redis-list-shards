package app

import "time"

// Config 描述 server 端的运行参数。
// 这些参数既可通过命令行 flags 注入，也可通过环境变量注入。
//
// 设计要点：
// - RedisAddrs 支持多地址（为后续迁移到 Redis Cluster 做准备）
// - ListKeyMatch 通过 SCAN + MATCH 获取所有 list keys
// - ListsPerShard 决定 shard 数量：shardCount = ceil(listCount / ListsPerShard)
// - RefreshEvery 周期性刷新 keys 以实现动态伸缩
// - PopEnabled/PopTimeout 控制 worker 的 BLPOP 行为
type Config struct {
	RedisAddrs    []string
	RedisPassword string

	ListKeyMatch  string
	ListsPerShard int
	ShardCount    int
	RefreshEvery  time.Duration

	// Multi-instance / global consistency
	InstanceID string
	MemberKey  string
	MemberTTL  time.Duration

	// Shard lease（用于更快 failover / 1s 量级再平衡）
	// 当启用时：允许多个实例为同一 shard 启动 standby worker，但只有拿到租约的实例才会真正 BLPOP 消费。
	LeaseEnabled    bool
	LeaseKeyPrefix  string
	LeaseTTL        time.Duration
	LeaseRenewEvery time.Duration
	StandbyDepth    int

	HTTPAddr   string
	PopEnabled bool
	PopTimeout time.Duration
}

// ClientConfig 描述 client 端（生产者）运行参数。
type ClientConfig struct {
	RedisAddrs    []string
	RedisPassword string

	KeyPrefix string

	PushEvery   time.Duration
	NewListProb float64
	DeleteProb  float64

	// Rng 由调用方传入，方便测试时做确定性。
	Rng interface {
		Float64() float64
		Intn(n int) int
	}
}
