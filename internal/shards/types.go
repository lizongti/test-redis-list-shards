package shards

// ShardInfo 用于对外展示当前 shard 的状态。
//
// 注意：
// - Lists 为完整 key 名称（例如 list:0001）
// - ListCount 便于 UI/脚本快速查看
// - ID 从 0 开始，连续编号
//
// 该结构会被 HTTP 接口序列化为 JSON。
type ShardInfo struct {
	ID        int      `json:"id"`
	// Owner 是“偏好 owner”（Rendezvous 计算出的 preferred owner）。
	Owner string `json:"owner"`
	// LeaseOwner 是“实际 owner”（当前持有 shard 租约、实际消费的实例）。
	// 未启用 lease 或租约不存在时可能为空。
	LeaseOwner string `json:"leaseOwner"`
	Local     bool     `json:"local"`
	ListCount int      `json:"listCount"`
	Lists     []string `json:"lists"`
}

// Snapshot 是一次快照，用于 HTTP 输出。
type Snapshot struct {
	InstanceID string   `json:"instanceId"`
	Members    []string `json:"members"`
	MemberKey  string   `json:"memberKey"`
	MemberTTL  string   `json:"memberTtl"`

	ShardCount    int         `json:"shardCount"`
	ListsPerShard int         `json:"listsPerShard"`
	ListMatch     string      `json:"listMatch"`
	Shards        []ShardInfo `json:"shards"`
}
