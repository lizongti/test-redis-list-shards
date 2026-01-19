package shards

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/lizongti/test-redis-list-shards/internal/redisx"
	"github.com/redis/go-redis/v9"
)

// ManagerConfig 描述 shard manager 的行为。
type ManagerConfig struct {
	Rdb redisx.Client

	ListKeyMatch  string
	ListsPerShard int
	// ShardCount 用于固定 shard 数量（推荐多实例模式使用）。
	// 当 ShardCount > 0 时：
	// - shard ID 固定为 [0..ShardCount)
	// - 每个 key 通过 hash(key)%ShardCount 分配到 shard
	// 这样所有实例能获得稳定且一致的分片视图。
	//
	// 当 ShardCount == 0 时：使用动态分片（按 ListsPerShard 计算 shard 数量并切片分配）。
	ShardCount   int
	RefreshEvery time.Duration

	// InstanceID 是当前 server 实例的唯一标识。
	// 多实例部署时必须不同；如果为空，NewManager 会生成一个临时 ID。
	InstanceID string

	// MemberKey 是存储成员列表的 Redis ZSET key。
	// 每个成员以 member=InstanceID 的方式注册，score 为最后心跳时间（毫秒）。
	MemberKey string
	// MemberTTL 是成员过期时间；超过该时间未刷新心跳的成员会被清理。
	MemberTTL time.Duration

	PopEnabled bool
	PopTimeout time.Duration
}

// Manager 负责：
// 1) 周期性扫描 Redis keys（SCAN MATCH ...）
// 2) 计算期望的 shard 数量并伸缩
// 3) 将 keys 分配给每个 shard worker
// 4) 提供快照供 HTTP 查询
//
// 设计：
// - 采用“排序后切片分段”的分配策略，简单、可预测
// - 扩缩容只会影响部分 shard 的 key 分配
// - Worker 内部使用 BLPOP(keys...) 阻塞拉取并打印
// - Stop() 可用于外部强制停止（例如 HTTP server 退出时）
type Manager struct {
	cfg ManagerConfig

	mu      sync.RWMutex
	workers map[int]*worker // shardID -> worker（仅保存本实例拥有的 shard）
	snap    Snapshot
	members []string

	stopOnce sync.Once
	stopCh   chan struct{}
}

func NewManager(cfg ManagerConfig) *Manager {
	if cfg.ListKeyMatch == "" {
		cfg.ListKeyMatch = "list:*"
	}
	if cfg.ListsPerShard <= 0 {
		cfg.ListsPerShard = 8
	}
	if cfg.RefreshEvery <= 0 {
		cfg.RefreshEvery = 1 * time.Second
	}
	if cfg.PopTimeout <= 0 {
		cfg.PopTimeout = 1 * time.Second
	}
	if cfg.InstanceID == "" {
		cfg.InstanceID = defaultInstanceID()
	}
	if cfg.MemberKey == "" {
		cfg.MemberKey = "test-redis-list-shards:members"
	}
	if cfg.MemberTTL <= 0 {
		cfg.MemberTTL = 6 * time.Second
	}

	m := &Manager{cfg: cfg, stopCh: make(chan struct{}), workers: make(map[int]*worker)}
	m.snap = Snapshot{
		InstanceID:    cfg.InstanceID,
		Members:       nil,
		MemberKey:     cfg.MemberKey,
		MemberTTL:     cfg.MemberTTL.String(),
		ShardCount:    0,
		ListsPerShard: cfg.ListsPerShard,
		ListMatch:     cfg.ListKeyMatch,
		Shards:        nil,
	}
	return m
}

// Run 启动 manager 主循环，直到 ctx.Done() 或 Stop()。
func (m *Manager) Run(ctx context.Context) error {
	if m.cfg.Rdb == nil {
		return errors.New("redis client is nil")
	}

	// 先做一次立即刷新，便于启动即获得正确的 shard 状态。
	if err := m.refreshOnce(ctx); err != nil {
		return err
	}

	ticker := time.NewTicker(m.cfg.RefreshEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.stopWorkers()
			return nil
		case <-m.stopCh:
			m.stopWorkers()
			return nil
		case <-ticker.C:
			if err := m.refreshOnce(ctx); err != nil {
				// Redis 短暂抖动时不直接退出，而是记录日志并继续
				log.Printf("refresh error: %v", err)
			}
		}
	}
}

// Stop 允许外部通知 manager 退出。
func (m *Manager) Stop() error {
	m.stopOnce.Do(func() { close(m.stopCh) })
	return nil
}

// Snapshot 返回当前快照。
func (m *Manager) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// 深拷贝 slices，避免并发读写。
	out := m.snap
	out.Members = append([]string(nil), m.snap.Members...)
	out.Shards = append([]ShardInfo(nil), m.snap.Shards...)
	for i := range out.Shards {
		out.Shards[i].Lists = append([]string(nil), out.Shards[i].Lists...)
	}
	return out
}

// refreshOnce 拉取 keys 并做扩缩容 + 重新分配。
func (m *Manager) refreshOnce(ctx context.Context) error {
	members, err := m.refreshMembership(ctx)
	if err != nil {
		return err
	}

	keys, err := scanAllKeys(ctx, m.cfg.Rdb, m.cfg.ListKeyMatch)
	if err != nil {
		return err
	}
	sort.Strings(keys)

	shardCount := m.cfg.ShardCount
	if shardCount <= 0 {
		shardCount = shardCountFor(len(keys), m.cfg.ListsPerShard)
	}

	var assignments [][]string
	if m.cfg.ShardCount > 0 {
		assignments = assignKeysByHash(keys, shardCount)
	} else {
		assignments = splitKeys(keys, shardCount)
	}

	owners := computeShardOwners(shardCount, members)
	m.reconcileWorkers(owners)
	m.applyAssignments(assignments, owners)
	m.updateSnapshot(assignments, owners, members)
	return nil
}

func assignKeysByHash(keys []string, shardCount int) [][]string {
	if shardCount <= 0 {
		shardCount = 1
	}
	out := make([][]string, shardCount)
	for _, key := range keys {
		shardID := int(xxhash.Sum64String(key) % uint64(shardCount))
		out[shardID] = append(out[shardID], key)
	}
	for i := range out {
		sort.Strings(out[i])
	}
	return out
}

func (m *Manager) reconcileWorkers(owners []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	owned := make(map[int]struct{}, len(owners))
	for shardID, owner := range owners {
		if owner == m.cfg.InstanceID {
			owned[shardID] = struct{}{}
		}
	}

	// stop workers we no longer own
	for shardID, w := range m.workers {
		if _, ok := owned[shardID]; ok {
			continue
		}
		w.stop()
		delete(m.workers, shardID)
		log.Printf("shard worker stopped: id=%d", shardID)
	}

	// start missing workers we should own
	for shardID := range owned {
		if _, ok := m.workers[shardID]; ok {
			continue
		}
		w := newWorker(shardID, m.cfg.Rdb, m.cfg.PopEnabled, m.cfg.PopTimeout)
		m.workers[shardID] = w
		w.start()
		log.Printf("shard worker started: id=%d", shardID)
	}
}

func (m *Manager) applyAssignments(assignments [][]string, owners []string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for shardID := 0; shardID < len(assignments); shardID++ {
		w, ok := m.workers[shardID]
		if !ok {
			continue
		}
		// 只会存在本实例拥有的 shard worker
		_ = owners // owners 已用于 reconcile
		w.setKeys(assignments[shardID])
	}
}

func (m *Manager) updateSnapshot(assignments [][]string, owners []string, members []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	shardsInfo := make([]ShardInfo, 0, len(assignments))
	for i, lists := range assignments {
		owner := ""
		if i < len(owners) {
			owner = owners[i]
		}
		shardsInfo = append(shardsInfo, ShardInfo{
			ID:        i,
			Owner:     owner,
			Local:     owner == m.cfg.InstanceID,
			ListCount: len(lists),
			Lists:     append([]string(nil), lists...),
		})
	}

	m.snap = Snapshot{
		InstanceID:    m.cfg.InstanceID,
		Members:       append([]string(nil), members...),
		MemberKey:     m.cfg.MemberKey,
		MemberTTL:     m.cfg.MemberTTL.String(),
		ShardCount:    len(assignments),
		ListsPerShard: m.cfg.ListsPerShard,
		ListMatch:     m.cfg.ListKeyMatch,
		Shards:        shardsInfo,
	}
}

func (m *Manager) stopWorkers() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, w := range m.workers {
		w.stop()
	}
	m.workers = make(map[int]*worker)
	m.snap.Shards = nil
	m.snap.ShardCount = 0
}

func shardCountFor(listCount int, listsPerShard int) int {
	if listCount <= 0 {
		return 1
	}
	if listsPerShard <= 0 {
		listsPerShard = 1
	}
	q := listCount / listsPerShard
	r := listCount % listsPerShard
	if r != 0 {
		q++
	}
	if q < 1 {
		q = 1
	}
	return q
}

func splitKeys(keys []string, shardCount int) [][]string {
	if shardCount <= 0 {
		shardCount = 1
	}
	out := make([][]string, shardCount)
	if len(keys) == 0 {
		for i := range out {
			out[i] = nil
		}
		return out
	}

	// 均匀切片：尽量让每个 shard 的 lists 数量接近。
	base := len(keys) / shardCount
	rem := len(keys) % shardCount

	idx := 0
	for i := 0; i < shardCount; i++ {
		sz := base
		if i < rem {
			sz++
		}
		if sz == 0 {
			out[i] = nil
			continue
		}
		out[i] = append([]string(nil), keys[idx:idx+sz]...)
		idx += sz
	}
	return out
}

func scanAllKeys(ctx context.Context, rdb redisx.Client, match string) ([]string, error) {
	var (
		cursor uint64
		keys   []string
	)
	for {
		res, next, err := rdb.Scan(ctx, cursor, match, 500).Result()
		if err != nil {
			// go-redis 在连接异常等情况下会返回 error
			return nil, err
		}
		keys = append(keys, res...)
		cursor = next
		if cursor == 0 {
			break
		}
	}

	// 只保留 list 类型 key：Redis 没有直接的“仅扫描 list”能力。
	// 为了保持简单，我们假设测试环境下 list:* 都是 list。
	// 如果需要更严格，可在这里对每个 key 执行 TYPE 命令进行过滤。
	return keys, nil
}

// 这里引用 redis.Nil 是为了提醒：worker BLPOP timeout 返回 redis.Nil。
var _ = redis.Nil

func (m *Manager) refreshMembership(ctx context.Context) ([]string, error) {
	nowMs := time.Now().UnixMilli()
	// 1) upsert self heartbeat
	if err := m.cfg.Rdb.ZAdd(ctx, m.cfg.MemberKey, redis.Z{Score: float64(nowMs), Member: m.cfg.InstanceID}).Err(); err != nil {
		return nil, err
	}

	// 2) cleanup expired
	expiredBefore := nowMs - m.cfg.MemberTTL.Milliseconds()
	if err := m.cfg.Rdb.ZRemRangeByScore(ctx, m.cfg.MemberKey, "-inf", fmt.Sprintf("%d", expiredBefore)).Err(); err != nil {
		return nil, err
	}

	// 3) list members
	members, err := m.cfg.Rdb.ZRange(ctx, m.cfg.MemberKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		members = []string{m.cfg.InstanceID}
	}
	sort.Strings(members)

	m.mu.Lock()
	m.members = append([]string(nil), members...)
	m.mu.Unlock()

	return members, nil
}

// computeShardOwners 使用 Rendezvous Hash（最高随机权重哈希）为每个 shard 选择 owner。
// 输入相同（members 顺序无关但建议已排序）时，所有实例能得到一致结果。
func computeShardOwners(shardCount int, members []string) []string {
	if shardCount <= 0 {
		shardCount = 1
	}
	if len(members) == 0 {
		return make([]string, shardCount)
	}
	out := make([]string, shardCount)
	for shardID := 0; shardID < shardCount; shardID++ {
		out[shardID] = rendezvousPickOwner(shardID, members)
	}
	return out
}

func rendezvousPickOwner(shardID int, members []string) string {
	shardKey := strconv.Itoa(shardID)
	bestOwner := members[0]
	bestScore := uint64(0)
	for _, member := range members {
		// score = hash(member + ":" + shardKey)
		score := xxhash.Sum64String(member + ":" + shardKey)
		if score >= bestScore {
			bestScore = score
			bestOwner = member
		}
	}
	return bestOwner
}

func defaultInstanceID() string {
	// 仅用于测试/本地运行，生产建议通过环境变量/容器名注入。
	return "node-" + strconv.FormatInt(time.Now().UnixNano(), 10)
}
