package shards

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/lizongti/test-redis-list-shards/internal/redisx"
	"github.com/redis/go-redis/v9"
)

// worker 对一组 lists 做循环 BLPOP。
//
// 关键点：
// - keys 会被 manager 动态更新
// - BLPOP 支持一次监听多个 keys（返回第一个可 pop 的 key/value）
// - 为了能响应 keys 更新/停止信号，BLPOP 使用较短超时（cfg.PopTimeout）
//
// 该 worker 只负责消费与打印，不负责决定 shard 数量和分配策略。
type worker struct {
	id int

	rdb        redisx.Client
	popEnabled bool
	popTimeout time.Duration

	leaseEnabled    bool
	leaseKey        string
	leaseValue      string
	leaseTTL        time.Duration
	leaseRenewEvery time.Duration

	leaseMu       sync.Mutex
	lastLeaseTry  time.Time
	lastLeaseRenew time.Time
	leaseHeld     bool

	mu   sync.RWMutex
	keys []string

	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once

	updateCh chan struct{}
}

type workerConfig struct {
	ID         int
	Rdb        redisx.Client
	PopEnabled bool
	PopTimeout time.Duration

	LeaseEnabled    bool
	LeaseKey        string
	LeaseValue      string
	LeaseTTL        time.Duration
	LeaseRenewEvery time.Duration
}

func newWorker(cfg workerConfig) *worker {
	ctx, cancel := context.WithCancel(context.Background())
	return &worker{
		id:              cfg.ID,
		rdb:             cfg.Rdb,
		popEnabled:      cfg.PopEnabled,
		popTimeout:      cfg.PopTimeout,
		leaseEnabled:    cfg.LeaseEnabled,
		leaseKey:        cfg.LeaseKey,
		leaseValue:      cfg.LeaseValue,
		leaseTTL:        cfg.LeaseTTL,
		leaseRenewEvery: cfg.LeaseRenewEvery,
		ctx:       ctx,
		cancel:    cancel,
		updateCh:  make(chan struct{}, 1),
	}
}

func (w *worker) start() {
	if w.leaseEnabled {
		go w.leaseLoop()
	}
	go w.loop()
}

func (w *worker) stop() {
	w.once.Do(func() {
		w.cancel()
		w.releaseLeaseBestEffort()
		// 非阻塞通知一下，确保 loop 尽快醒来
		select {
		case w.updateCh <- struct{}{}:
		default:
		}
	})
}

func (w *worker) setKeys(keys []string) {
	w.mu.Lock()
	w.keys = append([]string(nil), keys...)
	w.mu.Unlock()

	// 唤醒 loop，让它尽快使用新的 keys
	select {
	case w.updateCh <- struct{}{}:
	default:
	}
}

func (w *worker) getKeys() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return append([]string(nil), w.keys...)
}

func (w *worker) loop() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.updateCh:
			// keys 更新，不需要做额外处理，下一轮循环会使用最新 keys
		default:
			// fallthrough
		}

		if !w.popEnabled {
			// 不启用 pop 时，降低空转开销，同时仍能响应 stop/update
			select {
			case <-w.ctx.Done():
				return
			case <-w.updateCh:
				continue
			case <-time.After(200 * time.Millisecond):
				continue
			}
		}

		keys := w.getKeys()
		if len(keys) == 0 {
			// 没有分配到 keys，稍微 sleep 避免空转
			select {
			case <-w.ctx.Done():
				return
			case <-w.updateCh:
				continue
			case <-time.After(200 * time.Millisecond):
				continue
			}
		}

		if w.leaseEnabled {
			if !w.isLeaseHeld() {
				// standby：没拿到租约就不消费，短暂等待后重试
				select {
				case <-w.ctx.Done():
					return
				case <-w.updateCh:
					continue
				case <-time.After(80 * time.Millisecond):
					continue
				}
			}
		}

		// BLPOP 返回 [key, value]
		res, err := w.rdb.BLPop(w.ctx, w.popTimeout, keys...).Result()
		if err != nil {
			if err == redis.Nil {
				// timeout: 正常情况，用于检查 stop/update
				continue
			}
			// 连接错误等：打印后短暂退避
			log.Printf("shard=%d BLPOP error: %v", w.id, err)
			select {
			case <-w.ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}
		if len(res) >= 2 {
			log.Printf("shard=%d pop key=%s value=%s", w.id, res[0], res[1])
		}
	}
}

const leaseRenewScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
  return 0
end
`

const leaseReleaseScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
`

func (w *worker) isLeaseHeld() bool {
	w.leaseMu.Lock()
	defer w.leaseMu.Unlock()
	return w.leaseHeld
}

func (w *worker) leaseLoop() {
	if w.leaseKey == "" || w.leaseValue == "" {
		return
	}
	// 频率用较小值，既能快速接管，也避免过度空转。
	step := 50 * time.Millisecond
	if w.leaseRenewEvery > 0 && w.leaseRenewEvery < step {
		step = w.leaseRenewEvery
	}
	if step < 20*time.Millisecond {
		step = 20 * time.Millisecond
	}

	ticker := time.NewTicker(step)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
		}

		w.leaseMu.Lock()
		held := w.leaseHeld
		lastRenew := w.lastLeaseRenew
		lastTry := w.lastLeaseTry
		renewEvery := w.leaseRenewEvery
		w.leaseMu.Unlock()

		now := time.Now()
		if held {
			if renewEvery <= 0 {
				renewEvery = 300 * time.Millisecond
			}
			if now.Sub(lastRenew) < renewEvery {
				continue
			}

			cmd := w.rdb.Eval(w.ctx, leaseRenewScript, []string{w.leaseKey}, w.leaseValue, int(w.leaseTTL.Milliseconds()))
			n, err := cmd.Int()
			if err != nil || n == 0 {
				w.leaseMu.Lock()
				w.leaseHeld = false
				w.leaseMu.Unlock()
				continue
			}
			w.leaseMu.Lock()
			w.lastLeaseRenew = now
			w.leaseMu.Unlock()
			continue
		}

		// not held: try acquire (rate-limited)
		if now.Sub(lastTry) < 50*time.Millisecond {
			continue
		}
		ok, err := w.rdb.SetNX(w.ctx, w.leaseKey, w.leaseValue, w.leaseTTL).Result()
		w.leaseMu.Lock()
		w.lastLeaseTry = now
		if err == nil && ok {
			w.leaseHeld = true
			w.lastLeaseRenew = now
			w.leaseMu.Unlock()
			log.Printf("shard=%d lease acquired key=%s", w.id, w.leaseKey)
			continue
		}
		w.leaseMu.Unlock()
	}
}

func (w *worker) releaseLeaseBestEffort() {
	if !w.leaseEnabled || w.leaseKey == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = w.rdb.Eval(ctx, leaseReleaseScript, []string{w.leaseKey}, w.leaseValue).Err()
}
