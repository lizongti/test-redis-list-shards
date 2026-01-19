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

	mu   sync.RWMutex
	keys []string

	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once

	updateCh chan struct{}
}

func newWorker(id int, rdb redisx.Client, popEnabled bool, popTimeout time.Duration) *worker {
	ctx, cancel := context.WithCancel(context.Background())
	return &worker{
		id:        id,
		rdb:       rdb,
		popEnabled: popEnabled,
		popTimeout: popTimeout,
		ctx:       ctx,
		cancel:    cancel,
		updateCh:  make(chan struct{}, 1),
	}
}

func (w *worker) start() {
	go w.loop()
}

func (w *worker) stop() {
	w.once.Do(func() {
		w.cancel()
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
