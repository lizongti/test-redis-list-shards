package app

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/lizongti/test-redis-list-shards/internal/redisx"
	"github.com/lizongti/test-redis-list-shards/internal/shards"
)

// RunServer 启动 shard manager + HTTP server，并在 ctx 取消时优雅退出。
func RunServer(ctx context.Context, cfg Config) error {
	if cfg.ListsPerShard <= 0 {
		return errors.New("lists-per-shard must be > 0")
	}
	if cfg.RefreshEvery <= 0 {
		cfg.RefreshEvery = 1 * time.Second
	}
	if cfg.PopTimeout <= 0 {
		cfg.PopTimeout = 1 * time.Second
	}

	rdb, err := redisx.NewUniversalClient(ctx, redisx.Options{
		Addrs:    cfg.RedisAddrs,
		Password: cfg.RedisPassword,
	})
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()

	mgr := shards.NewManager(shards.ManagerConfig{
		Rdb:           rdb,
		ListKeyMatch:  cfg.ListKeyMatch,
		ListsPerShard: cfg.ListsPerShard,
		ShardCount:    cfg.ShardCount,
		RefreshEvery:  cfg.RefreshEvery,
		InstanceID:    cfg.InstanceID,
		MemberKey:     cfg.MemberKey,
		MemberTTL:     cfg.MemberTTL,
		PopEnabled:    cfg.PopEnabled,
		PopTimeout:    cfg.PopTimeout,
	})

	// 启动 shard manager
	mgrErrCh := make(chan error, 1)
	go func() {
		mgrErrCh <- mgr.Run(ctx)
	}()

	// 启动 HTTP server
	h := shards.NewHTTPHandler(mgr)
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: h}

	httpErrCh := make(chan error, 1)
	go func() {
		log.Printf("http listening on %s", cfg.HTTPAddr)
		httpErrCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		_ = mgr.Stop()
		return nil
	case err := <-mgrErrCh:
		_ = srv.Close()
		return err
	case err := <-httpErrCh:
		_ = mgr.Stop()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
