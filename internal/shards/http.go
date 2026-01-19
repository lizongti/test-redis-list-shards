package shards

import (
	"encoding/json"
	"net/http"
	"time"
)

// NewHTTPHandler 提供最小 HTTP API：
// - GET /healthz：健康检查
// - GET /shards：返回当前 shard 快照（JSON）
func NewHTTPHandler(mgr interface{ Snapshot() Snapshot }) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("/shards", func(w http.ResponseWriter, r *http.Request) {
		snap := mgr.Snapshot()
		resp := struct {
			Now string   `json:"now"`
			Data Snapshot `json:"data"`
		}{
			Now:  time.Now().Format(time.RFC3339Nano),
			Data: snap,
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(resp)
	})

	return mux
}
